#!/usr/bin/env python3
"""Source-oriented fetch and list helpers for llm-vault."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from vault_db import connect_vault_db
from vault_redaction import PersistentRedactionMap
from vault_sources import REGISTERED_SOURCES, SourceHandler, select_source_handlers
from vault_vector_index import _parse_dates_json, _parse_provenance_json, _sanitize_metadata_for_output, _stable_source_id

DEFAULT_CONTENT_LIMIT = 1200
DEFAULT_LIST_LIMIT = 5


class FetchNotFoundError(LookupError):
    """Raised when a source_id cannot be resolved."""


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _clip(text: Any, limit: int = DEFAULT_CONTENT_LIMIT) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(1, limit - 3)].rstrip() + "..."


def _join_parts(parts: list[str], *, limit: int = DEFAULT_CONTENT_LIMIT) -> str:
    text = "\n\n".join(part.strip() for part in parts if str(part or "").strip())
    return _clip(text, limit=limit)


def _redact_text(text: str, table: PersistentRedactionMap | None) -> str:
    if not text or table is None:
        return text
    return table.apply(text)


def _clean_text(text: Any) -> str:
    return " ".join(str(text or "").strip().split())


def _load_redaction_map(conn: sqlite3.Connection) -> PersistentRedactionMap | None:
    if not _table_exists(conn, "redaction_entries"):
        return None
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(redaction_entries)").fetchall()}
    if "status" in columns:
        sql = """
            SELECT key_name, placeholder, value_norm, original_value
            FROM redaction_entries
            WHERE scope_type = ? AND scope_id = ? AND COALESCE(status, 'active') = 'active'
            ORDER BY key_name, placeholder
        """
    else:
        sql = """
            SELECT key_name, placeholder, value_norm, original_value
            FROM redaction_entries
            WHERE scope_type = ? AND scope_id = ?
            ORDER BY key_name, placeholder
        """
    rows = [
        (str(row[0] or ""), str(row[1] or ""), str(row[2] or ""), str(row[3] or ""))
        for row in conn.execute(sql, ("vault", "global")).fetchall()
    ]
    if not any(row[1] for row in rows):
        return None
    return PersistentRedactionMap.from_rows(rows)


def _coalesce_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _list_sort_key(row: dict[str, Any], *, kind: str) -> tuple[str, str]:
    primary_date = str(row.get("primary_date") or "").strip()
    if kind == "photos":
        secondary_date = _coalesce_text(row.get("date_taken"), row.get("updated_at"))
    elif kind == "mail":
        secondary_date = _coalesce_text(row.get("date_iso"), row.get("updated_at"))
    else:
        secondary_date = str(row.get("updated_at") or "").strip()
    return primary_date, secondary_date


def _list_effective_date(row: dict[str, Any], *, kind: str) -> str | None:
    primary_date, secondary_date = _list_sort_key(row, kind=kind)
    chosen = primary_date or secondary_date
    clean = str(chosen or "").strip()
    return clean or None


def _preview_doc(row: dict[str, Any]) -> str:
    return _coalesce_text(row.get("summary_text"), row.get("text_content"))


def _preview_photo(row: dict[str, Any]) -> str:
    return _coalesce_text(row.get("caption"), row.get("notes"), row.get("ocr_text"))


def _preview_mail(row: dict[str, Any]) -> str:
    return _coalesce_text(row.get("subject"), row.get("summary_text"), row.get("snippet"))


def _build_list_item(
    row: dict[str, Any],
    *,
    handler: SourceHandler,
    clearance: str,
    redaction_map: PersistentRedactionMap | None,
) -> dict[str, Any]:
    filepath = str(row.get("filepath") or "")
    if handler.kind == "docs":
        preview = _preview_doc(row)
    elif handler.kind == "photos":
        preview = _preview_photo(row)
    else:
        preview = _preview_mail(row)
    preview = _clean_text(preview)
    if clearance != "full":
        preview = _clean_text(_redact_text(preview, redaction_map))

    return {
        "source_id": _stable_source_id(handler.table, filepath),
        "source_kind": handler.kind,
        "source_table": handler.table,
        "source_filepath": filepath if clearance == "full" else None,
        "primary_date": str(row.get("primary_date") or "").strip() or None,
        "source_updated_at": str(row.get("updated_at") or "").strip() or None,
        "preview": _clip(preview, limit=240),
    }


def _list_rows_for_handler(
    conn: sqlite3.Connection,
    *,
    handler: SourceHandler,
    from_date: str | None,
    to_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    if handler.kind == "docs":
        sql = """
            SELECT filepath, summary_text, text_content, primary_date, updated_at
            FROM docs_registry
            WHERE (
              ? IS NULL OR COALESCE(SUBSTR(primary_date, 1, 10), SUBSTR(updated_at, 1, 10)) >= ?
            ) AND (
              ? IS NULL OR COALESCE(SUBSTR(primary_date, 1, 10), SUBSTR(updated_at, 1, 10)) <= ?
            )
            ORDER BY COALESCE(primary_date, updated_at, '') DESC, filepath DESC
            LIMIT ?
        """
    elif handler.kind == "photos":
        sql = """
            SELECT filepath, caption, notes, ocr_text, date_taken, primary_date, updated_at
            FROM photos_registry
            WHERE (
              ? IS NULL OR COALESCE(SUBSTR(primary_date, 1, 10), SUBSTR(date_taken, 1, 10), SUBSTR(updated_at, 1, 10)) >= ?
            ) AND (
              ? IS NULL OR COALESCE(SUBSTR(primary_date, 1, 10), SUBSTR(date_taken, 1, 10), SUBSTR(updated_at, 1, 10)) <= ?
            )
            ORDER BY COALESCE(primary_date, date_taken, updated_at, '') DESC, filepath DESC
            LIMIT ?
        """
    else:
        sql = """
            SELECT filepath, subject, summary_text, snippet, primary_date, date_iso, updated_at
            FROM mail_registry
            WHERE (
              ? IS NULL OR COALESCE(SUBSTR(primary_date, 1, 10), SUBSTR(date_iso, 1, 10), SUBSTR(updated_at, 1, 10)) >= ?
            ) AND (
              ? IS NULL OR COALESCE(SUBSTR(primary_date, 1, 10), SUBSTR(date_iso, 1, 10), SUBSTR(updated_at, 1, 10)) <= ?
            )
            ORDER BY COALESCE(primary_date, date_iso, updated_at, '') DESC, filepath DESC
            LIMIT ?
        """
    return [dict(row) for row in conn.execute(sql, (from_date, from_date, to_date, to_date, limit)).fetchall()]


def _lookup_source_row(conn: sqlite3.Connection, source_id: str) -> tuple[SourceHandler, dict[str, Any]]:
    for handler in REGISTERED_SOURCES:
        if not _table_exists(conn, handler.table):
            continue
        for row in conn.execute(f"SELECT filepath FROM {handler.table} ORDER BY filepath"):
            filepath = str(row["filepath"] or "")
            if _stable_source_id(handler.table, filepath) != source_id:
                continue
            source_row = conn.execute(
                f"SELECT * FROM {handler.table} WHERE filepath = ? LIMIT 1",
                (filepath,),
            ).fetchone()
            if source_row is None:
                break
            return handler, dict(source_row)
    raise FetchNotFoundError(f"source not found: {source_id}")


def _build_doc_payload(row: dict[str, Any]) -> tuple[dict[str, Any], str]:
    metadata = {
        "kind": "doc",
        "source": str(row.get("source") or "").strip(),
        "parser": str(row.get("parser") or "").strip(),
        "summary_status": str(row.get("summary_status") or "").strip(),
        "summary_model": str(row.get("summary_model") or "").strip(),
        "primary_date": str(row.get("primary_date") or "").strip(),
        "dates": _parse_dates_json(row.get("dates_json")),
        "origin_kind": str(_parse_provenance_json(row.get("provenance_json")).get("origin_kind") or "").strip(),
    }
    summary_text = str(row.get("summary_text") or "").strip()
    body_text = str(row.get("text_content") or "").strip()
    content = _join_parts(
        [
            f"Summary: {summary_text}" if summary_text else "",
            f"Excerpt: {body_text}" if body_text else "",
        ]
    )
    return metadata, content


def _build_photo_payload(row: dict[str, Any]) -> tuple[dict[str, Any], str]:
    metadata = {
        "kind": "photo",
        "source": str(row.get("source") or "").strip(),
        "date_taken": str(row.get("date_taken") or "").strip(),
        "category_primary": str(row.get("category_primary") or "").strip(),
        "category_secondary": str(row.get("category_secondary") or "").strip(),
        "taxonomy": str(row.get("taxonomy") or "").strip(),
        "analyzer_status": str(row.get("analyzer_status") or "").strip(),
        "ocr_status": str(row.get("ocr_status") or "").strip(),
        "ocr_source": str(row.get("ocr_source") or "").strip(),
        "primary_date": str(row.get("primary_date") or "").strip(),
        "dates": _parse_dates_json(row.get("dates_json")),
        "origin_kind": str(_parse_provenance_json(row.get("provenance_json")).get("origin_kind") or "").strip(),
    }
    content = _join_parts(
        [
            f"Caption: {row.get('caption')}" if str(row.get("caption") or "").strip() else "",
            f"Notes: {row.get('notes')}" if str(row.get("notes") or "").strip() else "",
            f"OCR: {row.get('ocr_text')}" if str(row.get("ocr_text") or "").strip() else "",
        ]
    )
    return metadata, content


def _build_mail_payload(row: dict[str, Any]) -> tuple[dict[str, Any], str]:
    labels: list[str] = []
    try:
        parsed = json.loads(row.get("labels_json") or "[]")
        if isinstance(parsed, list):
            labels = [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        labels = []

    metadata = {
        "kind": "mail",
        "source": str(row.get("source") or "").strip(),
        "msg_id": str(row.get("msg_id") or "").strip(),
        "account_email": str(row.get("account_email") or "").strip(),
        "thread_id": str(row.get("thread_id") or "").strip(),
        "from_addr": str(row.get("from_addr") or "").strip(),
        "to_addr": str(row.get("to_addr") or "").strip(),
        "labels": labels,
        "date_iso": str(row.get("date_iso") or "").strip(),
        "primary_date": str(row.get("primary_date") or "").strip(),
        "dates": _parse_dates_json(row.get("dates_json")),
    }
    content = _join_parts(
        [
            f"Subject: {row.get('subject')}" if str(row.get("subject") or "").strip() else "",
            f"Snippet: {row.get('snippet')}" if str(row.get("snippet") or "").strip() else "",
            f"Summary: {row.get('summary_text')}" if str(row.get("summary_text") or "").strip() else "",
            f"Body: {row.get('body_text')}" if str(row.get("body_text") or "").strip() else "",
        ]
    )
    return metadata, content


def fetch_source(
    registry_db: str | Path,
    source_id: str,
    *,
    clearance: str = "redacted",
) -> dict[str, Any]:
    path = Path(registry_db)
    if not path.exists():
        raise FileNotFoundError(f"registry db not found: {path}")

    normalized_id = str(source_id or "").strip()
    if not normalized_id:
        raise ValueError("source_id is required")

    conn = connect_vault_db(path, timeout=30.0)
    try:
        handler, row = _lookup_source_row(conn, normalized_id)
        filepath = str(row.get("filepath") or "")
        source_updated_at = str(row.get("updated_at") or "")
        if handler.kind == "docs":
            metadata, content = _build_doc_payload(row)
        elif handler.kind == "photos":
            metadata, content = _build_photo_payload(row)
        else:
            metadata, content = _build_mail_payload(row)

        if clearance != "full":
            content = _redact_text(content, _load_redaction_map(conn))
        return {
            "source_id": normalized_id,
            "source_kind": handler.kind,
            "source_table": handler.table,
            "source_filepath": filepath if clearance == "full" else None,
            "source_updated_at": source_updated_at or None,
            "clearance": clearance,
            "metadata": _sanitize_metadata_for_output(
                metadata=metadata,
                clearance=clearance,
                source_table=handler.table,
                source_filepath=filepath,
            ),
            "content": content,
        }
    finally:
        conn.close()


def list_sources(
    registry_db: str | Path,
    *,
    source: str = "all",
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    clearance: str = "redacted",
) -> dict[str, Any]:
    path = Path(registry_db)
    if not path.exists():
        raise FileNotFoundError(f"registry db not found: {path}")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    handlers = select_source_handlers(source)
    conn = connect_vault_db(path, timeout=30.0)
    try:
        redaction_map = _load_redaction_map(conn) if clearance != "full" else None
        items: list[tuple[tuple[str, str], dict[str, Any]]] = []
        for handler in handlers:
            if not _table_exists(conn, handler.table):
                continue
            for row in _list_rows_for_handler(
                conn,
                handler=handler,
                from_date=from_date,
                to_date=to_date,
                limit=limit,
            ):
                item = _build_list_item(
                    row,
                    handler=handler,
                    clearance=clearance,
                    redaction_map=redaction_map,
                )
                sort_key = _list_sort_key(row, kind=handler.kind)
                items.append((sort_key, item))
        items.sort(key=lambda pair: (pair[0][0], pair[0][1], pair[1]["source_id"]), reverse=True)
        results = [item for _, item in items[:limit]]
        return {
            "source": source,
            "from_date": from_date,
            "to_date": to_date,
            "limit": limit,
            "count": len(results),
            "results": results,
        }
    finally:
        conn.close()
