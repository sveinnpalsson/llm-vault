#!/usr/bin/env python3
"""Source-oriented fetch helpers for llm-vault."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from vault_db import connect_vault_db
from vault_redaction import PersistentRedactionMap
from vault_sources import REGISTERED_SOURCES, SourceHandler
from vault_vector_index import _parse_dates_json, _parse_provenance_json, _sanitize_metadata_for_output, _stable_source_id

DEFAULT_CONTENT_LIMIT = 1200


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
