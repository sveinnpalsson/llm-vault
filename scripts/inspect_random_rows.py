#!/usr/bin/env python3
"""Print one random doc row and one random photo row with vector presence info."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Any

from vault_db import connect_vault_db
from vault_redaction import (
    PersistentRedactionMap,
    RedactionConfig,
    redact_chunks_with_persistent_map,
)
from vault_sources import REGISTERED_SOURCES, SourceHandler, source_handler_by_kind

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_DB = ROOT / "state" / "vault_registry.db"
DEFAULT_VECTORS_DB = ROOT / "state" / "vault_vectors.db"


def _clip(text: Any, limit: int) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(1, limit - 3)].rstrip() + "..."


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _random_row(conn: sqlite3.Connection, table: str) -> dict[str, Any] | None:
    if not _table_exists(conn, table):
        return None
    row = conn.execute(f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 1").fetchone()
    return dict(row) if row else None


def _load_redaction_rows(conn: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
    if not _table_exists(conn, "redaction_entries"):
        return []
    rows = conn.execute(
        """
        SELECT key_name, placeholder, value_norm, original_value
        FROM redaction_entries
        WHERE scope_type = 'vault' AND scope_id = 'global'
        ORDER BY key_name, placeholder
        """
    ).fetchall()
    return [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows]


def _vector_info(
    vec_conn: sqlite3.Connection | None,
    *,
    source_table: str,
    filepath: str,
) -> dict[str, Any]:
    if vec_conn is None:
        return {"embedding_present": False, "vector_items_count": 0, "source_indexed": False}

    if _table_exists(vec_conn, "vector_items_v2"):
        item_rows = vec_conn.execute(
            """
            SELECT index_level, COUNT(*)
            FROM vector_items_v2
            WHERE source_table = ? AND source_filepath = ?
            GROUP BY index_level
            ORDER BY index_level
            """,
            (source_table, filepath),
        ).fetchall()
        source_rows = []
        if _table_exists(vec_conn, "source_state_v2"):
            source_rows = vec_conn.execute(
                """
                SELECT index_level, indexed_at
                FROM source_state_v2
                WHERE source_table = ? AND source_filepath = ?
                ORDER BY index_level
                """,
                (source_table, filepath),
            ).fetchall()
        vector_items_by_level = {str(row[0]): int(row[1]) for row in item_rows}
        source_channels_by_level: dict[str, list[str]] = {}
        channel_key = ""
        if source_table == "photos_registry":
            channel_key = "photo_channel"
        elif source_table == "mail_registry":
            channel_key = "mail_channel"
        if channel_key:
            channel_rows = vec_conn.execute(
                """
                SELECT index_level, metadata_json
                FROM vector_items_v2
                WHERE source_table = ? AND source_filepath = ?
                ORDER BY index_level, chunk_index
                """,
                (source_table, filepath),
            ).fetchall()
            channel_sets: dict[str, set[str]] = {}
            for level, metadata_json in channel_rows:
                metadata = json.loads(metadata_json or "{}")
                channel = str(metadata.get(channel_key) or "").strip()
                if not channel:
                    continue
                channel_sets.setdefault(str(level), set()).add(channel)
            source_channels_by_level = {
                level: sorted(channels) for level, channels in sorted(channel_sets.items())
            }
        indexed_levels = [str(row[0]) for row in source_rows]
        indexed_at = {str(row[0]): row[1] for row in source_rows}
        vector_items_count = sum(vector_items_by_level.values())
        payload = {
            "embedding_present": vector_items_count > 0,
            "vector_items_count": vector_items_count,
            "vector_items_by_level": vector_items_by_level,
            "source_indexed": bool(source_rows),
            "indexed_levels": indexed_levels,
            "indexed_at": indexed_at,
        }
        if source_table == "photos_registry" and source_channels_by_level:
            payload["photo_channels_by_level"] = source_channels_by_level
        if source_table == "mail_registry" and source_channels_by_level:
            payload["mail_channels_by_level"] = source_channels_by_level
        return payload

    if not _table_exists(vec_conn, "vector_items"):
        return {"embedding_present": False, "vector_items_count": 0, "source_indexed": False}

    vector_items_count = int(
        vec_conn.execute(
            "SELECT COUNT(*) FROM vector_items WHERE source_table = ? AND source_filepath = ?",
            (source_table, filepath),
        ).fetchone()[0]
    )
    source_indexed = False
    indexed_at = None
    if _table_exists(vec_conn, "source_state"):
        row = vec_conn.execute(
            """
            SELECT indexed_at
            FROM source_state
            WHERE source_table = ? AND source_filepath = ?
            LIMIT 1
            """,
            (source_table, filepath),
        ).fetchone()
        if row:
            source_indexed = True
            indexed_at = row[0]
    return {
        "embedding_present": vector_items_count > 0,
        "vector_items_count": vector_items_count,
        "source_indexed": source_indexed,
        "indexed_at": indexed_at,
    }


def _redact_fields(fields: dict[str, str], redaction_rows: list[tuple[str, str, str, str]]) -> tuple[dict[str, str], list[dict[str, str]]]:
    table = PersistentRedactionMap.from_rows(redaction_rows)
    keys = list(fields.keys())
    texts = [str(fields[key] or "") for key in keys]
    run = redact_chunks_with_persistent_map(
        texts,
        mode="regex",
        table=table,
        cfg=RedactionConfig(mode="regex", enabled=True),
    )
    redacted = {key: run.chunk_text_redacted[idx] for idx, key in enumerate(keys)}
    placeholders_used = sorted(
        {
            match.group(0)
            for value in redacted.values()
            for match in re.finditer(r"<REDACTED_[A-Z0-9_]+>", value or "")
        }
    )
    pairs = [
        {
            "placeholder": placeholder,
            "key_name": str(table.placeholder_to_key.get(placeholder) or ""),
            "original_value": str(table.placeholder_to_value.get(placeholder) or ""),
        }
        for placeholder in placeholders_used
    ]
    return redacted, pairs


def _prepare_doc(
    row: dict[str, Any],
    vec_conn: sqlite3.Connection | None,
    redaction_rows: list[tuple[str, str, str, str]],
    limit: int,
) -> dict[str, Any]:
    out = dict(row)
    out["text_content"] = _clip(out.get("text_content"), limit)
    out["summary_text"] = _clip(out.get("summary_text"), limit)
    redacted, pairs = _redact_fields(
        {
            "filepath": str(out.get("filepath") or ""),
            "summary_text": str(out.get("summary_text") or ""),
            "text_content": str(out.get("text_content") or ""),
        },
        redaction_rows,
    )
    out["filepath_redacted"] = redacted["filepath"]
    out["summary_text_redacted"] = redacted["summary_text"]
    out["text_content_redacted"] = redacted["text_content"]
    out["redaction_pairs"] = pairs
    out.update(
        _vector_info(vec_conn, source_table="docs_registry", filepath=str(out.get("filepath") or ""))
    )
    return out


def _prepare_photo(
    row: dict[str, Any],
    vec_conn: sqlite3.Connection | None,
    redaction_rows: list[tuple[str, str, str, str]],
    limit: int,
) -> dict[str, Any]:
    out = dict(row)
    if "analyzer_raw" in out:
        out["analyzer_raw"] = _clip(out.get("analyzer_raw"), limit)
    if "ocr_text" in out:
        out["ocr_text"] = _clip(out.get("ocr_text"), limit)
    redacted, pairs = _redact_fields(
        {
            "filepath": str(out.get("filepath") or ""),
            "caption": str(out.get("caption") or ""),
            "notes": str(out.get("notes") or ""),
            "analyzer_raw": str(out.get("analyzer_raw") or ""),
            "ocr_text": str(out.get("ocr_text") or ""),
        },
        redaction_rows,
    )
    out["filepath_redacted"] = redacted["filepath"]
    out["caption_redacted"] = redacted["caption"]
    out["notes_redacted"] = redacted["notes"]
    out["analyzer_raw_redacted"] = redacted["analyzer_raw"]
    out["ocr_text_redacted"] = redacted["ocr_text"]
    out["redaction_pairs"] = pairs
    out.update(
        _vector_info(
            vec_conn,
            source_table="photos_registry",
            filepath=str(out.get("filepath") or ""),
        )
    )
    return out


def _prepare_mail(
    row: dict[str, Any],
    vec_conn: sqlite3.Connection | None,
    redaction_rows: list[tuple[str, str, str, str]],
    limit: int,
) -> dict[str, Any]:
    out = dict(row)
    for key in ("subject", "snippet", "body_text", "summary_text"):
        if key in out:
            out[key] = _clip(out.get(key), limit)
    redacted, pairs = _redact_fields(
        {
            "filepath": str(out.get("filepath") or ""),
            "account_email": str(out.get("account_email") or ""),
            "from_addr": str(out.get("from_addr") or ""),
            "to_addr": str(out.get("to_addr") or ""),
            "subject": str(out.get("subject") or ""),
            "snippet": str(out.get("snippet") or ""),
            "body_text": str(out.get("body_text") or ""),
            "summary_text": str(out.get("summary_text") or ""),
        },
        redaction_rows,
    )
    out["filepath_redacted"] = redacted["filepath"]
    out["account_email_redacted"] = redacted["account_email"]
    out["from_addr_redacted"] = redacted["from_addr"]
    out["to_addr_redacted"] = redacted["to_addr"]
    out["subject_redacted"] = redacted["subject"]
    out["snippet_redacted"] = redacted["snippet"]
    out["body_text_redacted"] = redacted["body_text"]
    out["summary_text_redacted"] = redacted["summary_text"]
    out["redaction_pairs"] = pairs
    out.update(
        _vector_info(
            vec_conn,
            source_table="mail_registry",
            filepath=str(out.get("filepath") or ""),
        )
    )
    return out


def _inspection_source_handlers() -> tuple[SourceHandler, ...]:
    docs_handler = replace(
        source_handler_by_kind("docs", handlers=REGISTERED_SOURCES),
        inspection_preparer=_prepare_doc,
    )
    photos_handler = replace(
        source_handler_by_kind("photos", handlers=REGISTERED_SOURCES),
        inspection_preparer=_prepare_photo,
    )
    mail_handler = replace(
        source_handler_by_kind("mail", handlers=REGISTERED_SOURCES),
        inspection_preparer=_prepare_mail,
    )
    return (docs_handler, photos_handler, mail_handler)


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect one random row per registered source")
    ap.add_argument("--registry-db", default=str(DEFAULT_REGISTRY_DB))
    ap.add_argument("--vectors-db", default=str(DEFAULT_VECTORS_DB))
    ap.add_argument("--text-limit", type=int, default=300, help="cap long text fields for output")
    ap.add_argument("--mail-bridge-enabled", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()

    registry_db = Path(args.registry_db)
    vectors_db = Path(args.vectors_db)
    if not registry_db.exists():
        raise SystemExit(f"registry DB not found: {registry_db}")

    reg_conn = connect_vault_db(registry_db, timeout=30.0)
    vec_conn = connect_vault_db(vectors_db, timeout=30.0) if vectors_db.exists() else None
    try:
        redaction_rows = _load_redaction_rows(reg_conn)
        payload = {"sources": {}}
        for handler in _inspection_source_handlers():
            row = _random_row(reg_conn, handler.table)
            prepared = (
                handler.inspection_preparer(dict(row), vec_conn, redaction_rows, args.text_limit)
                if row is not None and handler.inspection_preparer is not None
                else None
            )
            if isinstance(prepared, dict):
                prepared["source_kind"] = handler.kind
                prepared["source_table"] = handler.table
                prepared["source_label"] = handler.label
                if handler.kind == "mail":
                    prepared["bridge_enabled"] = bool(args.mail_bridge_enabled)
            payload["sources"][handler.kind] = prepared
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    finally:
        reg_conn.close()
        if vec_conn is not None:
            vec_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
