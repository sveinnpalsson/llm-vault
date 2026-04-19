#!/usr/bin/env python3
"""Purge already-indexed junk mail-attachment image rows from local llm-vault state."""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from vault_db import connect_vault_db
from vault_registry_sync import (
    MAIL_ATTACHMENT_SOURCE,
    MailAttachmentRecord,
    _should_skip_mail_attachment,
    now_iso,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_DB = ROOT / "state" / "vault_registry.db"
DEFAULT_VECTOR_DB = ROOT / "state" / "vault_vectors.db"


@dataclass(frozen=True)
class CandidateRow:
    attachment_ref: str
    attachment_key: str
    msg_id: str
    account_email: str
    part_id: str
    gmail_attachment_id: str
    mime_type: str
    filename: str
    size_bytes: int
    content_disposition: str
    content_id: str
    is_inline: bool
    inventory_state: str
    inventoried_at: str
    storage_kind: str
    storage_path: str
    content_sha256: str
    content_size_bytes: int
    materialized_at: str
    target_kind: str
    registry_table: str
    registry_filepath: str
    materialized_input_path: str
    ingest_status: str
    ingest_error: str

    def to_record(self) -> MailAttachmentRecord:
        return MailAttachmentRecord(
            attachment_ref=self.attachment_ref,
            attachment_key=self.attachment_key,
            msg_id=self.msg_id,
            account_email=self.account_email,
            part_id=self.part_id,
            gmail_attachment_id=self.gmail_attachment_id,
            mime_type=self.mime_type,
            filename=self.filename,
            size_bytes=self.size_bytes,
            content_disposition=self.content_disposition,
            content_id=self.content_id,
            is_inline=self.is_inline,
            inventory_state=self.inventory_state,
            inventoried_at=self.inventoried_at,
            storage_kind=self.storage_kind,
            storage_path=self.storage_path,
            content_sha256=self.content_sha256,
            content_size_bytes=self.content_size_bytes,
            materialized_at=self.materialized_at,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry-db", default=str(DEFAULT_REGISTRY_DB))
    parser.add_argument("--vector-db", default=str(DEFAULT_VECTOR_DB))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="show what would be removed (default)")
    mode.add_argument("--apply", action="store_true", help="delete matching registry/vector rows and mark bridge rows skipped")
    return parser.parse_args()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _iter_candidates(reg_conn: sqlite3.Connection) -> list[tuple[CandidateRow, str]]:
    rows = reg_conn.execute(
        """
        SELECT
          attachment_ref, attachment_key, msg_id, account_email, part_id, gmail_attachment_id,
          mime_type, filename, COALESCE(size_bytes, 0), content_disposition, content_id, COALESCE(is_inline, 0),
          inventory_state, inventoried_at, storage_kind, storage_path, content_sha256,
          COALESCE(content_size_bytes, 0), materialized_at, target_kind, registry_table, registry_filepath,
          materialized_input_path, ingest_status, ingest_error
        FROM mail_attachment_bridge
        WHERE source = ?
        ORDER BY account_email, msg_id, part_id
        """,
        (MAIL_ATTACHMENT_SOURCE,),
    ).fetchall()
    out: list[tuple[CandidateRow, str]] = []
    for row in rows:
        candidate = CandidateRow(
            attachment_ref=str(row[0] or ""),
            attachment_key=str(row[1] or ""),
            msg_id=str(row[2] or ""),
            account_email=str(row[3] or ""),
            part_id=str(row[4] or ""),
            gmail_attachment_id=str(row[5] or ""),
            mime_type=str(row[6] or ""),
            filename=str(row[7] or ""),
            size_bytes=int(row[8] or 0),
            content_disposition=str(row[9] or ""),
            content_id=str(row[10] or ""),
            is_inline=bool(int(row[11] or 0)),
            inventory_state=str(row[12] or ""),
            inventoried_at=str(row[13] or ""),
            storage_kind=str(row[14] or ""),
            storage_path=str(row[15] or ""),
            content_sha256=str(row[16] or ""),
            content_size_bytes=int(row[17] or 0),
            materialized_at=str(row[18] or ""),
            target_kind=str(row[19] or ""),
            registry_table=str(row[20] or ""),
            registry_filepath=str(row[21] or ""),
            materialized_input_path=str(row[22] or ""),
            ingest_status=str(row[23] or ""),
            ingest_error=str(row[24] or ""),
        )
        materialized_path = Path(candidate.materialized_input_path) if candidate.materialized_input_path else None
        should_skip, reason = _should_skip_mail_attachment(candidate.to_record(), materialized_path=materialized_path)
        if should_skip:
            out.append((candidate, reason))
    return out


def _delete_registry_row(reg_conn: sqlite3.Connection, *, table: str, filepath: str) -> int:
    if table not in {"docs_registry", "photos_registry"} or not filepath:
        return 0
    return int(reg_conn.execute(f"DELETE FROM {table} WHERE filepath = ?", (filepath,)).rowcount or 0)


def _delete_vector_rows(vec_conn: sqlite3.Connection | None, *, table: str, filepath: str) -> tuple[int, int]:
    if vec_conn is None or not table or not filepath:
        return 0, 0
    deleted_items = 0
    deleted_sources = 0
    if _table_exists(vec_conn, "vector_items_v2"):
        deleted_items += int(
            vec_conn.execute(
                "DELETE FROM vector_items_v2 WHERE source_table = ? AND source_filepath = ?",
                (table, filepath),
            ).rowcount
            or 0
        )
    if _table_exists(vec_conn, "source_state_v2"):
        deleted_sources += int(
            vec_conn.execute(
                "DELETE FROM source_state_v2 WHERE source_table = ? AND source_filepath = ?",
                (table, filepath),
            ).rowcount
            or 0
        )
    return deleted_items, deleted_sources


def main() -> int:
    args = parse_args()
    apply_changes = bool(args.apply)

    reg_conn = connect_vault_db(Path(args.registry_db))
    vec_conn = connect_vault_db(Path(args.vector_db)) if Path(args.vector_db).exists() else None
    try:
        if not _table_exists(reg_conn, "mail_attachment_bridge"):
            print("mail_attachment_bridge table not found")
            return 1

        candidates = _iter_candidates(reg_conn)
        bridge_updates = 0
        registry_deleted = 0
        vector_items_deleted = 0
        vector_sources_deleted = 0

        for candidate, reason in candidates:
            if not apply_changes:
                continue
            registry_deleted += _delete_registry_row(
                reg_conn,
                table=candidate.registry_table,
                filepath=candidate.registry_filepath,
            )
            deleted_items, deleted_sources = _delete_vector_rows(
                vec_conn,
                table=candidate.registry_table,
                filepath=candidate.registry_filepath,
            )
            vector_items_deleted += deleted_items
            vector_sources_deleted += deleted_sources
            bridge_updates += int(
                reg_conn.execute(
                    """
                    UPDATE mail_attachment_bridge
                    SET target_kind = 'photo',
                        registry_table = '',
                        registry_filepath = '',
                        ingest_status = 'skipped-junk-image',
                        ingest_error = ?,
                        indexed_at = NULL,
                        updated_at = ?
                    WHERE attachment_ref = ?
                    """,
                    (reason[:500], now_iso(), candidate.attachment_ref),
                ).rowcount
                or 0
            )

        if apply_changes:
            reg_conn.commit()
            if vec_conn is not None:
                vec_conn.commit()

        print(f"mode: {'apply' if apply_changes else 'dry-run'}")
        print(f"junk_candidates: {len(candidates)}")
        print(f"bridge_rows_updated: {bridge_updates}")
        print(f"registry_rows_deleted: {registry_deleted}")
        print(f"vector_items_deleted: {vector_items_deleted}")
        print(f"vector_source_rows_deleted: {vector_sources_deleted}")
        return 0
    finally:
        reg_conn.close()
        if vec_conn is not None:
            vec_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
