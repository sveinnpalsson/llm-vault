from __future__ import annotations

import array
import hashlib
import json
import math
import sqlite3
from pathlib import Path

import pytest
import vault_db
import vault_registry_sync
from vault_db import connect_vault_db
from vault_redaction import RedactionConfig
from vault_registry_sync import MailBridgeConfig, ensure_db, sync_mail_bridge
from vault_vector_index import chunk_text, query_index, update_index


class StubEmbeddingClient:
    def __init__(self, dim: int = 8):
        self.dim = dim

    def _embed_one(self, text: str) -> list[float]:
        out = [0.0] * self.dim
        for token in str(text or "").lower().split():
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            idx = int.from_bytes(digest[:4], "big") % self.dim
            sign = 1.0 if (digest[4] % 2 == 0) else -1.0
            out[idx] += sign
        norm = math.sqrt(sum(v * v for v in out))
        if norm > 0:
            out = [v / norm for v in out]
        return out

    def embed_texts(self, texts: list[str]) -> tuple[list[bytes], int]:
        blobs: list[bytes] = []
        for text in texts:
            blobs.append(array.array("f", self._embed_one(text)).tobytes())
        return blobs, self.dim


def _seed_inbox_db(inbox_db: Path) -> None:
    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.executescript(
            """
            CREATE TABLE messages (
              msg_id TEXT PRIMARY KEY,
              account_email TEXT NOT NULL,
              thread_id TEXT,
              date_iso TEXT,
              internal_ts INTEGER,
              from_addr TEXT,
              to_addr TEXT,
              subject TEXT,
              snippet TEXT,
              body_text TEXT,
              labels_json TEXT,
              history_id INTEGER,
              last_seen_at TEXT NOT NULL
            );

            CREATE TABLE message_enrichment (
              msg_id TEXT PRIMARY KEY,
              category TEXT,
              importance INTEGER,
              action TEXT,
              summary TEXT,
              model TEXT,
              enriched_at TEXT NOT NULL
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO messages (
              msg_id, account_email, thread_id, date_iso, internal_ts, from_addr, to_addr,
              subject, snippet, body_text, labels_json, history_id, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "msg-a-1",
                    "acct-a@example.com",
                    "thread-a",
                    "2026-03-20T10:00:00+00:00",
                    1710928800,
                    "boss@example.com",
                    "acct-a@example.com",
                    "Budget approval needed",
                    "Need approval by Friday",
                    "Please approve the operating budget by Friday afternoon.",
                    json.dumps(["INBOX", "IMPORTANT"]),
                    1,
                    "2026-03-20T11:00:00+00:00",
                ),
                (
                    "msg-a-2",
                    "acct-a@example.com",
                    "thread-a",
                    "2026-03-21T09:00:00+00:00",
                    1711011600,
                    "teammate@example.com",
                    "acct-a@example.com",
                    "Escalation review",
                    "Review the escalation memo",
                    "Escalation packet attached for policy review.",
                    json.dumps(["INBOX"]),
                    2,
                    "2026-03-21T10:00:00+00:00",
                ),
                (
                    "msg-b-1",
                    "acct-b@example.com",
                    "thread-b",
                    "2026-03-22T08:00:00+00:00",
                    1711094400,
                    "alerts@example.com",
                    "acct-b@example.com",
                    "Security notice",
                    "Rotation required",
                    "Rotate the API credential before Monday.",
                    json.dumps(["INBOX", "SECURITY"]),
                    3,
                    "2026-03-22T09:00:00+00:00",
                ),
            ],
        )
        conn.executemany(
            """
            INSERT INTO message_enrichment (
              msg_id, category, importance, action, summary, model, enriched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "msg-a-1",
                    "billing",
                    8,
                    "review",
                    "Budget deadline summary with Friday approval milestone.",
                    "local-test",
                    "2026-03-20T12:00:00+00:00",
                ),
                (
                    "msg-a-2",
                    "general",
                    5,
                    "review",
                    "Escalation keyword summary for audit trail review.",
                    "local-test",
                    "2026-03-21T10:30:00+00:00",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _append_long_mail_message(inbox_db: Path) -> None:
    token_count = 9000
    words = []
    for idx in range(token_count):
        if idx == 10:
            words.append("EARLYMARKER")
        elif idx == token_count - 10:
            words.append("LATEMARKER")
        else:
            words.append(f"bodytoken{idx:05d}")
    long_body = " ".join(words)

    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.execute(
            """
            INSERT INTO messages (
              msg_id, account_email, thread_id, date_iso, internal_ts, from_addr, to_addr,
              subject, snippet, body_text, labels_json, history_id, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "msg-long-1",
                "acct-a@example.com",
                "thread-long",
                "2026-03-23T08:00:00+00:00",
                1711180800,
                "archive@example.com",
                "acct-a@example.com",
                "Escalation archive digest",
                "Very long archived escalation thread",
                long_body,
                json.dumps(["INBOX", "ARCHIVE"]),
                4,
                "2026-03-23T09:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO message_enrichment (
              msg_id, category, importance, action, summary, model, enriched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "msg-long-1",
                "general",
                4,
                "review",
                "Escalation archive summary without late-only token.",
                "local-test",
                "2026-03-23T09:05:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _mail_cfg(
    inbox_db: Path,
    *,
    include_accounts: tuple[str, ...] = (),
    import_summary: bool = True,
) -> MailBridgeConfig:
    return MailBridgeConfig(
        enabled=True,
        db_path=str(inbox_db),
        password_env="INBOX_VAULT_DB_PASSWORD",
        include_accounts=include_accounts,
        import_summary=import_summary,
    )


def _set_plaintext_inbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vault_registry_sync, "SQLCIPHER_AVAILABLE", False)
    monkeypatch.setattr(vault_db, "SQLCIPHER_AVAILABLE", False)


def test_sync_mail_bridge_imports_summary_dates_and_per_account_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()

        assert updated == 2
        assert pruned == 0
        assert accounts_processed == 1

        rows = reg.execute(
            """
            SELECT msg_id, account_email, summary_text, primary_date, dates_json
            FROM mail_registry
            ORDER BY msg_id
            """
        ).fetchall()
        assert [tuple(row[:2]) for row in rows] == [
            ("msg-a-1", "acct-a@example.com"),
            ("msg-a-2", "acct-a@example.com"),
        ]
        assert rows[0][2] == "Budget deadline summary with Friday approval milestone."
        assert rows[0][3] == "2026-03-20T10:00:00+00:00"
        assert json.loads(rows[0][4]) == [
            {
                "value": "2026-03-20T10:00:00+00:00",
                "kind": "message_date",
                "source": "date_iso",
            }
        ]

        cursor_rows = reg.execute(
            """
            SELECT account_email, last_material_updated_at, last_material_msg_id
            FROM mail_sync_state
            ORDER BY account_email
            """
        ).fetchall()
        assert [tuple(row) for row in cursor_rows] == [
            ("acct-a@example.com", "2026-03-21T10:30:00+00:00", "msg-a-2"),
        ]
    finally:
        reg.close()


def test_sync_mail_bridge_incremental_rerun_is_noop_on_unchanged_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        cfg = _mail_cfg(inbox_db)
        first = sync_mail_bridge(
            reg,
            mail_cfg=cfg,
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()
        assert first == (3, 0, 2)

        before_row = reg.execute(
            "SELECT updated_at FROM mail_registry WHERE msg_id = ?",
            ("msg-a-1",),
        ).fetchone()
        before_cursor = reg.execute(
            """
            SELECT last_material_updated_at, last_material_msg_id
            FROM mail_sync_state
            WHERE account_email = ?
            """,
            ("acct-a@example.com",),
        ).fetchone()

        second = sync_mail_bridge(
            reg,
            mail_cfg=cfg,
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()

        after_row = reg.execute(
            "SELECT updated_at FROM mail_registry WHERE msg_id = ?",
            ("msg-a-1",),
        ).fetchone()
        after_cursor = reg.execute(
            """
            SELECT last_material_updated_at, last_material_msg_id
            FROM mail_sync_state
            WHERE account_email = ?
            """,
            ("acct-a@example.com",),
        ).fetchone()

        assert second == (0, 0, 2)
        assert before_row == after_row
        assert before_cursor == after_cursor
    finally:
        reg.close()


def test_sync_mail_bridge_repair_prunes_filtered_accounts_and_is_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()

        keep_before = reg.execute(
            "SELECT updated_at FROM mail_registry WHERE msg_id = ?",
            ("msg-a-1",),
        ).fetchone()

        repair = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=True,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()
        keep_after = reg.execute(
            "SELECT updated_at FROM mail_registry WHERE msg_id = ?",
            ("msg-a-1",),
        ).fetchone()

        assert repair == (0, 2, 1)
        assert keep_before == keep_after
        remaining = reg.execute(
            "SELECT msg_id, account_email FROM mail_registry ORDER BY msg_id"
        ).fetchall()
        assert [tuple(row) for row in remaining] == [
            ("msg-a-1", "acct-a@example.com"),
            ("msg-a-2", "acct-a@example.com"),
        ]

        repair_again = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=True,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()
        assert repair_again == (0, 0, 1)
    finally:
        reg.close()


def test_sync_mail_bridge_rolls_back_failing_account_without_advancing_its_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        original_upsert = vault_registry_sync.upsert_mail

        def failing_upsert(conn, *, record, checksum, primary_date, dates_json):
            if record.account_email == "acct-b@example.com":
                raise RuntimeError("boom")
            return original_upsert(
                conn,
                record=record,
                checksum=checksum,
                primary_date=primary_date,
                dates_json=dates_json,
            )

        monkeypatch.setattr(vault_registry_sync, "upsert_mail", failing_upsert)
        with pytest.raises(RuntimeError):
            sync_mail_bridge(
                reg,
                mail_cfg=_mail_cfg(inbox_db),
                full_scan=False,
                dry_run=False,
                deadline=float("inf"),
                verbose=False,
            )

        imported = reg.execute(
            "SELECT msg_id, account_email FROM mail_registry ORDER BY account_email, msg_id"
        ).fetchall()
        cursors = reg.execute(
            "SELECT account_email, last_material_msg_id FROM mail_sync_state ORDER BY account_email"
        ).fetchall()

        assert [tuple(row) for row in imported] == [
            ("msg-a-1", "acct-a@example.com"),
            ("msg-a-2", "acct-a@example.com"),
        ]
        assert [tuple(row) for row in cursors] == [
            ("acct-a@example.com", "msg-a-2"),
        ]
    finally:
        reg.close()


def test_mail_indexing_search_and_disabled_source_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()
    finally:
        reg.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    vec = connect_vault_db(vector_db)
    try:
        before_indexed_at = vec.execute(
            """
            SELECT indexed_at
            FROM source_state_v2
            WHERE source_table = 'mail_registry' AND source_filepath = ?
            """,
            ("mail://message/msg-a-1",),
        ).fetchone()
    finally:
        vec.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        rebuild=False,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    vec = connect_vault_db(vector_db)
    try:
        after_indexed_at = vec.execute(
            """
            SELECT indexed_at
            FROM source_state_v2
            WHERE source_table = 'mail_registry' AND source_filepath = ?
            """,
            ("mail://message/msg-a-1",),
        ).fetchone()
    finally:
        vec.close()
    assert before_indexed_at == after_indexed_at

    rc = query_index(
        registry_db,
        vector_db,
        "escalation keyword",
        top_k=3,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        clearance="redacted",
        search_level="auto",
        as_json=True,
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["results"][0]["source_kind"] == "mail"
    assert payload["results"][0]["metadata"]["mail_channel"] == "summary"

    rc = query_index(
        registry_db,
        vector_db,
        "escalation keyword",
        top_k=3,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="all",
        mail_bridge_enabled=False,
        clearance="redacted",
        search_level="auto",
        as_json=True,
    )
    assert rc == 0
    payload_disabled_all = json.loads(capsys.readouterr().out)
    assert payload_disabled_all["count"] == 0

    rc = query_index(
        registry_db,
        vector_db,
        "escalation keyword",
        top_k=3,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=False,
        clearance="redacted",
        search_level="auto",
        as_json=True,
    )
    assert rc == 2


def test_mail_body_chunk_cap_limits_long_messages_and_reindexes_on_cap_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)
    _append_long_mail_message(inbox_db)

    inbox_conn = sqlite3.connect(str(inbox_db))
    try:
        long_body = inbox_conn.execute(
            "SELECT body_text FROM messages WHERE msg_id = 'msg-long-1'"
        ).fetchone()[0]
    finally:
        inbox_conn.close()
    assert len(chunk_text(long_body)) > 20

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            verbose=False,
        )
        reg.commit()
    finally:
        reg.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        mail_max_body_chunks=12,
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    vec = connect_vault_db(vector_db)
    try:
        item_rows = vec.execute(
            """
            SELECT metadata_json, text_preview_redacted
            FROM vector_items_v2
            WHERE source_table = 'mail_registry'
              AND source_filepath = ?
              AND index_level = 'redacted'
            ORDER BY item_id
            """,
            ("mail://message/msg-long-1",),
        ).fetchall()
        source_state_before = vec.execute(
            """
            SELECT indexed_at, item_count
            FROM source_state_v2
            WHERE source_table = 'mail_registry'
              AND source_filepath = ?
              AND index_level = 'redacted'
            """,
            ("mail://message/msg-long-1",),
        ).fetchone()
    finally:
        vec.close()

    assert source_state_before is not None
    metadata_rows = [json.loads(row[0] or "{}") for row in item_rows]
    body_rows = [meta for meta in metadata_rows if meta.get("mail_channel") == "body"]
    assert len(body_rows) == 12
    assert {"metadata", "subject_snippet", "summary"}.issubset({meta.get("mail_channel") for meta in metadata_rows})
    assert body_rows[0]["mail_body_truncated"] is True
    assert body_rows[0]["mail_body_chunks_total"] > 20
    assert body_rows[0]["mail_body_chunks_indexed"] == 12
    previews = [str(row[1] or "") for row in item_rows]
    assert any("EARLYMARKER" in preview for preview in previews)
    assert all("LATEMARKER" not in preview for preview in previews)

    rc = query_index(
        registry_db,
        vector_db,
        "EARLYMARKER",
        top_k=5,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        mail_max_body_chunks=12,
        clearance="full",
        search_level="auto",
        as_json=True,
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert any(result["source_filepath"] == "mail://message/msg-long-1" for result in payload["results"])

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        mail_max_body_chunks=12,
        rebuild=False,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    vec = connect_vault_db(vector_db)
    try:
        source_state_same_cap = vec.execute(
            """
            SELECT indexed_at, item_count
            FROM source_state_v2
            WHERE source_table = 'mail_registry'
              AND source_filepath = ?
              AND index_level = 'redacted'
            """,
            ("mail://message/msg-long-1",),
        ).fetchone()
    finally:
        vec.close()
    assert source_state_same_cap == source_state_before

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        mail_max_body_chunks=20,
        rebuild=False,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    vec = connect_vault_db(vector_db)
    try:
        source_state_after = vec.execute(
            """
            SELECT indexed_at, item_count
            FROM source_state_v2
            WHERE source_table = 'mail_registry'
              AND source_filepath = ?
              AND index_level = 'redacted'
            """,
            ("mail://message/msg-long-1",),
        ).fetchone()
        body_count_after = vec.execute(
            """
            SELECT COUNT(*)
            FROM vector_items_v2
            WHERE source_table = 'mail_registry'
              AND source_filepath = ?
              AND index_level = 'redacted'
              AND json_extract(metadata_json, '$.mail_channel') = 'body'
            """,
            ("mail://message/msg-long-1",),
        ).fetchone()[0]
    finally:
        vec.close()

    assert source_state_after is not None
    assert source_state_after[0] != source_state_before[0]
    assert int(source_state_after[1]) > int(source_state_before[1])
    assert int(body_count_after) == 20
