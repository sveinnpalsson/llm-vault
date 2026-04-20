from __future__ import annotations

import array
import base64
import hashlib
import io
import json
import math
import sqlite3
import zipfile
from pathlib import Path

import pytest
import cleanup_junk_mail_attachments
import vault_db
import vault_registry_sync
from vault_db import connect_vault_db
from vault_redaction import RedactionConfig
from vault_registry_sync import (
    Config,
    MailBridgeConfig,
    PdfParseConfig,
    PhotoAnalysisConfig,
    PhotoAnalysisResult,
    SummaryConfig,
    WorkBudget,
    backfill_missing_photo_analysis,
    count_pending_photo_backfill,
    ensure_db,
    run,
    sync_mail_bridge,
)
from vault_vector_index import chunk_text, query_index, update_index

try:
    from PIL import Image as PILImage
except Exception:  # pragma: no cover - Pillow is optional in some environments
    PILImage = None


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


def test_sync_mail_bridge_verbose_logs_progress_for_mail_and_attachments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)
    _seed_inbox_attachment_inventory(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(
                enabled=False,
                parse_url="",
                timeout_seconds=60,
                profile="auto",
            ),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=True,
        )
        reg.commit()
    finally:
        reg.close()

    assert updated == 4
    assert pruned == 0
    assert accounts_processed == 1

    stdout = capsys.readouterr().out
    assert "stage=4/6.mail-sync.mail" in stdout
    assert "action=start account=acct-a@example.com" in stdout
    assert "action=account-done account=acct-a@example.com" in stdout
    assert "stage=4/6.mail-sync.attachments" in stdout
    assert ("stage=4/6.mail-sync.docs" in stdout) or ("stage=4/6.mail-sync.photos" in stdout)


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


def _seed_inbox_attachment_inventory(inbox_db: Path) -> None:
    note_bytes = b"Invoice note for jane.doe@example.com with due date 2026-04-01."
    if PILImage is not None:
        buf = io.BytesIO()
        PILImage.new("RGB", (96, 96), color=(240, 240, 240)).save(buf, format="PNG")
        png_bytes = buf.getvalue()
    else:
        png_bytes = base64.urlsafe_b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z0YQAAAAASUVORK5CYII="
        )
    note_rel = Path("attachment-cache") / "aa" / "att-note-inline.txt"
    png_rel = Path("attachment-cache") / "bb" / "att-receipt-inline.png"
    note_path = inbox_db.parent / note_rel
    png_path = inbox_db.parent / png_rel
    note_path.parent.mkdir(parents=True, exist_ok=True)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_bytes(note_bytes)
    png_path.write_bytes(png_bytes)

    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.executescript(
            """
            CREATE TABLE message_attachments (
              msg_id TEXT NOT NULL,
              account_email TEXT NOT NULL,
              attachment_key TEXT NOT NULL DEFAULT '',
              part_id TEXT NOT NULL,
              gmail_attachment_id TEXT NOT NULL DEFAULT '',
              mime_type TEXT NOT NULL DEFAULT '',
              filename TEXT NOT NULL DEFAULT '',
              size_bytes INTEGER,
              content_disposition TEXT NOT NULL DEFAULT '',
              content_id TEXT NOT NULL DEFAULT '',
              is_inline INTEGER NOT NULL DEFAULT 0,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              storage_kind TEXT NOT NULL DEFAULT '',
              storage_path TEXT NOT NULL DEFAULT '',
              content_sha256 TEXT NOT NULL DEFAULT '',
              content_size_bytes INTEGER NOT NULL DEFAULT 0,
              materialized_at TEXT NOT NULL DEFAULT '',
              last_seen_at TEXT NOT NULL,
              PRIMARY KEY (msg_id, part_id)
            );

            CREATE TABLE message_attachment_inventory_state (
              msg_id TEXT PRIMARY KEY,
              account_email TEXT NOT NULL,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              attachment_count INTEGER NOT NULL DEFAULT 0,
              inventoried_at TEXT NOT NULL
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO message_attachments (
              msg_id, account_email, attachment_key, part_id, gmail_attachment_id, mime_type, filename,
              size_bytes, content_disposition, content_id, is_inline, inventory_state,
              storage_kind, storage_path, content_sha256, content_size_bytes, materialized_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "msg-a-2",
                    "acct-a@example.com",
                    "att-note-inline",
                    "2",
                    "",
                    "text/plain",
                    "invoice-note.txt",
                    len(note_bytes),
                    "attachment",
                    "",
                    0,
                    "metadata_only",
                    "file",
                    str(note_rel),
                    hashlib.sha256(note_bytes).hexdigest(),
                    len(note_bytes),
                    "2026-03-21T10:35:00+00:00",
                    "2026-03-21T10:35:00+00:00",
                ),
                (
                    "msg-a-2",
                    "acct-a@example.com",
                    "att-receipt-inline",
                    "3",
                    "",
                    "image/png",
                    "receipt.png",
                    len(png_bytes),
                    "attachment",
                    "",
                    0,
                    "metadata_only",
                    "file",
                    str(png_rel),
                    hashlib.sha256(png_bytes).hexdigest(),
                    len(png_bytes),
                    "2026-03-21T10:35:00+00:00",
                    "2026-03-21T10:35:00+00:00",
                ),
            ],
        )
        conn.execute(
            """
            INSERT INTO message_attachment_inventory_state (
              msg_id, account_email, inventory_state, attachment_count, inventoried_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "metadata_only",
                2,
                "2026-03-21T10:35:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _build_minimal_docx_bytes(text: str) -> bytes:
    paragraphs = "".join(
        f"<w:p><w:r><w:t>{line}</w:t></w:r></w:p>"
        for line in str(text or "").splitlines()
    )
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{paragraphs}</w:body>"
        "</w:document>"
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/>'
        "</Relationships>"
    )

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types_xml)
        archive.writestr("_rels/.rels", rels_xml)
        archive.writestr("word/document.xml", document_xml)
    return out.getvalue()


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


class _FakeChatClient:
    def chat_json(self, messages, *, max_tokens, temperature):
        del messages, max_tokens, temperature
        return {"summary": "Invoice attachment summary with due date."}


class _FakePhotoClient:
    def __init__(self) -> None:
        self.cfg = PhotoAnalysisConfig(
            enabled=True,
            analyze_url="http://127.0.0.1:8081/analyze",
            timeout_seconds=30,
            force=False,
        )

    def analyze(self, _path: Path) -> PhotoAnalysisResult:
        return PhotoAnalysisResult(
            status="ok",
            route_kind="photo",
            taxonomy="docs",
            caption="Expense receipt attachment",
            category_primary="receipt",
            category_secondary="expense",
            analyzer_model="test-photo-model",
            analyzer_error="",
            analyzer_raw='{"caption":"Expense receipt attachment","text_raw":"Total $42.00"}',
            ocr_text="Total $42.00",
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


def test_sync_mail_bridge_full_scan_rerun_resumes_from_mail_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        first = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            budget=WorkBudget.from_max_items(1),
            verbose=False,
        )
        reg.commit()
        repaired = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=True,
            dry_run=False,
            deadline=float("inf"),
            verbose=True,
        )
        reg.commit()
    finally:
        reg.close()

    assert first == (1, 0, 1)
    assert repaired == (1, 0, 1)

    imported = connect_vault_db(registry_db)
    try:
        rows = imported.execute(
            "SELECT msg_id FROM mail_registry WHERE account_email = ? ORDER BY msg_id",
            ("acct-a@example.com",),
        ).fetchall()
        cursor_row = imported.execute(
            """
            SELECT last_material_updated_at, last_material_msg_id
            FROM mail_sync_state
            WHERE account_email = ?
            """,
            ("acct-a@example.com",),
        ).fetchone()
    finally:
        imported.close()

    assert [tuple(row) for row in rows] == [("msg-a-1",), ("msg-a-2",)]
    assert tuple(cursor_row) == ("2026-03-21T10:30:00+00:00", "msg-a-2")

    stdout = capsys.readouterr().out
    assert "action=resume account=acct-a@example.com" in stdout
    assert "[item=0/1]" in stdout
    assert "action=repaired account=acct-a@example.com" in stdout


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


def test_mail_bridge_ingests_inline_attachments_into_docs_and_photos(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)
    _seed_inbox_attachment_inventory(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(
                enabled=False,
                parse_url="",
                timeout_seconds=60,
                profile="auto",
            ),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
            counters={
                "docs_indexed": 0,
                "photos_indexed": 0,
                "mail_indexed": 0,
                "mail_pruned": 0,
                "mail_accounts_processed": 0,
                "summary_updated": 0,
                "summary_failed": 0,
                "photo_backfill_updated": 0,
                "photo_backfill_failed": 0,
                "inbox_routed": 0,
                "skipped": 0,
                "errors": 0,
            },
        )
        reg.commit()

        assert updated == 4
        assert pruned == 0
        assert accounts_processed == 1

        doc_row = reg.execute(
            """
            SELECT filepath, source, summary_text, provenance_json
            FROM docs_registry
            WHERE source = 'inbox-vault/mail-attachment'
            """
        ).fetchone()
        photo_row = reg.execute(
            """
            SELECT filepath, source, caption, ocr_text, provenance_json
            FROM photos_registry
            WHERE source = 'inbox-vault/mail-attachment'
            """
        ).fetchone()
        bridge_rows = reg.execute(
            """
            SELECT target_kind, registry_table, ingest_status
            FROM mail_attachment_bridge
            ORDER BY part_id
            """
        ).fetchall()
    finally:
        reg.close()

    assert doc_row is not None
    assert str(doc_row[0]).startswith("mail-attachment://doc/")
    assert doc_row[1] == "inbox-vault/mail-attachment"
    assert doc_row[2] == "Invoice attachment summary with due date."
    doc_provenance = json.loads(doc_row[3] or "{}")
    assert doc_provenance["origin_kind"] == "mail_attachment"
    assert doc_provenance["filename"] == "invoice-note.txt"

    assert photo_row is not None
    assert str(photo_row[0]).startswith("mail-attachment://photo/")
    assert photo_row[1] == "inbox-vault/mail-attachment"
    assert photo_row[2] == "Expense receipt attachment"
    assert photo_row[3] == "Total $42.00"
    photo_provenance = json.loads(photo_row[4] or "{}")
    assert photo_provenance["origin_kind"] == "mail_attachment"
    assert photo_provenance["filename"] == "receipt.png"

    assert [tuple(row) for row in bridge_rows] == [
        ("doc", "docs_registry", "indexed"),
        ("photo", "photos_registry", "indexed"),
    ]

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="all",
        mail_bridge_enabled=True,
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    rc = query_index(
        registry_db,
        vector_db,
        "invoice attachment",
        top_k=5,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="docs",
        mail_bridge_enabled=True,
        clearance="redacted",
        search_level="auto",
        as_json=True,
    )
    assert rc == 0
    docs_payload = json.loads(capsys.readouterr().out)
    assert any(
        result["source_kind"] == "docs" and result["metadata"].get("origin_kind") == "mail_attachment"
        for result in docs_payload["results"]
    )

    rc = query_index(
        registry_db,
        vector_db,
        "expense receipt",
        top_k=5,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="photos",
        mail_bridge_enabled=True,
        clearance="redacted",
        search_level="auto",
        as_json=True,
    )
    assert rc == 0
    photos_payload = json.loads(capsys.readouterr().out)
    assert any(
        result["source_kind"] == "photos" and result["metadata"].get("origin_kind") == "mail_attachment"
        for result in photos_payload["results"]
    )


def test_mail_bridge_leaves_unmaterialized_attachment_as_bridge_metadata_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.executescript(
            """
            CREATE TABLE message_attachments (
              msg_id TEXT NOT NULL,
              account_email TEXT NOT NULL,
              attachment_key TEXT NOT NULL DEFAULT '',
              part_id TEXT NOT NULL,
              gmail_attachment_id TEXT NOT NULL DEFAULT '',
              mime_type TEXT NOT NULL DEFAULT '',
              filename TEXT NOT NULL DEFAULT '',
              size_bytes INTEGER,
              content_disposition TEXT NOT NULL DEFAULT '',
              content_id TEXT NOT NULL DEFAULT '',
              is_inline INTEGER NOT NULL DEFAULT 0,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              storage_kind TEXT NOT NULL DEFAULT '',
              storage_path TEXT NOT NULL DEFAULT '',
              content_sha256 TEXT NOT NULL DEFAULT '',
              content_size_bytes INTEGER NOT NULL DEFAULT 0,
              materialized_at TEXT NOT NULL DEFAULT '',
              last_seen_at TEXT NOT NULL,
              PRIMARY KEY (msg_id, part_id)
            );

            CREATE TABLE message_attachment_inventory_state (
              msg_id TEXT PRIMARY KEY,
              account_email TEXT NOT NULL,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              attachment_count INTEGER NOT NULL DEFAULT 0,
              inventoried_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO message_attachments (
              msg_id, account_email, attachment_key, part_id, gmail_attachment_id, mime_type,
              filename, size_bytes, content_disposition, content_id, is_inline, inventory_state,
              storage_kind, storage_path, content_sha256, content_size_bytes, materialized_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "att-pdf-remote",
                "2",
                "gmail-att-2",
                "application/pdf",
                "invoice.pdf",
                2048,
                "attachment",
                "",
                0,
                "metadata_only",
                "",
                "",
                "",
                0,
                "",
                "2026-03-21T10:35:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO message_attachment_inventory_state (
              msg_id, account_email, inventory_state, attachment_count, inventoried_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "metadata_only",
                1,
                "2026-03-21T10:35:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(
                enabled=False,
                parse_url="",
                timeout_seconds=60,
                profile="auto",
            ),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        reg.commit()

        docs_rows = reg.execute(
            "SELECT COUNT(*) FROM docs_registry WHERE source = 'inbox-vault/mail-attachment'"
        ).fetchone()[0]
        bridge_row = reg.execute(
            """
            SELECT attachment_key, ingest_status, ingest_error
            FROM mail_attachment_bridge
            WHERE attachment_key = 'att-pdf-remote'
            """
        ).fetchone()
    finally:
        reg.close()

    assert updated == 3
    assert pruned == 0
    assert accounts_processed == 1
    assert docs_rows == 0
    assert bridge_row is not None
    assert bridge_row[1] in {"unmaterialized", "missing-raw-message"}


def test_mail_bridge_skips_tiny_generic_image_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    tiny_png = base64.urlsafe_b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z0YQAAAAASUVORK5CYII="
    )
    tiny_rel = Path("attachment-cache") / "ee" / "att-generic-logo.png"
    tiny_path = inbox_db.parent / tiny_rel
    tiny_path.parent.mkdir(parents=True, exist_ok=True)
    tiny_path.write_bytes(tiny_png)

    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.executescript(
            """
            CREATE TABLE message_attachments (
              msg_id TEXT NOT NULL,
              account_email TEXT NOT NULL,
              attachment_key TEXT NOT NULL DEFAULT '',
              part_id TEXT NOT NULL,
              gmail_attachment_id TEXT NOT NULL DEFAULT '',
              mime_type TEXT NOT NULL DEFAULT '',
              filename TEXT NOT NULL DEFAULT '',
              size_bytes INTEGER,
              content_disposition TEXT NOT NULL DEFAULT '',
              content_id TEXT NOT NULL DEFAULT '',
              is_inline INTEGER NOT NULL DEFAULT 0,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              storage_kind TEXT NOT NULL DEFAULT '',
              storage_path TEXT NOT NULL DEFAULT '',
              content_sha256 TEXT NOT NULL DEFAULT '',
              content_size_bytes INTEGER NOT NULL DEFAULT 0,
              materialized_at TEXT NOT NULL DEFAULT '',
              last_seen_at TEXT NOT NULL,
              PRIMARY KEY (msg_id, part_id)
            );

            CREATE TABLE message_attachment_inventory_state (
              msg_id TEXT PRIMARY KEY,
              account_email TEXT NOT NULL,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              attachment_count INTEGER NOT NULL DEFAULT 0,
              inventoried_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO message_attachments (
              msg_id, account_email, attachment_key, part_id, gmail_attachment_id, mime_type,
              filename, size_bytes, content_disposition, content_id, is_inline, inventory_state,
              storage_kind, storage_path, content_sha256, content_size_bytes, materialized_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "att-generic-logo",
                "9",
                "gmail-att-logo",
                "image/png",
                "image.png",
                len(tiny_png),
                "attachment",
                "",
                0,
                "materialized",
                "file",
                str(tiny_rel),
                hashlib.sha256(tiny_png).hexdigest(),
                len(tiny_png),
                "2026-03-21T10:50:00+00:00",
                "2026-03-21T10:50:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO message_attachment_inventory_state (
              msg_id, account_email, inventory_state, attachment_count, inventoried_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "materialized",
                1,
                "2026-03-21T10:50:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(enabled=False, parse_url="", timeout_seconds=60, profile="auto"),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        reg.commit()

        photo_count = reg.execute(
            "SELECT COUNT(*) FROM photos_registry WHERE source = 'inbox-vault/mail-attachment'"
        ).fetchone()[0]
        bridge_row = reg.execute(
            """
            SELECT target_kind, registry_table, ingest_status, ingest_error
            FROM mail_attachment_bridge
            WHERE attachment_key = 'att-generic-logo'
            """
        ).fetchone()
    finally:
        reg.close()

    assert updated == 3
    assert pruned == 0
    assert accounts_processed == 1
    assert photo_count == 0
    assert tuple(bridge_row[:3]) == ("photo", "", "skipped-junk-image")
    assert "size_bytes=" in str(bridge_row[3] or "")


def test_mail_bridge_ingests_materialized_docx_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    docx_bytes = _build_minimal_docx_bytes(
        "Quarterly budget for Jane Doe\nDue date 2026-04-01"
    )
    docx_rel = Path("attachment-cache") / "cc" / "att-budget-docx.docx"
    docx_path = inbox_db.parent / docx_rel
    docx_path.parent.mkdir(parents=True, exist_ok=True)
    docx_path.write_bytes(docx_bytes)

    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.executescript(
            """
            CREATE TABLE message_attachments (
              msg_id TEXT NOT NULL,
              account_email TEXT NOT NULL,
              attachment_key TEXT NOT NULL DEFAULT '',
              part_id TEXT NOT NULL,
              gmail_attachment_id TEXT NOT NULL DEFAULT '',
              mime_type TEXT NOT NULL DEFAULT '',
              filename TEXT NOT NULL DEFAULT '',
              size_bytes INTEGER,
              content_disposition TEXT NOT NULL DEFAULT '',
              content_id TEXT NOT NULL DEFAULT '',
              is_inline INTEGER NOT NULL DEFAULT 0,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              storage_kind TEXT NOT NULL DEFAULT '',
              storage_path TEXT NOT NULL DEFAULT '',
              content_sha256 TEXT NOT NULL DEFAULT '',
              content_size_bytes INTEGER NOT NULL DEFAULT 0,
              materialized_at TEXT NOT NULL DEFAULT '',
              last_seen_at TEXT NOT NULL,
              PRIMARY KEY (msg_id, part_id)
            );

            CREATE TABLE message_attachment_inventory_state (
              msg_id TEXT PRIMARY KEY,
              account_email TEXT NOT NULL,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              attachment_count INTEGER NOT NULL DEFAULT 0,
              inventoried_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO message_attachments (
              msg_id, account_email, attachment_key, part_id, gmail_attachment_id, mime_type,
              filename, size_bytes, content_disposition, content_id, is_inline, inventory_state,
              storage_kind, storage_path, content_sha256, content_size_bytes, materialized_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "att-budget-docx",
                "7",
                "gmail-att-docx",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "budget.docx",
                len(docx_bytes),
                "attachment",
                "",
                0,
                "materialized",
                "file",
                str(docx_rel),
                hashlib.sha256(docx_bytes).hexdigest(),
                len(docx_bytes),
                "2026-03-21T10:45:00+00:00",
                "2026-03-21T10:45:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO message_attachment_inventory_state (
              msg_id, account_email, inventory_state, attachment_count, inventoried_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "materialized",
                1,
                "2026-03-21T10:35:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(enabled=False, parse_url="", timeout_seconds=60, profile="auto"),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        reg.commit()

        doc_row = reg.execute(
            """
            SELECT parser, text_content, summary_status, summary_text
            FROM docs_registry
            WHERE source = 'inbox-vault/mail-attachment'
            """
        ).fetchone()
        bridge_row = reg.execute(
            """
            SELECT ingest_status, ingest_error
            FROM mail_attachment_bridge
            WHERE attachment_key = 'att-budget-docx'
            """
        ).fetchone()
    finally:
        reg.close()

    assert updated == 3
    assert pruned == 0
    assert accounts_processed == 1
    assert doc_row is not None
    assert doc_row[0] == "docx-xml"
    assert "Quarterly budget for Jane Doe" in str(doc_row[1] or "")
    assert "Due date 2026-04-01" in str(doc_row[1] or "")
    assert doc_row[2] != "empty-source"
    assert str(doc_row[3] or "") == "Invoice attachment summary with due date."
    assert tuple(bridge_row) == ("indexed", "")


def test_mail_bridge_incremental_rerun_picks_up_later_materialized_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)

    docx_bytes = _build_minimal_docx_bytes("Policy memo for April review")
    docx_rel = Path("attachment-cache") / "dd" / "att-policy-docx.docx"
    docx_path = inbox_db.parent / docx_rel
    docx_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.executescript(
            """
            CREATE TABLE message_attachments (
              msg_id TEXT NOT NULL,
              account_email TEXT NOT NULL,
              attachment_key TEXT NOT NULL DEFAULT '',
              part_id TEXT NOT NULL,
              gmail_attachment_id TEXT NOT NULL DEFAULT '',
              mime_type TEXT NOT NULL DEFAULT '',
              filename TEXT NOT NULL DEFAULT '',
              size_bytes INTEGER,
              content_disposition TEXT NOT NULL DEFAULT '',
              content_id TEXT NOT NULL DEFAULT '',
              is_inline INTEGER NOT NULL DEFAULT 0,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              storage_kind TEXT NOT NULL DEFAULT '',
              storage_path TEXT NOT NULL DEFAULT '',
              content_sha256 TEXT NOT NULL DEFAULT '',
              content_size_bytes INTEGER NOT NULL DEFAULT 0,
              materialized_at TEXT NOT NULL DEFAULT '',
              last_seen_at TEXT NOT NULL,
              PRIMARY KEY (msg_id, part_id)
            );

            CREATE TABLE message_attachment_inventory_state (
              msg_id TEXT PRIMARY KEY,
              account_email TEXT NOT NULL,
              inventory_state TEXT NOT NULL DEFAULT 'metadata_only',
              attachment_count INTEGER NOT NULL DEFAULT 0,
              inventoried_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO message_attachments (
              msg_id, account_email, attachment_key, part_id, gmail_attachment_id, mime_type,
              filename, size_bytes, content_disposition, content_id, is_inline, inventory_state,
              storage_kind, storage_path, content_sha256, content_size_bytes, materialized_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "att-policy-docx",
                "8",
                "gmail-att-policy",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "policy.docx",
                len(docx_bytes),
                "attachment",
                "",
                0,
                "metadata_only",
                "",
                "",
                "",
                0,
                "",
                "2026-03-21T10:35:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO message_attachment_inventory_state (
              msg_id, account_email, inventory_state, attachment_count, inventoried_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "msg-a-2",
                "acct-a@example.com",
                "metadata_only",
                1,
                "2026-03-21T10:35:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        first = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(enabled=False, parse_url="", timeout_seconds=60, profile="auto"),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        reg.commit()
    finally:
        reg.close()

    assert first == (3, 0, 1)

    docx_path.write_bytes(docx_bytes)
    conn = sqlite3.connect(str(inbox_db))
    try:
        conn.execute(
            """
            UPDATE message_attachments
            SET inventory_state = ?, storage_kind = ?, storage_path = ?, content_sha256 = ?,
                content_size_bytes = ?, materialized_at = ?, last_seen_at = ?
            WHERE attachment_key = ?
            """,
            (
                "materialized",
                "file",
                str(docx_rel),
                hashlib.sha256(docx_bytes).hexdigest(),
                len(docx_bytes),
                "2026-03-21T11:05:00+00:00",
                "2026-03-21T11:05:00+00:00",
                "att-policy-docx",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        second = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(enabled=False, parse_url="", timeout_seconds=60, profile="auto"),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        reg.commit()

        doc_row = reg.execute(
            """
            SELECT parser, text_content
            FROM docs_registry
            WHERE filepath LIKE 'mail-attachment://doc/%'
            """
        ).fetchone()
        bridge_row = reg.execute(
            """
            SELECT ingest_status, ingest_error
            FROM mail_attachment_bridge
            WHERE attachment_key = 'att-policy-docx'
            """
        ).fetchone()
    finally:
        reg.close()

    assert second == (1, 0, 1)
    assert doc_row is not None
    assert doc_row[0] == "docx-xml"
    assert "Policy memo for April review" in str(doc_row[1] or "")
    assert tuple(bridge_row) == ("indexed", "")


def test_cleanup_junk_mail_attachments_apply_removes_indexed_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    attachment_path = tmp_path / "attachment-cache" / "ff" / "logo.png"
    attachment_path.parent.mkdir(parents=True, exist_ok=True)
    attachment_bytes = base64.urlsafe_b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z0YQAAAAASUVORK5CYII="
    )
    attachment_path.write_bytes(attachment_bytes)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    vec = connect_vault_db(vector_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.execute(
            """
            INSERT INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, indexed_at, updated_at, notes,
              category_primary, category_secondary, taxonomy, caption, analyzer_model, analyzer_status,
              analyzer_error, analyzer_raw, analyzed_at, ocr_text, ocr_status, ocr_source, ocr_updated_at,
              dates_json, primary_date, provenance_json
            ) VALUES (?, ?, ?, '', 68, 0, ?, ?, '', '', '', 'misc', '', '', '', '', '', ?, '', '', '', ?, '[]', '', '')
            """,
            (
                "mail-attachment://photo/att-logo",
                "sha-logo",
                "inbox-vault/mail-attachment",
                "2026-03-21T12:00:00+00:00",
                "2026-03-21T12:00:00+00:00",
                "2026-03-21T12:00:00+00:00",
                "2026-03-21T12:00:00+00:00",
            ),
        )
        reg.execute(
            """
            INSERT INTO mail_attachment_bridge (
              attachment_ref, attachment_key, source, msg_id, account_email, part_id, gmail_attachment_id,
              mime_type, filename, size_bytes, content_disposition, content_id, is_inline, inventory_state,
              inventoried_at, storage_kind, storage_path, content_sha256, content_size_bytes, materialized_at,
              target_kind, registry_table, registry_filepath, materialized_input_path, ingest_status, ingest_error,
              indexed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "att-logo",
                "att-logo",
                "inbox-vault/mail-attachment",
                "msg-a-2",
                "acct-a@example.com",
                "11",
                "gmail-att-logo",
                "image/png",
                "logo.png",
                len(attachment_bytes),
                "attachment",
                "",
                0,
                "materialized",
                "2026-03-21T12:00:00+00:00",
                "file",
                "attachment-cache/ff/logo.png",
                hashlib.sha256(attachment_bytes).hexdigest(),
                len(attachment_bytes),
                "2026-03-21T12:00:00+00:00",
                "photo",
                "photos_registry",
                "mail-attachment://photo/att-logo",
                str(attachment_path),
                "indexed",
                "",
                "2026-03-21T12:00:00+00:00",
                "2026-03-21T12:00:00+00:00",
            ),
        )
        vec.execute(
            """
            CREATE TABLE vector_items_v2 (
              item_id INTEGER PRIMARY KEY AUTOINCREMENT,
              index_level TEXT,
              source_table TEXT,
              source_filepath TEXT
            )
            """
        )
        vec.execute(
            """
            CREATE TABLE source_state_v2 (
              source_table TEXT,
              source_filepath TEXT,
              index_level TEXT
            )
            """
        )
        vec.execute(
            "INSERT INTO vector_items_v2 (index_level, source_table, source_filepath) VALUES ('redacted', 'photos_registry', ?)",
            ("mail-attachment://photo/att-logo",),
        )
        vec.execute(
            "INSERT INTO source_state_v2 (source_table, source_filepath, index_level) VALUES ('photos_registry', ?, 'redacted')",
            ("mail-attachment://photo/att-logo",),
        )
        reg.commit()
        vec.commit()
    finally:
        reg.close()
        vec.close()

    monkeypatch.setattr(
        "sys.argv",
        [
            "cleanup_junk_mail_attachments.py",
            "--registry-db",
            str(registry_db),
            "--vector-db",
            str(vector_db),
            "--apply",
        ],
    )
    assert cleanup_junk_mail_attachments.main() == 0

    reg = connect_vault_db(registry_db)
    vec = connect_vault_db(vector_db)
    try:
        photo_count = reg.execute(
            "SELECT COUNT(*) FROM photos_registry WHERE filepath = 'mail-attachment://photo/att-logo'"
        ).fetchone()[0]
        bridge_row = reg.execute(
            """
            SELECT target_kind, registry_table, registry_filepath, ingest_status
            FROM mail_attachment_bridge
            WHERE attachment_ref = 'att-logo'
            """
        ).fetchone()
        vector_count = vec.execute(
            "SELECT COUNT(*) FROM vector_items_v2 WHERE source_filepath = 'mail-attachment://photo/att-logo'"
        ).fetchone()[0]
        source_state_count = vec.execute(
            "SELECT COUNT(*) FROM source_state_v2 WHERE source_filepath = 'mail-attachment://photo/att-logo'"
        ).fetchone()[0]
    finally:
        reg.close()
        vec.close()

    assert photo_count == 0
    assert tuple(bridge_row) == ("photo", "", "", "skipped-junk-image")
    assert vector_count == 0
    assert source_state_count == 0


def test_mail_photo_backfill_reanalyzes_virtual_mail_attachment_photo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)
    _seed_inbox_attachment_inventory(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        updated, pruned, accounts_processed = sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(
                enabled=False,
                parse_url="",
                timeout_seconds=60,
                profile="auto",
            ),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        reg.commit()

        assert updated == 4
        assert pruned == 0
        assert accounts_processed == 1

        photo_row = reg.execute(
            """
            SELECT filepath
            FROM photos_registry
            WHERE source = 'inbox-vault/mail-attachment'
            """
        ).fetchone()
        assert photo_row is not None
        registry_filepath = str(photo_row[0])
        assert registry_filepath.startswith("mail-attachment://photo/")

        bridge_row = reg.execute(
            """
            SELECT materialized_input_path
            FROM mail_attachment_bridge
            WHERE registry_filepath = ?
            """,
            (registry_filepath,),
        ).fetchone()
        assert bridge_row is not None
        assert Path(str(bridge_row[0] or "")).is_file()

        reg.execute(
            """
            UPDATE photos_registry
            SET caption = '',
                analyzer_status = 'error',
                analyzer_error = 'timed out',
                analyzer_raw = '',
                analyzer_model = '',
                category_primary = '',
                category_secondary = '',
                taxonomy = '',
                ocr_text = '',
                ocr_status = '',
                ocr_source = ''
            WHERE filepath = ?
            """,
            (registry_filepath,),
        )
        reg.commit()

        assert count_pending_photo_backfill(reg, -1, source_selection="mail") == 1
        assert count_pending_photo_backfill(reg, -1, source_selection="photos") == 0

        backfill_updated, backfill_failed = backfill_missing_photo_analysis(
            reg,
            photo_client=_FakePhotoClient(),
            limit=-1,
            deadline=float("inf"),
            verbose=False,
            source_selection="mail",
        )
        reg.commit()

        repaired_row = reg.execute(
            """
            SELECT filepath, caption, analyzer_status, analyzer_error, analyzer_model, category_primary, taxonomy
            FROM photos_registry
            WHERE filepath = ?
            """,
            (registry_filepath,),
        ).fetchone()
    finally:
        reg.close()

    assert backfill_updated == 1
    assert backfill_failed == 0
    assert repaired_row is not None
    assert repaired_row[0] == registry_filepath
    assert repaired_row[1] == "Expense receipt attachment"
    assert repaired_row[2] == "ok"
    assert repaired_row[3] == ""
    assert repaired_row[4] == "test-photo-model"
    assert repaired_row[5] == "receipt"
    assert repaired_row[6] == "docs"


def test_run_with_mail_source_executes_mail_photo_backfill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_plaintext_inbox(monkeypatch)
    registry_db = tmp_path / "state" / "vault_registry.db"
    inbox_db = tmp_path / "inbox.db"
    _seed_inbox_db(inbox_db)
    _seed_inbox_attachment_inventory(inbox_db)

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        sync_mail_bridge(
            reg,
            mail_cfg=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
            full_scan=False,
            dry_run=False,
            deadline=float("inf"),
            text_cap=40000,
            pdf_cfg=PdfParseConfig(enabled=False, parse_url="", timeout_seconds=60, profile="auto"),
            summary_cfg=SummaryConfig(
                enabled=True,
                base_url="http://127.0.0.1:8080/v1",
                model="test-summary-model",
                api_key="",
                timeout_seconds=30,
                max_input_chars=12000,
                max_output_chars=650,
            ),
            chat_client=_FakeChatClient(),
            photo_client=_FakePhotoClient(),
            verbose=False,
        )
        photo_row = reg.execute(
            """
            SELECT filepath
            FROM photos_registry
            WHERE source = 'inbox-vault/mail-attachment'
            """
        ).fetchone()
        assert photo_row is not None
        registry_filepath = str(photo_row[0])
        reg.execute(
            """
            UPDATE photos_registry
            SET caption = '',
                analyzer_status = 'error',
                analyzer_error = 'timed out',
                analyzer_raw = '',
                analyzer_model = '',
                category_primary = '',
                category_secondary = '',
                taxonomy = '',
                ocr_text = '',
                ocr_status = '',
                ocr_source = ''
            WHERE filepath = ?
            """,
            (registry_filepath,),
        )
        reg.commit()
    finally:
        reg.close()

    monkeypatch.setattr(vault_registry_sync, "LocalPhotoAnalyzerClient", lambda _cfg: _FakePhotoClient())

    cfg = Config(
        db_path=registry_db,
        docs_roots=[],
        photos_roots=[],
        inbox_scanner=tmp_path / "scanner_in",
        docs_dest_root=tmp_path / "docs_dest",
        photos_dest_root=tmp_path / "photos_dest",
        text_cap=40000,
        max_seconds=0.0,
        max_items=0,
        skip_inbox=False,
        verbose=False,
        summary=SummaryConfig(
            enabled=False,
            base_url="http://127.0.0.1:8080/v1",
            model="test-summary-model",
            api_key="",
            timeout_seconds=30,
            max_input_chars=12000,
            max_output_chars=650,
        ),
        photo_analysis=PhotoAnalysisConfig(
            enabled=True,
            analyze_url="http://127.0.0.1:8081/analyze",
            timeout_seconds=30,
            force=False,
        ),
        pdf_parse=PdfParseConfig(enabled=False, parse_url="", timeout_seconds=60, profile="auto"),
        summary_reprocess_missing_limit=0,
        photo_reprocess_missing_limit=-1,
        source_selection="mail",
        mail_bridge=_mail_cfg(inbox_db, include_accounts=("acct-a@example.com",)),
    )

    run(cfg, dry_run=False)

    reg = connect_vault_db(registry_db)
    try:
        repaired_row = reg.execute(
            """
            SELECT caption, analyzer_status, analyzer_error, category_primary, taxonomy
            FROM photos_registry
            WHERE filepath = ?
            """,
            (registry_filepath,),
        ).fetchone()
    finally:
        reg.close()

    assert repaired_row is not None
    assert repaired_row[0] == "Expense receipt attachment"
    assert repaired_row[1] == "ok"
    assert repaired_row[2] == ""
    assert repaired_row[3] == "receipt"
    assert repaired_row[4] == "docs"


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
