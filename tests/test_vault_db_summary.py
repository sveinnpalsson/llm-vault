from __future__ import annotations

import array
import hashlib
import json
import math
import sys
from pathlib import Path

import vault_db_summary
from vault_db import connect_vault_db
from vault_redaction import RedactionConfig
from vault_registry_sync import ensure_db
from vault_vector_index import update_index


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


def _seed_registry(registry_db: Path) -> None:
    conn = connect_vault_db(registry_db, ensure_parent=True)
    try:
        conn.execute(
            """
            CREATE TABLE docs_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              text_content TEXT,
              parser TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              summary_text TEXT,
              summary_model TEXT,
              summary_hash TEXT,
              summary_status TEXT,
              summary_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE photos_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              date_taken TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              notes TEXT,
              category_primary TEXT,
              category_secondary TEXT,
              taxonomy TEXT,
              caption TEXT,
              analyzer_status TEXT,
              ocr_text TEXT,
              ocr_status TEXT,
              ocr_source TEXT,
              ocr_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/docs/invoice.txt",
                "doc-summary-1",
                "generated",
                "Invoice for passport renewal with contact jane.doe@example.com.",
                "plain",
                100,
                1735689600.0,
                "2026-03-20T00:00:00+00:00",
                "Passport renewal invoice summary.",
                "local-test",
                "hash-doc",
                "ok",
                "2026-03-20T00:00:00+00:00",
                "[]",
                "2026-03-20T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, updated_at,
              notes, category_primary, category_secondary, taxonomy, caption, analyzer_status,
              ocr_text, ocr_status, ocr_source, ocr_updated_at, dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/photos/id-card.jpg",
                "photo-summary-1",
                "generated",
                "2025-01-04T10:30:00+00:00",
                200,
                1735689601.0,
                "2026-03-20T00:05:00+00:00",
                "identity card notes",
                "document",
                "",
                "docs",
                "Icelandic identification card",
                "ok",
                "Renfei identification reference",
                "ok",
                "analyzer:text_raw",
                "2026-03-20T00:05:00+00:00",
                "[]",
                "2025-01-04T10:30:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_registry_with_empty_source(registry_db: Path) -> None:
    conn = connect_vault_db(registry_db, ensure_parent=True)
    try:
        conn.execute(
            """
            CREATE TABLE docs_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              text_content TEXT,
              parser TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              summary_text TEXT,
              summary_model TEXT,
              summary_hash TEXT,
              summary_status TEXT,
              summary_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE photos_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              date_taken TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              notes TEXT,
              category_primary TEXT,
              category_secondary TEXT,
              taxonomy TEXT,
              caption TEXT,
              analyzer_status TEXT,
              ocr_text TEXT,
              ocr_status TEXT,
              ocr_source TEXT,
              ocr_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )

        docs_rows = [
            (
                "/vault/docs/a.txt",
                "doc-ok-1",
                "generated",
                "Alpha document body.",
                "plain",
                100,
                1735689600.0,
                "2026-03-20T00:00:00+00:00",
                "Alpha summary.",
                "local-test",
                "hash-doc-a",
                "ok",
                "2026-03-20T00:00:00+00:00",
                "[]",
                "2026-03-20T00:00:00+00:00",
            ),
            (
                "/vault/docs/b.txt",
                "doc-ok-2",
                "generated",
                "Beta document body.",
                "plain",
                101,
                1735689601.0,
                "2026-03-20T00:01:00+00:00",
                "Beta summary.",
                "local-test",
                "hash-doc-b",
                "ok",
                "2026-03-20T00:01:00+00:00",
                "[]",
                "2026-03-20T00:01:00+00:00",
            ),
            (
                "/vault/docs/c.txt",
                "doc-ok-3",
                "generated",
                "Gamma document body.",
                "plain",
                102,
                1735689602.0,
                "2026-03-20T00:02:00+00:00",
                "Gamma summary.",
                "local-test",
                "hash-doc-c",
                "ok",
                "2026-03-20T00:02:00+00:00",
                "[]",
                "2026-03-20T00:02:00+00:00",
            ),
            (
                "/vault/docs/empty.txt",
                "doc-empty-1",
                "generated",
                "",
                "plain",
                0,
                1735689603.0,
                "2026-03-20T00:03:00+00:00",
                "",
                "local-test",
                "hash-doc-empty",
                "empty-source",
                "2026-03-20T00:03:00+00:00",
                "[]",
                "2026-03-20T00:03:00+00:00",
            ),
        ]
        conn.executemany(
            """
            INSERT INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            docs_rows,
        )
        conn.execute(
            """
            INSERT INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, updated_at,
              notes, category_primary, category_secondary, taxonomy, caption, analyzer_status,
              ocr_text, ocr_status, ocr_source, ocr_updated_at, dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/photos/id-card.jpg",
                "photo-summary-1",
                "generated",
                "2025-01-04T10:30:00+00:00",
                200,
                1735689604.0,
                "2026-03-20T00:05:00+00:00",
                "identity card notes",
                "document",
                "",
                "docs",
                "Icelandic identification card",
                "ok",
                "Renfei identification reference",
                "ok",
                "analyzer:text_raw",
                "2026-03-20T00:05:00+00:00",
                "[]",
                "2025-01-04T10:30:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_status_json_uses_generic_source_keyed_structures(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_scanner = tmp_path / "scanner"
    inbox_scanner.mkdir()
    _seed_registry(registry_db)

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="all",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "vault_db_summary.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--inbox-scanner",
            str(inbox_scanner),
            "--json",
        ],
    )
    rc = vault_db_summary.main()
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    assert "sources" in payload["registry"]
    assert payload["registry"]["sources"]["docs"]["summary"]["present"] == 1
    assert payload["registry"]["sources"]["photos"]["analysis"]["analyzer_ok"] == 1
    assert payload["registry"]["sources"]["photos"]["ocr"]["status_counts"]["ok"] == 1
    assert "docs_files_total" not in payload["registry"]
    assert "levels" in payload["vectors"]
    assert payload["vectors"]["levels"]["redacted"]["sources"]["docs"]["sources_indexed"] == 1
    assert payload["vectors"]["levels"]["redacted"]["sources"]["photos"]["sources_indexed"] == 1


def test_status_json_ignores_empty_source_docs_for_vector_coverage_health(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_scanner = tmp_path / "scanner"
    inbox_scanner.mkdir()
    _seed_registry_with_empty_source(registry_db)

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="all",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "vault_db_summary.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--inbox-scanner",
            str(inbox_scanner),
            "--json",
        ],
    )
    rc = vault_db_summary.main()
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    docs_summary = payload["registry"]["sources"]["docs"]["summary"]
    assert docs_summary["status_counts"]["empty-source"] == 1
    assert docs_summary["vector_eligible"] == 3
    assert payload["vectors"]["levels"]["redacted"]["sources"]["docs"]["sources_indexed"] == 3
    assert payload["health"] == "ok"


def test_status_json_reports_mail_bridge_source_stats(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_scanner = tmp_path / "scanner"
    inbox_scanner.mkdir()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.execute(
            """
            INSERT INTO mail_registry (
              filepath, checksum, source, msg_id, account_email, thread_id,
              date_iso, from_addr, to_addr, subject, snippet, body_text,
              labels_json, summary_text, primary_date, dates_json, indexed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "mail://message/msg-summary-1",
                "mail-summary-1",
                "inbox-vault",
                "msg-summary-1",
                "acct@example.com",
                "thread-1",
                "2026-03-24T10:00:00+00:00",
                "boss@example.com",
                "acct@example.com",
                "Budget approval needed",
                "Please review by Friday",
                "Full body text for the approval request.",
                json.dumps(["INBOX", "IMPORTANT"]),
                "Budget approval summary for finance.",
                "2026-03-24T10:00:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2026-03-24T10:00:00+00:00",
                            "kind": "message_date",
                            "source": "date_iso",
                        }
                    ]
                ),
                "2026-03-24T11:00:00+00:00",
                "2026-03-24T11:00:00+00:00",
            ),
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

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "vault_db_summary.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--inbox-scanner",
            str(inbox_scanner),
            "--json",
            "--mail-bridge-enabled",
            "--mail-bridge-db-path",
            "/tmp/inbox.db",
            "--mail-bridge-include-account",
            "acct@example.com",
            "--mail-max-body-chunks",
            "7",
        ],
    )
    rc = vault_db_summary.main()
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["registry"]["sources"]["mail"]["bridge_enabled"] is True
    assert payload["registry"]["sources"]["mail"]["bridge_db_path"] == "/tmp/inbox.db"
    assert payload["registry"]["sources"]["mail"]["include_accounts"] == ["acct@example.com"]
    assert payload["registry"]["sources"]["mail"]["max_body_chunks"] == 7
    assert payload["registry"]["sources"]["mail"]["messages_total"] == 1
    assert payload["registry"]["sources"]["mail"]["with_summary"] == 1
    assert payload["vectors"]["levels"]["redacted"]["sources"]["mail"]["sources_indexed"] == 1


def test_status_json_emits_high_signal_config_warnings(tmp_path: Path, monkeypatch, capsys) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_scanner = tmp_path / "scanner"
    inbox_scanner.mkdir()
    docs_root = tmp_path / "missing-docs"

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.commit()
    finally:
        reg.close()

    monkeypatch.setenv("LLM_VAULT_DB_PASSWORD", "test-password")
    monkeypatch.delenv("VAULT_PHOTO_ANALYSIS_URL", raising=False)
    monkeypatch.delenv("VAULT_PDF_PARSE_URL", raising=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "vault_db_summary.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--inbox-scanner",
            str(inbox_scanner),
            "--docs-root",
            str(docs_root),
            "--summary-base-url",
            "http://127.0.0.1:1/v1",
            "--photo-analysis-url",
            "http://example.com/analyze",
            "--json",
        ],
    )
    rc = vault_db_summary.main()
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    categories = {warning["category"] for warning in payload["warnings"]}
    assert payload["warning_count"] == len(payload["warnings"])
    assert "content_root_missing" in categories
    assert "optional_service_disabled_unset" in categories
    assert "endpoint_unreachable" in categories
    assert "local_only_url_violation" in categories


def test_status_oneline_includes_warning_summary(tmp_path: Path, monkeypatch, capsys) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    inbox_scanner = tmp_path / "scanner"
    inbox_scanner.mkdir()

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.commit()
    finally:
        reg.close()

    monkeypatch.setenv("LLM_VAULT_DB_PASSWORD", "test-password")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "vault_db_summary.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--inbox-scanner",
            str(inbox_scanner),
            "--oneline",
        ],
    )
    rc = vault_db_summary.main()
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert "warnings=" in out
    assert "warning_categories=" in out


def test_build_warnings_includes_missing_db_password(monkeypatch) -> None:
    monkeypatch.delenv("LLM_VAULT_DB_PASSWORD", raising=False)
    warnings = vault_db_summary.build_warnings(
        docs_roots=[],
        photos_roots=[],
        summary_base_url="",
        embed_base_url="",
        redaction_base_url="",
        photo_analysis_url="",
        disable_photo_analysis=True,
        pdf_parse_url="",
        disable_pdf_service=True,
        mail_bridge_enabled=False,
        mail_bridge_db_path="",
        mail_bridge_password_env="INBOX_VAULT_DB_PASSWORD",
    )
    categories = {warning["category"] for warning in warnings}
    assert "missing_db_password" in categories
