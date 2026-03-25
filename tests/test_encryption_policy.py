from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import vault_db


def test_fail_closed_when_sqlcipher_missing_and_plaintext_override_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(vault_db, "SQLCIPHER_AVAILABLE", False)
    monkeypatch.delenv("LLM_VAULT_ALLOW_PLAINTEXT_FOR_TESTS", raising=False)
    with pytest.raises(vault_db.VaultDBEncryptionRequired):
        vault_db.connect_vault_db(tmp_path / "blocked.db", ensure_parent=True)


def test_plaintext_test_mode_allows_temp_db_when_sqlcipher_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(vault_db, "SQLCIPHER_AVAILABLE", False)
    monkeypatch.setenv("LLM_VAULT_ALLOW_PLAINTEXT_FOR_TESTS", "1")
    conn = vault_db.connect_vault_db(tmp_path / "allowed.db", ensure_parent=True)
    try:
        conn.execute("CREATE TABLE demo (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO demo (value) VALUES ('ok')")
        conn.commit()
        row = conn.execute("SELECT COUNT(*) FROM demo").fetchone()
        assert int(row[0]) == 1
    finally:
        conn.close()


def test_wrong_key_rejected_when_sqlcipher_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    if not vault_db.SQLCIPHER_AVAILABLE:
        pytest.skip("sqlcipher unavailable in local environment")

    db_path = tmp_path / "cipher.db"
    monkeypatch.setenv("LLM_VAULT_DB_PASSWORD", "correct-pass")
    conn = vault_db.connect_vault_db(db_path, ensure_parent=True)
    try:
        conn.execute("CREATE TABLE x (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO x (value) VALUES ('secret')")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(vault_db.VaultDBKeyError):
        vault_db.connect_vault_db(db_path, password="wrong-pass")


def test_plaintext_migration_preserves_row_counts_and_redaction_table(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    if not vault_db.SQLCIPHER_AVAILABLE:
        pytest.skip("sqlcipher unavailable in local environment")

    db_path = tmp_path / "vault_registry.db"
    plain = sqlite3.connect(str(db_path))
    try:
        plain.execute("CREATE TABLE docs_registry (filepath TEXT PRIMARY KEY, checksum TEXT)")
        plain.execute(
            """
            CREATE TABLE redaction_entries (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scope_type TEXT NOT NULL,
              scope_id TEXT NOT NULL,
              key_name TEXT NOT NULL,
              placeholder TEXT NOT NULL,
              value_norm TEXT NOT NULL,
              original_value TEXT NOT NULL,
              source_mode TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              hit_count INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        plain.execute(
            "INSERT INTO docs_registry (filepath, checksum) VALUES (?, ?)",
            ("/vault/docs/a.txt", "c1"),
        )
        plain.execute(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm,
              original_value, source_mode, first_seen_at, last_seen_at, hit_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "vault",
                "global",
                "EMAIL",
                "<REDACTED_EMAIL_A>",
                "amy@example.com",
                "amy@example.com",
                "regex",
                "2026-03-17T00:00:00+00:00",
                "2026-03-17T00:00:00+00:00",
                3,
            ),
        )
        plain.commit()
    finally:
        plain.close()

    monkeypatch.setenv("LLM_VAULT_DB_PASSWORD", "migration-pass")
    result = vault_db.migrate_plaintext_to_encrypted(db_path)
    assert result["status"] == "ok"
    assert Path(result["backup_path"]).exists()
    assert int(result["tables_verified"]) >= 2

    enc = vault_db.connect_vault_db(db_path, password="migration-pass")
    try:
        docs = enc.execute("SELECT COUNT(*) FROM docs_registry").fetchone()[0]
        redactions = enc.execute("SELECT COUNT(*) FROM redaction_entries").fetchone()[0]
        assert int(docs) == 1
        assert int(redactions) == 1
    finally:
        enc.close()
