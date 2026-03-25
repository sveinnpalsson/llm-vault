#!/usr/bin/env python3
"""Database helpers for llm-vault.

This module centralizes SQLCipher connection policy, password handling, and
plaintext -> encrypted migration helpers.
"""

from __future__ import annotations

import os
import shutil
import sqlite3 as sqlite_plain
import time
from pathlib import Path
from typing import Any, Callable

DB_PASSWORD_ENV = "LLM_VAULT_DB_PASSWORD"
ALLOW_PLAINTEXT_TEST_ENV = "LLM_VAULT_ALLOW_PLAINTEXT_FOR_TESTS"

try:  # pragma: no cover - exercised in integration/runtime
    from sqlcipher3 import dbapi2 as sqlcipher

    SQLCIPHER_AVAILABLE = True
except Exception:  # pragma: no cover - exercised when sqlcipher is missing
    sqlcipher = None
    SQLCIPHER_AVAILABLE = False


class VaultDBError(RuntimeError):
    """Base DB error for vault-ops helpers."""


class VaultDBEncryptionRequired(VaultDBError):
    """Raised when SQLCipher is required but unavailable."""


class VaultDBKeyError(VaultDBError):
    """Raised when encrypted DB key validation fails."""


def _sqlcipher_quote(value: str) -> str:
    return str(value or "").replace("'", "''")


def resolve_db_password(env_var: str = DB_PASSWORD_ENV) -> str:
    value = os.getenv(env_var)
    if value is None or not str(value).strip():
        raise VaultDBEncryptionRequired(
            f"Missing required encrypted DB password env var: {env_var}"
        )
    return str(value)


def _is_plaintext_test_mode() -> bool:
    return str(os.getenv(ALLOW_PLAINTEXT_TEST_ENV, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def connect_vault_db(
    db_path: str | Path,
    *,
    password: str | None = None,
    timeout: float = 30.0,
    ensure_parent: bool = False,
):
    """Open a vault DB using SQLCipher and enforce encrypted-at-rest policy."""

    path = Path(db_path)
    if ensure_parent:
        path.parent.mkdir(parents=True, exist_ok=True)

    if not SQLCIPHER_AVAILABLE:
        if _is_plaintext_test_mode():
            conn = sqlite_plain.connect(str(path), timeout=timeout)
            conn.row_factory = sqlite_plain.Row
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            return conn
        raise VaultDBEncryptionRequired(
            "sqlcipher3-binary is required for llm-vault encrypted storage. "
            "Install sqlcipher3-binary and libsqlcipher, then retry."
        )

    pwd = password if password is not None else resolve_db_password()
    conn = sqlcipher.connect(str(path), timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlcipher.Row
    try:
        escaped = _sqlcipher_quote(pwd)
        conn.execute(f"PRAGMA key='{escaped}';")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        conn.execute("SELECT count(*) FROM sqlite_master;").fetchone()
    except Exception as exc:
        conn.close()
        raise VaultDBKeyError(
            f"Unable to open encrypted database at {path}. "
            "Check LLM_VAULT_DB_PASSWORD and DB path."
        ) from exc
    return conn


def _user_tables(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _row_counts(conn) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in _user_tables(conn):
        counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return counts


def _row_counts_plaintext(db_path: Path) -> dict[str, int]:
    conn = sqlite_plain.connect(str(db_path))
    try:
        return _row_counts(conn)
    finally:
        conn.close()


def migrate_plaintext_to_encrypted(
    db_path: str | Path,
    *,
    password: str | None = None,
    backup_suffix: str = ".plaintext.bak",
    progress: Callable[[str, str, str], None] | None = None,
) -> dict[str, Any]:
    """Migrate one plaintext sqlite DB file to SQLCipher safely.

    The original file is preserved as a backup and replaced only after
    row-count verification succeeds.
    """

    if not SQLCIPHER_AVAILABLE:
        raise VaultDBEncryptionRequired(
            "sqlcipher3-binary is required for encryption migration."
        )

    path = Path(db_path)
    if not path.exists():
        return {"db_path": str(path), "status": "skipped", "reason": "missing"}

    pwd = password if password is not None else resolve_db_password()
    if progress is not None:
        progress(str(path), "prepare", "count-plaintext")
    before_counts = _row_counts_plaintext(path)

    ts = int(time.time())
    backup = path.with_name(f"{path.name}.{ts}{backup_suffix}")
    temp_encrypted = path.with_name(f"{path.name}.enc.tmp")
    if temp_encrypted.exists():
        temp_encrypted.unlink()

    src_conn = sqlcipher.connect(str(path), timeout=30.0, check_same_thread=False)
    try:
        src_conn.execute("SELECT count(*) FROM sqlite_master;").fetchone()
        quoted_temp = str(temp_encrypted).replace("'", "''")
        escaped_pwd = _sqlcipher_quote(pwd)
        if progress is not None:
            progress(str(path), "export", "sqlcipher-export")
        src_conn.execute(
            f"ATTACH DATABASE '{quoted_temp}' AS encrypted KEY '{escaped_pwd}';"
        )
        src_conn.execute("SELECT sqlcipher_export('encrypted');")
        src_conn.execute("DETACH DATABASE encrypted;")
    finally:
        src_conn.close()

    if progress is not None:
        progress(str(path), "verify", "count-encrypted")
    enc_conn = connect_vault_db(temp_encrypted, password=pwd, timeout=30.0)
    try:
        after_counts = _row_counts(enc_conn)
    finally:
        enc_conn.close()

    mismatches: list[dict[str, Any]] = []
    for table_name, before_count in before_counts.items():
        after_count = after_counts.get(table_name)
        if after_count is None or int(after_count) != int(before_count):
            mismatches.append(
                {
                    "table": table_name,
                    "before": int(before_count),
                    "after": None if after_count is None else int(after_count),
                }
            )
    if mismatches:
        raise VaultDBError(
            "Migration verification failed (row-count mismatch): "
            f"{mismatches[:6]}"
        )

    if progress is not None:
        progress(str(path), "backup", "copy-plaintext-backup")
    shutil.copy2(path, backup)
    if progress is not None:
        progress(str(path), "replace", "swap-encrypted-db")
    path.unlink()
    temp_encrypted.replace(path)

    return {
        "db_path": str(path),
        "status": "ok",
        "backup_path": str(backup),
        "tables_verified": len(before_counts),
        "rows_verified_total": int(sum(before_counts.values())),
    }
