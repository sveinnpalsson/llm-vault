#!/usr/bin/env python3
"""Summarize vault registry/vector DB status for quick ops reporting.

Usage:
  python3 vault_db_summary.py
  python3 vault_db_summary.py --oneline
  python3 vault_db_summary.py --json
  python3 vault_db_summary.py --registry-db /path/to/vault_registry.db --vectors-db /path/to/vault_vectors.db
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from vault_db import connect_vault_db
from vault_redaction import REDACTION_POLICY_VERSION, is_redaction_value_allowed
from vault_sources import REGISTERED_SOURCES, SourceHandler

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_DB = str(ROOT / "state" / "vault_registry.db")
DEFAULT_VECTORS_DB = str(ROOT / "state" / "vault_vectors.db")
DEFAULT_INBOX_SCANNER = str(ROOT / "state" / "scanner_inbox")
DEFAULT_MAIL_BRIDGE_PASSWORD_ENV = "INBOX_VAULT_DB_PASSWORD"


def to_iso_utc(epoch_value: Any) -> str | None:
    if epoch_value is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch_value), tz=timezone.utc).isoformat()
    except Exception:
        return None


def safe_scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not table_exists(conn, table):
        return False
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return False
    return any(str(row[1]) == column for row in rows)


def _is_local_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


def _endpoint_reachable(url: str, *, timeout_seconds: float = 1.0) -> tuple[bool, str]:
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").strip()
    if not host:
        return False, "missing host"
    try:
        port = parsed.port
    except ValueError:
        return False, "invalid port"
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    try:
        with socket.create_connection((host, int(port)), timeout=max(0.2, float(timeout_seconds))):
            return True, ""
    except OSError as exc:
        return False, str(exc)


def _warning(category: str, message: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "category": category,
        "severity": "warning",
        "message": message,
    }
    if details:
        payload["details"] = details
    return payload


def build_warnings(
    *,
    docs_roots: list[str],
    photos_roots: list[str],
    summary_base_url: str,
    embed_base_url: str,
    redaction_base_url: str,
    photo_analysis_url: str,
    disable_photo_analysis: bool,
    pdf_parse_url: str,
    disable_pdf_service: bool,
    mail_bridge_enabled: bool,
    mail_bridge_db_path: str,
    mail_bridge_password_env: str,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []

    if not str(os.getenv("LLM_VAULT_DB_PASSWORD", "")).strip():
        warnings.append(
            _warning(
                "missing_db_password",
                "LLM_VAULT_DB_PASSWORD is unset; encrypted DB operations will fail.",
                details={"env_var": "LLM_VAULT_DB_PASSWORD"},
            )
        )

    docs_roots_clean = [str(root).strip() for root in docs_roots if str(root).strip()]
    photos_roots_clean = [str(root).strip() for root in photos_roots if str(root).strip()]
    if not docs_roots_clean and not photos_roots_clean:
        warnings.append(
            _warning(
                "missing_content_roots",
                "No docs_roots or photos_roots configured; update/repair has no local content to ingest.",
            )
        )
    for kind, roots in (("docs", docs_roots_clean), ("photos", photos_roots_clean)):
        for root in roots:
            path = Path(root).expanduser()
            if not path.exists():
                warnings.append(
                    _warning(
                        "content_root_missing",
                        f"{kind} root does not exist: {root}",
                        details={"source_kind": kind, "path": root},
                    )
                )
                continue
            if not path.is_dir():
                warnings.append(
                    _warning(
                        "content_root_not_directory",
                        f"{kind} root is not a directory: {root}",
                        details={"source_kind": kind, "path": root},
                    )
                )

    service_urls = {
        "summary": str(summary_base_url or "").strip(),
        "embedding": str(embed_base_url or "").strip(),
        "redaction": str(redaction_base_url or "").strip(),
    }
    for service_name, endpoint in service_urls.items():
        if not endpoint:
            continue
        if not _is_local_url(endpoint):
            warnings.append(
                _warning(
                    "local_only_url_violation",
                    f"{service_name} URL must be local-only, got: {endpoint}",
                    details={"service": service_name, "url": endpoint},
                )
            )
            continue
        ok, reason = _endpoint_reachable(endpoint)
        if not ok:
            warnings.append(
                _warning(
                    "endpoint_unreachable",
                    f"{service_name} endpoint is configured but unreachable: {endpoint}",
                    details={"service": service_name, "url": endpoint, "reason": reason},
                )
            )

    photo_url = str(photo_analysis_url or "").strip() or str(os.getenv("VAULT_PHOTO_ANALYSIS_URL", "")).strip()
    if disable_photo_analysis:
        pass
    elif not photo_url:
        warnings.append(
            _warning(
                "optional_service_disabled_unset",
                "photo_analysis is disabled because URL is unset.",
                details={"service": "photo_analysis", "env_var": "VAULT_PHOTO_ANALYSIS_URL"},
            )
        )
    elif not _is_local_url(photo_url):
        warnings.append(
            _warning(
                "local_only_url_violation",
                f"photo_analysis URL must be local-only, got: {photo_url}",
                details={"service": "photo_analysis", "url": photo_url},
            )
        )
    else:
        ok, reason = _endpoint_reachable(photo_url)
        if not ok:
            warnings.append(
                _warning(
                    "endpoint_unreachable",
                    f"photo_analysis endpoint is configured but unreachable: {photo_url}",
                    details={"service": "photo_analysis", "url": photo_url, "reason": reason},
                )
            )

    pdf_url = str(pdf_parse_url or "").strip() or str(os.getenv("VAULT_PDF_PARSE_URL", "")).strip()
    if disable_pdf_service:
        pass
    elif not pdf_url:
        warnings.append(
            _warning(
                "optional_service_disabled_unset",
                "pdf_parse is disabled because parse_url is unset.",
                details={"service": "pdf_parse", "env_var": "VAULT_PDF_PARSE_URL"},
            )
        )
    elif not _is_local_url(pdf_url):
        warnings.append(
            _warning(
                "local_only_url_violation",
                f"pdf_parse URL must be local-only, got: {pdf_url}",
                details={"service": "pdf_parse", "url": pdf_url},
            )
        )
    else:
        ok, reason = _endpoint_reachable(pdf_url)
        if not ok:
            warnings.append(
                _warning(
                    "endpoint_unreachable",
                    f"pdf_parse endpoint is configured but unreachable: {pdf_url}",
                    details={"service": "pdf_parse", "url": pdf_url, "reason": reason},
                )
            )

    if mail_bridge_enabled:
        bridge_db_path = str(mail_bridge_db_path or "").strip()
        if not bridge_db_path:
            warnings.append(
                _warning(
                    "mail_bridge_missing_db_path",
                    "mail_bridge is enabled but db_path is unset.",
                    details={"service": "mail_bridge"},
                )
            )
        elif not Path(bridge_db_path).expanduser().exists():
            warnings.append(
                _warning(
                    "mail_bridge_db_missing",
                    f"mail_bridge db_path does not exist: {bridge_db_path}",
                    details={"service": "mail_bridge", "path": bridge_db_path},
                )
            )
        password_env = str(mail_bridge_password_env or DEFAULT_MAIL_BRIDGE_PASSWORD_ENV).strip() or DEFAULT_MAIL_BRIDGE_PASSWORD_ENV
        if not str(os.getenv(password_env, "")).strip():
            warnings.append(
                _warning(
                    "mail_bridge_missing_password",
                    f"mail_bridge is enabled but password env var is unset: {password_env}",
                    details={"service": "mail_bridge", "env_var": password_env},
                )
            )

    return warnings


def summary_status_counts(conn: sqlite3.Connection) -> dict[str, int]:
    if not table_exists(conn, "docs_registry"):
        return {}
    out: dict[str, int] = {}
    for status, n in conn.execute(
        "SELECT COALESCE(summary_status, '<null>') as s, COUNT(*) FROM docs_registry GROUP BY s ORDER BY COUNT(*) DESC"
    ):
        out[str(status)] = int(n)
    return out


def count_repairable_summary_backfill(conn: sqlite3.Connection) -> int:
    if not table_exists(conn, "docs_registry"):
        return 0
    return int(
        safe_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM docs_registry
            WHERE summary_status IS NULL
               OR summary_status IN ('error', 'disabled', 'stale', 'fallback-text')
               OR (
                    COALESCE(TRIM(summary_text), '') = ''
                    AND COALESCE(TRIM(summary_status), '') = ''
               )
            """,
        )
        or 0
    )


def count_repairable_photo_backfill(conn: sqlite3.Connection) -> int:
    if not table_exists(conn, "photos_registry"):
        return 0
    ocr_pending_clause = ""
    if table_has_column(conn, "photos_registry", "ocr_status"):
        ocr_pending_clause = """
               OR (
                    (
                        COALESCE(TRIM(category_primary), '') IN ('document', 'receipt')
                        OR COALESCE(TRIM(taxonomy), '') = 'docs'
                    )
                    AND COALESCE(TRIM(ocr_status), '') IN ('', 'empty')
               )
        """
    return int(
        safe_scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM photos_registry
            WHERE COALESCE(TRIM(category_primary), '') = ''
               OR COALESCE(TRIM(taxonomy), '') = ''
               OR COALESCE(TRIM(caption), '') = ''
               OR COALESCE(TRIM(analyzer_status), '') IN ('', 'error', 'disabled')
            {ocr_pending_clause}
            """,
        )
        or 0
    )


def redaction_stats(conn: sqlite3.Connection) -> dict[str, int]:
    if not table_exists(conn, "redaction_entries"):
        return {
            "redaction_entries_total": 0,
            "redaction_hit_count_total": 0,
            "redaction_distinct_placeholders": 0,
            "redaction_entries_rejected": 0,
        }

    has_status = table_has_column(conn, "redaction_entries", "status")
    if has_status:
        rows = conn.execute(
            """
            SELECT scope_type, scope_id, key_name, placeholder, original_value, hit_count, COALESCE(status, 'active')
            FROM redaction_entries
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT scope_type, scope_id, key_name, placeholder, original_value, hit_count, 'active'
            FROM redaction_entries
            """
        ).fetchall()

    valid_rows = [
        row
        for row in rows
        if str(row[6] or "active") == "active" and is_redaction_value_allowed(str(row[2]), str(row[4]))
    ]
    rejected_rows = [row for row in rows if str(row[6] or "active") == "rejected"]
    total = len(valid_rows)
    hit_total = sum(int(row[5] or 0) for row in valid_rows)
    placeholders = len({f"{row[0]}|{row[1]}|{row[3]}" for row in valid_rows})
    return {
        "redaction_entries_total": total,
        "redaction_hit_count_total": hit_total,
        "redaction_distinct_placeholders": placeholders,
        "redaction_entries_rejected": len(rejected_rows),
    }


def count_files_recursive(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file())


def latest_sync_run(conn: sqlite3.Connection) -> dict[str, Any] | None:
    if not table_exists(conn, "sync_runs"):
        return None
    row = conn.execute(
        "SELECT id, started_at, finished_at, status, errors, detail FROM sync_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    detail_obj: dict[str, Any] = {}
    try:
        detail_obj = json.loads(row[5] or "{}") if row[5] else {}
    except Exception:
        detail_obj = {}
    return {
        "id": int(row[0]),
        "started_at": row[1],
        "finished_at": row[2],
        "status": str(row[3] or ""),
        "errors": int(row[4] or 0),
        "detail": detail_obj,
    }


def derive_health(
    *,
    inbox_pending: int,
    docs_total: int,
    photos_total: int,
    summaries_ok: int,
    summaries_error: int,
    docs_vector_sources: int,
    photos_vector_sources: int,
    redacted_search_available: bool,
    policy_drift_vectors: int,
    last_run: dict[str, Any] | None,
) -> str:
    if docs_total <= 0:
        return "degraded" if inbox_pending > 0 else "ok"

    missing = max(0, docs_total - summaries_ok)
    missing_ratio = (missing / docs_total) if docs_total else 0.0

    last_status = (last_run or {}).get("status", "") if isinstance(last_run, dict) else ""
    last_errors = int((last_run or {}).get("errors", 0)) if isinstance(last_run, dict) else 0

    if last_status in {"error", "failed"} or last_errors > 0:
        return "error"
    if (
        last_status in {"timeout", "bounded"}
        or summaries_error > 0
        or missing_ratio > 0.25
        or inbox_pending > 25
        or not redacted_search_available
        or policy_drift_vectors > 0
        or (docs_total > 0 and docs_vector_sources < docs_total)
        or (photos_total > 0 and photos_vector_sources < photos_total)
    ):
        return "degraded"
    return "ok"


def _docs_registry_stats(conn: sqlite3.Connection, handler: SourceHandler) -> dict[str, Any]:
    files_total = int(safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table}") or 0)
    unique_checksums = int(safe_scalar(conn, f"SELECT COUNT(DISTINCT checksum) FROM {handler.table}") or 0)
    summary_present = int(
        safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(summary_text), '') <> ''") or 0
    )
    summary_ok = int(safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE summary_status = 'ok'") or 0)
    summary_error = int(safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE summary_status = 'error'") or 0)
    summary_missing = max(0, files_total - summary_present)
    return {
        "kind": handler.kind,
        "label": handler.label,
        "table": handler.table,
        "files_total": files_total,
        "unique_checksums": unique_checksums,
        "duplicate_files": max(0, files_total - unique_checksums),
        "oldest_file_mtime_epoch": safe_scalar(conn, f"SELECT MIN(mtime) FROM {handler.table}"),
        "oldest_file_mtime_utc": to_iso_utc(safe_scalar(conn, f"SELECT MIN(mtime) FROM {handler.table}")),
        "newest_file_mtime_epoch": safe_scalar(conn, f"SELECT MAX(mtime) FROM {handler.table}"),
        "newest_file_mtime_utc": to_iso_utc(safe_scalar(conn, f"SELECT MAX(mtime) FROM {handler.table}")),
        "summary": {
            "present": summary_present,
            "ok": summary_ok,
            "error": summary_error,
            "missing": summary_missing,
            "text_missing": summary_missing,
            "repairable_pending": count_repairable_summary_backfill(conn),
            "status_counts": summary_status_counts(conn),
            "oldest_updated_at": safe_scalar(conn, f"SELECT MIN(summary_updated_at) FROM {handler.table}"),
            "newest_updated_at": safe_scalar(conn, f"SELECT MAX(summary_updated_at) FROM {handler.table}"),
        },
    }


def _photos_registry_stats(conn: sqlite3.Connection, handler: SourceHandler) -> dict[str, Any]:
    files_total = int(safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table}") or 0)
    unique_checksums = int(safe_scalar(conn, f"SELECT COUNT(DISTINCT checksum) FROM {handler.table}") or 0)
    analyzer_ok = int(
        safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(analyzer_status), '') = 'ok'") or 0
    )
    analyzer_error = int(
        safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(analyzer_status), '') = 'error'") or 0
    )
    analyzer_unset = int(
        safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(analyzer_status), '') = ''") or 0
    )
    ocr_status_counts: dict[str, int] = {}
    if table_has_column(conn, handler.table, "ocr_status"):
        for status, count in conn.execute(
            f"""
            SELECT COALESCE(NULLIF(TRIM(ocr_status), ''), '<blank>') AS status, COUNT(*)
            FROM {handler.table}
            GROUP BY status
            ORDER BY COUNT(*) DESC, status
            """
        ):
            ocr_status_counts[str(status)] = int(count)
    return {
        "kind": handler.kind,
        "label": handler.label,
        "table": handler.table,
        "files_total": files_total,
        "unique_checksums": unique_checksums,
        "duplicate_files": max(0, files_total - unique_checksums),
        "oldest_file_mtime_epoch": safe_scalar(conn, f"SELECT MIN(mtime) FROM {handler.table}"),
        "oldest_file_mtime_utc": to_iso_utc(safe_scalar(conn, f"SELECT MIN(mtime) FROM {handler.table}")),
        "newest_file_mtime_epoch": safe_scalar(conn, f"SELECT MAX(mtime) FROM {handler.table}"),
        "newest_file_mtime_utc": to_iso_utc(safe_scalar(conn, f"SELECT MAX(mtime) FROM {handler.table}")),
        "analysis": {
            "with_category": int(
                safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(category_primary), '') <> ''")
                or 0
            ),
            "with_taxonomy": int(
                safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(taxonomy), '') <> ''")
                or 0
            ),
            "with_caption": int(
                safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(caption), '') <> ''")
                or 0
            ),
            "analyzer_ok": analyzer_ok,
            "analyzer_error": analyzer_error,
            "analyzer_unset": analyzer_unset,
            "pending": analyzer_error + analyzer_unset,
            "repairable_pending": count_repairable_photo_backfill(conn),
        },
        "ocr": {
            "status_counts": ocr_status_counts,
            "with_text": int(
                safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(ocr_text), '') <> ''") or 0
            )
            if table_has_column(conn, handler.table, "ocr_text")
            else 0,
        },
    }


def _mail_registry_stats(
    conn: sqlite3.Connection,
    handler: SourceHandler,
    *,
    bridge_enabled: bool,
    db_path: str,
    include_accounts: list[str],
    import_summary: bool,
    max_body_chunks: int,
) -> dict[str, Any]:
    messages_total = int(safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table}") or 0)
    unique_checksums = int(safe_scalar(conn, f"SELECT COUNT(DISTINCT checksum) FROM {handler.table}") or 0)
    return {
        "kind": handler.kind,
        "label": handler.label,
        "table": handler.table,
        "files_total": messages_total,
        "messages_total": messages_total,
        "unique_checksums": unique_checksums,
        "duplicate_files": max(0, messages_total - unique_checksums),
        "bridge_enabled": bridge_enabled,
        "bridge_db_path": db_path,
        "include_accounts": list(include_accounts),
        "import_summary": bool(import_summary),
        "max_body_chunks": max(0, int(max_body_chunks)),
        "accounts_total": int(safe_scalar(conn, f"SELECT COUNT(DISTINCT account_email) FROM {handler.table}") or 0),
        "with_body": int(
            safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(body_text), '') <> ''") or 0
        ),
        "with_summary": int(
            safe_scalar(conn, f"SELECT COUNT(*) FROM {handler.table} WHERE COALESCE(TRIM(summary_text), '') <> ''") or 0
        ),
        "oldest_message_date": safe_scalar(conn, f"SELECT MIN(date_iso) FROM {handler.table}"),
        "newest_message_date": safe_scalar(conn, f"SELECT MAX(date_iso) FROM {handler.table}"),
    }


def _source_registry_stats(
    conn: sqlite3.Connection,
    handler: SourceHandler,
    *,
    mail_bridge_enabled: bool,
    mail_bridge_db_path: str,
    mail_bridge_include_accounts: list[str],
    mail_bridge_import_summary: bool,
    mail_bridge_max_body_chunks: int,
) -> dict[str, Any]:
    if handler.kind == "docs":
        return _docs_registry_stats(conn, handler)
    if handler.kind == "photos":
        return _photos_registry_stats(conn, handler)
    if handler.kind == "mail":
        return _mail_registry_stats(
            conn,
            handler,
            bridge_enabled=mail_bridge_enabled,
            db_path=mail_bridge_db_path,
            include_accounts=mail_bridge_include_accounts,
            import_summary=mail_bridge_import_summary,
            max_body_chunks=mail_bridge_max_body_chunks,
        )
    raise ValueError(f"unsupported source handler: {handler.kind}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize vault DB state")
    ap.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    ap.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    ap.add_argument("--inbox-scanner", default=DEFAULT_INBOX_SCANNER)
    ap.add_argument("--json", action="store_true", help="emit JSON instead of text")
    ap.add_argument("--oneline", action="store_true", help="emit compact one-line status")
    ap.add_argument("--mail-bridge-enabled", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--mail-bridge-db-path", default="", help=argparse.SUPPRESS)
    ap.add_argument("--mail-bridge-password-env", default=DEFAULT_MAIL_BRIDGE_PASSWORD_ENV, help=argparse.SUPPRESS)
    ap.add_argument("--mail-bridge-include-account", action="append", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--mail-bridge-no-import-summary", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--mail-max-body-chunks", type=int, default=12, help=argparse.SUPPRESS)
    ap.add_argument("--docs-root", action="append", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--photos-root", action="append", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--summary-base-url", default="", help=argparse.SUPPRESS)
    ap.add_argument("--embed-base-url", default="", help=argparse.SUPPRESS)
    ap.add_argument("--redaction-base-url", default="", help=argparse.SUPPRESS)
    ap.add_argument("--photo-analysis-url", default="", help=argparse.SUPPRESS)
    ap.add_argument("--disable-photo-analysis", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--pdf-parse-url", default="", help=argparse.SUPPRESS)
    ap.add_argument("--disable-pdf-service", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()

    registry_db = Path(args.registry_db)
    vectors_db = Path(args.vectors_db)
    inbox_scanner = Path(args.inbox_scanner)

    if not registry_db.exists():
        raise SystemExit(f"registry DB not found: {registry_db}")

    reg = connect_vault_db(registry_db, timeout=30.0)

    vec = None
    if vectors_db.exists():
        vec = connect_vault_db(vectors_db, timeout=30.0)

    try:
        data: dict[str, Any] = {
            "paths": {
                "registry_db": str(registry_db),
                "vectors_db": str(vectors_db),
                "vectors_db_exists": bool(vectors_db.exists()),
                "inbox_scanner": str(inbox_scanner),
            },
            "registry": {},
            "vectors": {},
        }

        registry_sources = {
            handler.kind: _source_registry_stats(
                reg,
                handler,
                mail_bridge_enabled=bool(args.mail_bridge_enabled),
                mail_bridge_db_path=str(args.mail_bridge_db_path or ""),
                mail_bridge_include_accounts=list(args.mail_bridge_include_account or []),
                mail_bridge_import_summary=not bool(args.mail_bridge_no_import_summary),
                mail_bridge_max_body_chunks=max(0, int(args.mail_max_body_chunks)),
            )
            for handler in REGISTERED_SOURCES
            if table_exists(reg, handler.table)
        }
        overall_oldest_mtime = min(
            (value for value in (source.get("oldest_file_mtime_epoch") for source in registry_sources.values()) if value is not None),
            default=None,
        )
        overall_newest_mtime = max(
            (value for value in (source.get("newest_file_mtime_epoch") for source in registry_sources.values()) if value is not None),
            default=None,
        )
        redaction = redaction_stats(reg)
        inbox_pending = count_files_recursive(inbox_scanner)
        last_run = latest_sync_run(reg)

        data["registry"] = {
            "overall_oldest_file_mtime_epoch": overall_oldest_mtime,
            "overall_oldest_file_mtime_utc": to_iso_utc(overall_oldest_mtime),
            "overall_newest_file_mtime_epoch": overall_newest_mtime,
            "overall_newest_file_mtime_utc": to_iso_utc(overall_newest_mtime),
            "sources": registry_sources,
            "redaction": redaction,
            "inbox_pending_files": inbox_pending,
            "last_sync_run": last_run,
        }

        docs_registry = registry_sources.get("docs", {})
        docs_summary = docs_registry.get("summary", {})
        photos_registry = registry_sources.get("photos", {})
        photos_analysis = photos_registry.get("analysis", {})
        mail_registry = registry_sources.get("mail", {})

        if vec is not None and (table_exists(vec, "vector_items_v2") or table_exists(vec, "vector_items")):
            legacy_items = int(safe_scalar(vec, "SELECT COUNT(*) FROM vector_items") or 0) if table_exists(vec, "vector_items") else 0
            legacy_sources = int(safe_scalar(vec, "SELECT COUNT(*) FROM source_state") or 0) if table_exists(vec, "source_state") else 0

            available_index_levels: list[str] = []
            level_counts: dict[str, dict[str, Any]] = {}
            if table_exists(vec, "vector_items_v2"):
                for (level,) in vec.execute(
                    "SELECT index_level FROM vector_items_v2 GROUP BY index_level HAVING COUNT(*) > 0 ORDER BY index_level"
                ):
                    level_name = str(level)
                    available_index_levels.append(level_name)
                    level_sources: dict[str, dict[str, Any]] = {}
                    for handler in REGISTERED_SOURCES:
                        source_total = int(
                            safe_scalar(
                                vec,
                                "SELECT COUNT(*) FROM source_state_v2 WHERE index_level = ? AND source_table = ?",
                                (level_name, handler.table),
                            )
                            or 0
                        )
                        files_total = int((registry_sources.get(handler.kind) or {}).get("files_total") or 0)
                        level_sources[handler.kind] = {
                            "table": handler.table,
                            "items": int(
                                safe_scalar(
                                    vec,
                                    "SELECT COUNT(*) FROM vector_items_v2 WHERE index_level = ? AND source_table = ?",
                                    (level_name, handler.table),
                                )
                                or 0
                            ),
                            "sources_indexed": source_total,
                            "sources_pending_estimate": max(0, files_total - source_total),
                        }
                    level_counts[level_name] = {
                        "items_total": int(
                            safe_scalar(vec, "SELECT COUNT(*) FROM vector_items_v2 WHERE index_level = ?", (level_name,)) or 0
                        ),
                        "sources_total": int(
                            safe_scalar(vec, "SELECT COUNT(*) FROM source_state_v2 WHERE index_level = ?", (level_name,)) or 0
                        ),
                        "sources": level_sources,
                    }

            v_dims = []
            if table_exists(vec, "vector_items_v2"):
                try:
                    for level, dim, n in vec.execute(
                        """
                        SELECT index_level, embedding_dim, COUNT(*)
                        FROM vector_items_v2
                        GROUP BY index_level, embedding_dim
                        ORDER BY index_level, embedding_dim
                        """
                    ):
                        v_dims.append({"index_level": str(level), "embedding_dim": int(dim), "count": int(n)})
                except sqlite3.Error:
                    v_dims = []

            policy_drift_vectors = int(
                safe_scalar(
                    vec,
                    "SELECT COUNT(*) FROM source_state_v2 WHERE COALESCE(redaction_policy_version, '') != ?",
                    (REDACTION_POLICY_VERSION,),
                )
                or 0
            ) if table_exists(vec, "source_state_v2") else 0

            data["vectors"] = {
                "available": True,
                "redaction_policy_version": REDACTION_POLICY_VERSION,
                "available_index_levels": available_index_levels,
                "full_search_available": "full" in available_index_levels,
                "policy_drift_vectors": policy_drift_vectors,
                "upgrade_needed": policy_drift_vectors > 0 or "redacted" not in available_index_levels,
                "legacy": {
                    "items_total": legacy_items,
                    "sources_total": legacy_sources,
                },
                "levels": level_counts,
                "embedding_dims": v_dims,
            }
        else:
            data["vectors"] = {"available": False, "levels": {}}

        redacted_level = data["vectors"].get("levels", {}).get("redacted", {})
        redacted_sources = redacted_level.get("sources", {}) if isinstance(redacted_level, dict) else {}
        docs_vector = redacted_sources.get("docs", {})
        photos_vector = redacted_sources.get("photos", {})

        health = derive_health(
            inbox_pending=int(data["registry"].get("inbox_pending_files") or 0),
            docs_total=int(docs_registry.get("files_total") or 0),
            photos_total=int(photos_registry.get("files_total") or 0),
            summaries_ok=int(docs_summary.get("present") or 0),
            summaries_error=int(docs_summary.get("error") or 0),
            docs_vector_sources=int(docs_vector.get("sources_indexed") or 0),
            photos_vector_sources=int(photos_vector.get("sources_indexed") or 0),
            redacted_search_available="redacted" in (data["vectors"].get("available_index_levels") or []),
            policy_drift_vectors=int(data["vectors"].get("policy_drift_vectors") or 0),
            last_run=data["registry"].get("last_sync_run"),
        )
        data["health"] = health
        warnings = build_warnings(
            docs_roots=list(args.docs_root or []),
            photos_roots=list(args.photos_root or []),
            summary_base_url=str(args.summary_base_url or ""),
            embed_base_url=str(args.embed_base_url or ""),
            redaction_base_url=str(args.redaction_base_url or ""),
            photo_analysis_url=str(args.photo_analysis_url or ""),
            disable_photo_analysis=bool(args.disable_photo_analysis),
            pdf_parse_url=str(args.pdf_parse_url or ""),
            disable_pdf_service=bool(args.disable_pdf_service),
            mail_bridge_enabled=bool(args.mail_bridge_enabled),
            mail_bridge_db_path=str(args.mail_bridge_db_path or ""),
            mail_bridge_password_env=str(args.mail_bridge_password_env or DEFAULT_MAIL_BRIDGE_PASSWORD_ENV),
        )
        data["warnings"] = warnings
        data["warning_count"] = len(warnings)

        if args.oneline:
            last = data["registry"].get("last_sync_run") or {}
            last_status = str(last.get("status") or "none")
            last_finished = str(last.get("finished_at") or "never")
            warning_categories = sorted({str(w.get("category") or "") for w in warnings if str(w.get("category") or "")})
            print(
                " ".join(
                    [
                        f"inbox={data['registry'].get('inbox_pending_files', 0)}",
                        f"docs={docs_registry.get('files_total', 0)}",
                        f"photos={photos_registry.get('files_total', 0)}",
                        f"docs_summary_present={docs_summary.get('present', 0)}/{docs_registry.get('files_total', 0)}",
                        f"docs_summary_ok={docs_summary.get('ok', 0)}",
                        f"docs_summary_error={docs_summary.get('error', 0)}",
                        f"docs_summary_pending={docs_summary.get('repairable_pending', 0)}",
                        f"photo_analysis_ok={photos_analysis.get('analyzer_ok', 0)}/{photos_registry.get('files_total', 0)}",
                        f"photo_analysis_error={photos_analysis.get('analyzer_error', 0)}",
                        f"photo_analysis_unset={photos_analysis.get('analyzer_unset', 0)}",
                        f"photo_backfill_pending={photos_analysis.get('repairable_pending', 0)}",
                        f"mail_bridge_enabled={1 if mail_registry.get('bridge_enabled') else 0}",
                        f"mail_messages={mail_registry.get('messages_total', 0)}",
                        f"mail_accounts={mail_registry.get('accounts_total', 0)}",
                        f"mail_with_summary={mail_registry.get('with_summary', 0)}",
                        f"mail_max_body_chunks={mail_registry.get('max_body_chunks', 12)}",
                        f"redaction_entries={data['registry'].get('redaction', {}).get('redaction_entries_total', 0)}",
                        f"redaction_rejected={data['registry'].get('redaction', {}).get('redaction_entries_rejected', 0)}",
                        f"policy_version={data['vectors'].get('redaction_policy_version', REDACTION_POLICY_VERSION)}",
                        f"available_levels={','.join(data['vectors'].get('available_index_levels', [])) or 'none'}",
                        f"full_search={1 if data['vectors'].get('full_search_available') else 0}",
                        f"policy_drift_vectors={data['vectors'].get('policy_drift_vectors', 0)}",
                        f"upgrade_needed={1 if data['vectors'].get('upgrade_needed') else 0}",
                        f"redacted_items={redacted_level.get('items_total', 0)}",
                        f"redacted_doc_items={docs_vector.get('items', 0)}",
                        f"redacted_photo_items={photos_vector.get('items', 0)}",
                        f"redacted_doc_sources={docs_vector.get('sources_indexed', 0)}/{docs_registry.get('files_total', 0)}",
                        f"redacted_photo_sources={photos_vector.get('sources_indexed', 0)}/{photos_registry.get('files_total', 0)}",
                        f"last_run={last_status}@{last_finished}",
                        f"warnings={len(warnings)}",
                        f"warning_categories={','.join(warning_categories) if warning_categories else 'none'}",
                        f"health={health}",
                    ]
                )
            )
            return 0

        if args.json:
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return 0

        print("Vault DB Summary")
        print(f"- Registry DB: {data['paths']['registry_db']}")
        print(f"- Vectors DB:  {data['paths']['vectors_db']} (exists={data['paths']['vectors_db_exists']})")
        print(f"- Inbox scanner: {data['paths']['inbox_scanner']}")
        print(f"- Health: {data['health']}")
        print(f"- Warnings: {data['warning_count']}")
        if warnings:
            print("- WARNING DETAILS (action recommended):")
            for warning in warnings:
                category = str(warning.get("category") or "warning")
                message = str(warning.get("message") or "")
                print(f"  - [{category}] {message}")
        print("")
        print("Registry")
        print(f"- Oldest file mtime (UTC): {data['registry']['overall_oldest_file_mtime_utc']}")
        print(f"- Newest file mtime (UTC): {data['registry']['overall_newest_file_mtime_utc']}")
        print(f"- Inbox pending files: {data['registry']['inbox_pending_files']}")
        print(
            "- Redaction entries total/hits/distinct_placeholders: "
            f"{data['registry']['redaction']['redaction_entries_total']}/"
            f"{data['registry']['redaction']['redaction_hit_count_total']}/"
            f"{data['registry']['redaction']['redaction_distinct_placeholders']}"
        )
        print(f"- Redaction entries rejected: {data['registry']['redaction']['redaction_entries_rejected']}")
        print(f"- Last sync run: {data['registry']['last_sync_run']}")
        for kind, source_data in data["registry"]["sources"].items():
            print(f"- Source {kind}: files={source_data.get('files_total', 0)} unique_checksums={source_data.get('unique_checksums', 0)} duplicates={source_data.get('duplicate_files', 0)}")
            if kind == "docs":
                summary = source_data.get("summary", {})
                print(
                    f"  summary present/ok/error/missing={summary.get('present', 0)}/{summary.get('ok', 0)}/{summary.get('error', 0)}/{summary.get('missing', 0)}"
                )
                print(f"  summary repairable_pending={summary.get('repairable_pending', 0)} status_counts={summary.get('status_counts', {})}")
            elif kind == "photos":
                analysis = source_data.get("analysis", {})
                ocr = source_data.get("ocr", {})
                print(
                    f"  analysis category/taxonomy/caption={analysis.get('with_category', 0)}/{analysis.get('with_taxonomy', 0)}/{analysis.get('with_caption', 0)}"
                )
                print(
                    f"  analysis analyzer_ok/error/unset={analysis.get('analyzer_ok', 0)}/{analysis.get('analyzer_error', 0)}/{analysis.get('analyzer_unset', 0)} repairable_pending={analysis.get('repairable_pending', 0)}"
                )
                print(f"  ocr with_text={ocr.get('with_text', 0)} status_counts={ocr.get('status_counts', {})}")
            elif kind == "mail":
                print(
                    f"  bridge enabled={source_data.get('bridge_enabled')} db_path={source_data.get('bridge_db_path', '')}"
                )
                print(
                    f"  messages/accounts/with_body/with_summary={source_data.get('messages_total', 0)}/{source_data.get('accounts_total', 0)}/{source_data.get('with_body', 0)}/{source_data.get('with_summary', 0)}"
                )
                print(
                    f"  import_summary={source_data.get('import_summary')} max_body_chunks={source_data.get('max_body_chunks', 12)} include_accounts={source_data.get('include_accounts', [])}"
                )

        print("")
        print("Vectors")
        if data["vectors"].get("available") is False:
            print("- Not available")
        else:
            print(f"- Redaction policy version: {data['vectors']['redaction_policy_version']}")
            print(f"- Available index levels: {data['vectors']['available_index_levels']}")
            print(f"- Full search available: {data['vectors']['full_search_available']}")
            print(f"- Policy drift vectors: {data['vectors']['policy_drift_vectors']}")
            print(f"- Upgrade needed: {data['vectors']['upgrade_needed']}")
            print(
                f"- Legacy vector items/sources: {data['vectors']['legacy']['items_total']}/{data['vectors']['legacy']['sources_total']}"
            )
            print(f"- Embedding dims: {data['vectors']['embedding_dims']}")
            print(f"- Level counts: {data['vectors']['levels']}")

        return 0
    finally:
        reg.close()
        if vec is not None:
            vec.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import sys

        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
