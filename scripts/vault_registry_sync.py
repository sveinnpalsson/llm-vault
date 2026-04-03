#!/usr/bin/env python3
"""Vault registry sync: lightweight local index for docs/photos + inbox routing.

- Maintains local SQLite registry in workspace (docs + photos tables)
- Indexes existing vault content under raw/documents + raw/photos
- Polls inbox/scanner for new files, routes by type, then indexes
- Uses openclaw-pdf for PDF extraction (text capped)
- Maintains per-document local-LLM summaries for downstream retrieval/indexing
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from vault_db import SQLCIPHER_AVAILABLE, connect_vault_db, resolve_db_password
from vault_service_defaults import (
    DEFAULT_LOCAL_MODEL_BASE_URL,
)
from vault_sources import select_active_source_handlers, source_choices

try:
    from PIL import Image, ImageStat
except Exception:  # pragma: no cover - Pillow is optional for runtime portability
    Image = None
    ImageStat = None

DOC_EXTS = {".pdf", ".txt", ".md", ".rtf", ".doc", ".docx"}
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif", ".gif", ".tif", ".tiff"}

DEFAULT_SUMMARY_BASE_URL = DEFAULT_LOCAL_MODEL_BASE_URL
DEFAULT_SUMMARY_MODEL = "qwen3-14b"
DEFAULT_SUMMARY_TIMEOUT_SECONDS = 90
DEFAULT_SUMMARY_MAX_INPUT_CHARS = 12000
DEFAULT_SUMMARY_MAX_OUTPUT_CHARS = 650

DEFAULT_PHOTO_ANALYZER_TIMEOUT_SECONDS = 45
DEFAULT_PHOTO_ANALYZER_FORCE = False
DEFAULT_PDF_PARSE_TIMEOUT_SECONDS = 600
DEFAULT_PDF_PARSE_PROFILE = "auto"
ROOT = Path(__file__).resolve().parents[1]
PROGRESS_HEARTBEAT_SECONDS = 5.0
MAIL_BRIDGE_SOURCE = "inbox-vault"
DEFAULT_MAIL_BRIDGE_PASSWORD_ENV = "INBOX_VAULT_DB_PASSWORD"
DEFAULT_VAULT_ROOT_ENV = "LLM_VAULT_CONTENT_ROOT"

# Preferred non-document photo taxonomy requested by user.
PHOTO_TAXONOMY_PERSONAL = {
    "selfie",
    "portrait",
    "group_photo",
}
PHOTO_TAXONOMY_SCREENSHOTS = {"screenshot"}
PHOTO_TAXONOMY_NOTES = {"whiteboard"}
PHOTO_TAXONOMY_DOCS = {"document", "receipt"}

# Filename hints are a cheap first pass for routed scanner uploads.
DOC_IMAGE_HINT_TOKENS = {
    "scan",
    "scanned",
    "scanner",
    "receipt",
    "invoice",
    "statement",
    "form",
    "document",
    "paperwork",
    "contract",
    "agreement",
    "bill",
    "passport",
    "license",
    "licence",
    "permit",
    "id",
    "w2",
    "w9",
    "1099",
    "tax",
}
DOC_IMAGE_HINT_PHRASES = {
    "id card",
    "driver license",
    "drivers license",
    "social security",
    "bank statement",
    "tax return",
    "medical record",
}

MONTH_NAME_TO_NUM = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

DATE_PATTERN_ISO = re.compile(r"\b((?:19|20)\d{2})-(\d{2})-(\d{2})\b")
DATE_PATTERN_US = re.compile(r"\b(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})\b")
DATE_PATTERN_MONTH_NAME = re.compile(
    r"\b("
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?"
    r")\s+(\d{1,2})(?:st|nd|rd|th)?(?:,)?\s+((?:19|20)\d{2})\b",
    flags=re.I,
)
DATE_PATTERN_DAY_MONTH_NAME = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+("
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?"
    r")(?:,)?\s+((?:19|20)\d{2})\b",
    flags=re.I,
)


@dataclass
class SummaryConfig:
    enabled: bool
    base_url: str
    model: str
    api_key: str
    timeout_seconds: int
    max_input_chars: int
    max_output_chars: int


@dataclass
class PhotoAnalysisConfig:
    enabled: bool
    analyze_url: str
    timeout_seconds: int
    force: bool


@dataclass
class PdfParseConfig:
    enabled: bool
    parse_url: str
    timeout_seconds: int
    profile: str


@dataclass
class Config:
    db_path: Path
    docs_roots: list[Path]
    photos_roots: list[Path]
    inbox_scanner: Path
    docs_dest_root: Path
    photos_dest_root: Path
    text_cap: int
    max_seconds: float
    max_items: int
    skip_inbox: bool
    verbose: bool
    summary: SummaryConfig
    photo_analysis: PhotoAnalysisConfig
    pdf_parse: PdfParseConfig
    summary_reprocess_missing_limit: int
    photo_reprocess_missing_limit: int
    source_selection: str = "all"
    mail_bridge: "MailBridgeConfig" = field(default_factory=lambda: MailBridgeConfig())


@dataclass(frozen=True)
class MailBridgeConfig:
    enabled: bool = False
    db_path: str = ""
    password_env: str = DEFAULT_MAIL_BRIDGE_PASSWORD_ENV
    include_accounts: tuple[str, ...] = ()
    import_summary: bool = True


@dataclass
class WorkBudget:
    remaining_items: int | None = None

    @classmethod
    def from_max_items(cls, max_items: int) -> "WorkBudget | None":
        if int(max_items) <= 0:
            return None
        return cls(remaining_items=int(max_items))

    def exhausted(self) -> bool:
        return self.remaining_items is not None and self.remaining_items <= 0

    def consume(self, count: int = 1) -> bool:
        if self.remaining_items is None:
            return True
        if self.remaining_items <= 0:
            return False
        self.remaining_items = max(0, self.remaining_items - max(1, int(count)))
        return True


@dataclass
class SummaryResult:
    text: str
    status: str
    error: str


@dataclass
class DocIndexResult:
    indexed: bool
    summary_updated: bool
    summary_failed: bool


@dataclass
class PhotoAnalysisResult:
    status: str
    route_kind: str
    taxonomy: str
    caption: str
    category_primary: str
    category_secondary: str
    analyzer_model: str
    analyzer_error: str
    analyzer_raw: str
    ocr_text: str


@dataclass(frozen=True)
class MailMessageRecord:
    msg_id: str
    account_email: str
    thread_id: str
    date_iso: str
    internal_ts: int
    from_addr: str
    to_addr: str
    subject: str
    snippet: str
    body_text: str
    labels_json: str
    summary_text: str
    material_updated_at: str


def _vault_content_root() -> Path | None:
    raw = str(os.getenv(DEFAULT_VAULT_ROOT_ENV, "")).strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def default_docs_roots() -> list[str]:
    root = _vault_content_root()
    if root is None:
        return []
    docs_root = root / "raw" / "documents"
    testing_docs_root = root / "raw" / "testing" / "docs"
    return [str(docs_root), str(testing_docs_root)]


def default_photos_roots() -> list[str]:
    root = _vault_content_root()
    if root is None:
        return []
    return [str(root / "raw" / "photos")]


def default_inbox_scanner() -> str:
    root = _vault_content_root()
    if root is None:
        return str(ROOT / "state" / "scanner_inbox")
    return str(root / "inbox" / "scanner_in")


def default_docs_dest_root() -> str:
    root = _vault_content_root()
    if root is None:
        return str(ROOT / "state" / "documents_scanner_inbox")
    return str(root / "raw" / "documents" / "scanner_inbox")


def default_photos_dest_root() -> str:
    root = _vault_content_root()
    if root is None:
        return str(ROOT / "state" / "photos_scanner_inbox")
    return str(root / "raw" / "photos" / "scanner_inbox")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", default=str(ROOT / "state" / "vault_registry.db"))
    p.add_argument("--docs-root", action="append", default=None)
    p.add_argument("--photos-root", action="append", default=None)
    p.add_argument("--source", choices=source_choices(), default="all")
    p.add_argument("--inbox-scanner", default=default_inbox_scanner())
    p.add_argument("--docs-dest-root", default=default_docs_dest_root())
    p.add_argument("--photos-dest-root", default=default_photos_dest_root())
    p.add_argument("--text-cap", type=int, default=40000)
    p.add_argument(
        "--max-seconds",
        type=float,
        default=0.0,
        help="time budget in seconds (<=0 means no limit)",
    )
    p.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="process at most N docs/photos/mail source items in this run (<=0 means no limit)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-inbox", action="store_true", help="skip inbox/scanner routing phase")
    p.add_argument("--verbose", action="store_true", help="print detailed pipeline step progress")

    p.add_argument("--disable-summary", action="store_true", help="skip local LLM doc summarization")
    p.add_argument("--summary-base-url", default=None)
    p.add_argument("--summary-model", default=None)
    p.add_argument("--summary-api-key", default=None)
    p.add_argument("--summary-timeout", type=int, default=None)
    p.add_argument("--summary-max-input-chars", type=int, default=None)
    p.add_argument("--summary-max-output-chars", type=int, default=None)

    p.add_argument("--disable-photo-analysis", action="store_true", help="skip local photo analysis endpoint")
    p.add_argument("--photo-analysis-url", default=None, help="local HTTP endpoint for photo analyzer")
    p.add_argument("--photo-analysis-timeout", type=int, default=None)
    p.add_argument("--photo-analysis-force", action="store_true", help="send force=true to analyzer endpoint")
    p.add_argument("--disable-pdf-service", action="store_true", help="skip HTTP PDF parse service and use local parsers only")
    p.add_argument("--pdf-parse-url", default=None, help="local HTTP endpoint for PDF parse service")
    p.add_argument("--pdf-parse-timeout", type=int, default=None)
    p.add_argument("--pdf-parse-profile", choices=["auto", "native", "ocr"], default=None)

    p.add_argument(
        "--reprocess-missing-summaries",
        type=int,
        default=-1,
        help="backfill docs with missing/error summaries even if files are unchanged (-1 means all)",
    )
    p.add_argument(
        "--reprocess-missing-photo-analysis",
        type=int,
        default=-1,
        help="backfill photos with missing category/taxonomy/caption (-1 means all)",
    )
    p.add_argument("--mail-bridge-enabled", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--mail-bridge-db-path", default=None, help=argparse.SUPPRESS)
    p.add_argument("--mail-bridge-password-env", default=None, help=argparse.SUPPRESS)
    p.add_argument("--mail-bridge-include-account", action="append", default=None, help=argparse.SUPPRESS)
    p.add_argument("--mail-bridge-no-import-summary", action="store_true", help=argparse.SUPPRESS)
    return p.parse_args()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_local_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


def _resolve_summary_config(args: argparse.Namespace) -> SummaryConfig:
    base_url = (args.summary_base_url or "").strip() or (
        str(os.getenv("VAULT_SUMMARY_BASE_URL", DEFAULT_SUMMARY_BASE_URL)).strip()
    )
    model = (args.summary_model or "").strip() or (
        str(os.getenv("VAULT_SUMMARY_MODEL", DEFAULT_SUMMARY_MODEL)).strip()
    )
    env_api_key = os.getenv("VAULT_SUMMARY_API_KEY")
    api_key = args.summary_api_key if args.summary_api_key is not None else (env_api_key if env_api_key is not None else "local")
    timeout_seconds = int(
        args.summary_timeout
        if args.summary_timeout is not None
        else int(os.getenv("VAULT_SUMMARY_TIMEOUT_SECONDS", str(DEFAULT_SUMMARY_TIMEOUT_SECONDS)))
    )
    max_input_chars = int(
        args.summary_max_input_chars
        if args.summary_max_input_chars is not None
        else int(os.getenv("VAULT_SUMMARY_MAX_INPUT_CHARS", str(DEFAULT_SUMMARY_MAX_INPUT_CHARS)))
    )
    max_output_chars = int(
        args.summary_max_output_chars
        if args.summary_max_output_chars is not None
        else int(os.getenv("VAULT_SUMMARY_MAX_OUTPUT_CHARS", str(DEFAULT_SUMMARY_MAX_OUTPUT_CHARS)))
    )

    enabled = not bool(args.disable_summary)
    if enabled and not _is_local_url(base_url):
        raise ValueError(f"summary base URL must be local-only, got: {base_url}")

    return SummaryConfig(
        enabled=enabled,
        base_url=base_url,
        model=model,
        api_key=str(api_key or "local"),
        timeout_seconds=max(1, timeout_seconds),
        max_input_chars=max(1000, max_input_chars),
        max_output_chars=max(120, max_output_chars),
    )


def _resolve_photo_analysis_config(args: argparse.Namespace) -> PhotoAnalysisConfig:
    analyze_url = (args.photo_analysis_url or "").strip() or str(os.getenv("VAULT_PHOTO_ANALYSIS_URL", "")).strip()
    timeout_seconds = int(
        args.photo_analysis_timeout
        if args.photo_analysis_timeout is not None
        else int(os.getenv("VAULT_PHOTO_ANALYSIS_TIMEOUT_SECONDS", str(DEFAULT_PHOTO_ANALYZER_TIMEOUT_SECONDS)))
    )
    force_env = str(os.getenv("VAULT_PHOTO_ANALYSIS_FORCE", "")).strip().lower()
    force = DEFAULT_PHOTO_ANALYZER_FORCE
    force = force or bool(args.photo_analysis_force) or force_env in {"1", "true", "yes", "on"}

    enabled = not bool(args.disable_photo_analysis) and bool(analyze_url)
    if enabled and not _is_local_url(analyze_url):
        raise ValueError(f"photo analysis URL must be local-only, got: {analyze_url}")

    return PhotoAnalysisConfig(
        enabled=enabled,
        analyze_url=analyze_url,
        timeout_seconds=max(3, timeout_seconds),
        force=force if enabled else False,
    )


def _resolve_pdf_parse_config(args: argparse.Namespace) -> PdfParseConfig:
    parse_url = (args.pdf_parse_url or "").strip() or str(os.getenv("VAULT_PDF_PARSE_URL", "")).strip()
    timeout_seconds = int(
        args.pdf_parse_timeout
        if args.pdf_parse_timeout is not None
        else int(os.getenv("VAULT_PDF_PARSE_TIMEOUT_SECONDS", str(DEFAULT_PDF_PARSE_TIMEOUT_SECONDS)))
    )
    profile = (args.pdf_parse_profile or "").strip().lower() or (
        str(os.getenv("VAULT_PDF_PARSE_PROFILE", DEFAULT_PDF_PARSE_PROFILE)).strip().lower()
    )
    if profile not in {"auto", "native", "ocr"}:
        raise ValueError(f"unsupported PDF parse profile: {profile}")

    enabled = not bool(args.disable_pdf_service) and bool(parse_url)
    if enabled and not _is_local_url(parse_url):
        raise ValueError(f"PDF parse URL must be local-only, got: {parse_url}")

    return PdfParseConfig(
        enabled=enabled,
        parse_url=parse_url,
        timeout_seconds=max(3, timeout_seconds),
        profile=profile,
    )


def _resolve_mail_bridge_config(args: argparse.Namespace) -> MailBridgeConfig:
    include_accounts = tuple(
        sorted(
            {
                str(account or "").strip()
                for account in list(args.mail_bridge_include_account or [])
                if str(account or "").strip()
            }
        )
    )
    password_env = str(args.mail_bridge_password_env or DEFAULT_MAIL_BRIDGE_PASSWORD_ENV).strip()
    return MailBridgeConfig(
        enabled=bool(args.mail_bridge_enabled),
        db_path=str(args.mail_bridge_db_path or "").strip(),
        password_env=password_env or DEFAULT_MAIL_BRIDGE_PASSWORD_ENV,
        include_accounts=include_accounts,
        import_summary=not bool(args.mail_bridge_no_import_summary),
    )


def _enabled_source_kinds(cfg: Config) -> set[str]:
    enabled = {"docs", "photos"}
    if cfg.mail_bridge.enabled:
        enabled.add("mail")
    return enabled


def _selected_source_kinds(cfg: Config) -> set[str]:
    handlers = select_active_source_handlers(
        cfg.source_selection,
        enabled_kinds=_enabled_source_kinds(cfg),
    )
    return {handler.kind for handler in handlers}


def _map_photo_taxonomy(category_primary: str) -> tuple[str, str]:
    key = (category_primary or "").strip().lower()
    if key in PHOTO_TAXONOMY_DOCS:
        return "doc", "docs"
    if key in PHOTO_TAXONOMY_SCREENSHOTS:
        return "photo", "screenshots"
    if key in PHOTO_TAXONOMY_NOTES:
        return "photo", "notes"
    if key in PHOTO_TAXONOMY_PERSONAL:
        return "photo", "personal"
    return "photo", "misc"


def _is_document_like_photo(*, category_primary: str, taxonomy: str) -> bool:
    primary = (category_primary or "").strip().lower()
    photo_taxonomy = (taxonomy or "").strip().lower()
    return primary in PHOTO_TAXONOMY_DOCS or photo_taxonomy == "docs"


def _resolve_photo_ocr_fields(
    *,
    analyzer_status: str,
    category_primary: str,
    taxonomy: str,
    ocr_text: str,
) -> tuple[str, str, str]:
    status = (analyzer_status or "").strip().lower()
    trimmed_ocr = str(ocr_text or "").strip()
    if status == "ok":
        if _is_document_like_photo(category_primary=category_primary, taxonomy=taxonomy):
            if trimmed_ocr:
                return trimmed_ocr, "ok", "analyzer:text_raw"
            return "", "empty", ""
        return "", "not_applicable", ""
    if status == "disabled":
        return "", "disabled", ""
    if status == "error":
        return "", "error", ""
    return "", status or "error", ""


def _encode_multipart_form(fields: dict[str, str], file_field: str, file_path: Path) -> tuple[bytes, str]:
    boundary = f"----vaultops-{int(time.time() * 1000)}-{os.getpid()}"
    chunks: list[bytes] = []

    for key, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )

    mime = "application/octet-stream"
    ext = file_path.suffix.lower()
    if ext == ".pdf":
        mime = "application/pdf"
    if ext in {".jpg", ".jpeg"}:
        mime = "image/jpeg"
    elif ext == ".png":
        mime = "image/png"
    elif ext == ".webp":
        mime = "image/webp"

    file_bytes = file_path.read_bytes()
    chunks.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field}"; filename="{file_path.name}"\r\n'
                f"Content-Type: {mime}\r\n\r\n"
            ).encode("utf-8"),
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    body = b"".join(chunks)
    return body, boundary


class LocalPhotoAnalyzerClient:
    def __init__(self, cfg: PhotoAnalysisConfig):
        self.cfg = cfg

    def analyze(self, path: Path) -> PhotoAnalysisResult:
        if not self.cfg.enabled:
            return PhotoAnalysisResult(
                status="disabled",
                route_kind="photo",
                taxonomy="misc",
                caption="",
                category_primary="",
                category_secondary="",
                analyzer_model="",
                analyzer_error="",
                analyzer_raw="",
                ocr_text="",
            )

        try:
            fields = {"force": "true" if self.cfg.force else "false"}
            body, boundary = _encode_multipart_form(fields, "file", path)
            req = urllib.request.Request(
                self.cfg.analyze_url,
                data=body,
                headers={
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                    "Accept": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=max(1, int(self.cfg.timeout_seconds))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise RuntimeError("photo analyzer response must be an object")

            sidecar = data.get("sidecar") if isinstance(data.get("sidecar"), dict) else {}
            caption_obj = sidecar.get("caption") if isinstance(sidecar.get("caption"), dict) else {}
            category_obj = sidecar.get("category") if isinstance(sidecar.get("category"), dict) else {}
            pipeline_obj = sidecar.get("pipeline") if isinstance(sidecar.get("pipeline"), dict) else {}
            text_obj = sidecar.get("text") if isinstance(sidecar.get("text"), dict) else {}

            caption = str(caption_obj.get("text") or "").strip()
            primary = str(category_obj.get("primary") or "").strip().lower()
            secondary_list = category_obj.get("secondary") if isinstance(category_obj.get("secondary"), list) else []
            secondary = ",".join(str(x).strip().lower() for x in secondary_list if str(x).strip())
            route_kind, taxonomy = _map_photo_taxonomy(primary)
            ocr_text = str(text_obj.get("raw") or "").strip()
            model = str(
                pipeline_obj.get("caption_model")
                or caption_obj.get("model")
                or category_obj.get("model")
                or ""
            ).strip()

            compact = {
                "ok": bool(data.get("ok")),
                "cached": bool(data.get("cached")),
                "category": {
                    "primary": primary,
                    "secondary": secondary_list,
                    "scores": category_obj.get("scores") if isinstance(category_obj.get("scores"), dict) else {},
                },
                "caption": caption,
                "people_count": len(sidecar.get("people") or []) if isinstance(sidecar.get("people"), list) else 0,
                "text_raw": ocr_text or None,
                "pipeline": pipeline_obj,
            }
            return PhotoAnalysisResult(
                status="ok",
                route_kind=route_kind,
                taxonomy=taxonomy,
                caption=caption,
                category_primary=primary,
                category_secondary=secondary,
                analyzer_model=model,
                analyzer_error="",
                analyzer_raw=json.dumps(compact, ensure_ascii=False, sort_keys=True),
                ocr_text=ocr_text,
            )
        except Exception as exc:  # noqa: BLE001
            return PhotoAnalysisResult(
                status="error",
                route_kind="photo",
                taxonomy="misc",
                caption="",
                category_primary="",
                category_secondary="",
                analyzer_model="",
                analyzer_error=str(exc)[:500],
                analyzer_raw="",
                ocr_text="",
            )


def _primary_key_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows if int(r[5]) > 0]


def _migrate_docs_if_needed(conn: sqlite3.Connection) -> None:
    if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='docs_registry'").fetchone():
        return
    pk = _primary_key_columns(conn, "docs_registry")
    if pk == ["filepath"]:
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS docs_registry_new (
          filepath TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          source TEXT,
          text_content TEXT,
          text_chars_total INTEGER,
          text_capped INTEGER DEFAULT 0,
          parser TEXT,
          ocr_used INTEGER DEFAULT 0,
          extraction_method TEXT,
          summary_text TEXT,
          summary_model TEXT,
          summary_hash TEXT,
          summary_status TEXT,
          summary_updated_at TEXT,
          summary_error TEXT,
          dates_json TEXT,
          primary_date TEXT,
          size INTEGER,
          mtime REAL,
          indexed_at TEXT,
          updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO docs_registry_new (
          filepath, checksum, source, text_content, text_chars_total, text_capped,
          parser, ocr_used, extraction_method, size, mtime, indexed_at, updated_at
        )
        SELECT filepath, checksum, source, text_content, text_chars_total, text_capped,
               parser, ocr_used, extraction_method, size, mtime, indexed_at, updated_at
        FROM docs_registry
        """
    )
    conn.execute("DROP TABLE docs_registry")
    conn.execute("ALTER TABLE docs_registry_new RENAME TO docs_registry")


def _migrate_photos_if_needed(conn: sqlite3.Connection) -> None:
    if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='photos_registry'").fetchone():
        return
    pk = _primary_key_columns(conn, "photos_registry")
    if pk == ["filepath"]:
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS photos_registry_new (
          filepath TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          source TEXT,
          date_taken TEXT,
          size INTEGER,
          mtime REAL,
          indexed_at TEXT,
          updated_at TEXT,
          notes TEXT,
          category_primary TEXT,
          category_secondary TEXT,
          taxonomy TEXT,
          caption TEXT,
          analyzer_model TEXT,
          analyzer_status TEXT,
          analyzer_error TEXT,
          analyzer_raw TEXT,
          analyzed_at TEXT,
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
        INSERT OR REPLACE INTO photos_registry_new (
          filepath, checksum, source, date_taken, size, mtime, indexed_at, updated_at, notes,
          category_primary, category_secondary, taxonomy, caption,
          analyzer_model, analyzer_status, analyzer_error, analyzer_raw, analyzed_at,
          ocr_text, ocr_status, ocr_source, ocr_updated_at
        )
        SELECT filepath, checksum, source, date_taken, size, mtime, indexed_at, updated_at, notes,
               '', '', '', '', '', '', '', '', NULL, '', '', '', NULL
        FROM photos_registry
        """
    )
    conn.execute("DROP TABLE photos_registry")
    conn.execute("ALTER TABLE photos_registry_new RENAME TO photos_registry")


def _ensure_column(conn: sqlite3.Connection, table: str, col_name: str, col_sql_type: str) -> None:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if col_name not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_sql_type}")


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS docs_registry (
          filepath TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          source TEXT,
          text_content TEXT,
          text_chars_total INTEGER,
          text_capped INTEGER DEFAULT 0,
          parser TEXT,
          ocr_used INTEGER DEFAULT 0,
          extraction_method TEXT,
          summary_text TEXT,
          summary_model TEXT,
          summary_hash TEXT,
          summary_status TEXT,
          summary_updated_at TEXT,
          summary_error TEXT,
          dates_json TEXT,
          primary_date TEXT,
          size INTEGER,
          mtime REAL,
          indexed_at TEXT,
          updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS photos_registry (
          filepath TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          source TEXT,
          date_taken TEXT,
          size INTEGER,
          mtime REAL,
          indexed_at TEXT,
          updated_at TEXT,
          notes TEXT,
          category_primary TEXT,
          category_secondary TEXT,
          taxonomy TEXT,
          caption TEXT,
          analyzer_model TEXT,
          analyzer_status TEXT,
          analyzer_error TEXT,
          analyzer_raw TEXT,
          analyzed_at TEXT,
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
        CREATE TABLE IF NOT EXISTS mail_registry (
          filepath TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          source TEXT,
          msg_id TEXT NOT NULL UNIQUE,
          account_email TEXT NOT NULL,
          thread_id TEXT,
          date_iso TEXT,
          from_addr TEXT,
          to_addr TEXT,
          subject TEXT,
          snippet TEXT,
          body_text TEXT,
          labels_json TEXT,
          summary_text TEXT,
          primary_date TEXT,
          dates_json TEXT,
          indexed_at TEXT,
          updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mail_sync_state (
          bridge_key TEXT NOT NULL,
          account_email TEXT NOT NULL,
          last_material_updated_at TEXT NOT NULL,
          last_material_msg_id TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (bridge_key, account_email)
        )
        """
    )
    _migrate_docs_if_needed(conn)
    _migrate_photos_if_needed(conn)

    _ensure_column(conn, "docs_registry", "summary_text", "TEXT")
    _ensure_column(conn, "docs_registry", "summary_model", "TEXT")
    _ensure_column(conn, "docs_registry", "summary_hash", "TEXT")
    _ensure_column(conn, "docs_registry", "summary_status", "TEXT")
    _ensure_column(conn, "docs_registry", "summary_updated_at", "TEXT")
    _ensure_column(conn, "docs_registry", "summary_error", "TEXT")
    _ensure_column(conn, "docs_registry", "dates_json", "TEXT")
    _ensure_column(conn, "docs_registry", "primary_date", "TEXT")

    _ensure_column(conn, "photos_registry", "category_primary", "TEXT")
    _ensure_column(conn, "photos_registry", "category_secondary", "TEXT")
    _ensure_column(conn, "photos_registry", "taxonomy", "TEXT")
    _ensure_column(conn, "photos_registry", "caption", "TEXT")
    _ensure_column(conn, "photos_registry", "analyzer_model", "TEXT")
    _ensure_column(conn, "photos_registry", "analyzer_status", "TEXT")
    _ensure_column(conn, "photos_registry", "analyzer_error", "TEXT")
    _ensure_column(conn, "photos_registry", "analyzer_raw", "TEXT")
    _ensure_column(conn, "photos_registry", "analyzed_at", "TEXT")
    _ensure_column(conn, "photos_registry", "ocr_text", "TEXT")
    _ensure_column(conn, "photos_registry", "ocr_status", "TEXT")
    _ensure_column(conn, "photos_registry", "ocr_source", "TEXT")
    _ensure_column(conn, "photos_registry", "ocr_updated_at", "TEXT")
    _ensure_column(conn, "photos_registry", "dates_json", "TEXT")
    _ensure_column(conn, "photos_registry", "primary_date", "TEXT")
    _ensure_column(conn, "mail_registry", "source", "TEXT")
    _ensure_column(conn, "mail_registry", "msg_id", "TEXT")
    _ensure_column(conn, "mail_registry", "account_email", "TEXT")
    _ensure_column(conn, "mail_registry", "thread_id", "TEXT")
    _ensure_column(conn, "mail_registry", "date_iso", "TEXT")
    _ensure_column(conn, "mail_registry", "from_addr", "TEXT")
    _ensure_column(conn, "mail_registry", "to_addr", "TEXT")
    _ensure_column(conn, "mail_registry", "subject", "TEXT")
    _ensure_column(conn, "mail_registry", "snippet", "TEXT")
    _ensure_column(conn, "mail_registry", "body_text", "TEXT")
    _ensure_column(conn, "mail_registry", "labels_json", "TEXT")
    _ensure_column(conn, "mail_registry", "summary_text", "TEXT")
    _ensure_column(conn, "mail_registry", "primary_date", "TEXT")
    _ensure_column(conn, "mail_registry", "dates_json", "TEXT")
    _ensure_column(conn, "mail_registry", "indexed_at", "TEXT")
    _ensure_column(conn, "mail_registry", "updated_at", "TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_checksum ON docs_registry(checksum)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_summary_hash ON docs_registry(summary_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_primary_date ON docs_registry(primary_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_photos_checksum ON photos_registry(checksum)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_photos_primary_date ON photos_registry(primary_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mail_checksum ON mail_registry(checksum)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mail_msg_id ON mail_registry(msg_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mail_account ON mail_registry(account_email)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mail_primary_date ON mail_registry(primary_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mail_sync_state_account ON mail_sync_state(account_email)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at TEXT,
          finished_at TEXT,
          docs_indexed INTEGER DEFAULT 0,
          photos_indexed INTEGER DEFAULT 0,
          inbox_routed INTEGER DEFAULT 0,
          skipped INTEGER DEFAULT 0,
          errors INTEGER DEFAULT 0,
          status TEXT,
          detail TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS redaction_entries (
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
          hit_count INTEGER NOT NULL DEFAULT 1,
          UNIQUE(scope_type, scope_id, key_name, value_norm),
          UNIQUE(scope_type, scope_id, placeholder)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_redaction_scope "
        "ON redaction_entries(scope_type, scope_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_redaction_placeholder "
        "ON redaction_entries(scope_type, scope_id, placeholder)"
    )
    conn.commit()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _mail_bridge_key(cfg: MailBridgeConfig) -> str:
    payload = {
        "db_path": str(Path(cfg.db_path).expanduser().resolve()) if cfg.db_path else "",
        "import_summary": bool(cfg.import_summary),
    }
    return hashlib.sha256(_json_compact(payload).encode("utf-8")).hexdigest()


def _mail_source_filepath(msg_id: str) -> str:
    return f"mail://message/{msg_id}"


def _normalize_labels_json(raw: str | None) -> str:
    text = str(raw or "").strip()
    if not text:
        return "[]"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return "[]"
    if isinstance(parsed, list):
        cleaned = [str(item).strip() for item in parsed if str(item).strip()]
        return _json_compact(cleaned)
    return "[]"


def _mail_dates_payload(date_iso: str) -> tuple[str, str]:
    normalized = str(date_iso or "").strip()
    if not normalized:
        return "[]", ""
    return (
        _json_compact(
            [
                {
                    "value": normalized,
                    "kind": "message_date",
                    "source": "date_iso",
                }
            ]
        ),
        normalized,
    )


def _mail_checksum(record: MailMessageRecord, *, primary_date: str, dates_json: str) -> str:
    payload = {
        "msg_id": record.msg_id,
        "account_email": record.account_email,
        "thread_id": record.thread_id,
        "date_iso": record.date_iso,
        "from_addr": record.from_addr,
        "to_addr": record.to_addr,
        "subject": record.subject,
        "snippet": record.snippet,
        "body_text": record.body_text,
        "labels_json": record.labels_json,
        "summary_text": record.summary_text,
        "primary_date": primary_date,
        "dates_json": dates_json,
    }
    return hashlib.sha256(_json_compact(payload).encode("utf-8")).hexdigest()


def _connect_mail_bridge_db(cfg: MailBridgeConfig) -> sqlite3.Connection:
    db_path = str(cfg.db_path or "").strip()
    if not db_path:
        raise ValueError("mail bridge is enabled but mail_bridge.db_path is not configured")
    password: str | None = None
    if SQLCIPHER_AVAILABLE:
        password = resolve_db_password(cfg.password_env)
    return connect_vault_db(Path(db_path), password=password, timeout=30.0)


def _mail_sync_cursor(
    conn: sqlite3.Connection,
    *,
    bridge_key: str,
    account_email: str,
) -> tuple[str, str]:
    row = conn.execute(
        """
        SELECT last_material_updated_at, last_material_msg_id
        FROM mail_sync_state
        WHERE bridge_key = ? AND account_email = ?
        """,
        (bridge_key, account_email),
    ).fetchone()
    if not row:
        return "", ""
    return str(row[0] or ""), str(row[1] or "")


def _store_mail_sync_cursor(
    conn: sqlite3.Connection,
    *,
    bridge_key: str,
    account_email: str,
    last_material_updated_at: str,
    last_material_msg_id: str,
) -> bool:
    prior = _mail_sync_cursor(conn, bridge_key=bridge_key, account_email=account_email)
    current = (str(last_material_updated_at or ""), str(last_material_msg_id or ""))
    if prior == current:
        return False
    conn.execute(
        """
        INSERT INTO mail_sync_state (
          bridge_key, account_email, last_material_updated_at, last_material_msg_id, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(bridge_key, account_email) DO UPDATE SET
          last_material_updated_at=excluded.last_material_updated_at,
          last_material_msg_id=excluded.last_material_msg_id,
          updated_at=excluded.updated_at
        """,
        (
            bridge_key,
            account_email,
            current[0],
            current[1],
            now_iso(),
        ),
    )
    return True


def _material_updated_at_sql(*, import_summary: bool) -> str:
    if not import_summary:
        return "COALESCE(m.last_seen_at, '')"
    return (
        "CASE "
        "WHEN COALESCE(e.enriched_at, '') > COALESCE(m.last_seen_at, '') THEN COALESCE(e.enriched_at, '') "
        "ELSE COALESCE(m.last_seen_at, '') "
        "END"
    )


def _mail_account_scope(
    inbox_conn: sqlite3.Connection,
    *,
    include_accounts: tuple[str, ...],
) -> list[str]:
    if include_accounts:
        return list(include_accounts)
    rows = inbox_conn.execute(
        "SELECT DISTINCT account_email FROM messages ORDER BY account_email"
    ).fetchall()
    return [str(row[0] or "").strip() for row in rows if str(row[0] or "").strip()]


def _fetch_mail_records(
    inbox_conn: sqlite3.Connection,
    *,
    account_email: str,
    import_summary: bool,
    cursor: tuple[str, str] | None = None,
) -> list[MailMessageRecord]:
    material_sql = _material_updated_at_sql(import_summary=import_summary)
    join_sql = (
        "LEFT JOIN message_enrichment e ON e.msg_id = m.msg_id"
        if import_summary
        else ""
    )
    select_summary = "COALESCE(e.summary, '')" if import_summary else "''"
    sql = f"""
        SELECT
          m.msg_id,
          m.account_email,
          COALESCE(m.thread_id, ''),
          COALESCE(m.date_iso, ''),
          COALESCE(m.internal_ts, 0),
          COALESCE(m.from_addr, ''),
          COALESCE(m.to_addr, ''),
          COALESCE(m.subject, ''),
          COALESCE(m.snippet, ''),
          COALESCE(m.body_text, ''),
          COALESCE(m.labels_json, '[]'),
          {select_summary} AS summary_text,
          {material_sql} AS material_updated_at
        FROM messages m
        {join_sql}
        WHERE m.account_email = ?
    """
    params: list[Any] = [account_email]
    if cursor is not None:
        sql += f"""
          AND (
            {material_sql} > ?
            OR ({material_sql} = ? AND m.msg_id > ?)
          )
        """
        params.extend([cursor[0], cursor[0], cursor[1]])
    sql += f" ORDER BY {material_sql}, m.msg_id"
    rows = inbox_conn.execute(sql, params).fetchall()
    return [
        MailMessageRecord(
            msg_id=str(row[0] or ""),
            account_email=str(row[1] or ""),
            thread_id=str(row[2] or ""),
            date_iso=str(row[3] or ""),
            internal_ts=int(row[4] or 0),
            from_addr=str(row[5] or ""),
            to_addr=str(row[6] or ""),
            subject=str(row[7] or ""),
            snippet=str(row[8] or ""),
            body_text=str(row[9] or ""),
            labels_json=_normalize_labels_json(row[10]),
            summary_text=str(row[11] or "").strip(),
            material_updated_at=str(row[12] or ""),
        )
        for row in rows
    ]


def _mail_registry_snapshot(conn: sqlite3.Connection, *, filepath: str) -> tuple[str, ...] | None:
    row = conn.execute(
        """
        SELECT
          checksum,
          source,
          msg_id,
          account_email,
          COALESCE(thread_id, ''),
          COALESCE(date_iso, ''),
          COALESCE(from_addr, ''),
          COALESCE(to_addr, ''),
          COALESCE(subject, ''),
          COALESCE(snippet, ''),
          COALESCE(body_text, ''),
          COALESCE(labels_json, '[]'),
          COALESCE(summary_text, ''),
          COALESCE(primary_date, ''),
          COALESCE(dates_json, '[]')
        FROM mail_registry
        WHERE filepath = ?
        """,
        (filepath,),
    ).fetchone()
    if not row:
        return None
    return tuple(str(value or "") for value in row)


def upsert_mail(
    conn: sqlite3.Connection,
    *,
    record: MailMessageRecord,
    checksum: str,
    primary_date: str,
    dates_json: str,
) -> None:
    ts = now_iso()
    filepath = _mail_source_filepath(record.msg_id)
    conn.execute(
        """
        INSERT INTO mail_registry (
          filepath, checksum, source, msg_id, account_email, thread_id,
          date_iso, from_addr, to_addr, subject, snippet, body_text, labels_json,
          summary_text, primary_date, dates_json, indexed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(filepath) DO UPDATE SET
          checksum=excluded.checksum,
          source=excluded.source,
          msg_id=excluded.msg_id,
          account_email=excluded.account_email,
          thread_id=excluded.thread_id,
          date_iso=excluded.date_iso,
          from_addr=excluded.from_addr,
          to_addr=excluded.to_addr,
          subject=excluded.subject,
          snippet=excluded.snippet,
          body_text=excluded.body_text,
          labels_json=excluded.labels_json,
          summary_text=excluded.summary_text,
          primary_date=excluded.primary_date,
          dates_json=excluded.dates_json,
          updated_at=excluded.updated_at
        """,
        (
            filepath,
            checksum,
            MAIL_BRIDGE_SOURCE,
            record.msg_id,
            record.account_email,
            record.thread_id,
            record.date_iso,
            record.from_addr,
            record.to_addr,
            record.subject,
            record.snippet,
            record.body_text,
            record.labels_json,
            record.summary_text,
            primary_date,
            dates_json,
            ts,
            ts,
        ),
    )


def _prune_mail_registry(
    conn: sqlite3.Connection,
    *,
    live_records: dict[str, set[str]],
    active_accounts: set[str],
) -> int:
    rows = conn.execute(
        """
        SELECT filepath, msg_id, account_email
        FROM mail_registry
        WHERE source = ?
        """,
        (MAIL_BRIDGE_SOURCE,),
    ).fetchall()
    deleted = 0
    for filepath, msg_id, account_email in rows:
        account = str(account_email or "")
        keep_ids = live_records.get(account)
        should_delete = False
        if account not in active_accounts:
            should_delete = True
        elif keep_ids is not None and str(msg_id or "") not in keep_ids:
            should_delete = True
        if should_delete:
            deleted += int(
                conn.execute(
                    "DELETE FROM mail_registry WHERE filepath = ?",
                    (str(filepath or ""),),
                ).rowcount
                or 0
            )
    return deleted


def _prune_mail_sync_state(
    conn: sqlite3.Connection,
    *,
    bridge_key: str,
    active_accounts: set[str],
) -> int:
    rows = conn.execute(
        """
        SELECT account_email
        FROM mail_sync_state
        WHERE bridge_key = ?
        """,
        (bridge_key,),
    ).fetchall()
    deleted = 0
    for (account_email,) in rows:
        account = str(account_email or "")
        if account in active_accounts:
            continue
        deleted += int(
            conn.execute(
                "DELETE FROM mail_sync_state WHERE bridge_key = ? AND account_email = ?",
                (bridge_key, account),
            ).rowcount
            or 0
        )
    return deleted


def sync_mail_bridge(
    conn: sqlite3.Connection,
    *,
    mail_cfg: MailBridgeConfig,
    full_scan: bool,
    dry_run: bool,
    deadline: float,
    budget: WorkBudget | None = None,
    verbose: bool = False,
) -> tuple[int, int, int]:
    if not mail_cfg.enabled:
        return 0, 0, 0

    bridge_key = _mail_bridge_key(mail_cfg)
    updated = 0
    pruned = 0
    accounts_processed = 0
    live_records: dict[str, set[str]] = {}
    interrupted = False

    inbox_conn = _connect_mail_bridge_db(mail_cfg)
    try:
        accounts = _mail_account_scope(inbox_conn, include_accounts=mail_cfg.include_accounts)
        active_accounts = set(accounts)
        for account_email in accounts:
            if should_stop(deadline, budget):
                interrupted = True
                break
            cursor = None if full_scan else _mail_sync_cursor(
                conn,
                bridge_key=bridge_key,
                account_email=account_email,
            )
            records = _fetch_mail_records(
                inbox_conn,
                account_email=account_email,
                import_summary=mail_cfg.import_summary,
                cursor=cursor,
            )
            if full_scan:
                live_records[account_email] = {record.msg_id for record in records}

            accounts_processed += 1
            max_cursor = ("", "")

            if dry_run:
                for record in records:
                    if should_stop(deadline, budget):
                        interrupted = True
                        break
                    dates_json, primary_date = _mail_dates_payload(record.date_iso)
                    checksum = _mail_checksum(record, primary_date=primary_date, dates_json=dates_json)
                    filepath = _mail_source_filepath(record.msg_id)
                    new_snapshot = (
                        checksum,
                        MAIL_BRIDGE_SOURCE,
                        record.msg_id,
                        record.account_email,
                        record.thread_id,
                        record.date_iso,
                        record.from_addr,
                        record.to_addr,
                        record.subject,
                        record.snippet,
                        record.body_text,
                        record.labels_json,
                        record.summary_text,
                        primary_date,
                        dates_json,
                    )
                    if _mail_registry_snapshot(conn, filepath=filepath) != new_snapshot:
                        if budget is not None and not budget.consume():
                            interrupted = True
                            break
                        updated += 1
                if interrupted:
                    break
                continue

            try:
                conn.execute("BEGIN")
                for record in records:
                    if should_stop(deadline, budget):
                        interrupted = True
                        break
                    dates_json, primary_date = _mail_dates_payload(record.date_iso)
                    checksum = _mail_checksum(record, primary_date=primary_date, dates_json=dates_json)
                    filepath = _mail_source_filepath(record.msg_id)
                    new_snapshot = (
                        checksum,
                        MAIL_BRIDGE_SOURCE,
                        record.msg_id,
                        record.account_email,
                        record.thread_id,
                        record.date_iso,
                        record.from_addr,
                        record.to_addr,
                        record.subject,
                        record.snippet,
                        record.body_text,
                        record.labels_json,
                        record.summary_text,
                        primary_date,
                        dates_json,
                    )
                    if _mail_registry_snapshot(conn, filepath=filepath) == new_snapshot:
                        continue
                    if budget is not None and not budget.consume():
                        interrupted = True
                        break
                    upsert_mail(
                        conn,
                        record=record,
                        checksum=checksum,
                        primary_date=primary_date,
                        dates_json=dates_json,
                    )
                    updated += 1
                    max_cursor = (record.material_updated_at, record.msg_id)
                if full_scan:
                    _store_mail_sync_cursor(
                        conn,
                        bridge_key=bridge_key,
                        account_email=account_email,
                        last_material_updated_at=max_cursor[0],
                        last_material_msg_id=max_cursor[1],
                    )
                elif max_cursor != ("", ""):
                    _store_mail_sync_cursor(
                        conn,
                        bridge_key=bridge_key,
                        account_email=account_email,
                        last_material_updated_at=max_cursor[0],
                        last_material_msg_id=max_cursor[1],
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            if interrupted:
                break

        if full_scan and not dry_run and not interrupted:
            conn.execute("BEGIN")
            pruned += _prune_mail_registry(
                conn,
                live_records=live_records,
                active_accounts=active_accounts,
            )
            pruned += _prune_mail_sync_state(
                conn,
                bridge_key=bridge_key,
                active_accounts=active_accounts,
            )
            conn.commit()
    finally:
        inbox_conn.close()

    if verbose and mail_cfg.enabled:
        print(
            f"[mail-bridge] [accounts_processed={accounts_processed}] [updated={updated}] [pruned={pruned}]",
            flush=True,
        )
    return updated, pruned, accounts_processed


def _normalize_date_value(year: int, month: int, day: int) -> str | None:
    try:
        return datetime(year, month, day, tzinfo=timezone.utc).date().isoformat()
    except ValueError:
        return None


def _date_kind_from_context(context: str, *, default: str = "mentioned_date") -> str:
    lowered = (context or "").strip().lower()
    hints = [
        (("due", "respond by", "reply by", "pay by", "deadline"), "due_date"),
        (("bill", "billing", "invoice", "statement"), "billing_date"),
        (("issued", "issue date", "dated", "sent", "mailed"), "issued_date"),
        (("service", "visit", "appointment", "treatment"), "service_date"),
        (("expires", "expiration", "expiry"), "expiry_date"),
    ]
    for needles, label in hints:
        if any(needle in lowered for needle in needles):
            return label
    return default


def extract_relevant_dates_from_text(
    text: str,
    *,
    source: str,
    default_kind: str = "mentioned_date",
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    raw_text = text or ""
    patterns = [
        ("iso", DATE_PATTERN_ISO),
        ("us", DATE_PATTERN_US),
        ("month_name", DATE_PATTERN_MONTH_NAME),
        ("day_month_name", DATE_PATTERN_DAY_MONTH_NAME),
    ]
    for pattern_name, pattern in patterns:
        for match in pattern.finditer(raw_text):
            value: str | None = None
            if pattern_name == "iso":
                value = _normalize_date_value(
                    int(match.group(1)),
                    int(match.group(2)),
                    int(match.group(3)),
                )
            elif pattern_name == "us":
                value = _normalize_date_value(
                    int(match.group(3)),
                    int(match.group(1)),
                    int(match.group(2)),
                )
            elif pattern_name == "month_name":
                value = _normalize_date_value(
                    int(match.group(3)),
                    int(MONTH_NAME_TO_NUM[match.group(1).lower()]),
                    int(match.group(2)),
                )
            elif pattern_name == "day_month_name":
                value = _normalize_date_value(
                    int(match.group(3)),
                    int(MONTH_NAME_TO_NUM[match.group(2).lower()]),
                    int(match.group(1)),
                )
            if not value:
                continue
            start = max(0, match.start() - 36)
            end = min(len(raw_text), match.end() + 36)
            kind = _date_kind_from_context(raw_text[start:end], default=default_kind)
            key = (value, kind, source)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "value": value,
                    "kind": kind,
                    "source": source,
                    "raw": match.group(0),
                }
            )
    return out


def _parse_photo_capture_datetime(path: Path) -> tuple[str | None, str | None]:
    if Image is None:
        return None, None
    try:
        with Image.open(path) as img:
            exif = img.getexif()
            if not exif:
                return None, None
            for tag in (36867, 36868, 306):  # DateTimeOriginal, DateTimeDigitized, DateTime
                raw = exif.get(tag)
                if not raw:
                    continue
                cleaned = str(raw).strip()
                try:
                    parsed = datetime.strptime(cleaned, "%Y:%m:%d %H:%M:%S").replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    continue
                return parsed.isoformat(), "exif"
    except Exception:
        return None, None
    return None, None


def _select_primary_date(entries: list[dict[str, str]]) -> str:
    if not entries:
        return ""
    priority = {
        "date_taken": 0,
        "due_date": 1,
        "billing_date": 2,
        "issued_date": 3,
        "service_date": 4,
        "expiry_date": 5,
        "mentioned_date": 6,
        "file_mtime": 7,
    }
    ranked = sorted(
        entries,
        key=lambda item: (
            priority.get(str(item.get("kind") or ""), 99),
            str(item.get("value") or ""),
        ),
    )
    return str(ranked[0].get("value") or "")


def _serialize_dates(entries: list[dict[str, str]]) -> str:
    if not entries:
        return "[]"
    return json.dumps(entries, ensure_ascii=False)


def _extract_doc_dates(text: str) -> tuple[str, str]:
    entries = extract_relevant_dates_from_text(text, source="text_content")
    return _serialize_dates(entries), _select_primary_date(entries)


def _extract_photo_dates(
    path: Path,
    *,
    caption: str,
    notes: str,
    analyzer_raw: str,
    fallback_mtime: float,
) -> tuple[str, str, str]:
    entries: list[dict[str, str]] = []
    date_taken, date_taken_source = _parse_photo_capture_datetime(path)
    if date_taken:
        entries.append(
            {
                "value": date_taken,
                "kind": "date_taken",
                "source": str(date_taken_source or "metadata"),
                "raw": str(date_taken),
            }
        )
    else:
        fallback_dt = datetime.fromtimestamp(fallback_mtime, tz=timezone.utc).isoformat()
        date_taken = fallback_dt
        entries.append(
            {
                "value": fallback_dt,
                "kind": "file_mtime",
                "source": "file_mtime",
                "raw": fallback_dt,
            }
        )

    text_blobs = [
        ("caption", caption, "mentioned_date"),
        ("notes", notes, "mentioned_date"),
        ("analyzer_raw", analyzer_raw, "mentioned_date"),
    ]
    for source_name, blob, default_kind in text_blobs:
        if not blob:
            continue
        entries.extend(
            extract_relevant_dates_from_text(
                blob,
                source=source_name,
                default_kind=default_kind,
            )
        )
    return date_taken, _serialize_dates(entries), _select_primary_date(entries)


def should_stop(deadline: float, budget: WorkBudget | None = None) -> bool:
    return time.monotonic() >= deadline or (budget.exhausted() if budget is not None else False)


def log_verbose(cfg: Config, message: str) -> None:
    if cfg.verbose:
        print(f"[sync] {message}", flush=True)


def _format_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "unknown"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _estimate_eta(elapsed_seconds: float, completed: int, total: int) -> float | None:
    if completed <= 0 or total <= 0 or completed >= total:
        return None
    return (elapsed_seconds / completed) * max(0, total - completed)


def _should_emit_progress(
    *,
    verbose: bool,
    now_mono: float,
    last_emit_mono: float,
    completed: int,
    total: int,
    force: bool = False,
) -> bool:
    if force or verbose:
        return True
    if completed <= 1:
        return True
    if total > 0 and completed >= total:
        return True
    if completed % 25 == 0:
        return True
    return (now_mono - last_emit_mono) >= PROGRESS_HEARTBEAT_SECONDS


def emit_sync_progress(
    *,
    stage: str,
    stage_done: int,
    stage_total: int,
    action: str,
    started_mono: float,
    last_emit_mono: float,
    verbose: bool,
    counters: dict[str, int],
    force: bool = False,
) -> float:
    now_mono = time.monotonic()
    if not _should_emit_progress(
        verbose=verbose,
        now_mono=now_mono,
        last_emit_mono=last_emit_mono,
        completed=stage_done,
        total=stage_total,
        force=force,
    ):
        return last_emit_mono

    elapsed = max(0.0, now_mono - started_mono)
    eta = _estimate_eta(elapsed, stage_done, stage_total)
    total_text = str(max(stage_total, 0))
    print(
        "[progress] "
        f"[stage={stage}] "
        f"[item={stage_done}/{total_text}] "
        f"[action={action}] "
        f"[elapsed={elapsed:.1f}s] "
        f"[eta={_format_eta(eta)}] "
        f"[docs_indexed={counters['docs_indexed']}] "
        f"[photos_indexed={counters['photos_indexed']}] "
        f"[summary_updated={counters['summary_updated']}] "
        f"[skipped={counters['skipped']}] "
        f"[errors={counters['errors']}]",
        flush=True,
    )
    return now_mono


def emit_stage_progress(
    *,
    stage: str,
    stage_done: int,
    stage_total: int,
    action: str,
    started_mono: float,
    last_emit_mono: float,
    verbose: bool,
    force: bool = False,
) -> float:
    now_mono = time.monotonic()
    if not _should_emit_progress(
        verbose=verbose,
        now_mono=now_mono,
        last_emit_mono=last_emit_mono,
        completed=stage_done,
        total=stage_total,
        force=force,
    ):
        return last_emit_mono

    elapsed = max(0.0, now_mono - started_mono)
    eta = _estimate_eta(elapsed, stage_done, stage_total)
    total_text = str(max(stage_total, 0))
    print(
        "[progress] "
        f"[stage={stage}] "
        f"[item={stage_done}/{total_text}] "
        f"[action={action}] "
        f"[elapsed={elapsed:.1f}s] "
        f"[eta={_format_eta(eta)}]",
        flush=True,
    )
    return now_mono


def iter_files(root: Path) -> Iterable[Path]:
    if not root.exists() or not root.is_dir():
        return
    for p in sorted(root.rglob("*")):
        if p.is_file():
            yield p


def classify_ext(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in DOC_EXTS:
        return "doc"
    if ext in PHOTO_EXTS:
        return "photo"
    return "unknown"


def _looks_like_document_name(path: Path) -> Optional[str]:
    stem = path.stem.lower().replace("_", " ").replace("-", " ")
    tokens = set(re.findall(r"[a-z0-9]+", stem))
    for phrase in DOC_IMAGE_HINT_PHRASES:
        if phrase in stem:
            return f"name-phrase:{phrase}"
    for tok in DOC_IMAGE_HINT_TOKENS:
        if tok in tokens:
            return f"name-token:{tok}"
    return None


def _looks_like_document_pixels(path: Path) -> Optional[str]:
    if Image is None or ImageStat is None:
        return None

    try:
        # Keep image IO cheap: downsample before extracting simple stats.
        with Image.open(path) as img:
            img = img.convert("RGB")
            img.thumbnail((512, 512))
            w, h = img.size
            if w == 0 or h == 0:
                return None

            gray = img.convert("L")
            gray_stats = ImageStat.Stat(gray)
            rgb_stats = ImageStat.Stat(img)
            mean_luma = float(gray_stats.mean[0])
            std_luma = float(gray_stats.stddev[0])
            color_std = float(sum(rgb_stats.stddev) / 3.0)
            ratio = max(w, h) / float(min(w, h))

            # Heuristics tuned for scanned pages/IDs/receipts: bright, low color variance,
            # and either document-like aspect ratio or receipt-like tall aspect.
            if ratio >= 2.0 and mean_luma >= 145 and color_std <= 38:
                return f"image-heuristic:tall-bright ratio={ratio:.2f}"
            if 1.2 <= ratio <= 1.8 and mean_luma >= 165 and color_std <= 45 and std_luma <= 70:
                return f"image-heuristic:page-like ratio={ratio:.2f}"
            if mean_luma >= 190 and color_std <= 30 and std_luma <= 60:
                return "image-heuristic:flat-bright"
    except Exception:
        return None

    return None


def classify_inbox_kind(path: Path, photo_client: LocalPhotoAnalyzerClient | None = None) -> tuple[str, str]:
    kind = classify_ext(path)
    ext = path.suffix.lower() or "(none)"
    if kind == "doc":
        return "doc", f"extension:{ext}"
    if kind == "unknown":
        return "unknown", f"extension:{ext}"

    # Primary classifier: local photo analyzer endpoint.
    if photo_client is not None and photo_client.cfg.enabled:
        analyzed = photo_client.analyze(path)
        if analyzed.status == "ok":
            if analyzed.route_kind == "doc":
                return "doc", f"image-doc:analyzer category={analyzed.category_primary}"
            return "photo", f"image-photo:analyzer taxonomy={analyzed.taxonomy} category={analyzed.category_primary}"

    # Fallback classifier: cheap local heuristics.
    name_reason = _looks_like_document_name(path)
    if name_reason:
        return "doc", f"image-doc:{name_reason}"

    pixel_reason = _looks_like_document_pixels(path)
    if pixel_reason:
        return "doc", f"image-doc:{pixel_reason}"

    return "photo", f"image-photo:default extension:{ext}"


def _extract_pdf_text_pdftotext(path: Path) -> tuple[str, str, int, bool]:
    proc = subprocess.run(
        ["pdftotext", "-q", str(path), "-"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"pdftotext failed: {proc.stderr.strip() or proc.stdout.strip()}")
    text = proc.stdout or ""
    return text, "pdftotext", len(text), False


def _extract_pdf_text_service(path: Path, cfg: PdfParseConfig) -> tuple[str, str, int, bool]:
    if not cfg.enabled:
        raise RuntimeError("PDF parse service disabled")

    fields = {
        "profile": cfg.profile,
        "include_json": "false",
        "ocr_language": "eng",
        "timeout_seconds": str(cfg.timeout_seconds),
    }
    body, boundary = _encode_multipart_form(fields, "file", path)
    req = urllib.request.Request(
        cfg.parse_url,
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=max(1, int(cfg.timeout_seconds))) as resp:
        raw = resp.read().decode("utf-8", errors="replace")

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise RuntimeError("PDF parse response must be an object")

    outputs = data.get("outputs") if isinstance(data.get("outputs"), dict) else {}
    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
    text = str(outputs.get("text") or outputs.get("markdown") or "").strip()
    parser = str(pipeline.get("parser") or "pdf-service")
    ocr_used = bool(pipeline.get("ocr_used"))
    return text, parser, len(text), ocr_used


def _pdf_service_base_url(parse_url: str) -> str:
    url = (parse_url or "").rstrip("/")
    suffix = "/pdf/parse"
    if url.endswith(suffix):
        return url[: -len(suffix)]
    return url


def _extract_pdf_text_openclaw(path: Path, cfg: PdfParseConfig | None = None) -> tuple[str, str, int, bool]:
    cmd = ["openclaw-pdf", "parse", str(path)]
    if cfg is not None:
        cmd += [
            "--base-url",
            _pdf_service_base_url(cfg.parse_url),
            "--profile",
            cfg.profile,
            "--timeout",
            str(cfg.timeout_seconds),
            "--raw",
        ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"openclaw-pdf parse failed: {proc.stderr.strip() or proc.stdout.strip()}")

    data = json.loads(proc.stdout)
    outputs = data.get("outputs", {})
    text = outputs.get("text") or outputs.get("markdown") or ""
    pipeline = data.get("pipeline", {})
    parser = str(pipeline.get("parser") or "openclaw-pdf")
    ocr_used = bool(pipeline.get("ocr_used"))
    return text, parser, len(text), ocr_used


def _is_sparse_pdf_text(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", (text or "")).strip()
    if len(normalized) >= 160:
        return False
    alnum = sum(ch.isalnum() for ch in normalized)
    return alnum < 70


def extract_doc_text(path: Path, pdf_cfg: PdfParseConfig) -> tuple[str, str, int, bool]:
    """Return text, parser, text_chars_total, ocr_used.

    Policy for PDFs:
    - Prefer cheap direct extraction (`pdftotext`) when text is already searchable.
    - Only invoke heavier `openclaw-pdf` path when direct extraction is sparse/low-signal
      (likely scanned/image PDFs) or when `pdftotext` is unavailable.
    """
    ext = path.suffix.lower()
    if ext == ".pdf":
        openclaw_pdf_available = shutil.which("openclaw-pdf") is not None
        pdftotext_available = shutil.which("pdftotext") is not None
        pdf_service_enabled = bool(pdf_cfg.enabled and pdf_cfg.parse_url)

        # First pass: keep already-searchable PDFs on the fast parser.
        if pdftotext_available:
            try:
                base_text, base_parser, base_chars, _ = _extract_pdf_text_pdftotext(path)
            except Exception:
                base_text, base_parser, base_chars = "", "pdftotext-error", 0

            if base_parser == "pdftotext" and not _is_sparse_pdf_text(base_text):
                return base_text, base_parser, base_chars, False

            # Sparse/low-signal output: escalate to PDF service first, then CLI wrapper if available.
            if pdf_service_enabled:
                try:
                    rich_text, rich_parser, rich_chars, rich_ocr = _extract_pdf_text_service(path, pdf_cfg)
                    if rich_chars > base_chars and not _is_sparse_pdf_text(rich_text):
                        return rich_text, rich_parser, rich_chars, rich_ocr
                except Exception:
                    pass
            if openclaw_pdf_available:
                try:
                    rich_text, rich_parser, rich_chars, rich_ocr = _extract_pdf_text_openclaw(path, pdf_cfg)
                    if rich_chars > base_chars and not _is_sparse_pdf_text(rich_text):
                        return rich_text, rich_parser, rich_chars, rich_ocr
                except Exception:
                    pass

            # If escalation failed or didn't materially improve, keep direct extraction result.
            if base_parser == "pdftotext":
                return base_text, base_parser, base_chars, False

        # Fallback: no pdftotext available, use PDF service first, then CLI wrapper.
        if pdf_service_enabled:
            return _extract_pdf_text_service(path, pdf_cfg)
        if openclaw_pdf_available:
            return _extract_pdf_text_openclaw(path, pdf_cfg)

        raise RuntimeError(
            "no PDF parser available (need pdftotext, local PDF parse service, or openclaw-pdf)"
        )

    if ext in {".txt", ".md", ".rtf"}:
        text = path.read_text(encoding="utf-8", errors="replace")
        return text, "plain", len(text), False

    # Minimal fallback for office docs in this first version.
    return "", "unsupported-doc-binary", 0, False


def extract_first_json(text: str) -> dict[str, Any] | None:
    in_string = False
    escaped = False
    depth = 0
    start: int | None = None

    for idx, char in enumerate(text or ""):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue

        if char == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue

        if char == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start is not None:
                candidate = text[start : idx + 1]
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    start = None
                    continue
                if isinstance(parsed, dict):
                    return parsed
                start = None

    return None


def _coerce_chat_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            if isinstance(item, str):
                out.append(item)
                continue
            if isinstance(item, dict):
                txt = item.get("text")
                if isinstance(txt, str):
                    out.append(txt)
                    continue
                content = item.get("content")
                if isinstance(content, str):
                    out.append(content)
        return "".join(out)
    if isinstance(value, dict):
        for key in ("text", "content", "value"):
            txt = value.get(key)
            if isinstance(txt, str):
                return txt
    return ""


def _extract_choice_text(choice: dict[str, Any]) -> str:
    message = choice.get("message") if isinstance(choice, dict) else None
    if not isinstance(message, dict):
        message = {}

    content_text = _coerce_chat_text(message.get("content"))
    if content_text.strip():
        return content_text

    direct_text = _coerce_chat_text(choice.get("text"))
    if direct_text.strip():
        return direct_text

    # Deliberately ignore reasoning_content to avoid leaking model thinking traces
    # into persisted summaries.
    return ""


class LocalOpenAIChatClient:
    def __init__(self, cfg: SummaryConfig):
        self.cfg = cfg
        self.chat_url = f"{cfg.base_url.rstrip('/')}" + "/chat/completions"

    def _post_chat(self, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.cfg.api_key:
            headers["Authorization"] = f"Bearer {self.cfg.api_key}"

        req = urllib.request.Request(self.chat_url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=max(1, int(self.cfg.timeout_seconds))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            err = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            raise RuntimeError(f"summary chat HTTP {exc.code}: {err[:400]}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"summary chat connection failed: {exc}") from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"summary chat response is not JSON: {raw[:300]}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("summary chat response must be a JSON object")
        return parsed

    def chat_text(
        self,
        messages: list[dict[str, str]],
        *,
        max_tokens: int,
        temperature: float,
        response_format: dict[str, Any] | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "model": self.cfg.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max(80, int(max_tokens)),
            # Qwen/llama.cpp style switch: keep reasoning in content channel off.
            "chat_template_kwargs": {"enable_thinking": False},
        }
        if response_format is not None:
            payload["response_format"] = response_format

        try:
            body = self._post_chat(payload)
        except RuntimeError as exc:
            err = str(exc)
            # Some OpenAI-compatible servers reject response_format.
            if response_format is not None and "HTTP 400" in err:
                payload.pop("response_format", None)
                try:
                    body = self._post_chat(payload)
                except RuntimeError as exc2:
                    err2 = str(exc2)
                    # Some servers also reject chat_template_kwargs.
                    if "HTTP 400" in err2:
                        payload.pop("chat_template_kwargs", None)
                        body = self._post_chat(payload)
                    else:
                        raise
            elif "HTTP 400" in err:
                payload.pop("chat_template_kwargs", None)
                body = self._post_chat(payload)
            else:
                raise

        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("summary chat response missing choices")
        first = choices[0]
        if not isinstance(first, dict):
            raise RuntimeError("summary chat response invalid first choice")

        text = _extract_choice_text(first)
        if not text.strip():
            raise RuntimeError("summary chat returned empty text")
        return text

    def chat_json(self, messages: list[dict[str, str]], *, max_tokens: int, temperature: float) -> dict[str, Any] | None:
        text = self.chat_text(
            messages,
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        parsed = extract_first_json(text)
        if parsed is not None:
            return parsed

        retry_messages = [
            *messages,
            {
                "role": "user",
                "content": (
                    "Your previous reply was not valid JSON. "
                    "Return exactly one JSON object with key summary. "
                    "No markdown, no prose, no code fences."
                ),
            },
        ]
        retry_text = self.chat_text(
            retry_messages,
            max_tokens=max_tokens,
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        return extract_first_json(retry_text)


def _truncate(text: str, max_chars: int) -> str:
    clean = " ".join((text or "").split()).strip()
    if len(clean) <= max_chars:
        return clean
    return clean[: max(1, max_chars - 1)].rstrip() + "…"


def _clean_summary_candidate(text: str, max_chars: int) -> str:
    clean = _truncate(text or "", max_chars)
    if not clean:
        return ""

    # Remove common meta/instructional leakage from weaker local model outputs.
    patterns = [
        r"^okay,?\s+let'?s\s+tackle\s+this\s+query\.?\s*",
        r"^the\s+user\s+wants\s+.*?(?=\.|$)\.?\s*",
        r"^return\s+only\s+plain\s+text.*?(?=\.|$)\.?\s*",
    ]
    lowered = clean.lower()
    for pat in patterns:
        cleaned2 = re.sub(pat, "", lowered, flags=re.IGNORECASE)
        if cleaned2 != lowered:
            # apply same removal to original-case text for readability
            clean = re.sub(pat, "", clean, flags=re.IGNORECASE).strip()
            break

    # If output is still mostly prompt-instruction text, reject.
    bad_markers = ["the user wants", "return only plain text", "json", "no markdown"]
    marker_hits = sum(1 for m in bad_markers if m in clean.lower())
    if marker_hits >= 2:
        return ""

    return _truncate(clean, max_chars)


def _fallback_summary_from_text(source_text: str, max_chars: int) -> str:
    clean = " ".join((source_text or "").split())
    if not clean:
        return ""
    sentence_like = re.split(r"(?<=[.!?])\s+", clean)
    summary = " ".join(sentence_like[:2]).strip() or clean
    return _truncate(summary, max_chars)


def summarize_doc_text(chat_client: LocalOpenAIChatClient, cfg: SummaryConfig, text: str, filepath: str) -> SummaryResult:
    source = (text or "").strip()
    if not source:
        return SummaryResult(text="", status="empty-source", error="")

    clipped_source = source[: cfg.max_input_chars]
    messages = [
        {
            "role": "system",
            "content": (
                "You summarize extracted document text for local retrieval. "
                "Return a concise factual summary with concrete entities/dates when available. "
                "Do not invent facts."
            ),
        },
        {
            "role": "user",
            "content": (
                "Return JSON with exactly one key: summary. "
                f"summary must be <= {cfg.max_output_chars} characters, one paragraph, and useful for semantic search.\n"
                "If the text is noisy OCR, still extract core signal (document type, parties, amounts, dates).\n"
                f"File: {Path(filepath).name}\n"
                f"Document text:\n{clipped_source}"
            ),
        },
    ]

    try:
        parsed = chat_client.chat_json(messages, max_tokens=420, temperature=0.0)
    except Exception as exc:  # noqa: BLE001
        return SummaryResult(text="", status="error", error=str(exc)[:500])

    if isinstance(parsed, dict):
        candidate = parsed.get("summary")
        if not isinstance(candidate, str):
            # tolerate weak schema adherence
            for key in ("abstract", "text", "result"):
                value = parsed.get(key)
                if isinstance(value, str):
                    candidate = value
                    break
        if isinstance(candidate, str) and candidate.strip():
            cleaned = _clean_summary_candidate(candidate, cfg.max_output_chars)
            if cleaned:
                return SummaryResult(text=cleaned, status="ok", error="")

    # Secondary fallback: ask for plain text summary directly (handles weak JSON adherence).
    try:
        plain_messages = [
            messages[0],
            {
                "role": "user",
                "content": (
                    f"Return only plain summary text (no JSON) <= {cfg.max_output_chars} chars. "
                    "One paragraph, factual, no markdown.\n"
                    f"File: {Path(filepath).name}\n"
                    f"Document text:\n{clipped_source}"
                ),
            },
        ]
        plain = chat_client.chat_text(plain_messages, max_tokens=320, temperature=0.0, response_format=None)
        plain = _clean_summary_candidate(plain, cfg.max_output_chars)
        if plain.strip():
            return SummaryResult(text=plain, status="ok-plain", error="non-json-llm-output")
    except Exception:
        pass

    # Last resort: heuristic extraction from source text.
    fallback = _fallback_summary_from_text(source, cfg.max_output_chars)
    if fallback:
        return SummaryResult(text=fallback, status="fallback-text", error="non-json-llm-output")
    return SummaryResult(text="", status="error", error="missing-summary-field")


def upsert_doc(
    conn: sqlite3.Connection,
    *,
    checksum: str,
    filepath: Path,
    source: str,
    text_content: str,
    text_chars_total: int,
    text_capped: bool,
    parser: str,
    ocr_used: bool,
    extraction_method: str,
    summary_text: str,
    summary_model: str,
    summary_hash: str,
    summary_status: str,
    summary_updated_at: str,
    summary_error: str,
    dates_json: str,
    primary_date: str,
    size: int,
    mtime: float,
) -> None:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO docs_registry (
          checksum, filepath, source, text_content, text_chars_total, text_capped,
          parser, ocr_used, extraction_method,
          summary_text, summary_model, summary_hash, summary_status, summary_updated_at, summary_error,
          dates_json, primary_date, size, mtime, indexed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(filepath) DO UPDATE SET
          checksum=excluded.checksum,
          source=excluded.source,
          text_content=excluded.text_content,
          text_chars_total=excluded.text_chars_total,
          text_capped=excluded.text_capped,
          parser=excluded.parser,
          ocr_used=excluded.ocr_used,
          extraction_method=excluded.extraction_method,
          summary_text=excluded.summary_text,
          summary_model=excluded.summary_model,
          summary_hash=excluded.summary_hash,
          summary_status=excluded.summary_status,
          summary_updated_at=excluded.summary_updated_at,
          summary_error=excluded.summary_error,
          dates_json=excluded.dates_json,
          primary_date=excluded.primary_date,
          size=excluded.size,
          mtime=excluded.mtime,
          updated_at=excluded.updated_at
        """,
        (
            checksum,
            str(filepath),
            source,
            text_content,
            text_chars_total,
            1 if text_capped else 0,
            parser,
            1 if ocr_used else 0,
            extraction_method,
            summary_text,
            summary_model,
            summary_hash,
            summary_status,
            summary_updated_at,
            summary_error,
            dates_json,
            primary_date,
            size,
            mtime,
            ts,
            ts,
        ),
    )


def upsert_photo(
    conn: sqlite3.Connection,
    *,
    checksum: str,
    filepath: Path,
    source: str,
    date_taken: Optional[str],
    size: int,
    mtime: float,
    category_primary: str,
    category_secondary: str,
    taxonomy: str,
    caption: str,
    analyzer_model: str,
    analyzer_status: str,
    analyzer_error: str,
    analyzer_raw: str,
    ocr_text: str,
    ocr_status: str,
    ocr_source: str,
    ocr_updated_at: str,
    dates_json: str,
    primary_date: str,
) -> None:
    ts = now_iso()
    note_parts = []
    if taxonomy:
        note_parts.append(f"taxonomy:{taxonomy}")
    if category_primary:
        note_parts.append(f"category:{category_primary}")
    if caption:
        note_parts.append(f"caption:{_truncate(caption, 240)}")
    notes = " | ".join(note_parts)

    conn.execute(
        """
        INSERT INTO photos_registry (
          checksum, filepath, source, date_taken, size, mtime, indexed_at, updated_at, notes,
          category_primary, category_secondary, taxonomy, caption,
          analyzer_model, analyzer_status, analyzer_error, analyzer_raw, analyzed_at,
          ocr_text, ocr_status, ocr_source, ocr_updated_at,
          dates_json, primary_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(filepath) DO UPDATE SET
          checksum=excluded.checksum,
          source=excluded.source,
          date_taken=excluded.date_taken,
          size=excluded.size,
          mtime=excluded.mtime,
          updated_at=excluded.updated_at,
          notes=excluded.notes,
          category_primary=excluded.category_primary,
          category_secondary=excluded.category_secondary,
          taxonomy=excluded.taxonomy,
          caption=excluded.caption,
          analyzer_model=excluded.analyzer_model,
          analyzer_status=excluded.analyzer_status,
          analyzer_error=excluded.analyzer_error,
          analyzer_raw=excluded.analyzer_raw,
          analyzed_at=excluded.analyzed_at,
          ocr_text=excluded.ocr_text,
          ocr_status=excluded.ocr_status,
          ocr_source=excluded.ocr_source,
          ocr_updated_at=excluded.ocr_updated_at,
          dates_json=excluded.dates_json,
          primary_date=excluded.primary_date
        """,
        (
            checksum,
            str(filepath),
            source,
            date_taken,
            size,
            mtime,
            ts,
            ts,
            notes,
            category_primary,
            category_secondary,
            taxonomy,
            caption,
            analyzer_model,
            analyzer_status,
            analyzer_error,
            analyzer_raw,
            ts,
            ocr_text,
            ocr_status,
            ocr_source,
            ocr_updated_at,
            dates_json,
            primary_date,
        ),
    )


def existing_by_filepath(conn: sqlite3.Connection, table: str, filepath: Path) -> Optional[tuple[float, int]]:
    row = conn.execute(
        f"SELECT mtime, size FROM {table} WHERE filepath = ?",
        (str(filepath),),
    ).fetchone()
    if not row:
        return None
    return float(row[0]), int(row[1])


def _has_dates_payload(conn: sqlite3.Connection, table: str, filepath: Path) -> bool:
    row = conn.execute(
        f"SELECT COALESCE(TRIM(dates_json), '') FROM {table} WHERE filepath = ?",
        (str(filepath),),
    ).fetchone()
    return bool(row and str(row[0] or "").strip())


def is_unchanged_source(conn: sqlite3.Connection, table: str, filepath: Path) -> bool:
    prior = existing_by_filepath(conn, table, filepath)
    if not prior:
        return False
    if not _has_dates_payload(conn, table, filepath):
        return False
    stat = filepath.stat()
    return int(prior[0]) == int(stat.st_mtime) and int(prior[1]) == int(stat.st_size)


def flush_sync_skip_batch(
    *,
    batch_count: int,
    batch_stage_done: int,
    stage: str,
    stage_total: int,
    started_mono: float,
    last_emit_mono: float,
    verbose: bool,
    counters: dict[str, int],
) -> tuple[float, int, int]:
    if batch_count <= 0:
        return last_emit_mono, 0, 0
    next_emit = emit_sync_progress(
        stage=stage,
        stage_done=batch_stage_done,
        stage_total=stage_total,
        action=f"skipping-already-processed count={batch_count}",
        started_mono=started_mono,
        last_emit_mono=last_emit_mono,
        verbose=verbose,
        counters=counters,
    )
    return next_emit, 0, 0


def route_inbox_file(
    path: Path,
    cfg: Config,
    dry_run: bool,
    *,
    photo_client: LocalPhotoAnalyzerClient | None,
) -> tuple[Path, str, str]:
    kind, reason = classify_inbox_kind(path, photo_client=photo_client)
    if kind == "doc":
        dest_root = cfg.docs_dest_root
    elif kind == "photo":
        dest_root = cfg.photos_dest_root
    else:
        return path, "unknown", reason

    stat = path.stat()
    dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    dest_dir = dest_root / f"{dt.year:04d}" / f"{dt.month:02d}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / path.name
    if dest.exists():
        # avoid overwrite
        stem = path.stem
        suffix = path.suffix
        n = 1
        while True:
            cand = dest_dir / f"{stem}.{n}{suffix}"
            if not cand.exists():
                dest = cand
                break
            n += 1

    if not dry_run:
        shutil.move(str(path), str(dest))

    return dest, kind, reason


def _build_summary_for_doc(
    text_capped: str,
    filepath: Path,
    summary_cfg: SummaryConfig,
    chat_client: LocalOpenAIChatClient | None,
) -> SummaryResult:
    if not summary_cfg.enabled or chat_client is None:
        return SummaryResult(text="", status="disabled", error="")
    return summarize_doc_text(chat_client, summary_cfg, text_capped, str(filepath))


def index_doc_file(
    conn: sqlite3.Connection,
    path: Path,
    source: str,
    text_cap: int,
    dry_run: bool,
    *,
    pdf_cfg: PdfParseConfig,
    summary_cfg: SummaryConfig,
    chat_client: LocalOpenAIChatClient | None,
    verbose: bool = False,
) -> DocIndexResult:
    stat = path.stat()
    prior = existing_by_filepath(conn, "docs_registry", path)
    if (
        prior
        and _has_dates_payload(conn, "docs_registry", path)
        and int(prior[0]) == int(stat.st_mtime)
        and int(prior[1]) == int(stat.st_size)
    ):
        return DocIndexResult(indexed=False, summary_updated=False, summary_failed=False)

    checksum = sha256_file(path)
    text, parser, total_chars, ocr_used = extract_doc_text(path, pdf_cfg)
    capped = len(text) > text_cap
    text_capped = text[:text_cap]
    summary_hash = _sha256_text(text_capped)
    dates_json, primary_date = _extract_doc_dates(text_capped)

    if verbose and summary_cfg.enabled and not dry_run:
        print("[progress] [stage=doc-summary] [action=start]", flush=True)
    summary = _build_summary_for_doc(text_capped, path, summary_cfg, chat_client)
    summary_updated = summary.status in {"ok", "fallback-text", "empty-source", "error"}
    summary_failed = summary.status == "error"
    if verbose and summary_cfg.enabled and not dry_run:
        print(
            f"[progress] [stage=doc-summary] [action=done status={summary.status}]",
            flush=True,
        )

    if not dry_run:
        upsert_doc(
            conn,
            checksum=checksum,
            filepath=path,
            source=source,
            text_content=text_capped,
            text_chars_total=total_chars,
            text_capped=capped,
            parser=parser,
            ocr_used=ocr_used,
            extraction_method=(parser if path.suffix.lower() == ".pdf" else "plain"),
            summary_text=summary.text,
            summary_model=(summary_cfg.model if summary_cfg.enabled else ""),
            summary_hash=summary_hash,
            summary_status=summary.status,
            summary_updated_at=now_iso(),
            summary_error=summary.error,
            dates_json=dates_json,
            primary_date=primary_date,
            size=stat.st_size,
            mtime=stat.st_mtime,
        )
    return DocIndexResult(indexed=True, summary_updated=summary_updated, summary_failed=summary_failed)


def index_photo_file(
    conn: sqlite3.Connection,
    path: Path,
    source: str,
    dry_run: bool,
    *,
    photo_client: LocalPhotoAnalyzerClient | None,
    force_analyze: bool = False,
) -> bool:
    stat = path.stat()
    prior = existing_by_filepath(conn, "photos_registry", path)
    prior_snapshot = photo_registry_snapshot(conn, path)
    if (
        (not force_analyze)
        and prior
        and _has_dates_payload(conn, "photos_registry", path)
        and int(prior[0]) == int(stat.st_mtime)
        and int(prior[1]) == int(stat.st_size)
    ):
        return False

    analyzed = (
        photo_client.analyze(path)
        if (photo_client is not None and photo_client.cfg.enabled)
        else PhotoAnalysisResult(
            status="disabled",
            route_kind="photo",
            taxonomy="misc",
            caption="",
            category_primary="",
            category_secondary="",
            analyzer_model="",
            analyzer_error="",
            analyzer_raw="",
            ocr_text="",
        )
    )
    ocr_text, ocr_status, ocr_source = _resolve_photo_ocr_fields(
        analyzer_status=analyzed.status,
        category_primary=analyzed.category_primary,
        taxonomy=analyzed.taxonomy,
        ocr_text=analyzed.ocr_text,
    )
    ocr_updated_at = now_iso()
    checksum = sha256_file(path)
    preview_notes = " | ".join(
        part
        for part in (
            f"taxonomy:{analyzed.taxonomy}" if analyzed.taxonomy else "",
            f"category:{analyzed.category_primary}" if analyzed.category_primary else "",
            f"caption:{_truncate(analyzed.caption, 240)}" if analyzed.caption else "",
        )
        if part
    )
    date_taken, dates_json, primary_date = _extract_photo_dates(
        path,
        caption=analyzed.caption,
        notes=preview_notes,
        analyzer_raw=analyzed.analyzer_raw,
        fallback_mtime=stat.st_mtime,
    )

    if not dry_run:
        new_snapshot = (
            str(checksum),
            str(source),
            str(date_taken or ""),
            str(stat.st_size),
            str(int(stat.st_mtime)),
            str(analyzed.category_primary or ""),
            str(analyzed.category_secondary or ""),
            str(analyzed.taxonomy or ""),
            str(analyzed.caption or ""),
            str(analyzed.analyzer_model or ""),
            str(analyzed.status or ""),
            str(analyzed.analyzer_error or ""),
            str(analyzed.analyzer_raw or ""),
            str(ocr_text or ""),
            str(ocr_status or ""),
            str(ocr_source or ""),
            str(dates_json or ""),
            str(primary_date or ""),
        )
        if prior_snapshot == new_snapshot:
            conn.execute(
                """
                UPDATE photos_registry
                SET analyzed_at = ?, ocr_updated_at = ?
                WHERE filepath = ?
                """,
                (ocr_updated_at, ocr_updated_at, str(path)),
            )
            return False
        upsert_photo(
            conn,
            checksum=checksum,
            filepath=path,
            source=source,
            date_taken=date_taken,
            size=stat.st_size,
            mtime=stat.st_mtime,
            category_primary=analyzed.category_primary,
            category_secondary=analyzed.category_secondary,
            taxonomy=analyzed.taxonomy,
            caption=analyzed.caption,
            analyzer_model=analyzed.analyzer_model,
            analyzer_status=analyzed.status,
            analyzer_error=analyzed.analyzer_error,
            analyzer_raw=analyzed.analyzer_raw,
            ocr_text=ocr_text,
            ocr_status=ocr_status,
            ocr_source=ocr_source,
            ocr_updated_at=ocr_updated_at,
            dates_json=dates_json,
            primary_date=primary_date,
        )
    return True


def backfill_missing_summaries(
    conn: sqlite3.Connection,
    *,
    summary_cfg: SummaryConfig,
    chat_client: LocalOpenAIChatClient | None,
    limit: int,
    deadline: float,
    budget: WorkBudget | None = None,
    verbose: bool = False,
) -> tuple[int, int]:
    if limit == 0 or not summary_cfg.enabled or chat_client is None:
        return 0, 0

    if limit < 0:
        rows = conn.execute(
            """
            SELECT filepath, text_content, summary_hash, summary_status
            FROM docs_registry
            WHERE summary_status IS NULL
               OR summary_status IN ('error', 'disabled', 'stale', 'fallback-text')
               OR (
                    COALESCE(TRIM(summary_text), '') = ''
                    AND COALESCE(TRIM(summary_status), '') = ''
               )
            ORDER BY updated_at DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT filepath, text_content, summary_hash, summary_status
            FROM docs_registry
            WHERE summary_status IS NULL
               OR summary_status IN ('error', 'disabled', 'stale', 'fallback-text')
               OR (
                    COALESCE(TRIM(summary_text), '') = ''
                    AND COALESCE(TRIM(summary_status), '') = ''
               )
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

    updated = 0
    failed = 0
    started_mono = time.monotonic()
    last_emit_mono = started_mono
    total_rows = len(rows)
    last_emit_mono = emit_stage_progress(
        stage="5/6.summary-backfill",
        stage_done=0,
        stage_total=total_rows,
        action="start",
        started_mono=started_mono,
        last_emit_mono=last_emit_mono,
        verbose=verbose,
        force=True,
    )
    for idx, (filepath, text_content, _summary_hash, _summary_status) in enumerate(rows, start=1):
        if should_stop(deadline, budget):
            break
        if budget is not None and not budget.consume():
            break
        text_seed = str(text_content or "")
        summary_hash = _sha256_text(text_seed)
        summary = summarize_doc_text(chat_client, summary_cfg, text_seed, str(filepath))
        conn.execute(
            """
            UPDATE docs_registry
            SET summary_text = ?,
                summary_model = ?,
                summary_hash = ?,
                summary_status = ?,
                summary_updated_at = ?,
                summary_error = ?,
                updated_at = ?
            WHERE filepath = ?
            """,
            (
                summary.text,
                summary_cfg.model,
                summary_hash,
                summary.status,
                now_iso(),
                summary.error,
                now_iso(),
                str(filepath),
            ),
        )
        conn.commit()
        updated += 1
        action = "updated"
        if summary.status == "error":
            failed += 1
            action = "updated-error"
        last_emit_mono = emit_stage_progress(
            stage="5/6.summary-backfill",
            stage_done=idx,
            stage_total=total_rows,
            action=action,
            started_mono=started_mono,
            last_emit_mono=last_emit_mono,
            verbose=verbose,
        )

    return updated, failed


def backfill_missing_photo_analysis(
    conn: sqlite3.Connection,
    *,
    photo_client: LocalPhotoAnalyzerClient | None,
    limit: int,
    deadline: float,
    verbose: bool,
    budget: WorkBudget | None = None,
) -> tuple[int, int]:
    if limit == 0 or photo_client is None or not photo_client.cfg.enabled:
        return 0, 0

    if limit < 0:
        rows = conn.execute(
            """
            SELECT filepath, source
            FROM photos_registry
            WHERE COALESCE(TRIM(category_primary), '') = ''
               OR COALESCE(TRIM(taxonomy), '') = ''
               OR COALESCE(TRIM(caption), '') = ''
               OR (
                    (
                      LOWER(COALESCE(TRIM(category_primary), '')) IN ('document', 'receipt')
                      OR LOWER(COALESCE(TRIM(taxonomy), '')) = 'docs'
                    )
                    AND LOWER(COALESCE(TRIM(ocr_status), '')) IN ('', 'empty')
                  )
               OR COALESCE(TRIM(analyzer_status), '') IN ('', 'error', 'disabled')
            ORDER BY updated_at DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT filepath, source
            FROM photos_registry
            WHERE COALESCE(TRIM(category_primary), '') = ''
               OR COALESCE(TRIM(taxonomy), '') = ''
               OR COALESCE(TRIM(caption), '') = ''
               OR (
                    (
                      LOWER(COALESCE(TRIM(category_primary), '')) IN ('document', 'receipt')
                      OR LOWER(COALESCE(TRIM(taxonomy), '')) = 'docs'
                    )
                    AND LOWER(COALESCE(TRIM(ocr_status), '')) IN ('', 'empty')
                  )
               OR COALESCE(TRIM(analyzer_status), '') IN ('', 'error', 'disabled')
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

    updated = 0
    failed = 0
    started_mono = time.monotonic()
    last_emit_mono = started_mono
    total_rows = len(rows)
    last_emit_mono = emit_stage_progress(
        stage="6/6.photo-backfill",
        stage_done=0,
        stage_total=total_rows,
        action="start",
        started_mono=started_mono,
        last_emit_mono=last_emit_mono,
        verbose=verbose,
        force=True,
    )
    for idx, (filepath, source) in enumerate(rows, start=1):
        if should_stop(deadline, budget):
            break
        if budget is not None and not budget.consume():
            break
        action = "checked"
        try:
            p = Path(str(filepath))
            if not p.exists() or not p.is_file():
                failed += 1
                action = "missing-file"
                continue
            changed = index_photo_file(
                conn,
                p,
                str(source or "backfill/photos"),
                dry_run=False,
                photo_client=photo_client,
                force_analyze=True,
            )
            if changed:
                conn.commit()
                updated += 1
                action = "updated"
            row = conn.execute(
                "SELECT COALESCE(TRIM(analyzer_status), '') FROM photos_registry WHERE filepath = ?",
                (str(p),),
            ).fetchone()
            st = (row[0] if row else "")
            if st == "error":
                failed += 1
                action = "updated-error"
        except Exception:
            failed += 1
            action = "error"
        last_emit_mono = emit_stage_progress(
            stage="6/6.photo-backfill",
            stage_done=idx,
            stage_total=total_rows,
            action=action,
            started_mono=started_mono,
            last_emit_mono=last_emit_mono,
            verbose=verbose,
        )

    return updated, failed


def count_pending_summary_backfill(conn: sqlite3.Connection, limit: int) -> int:
    if limit == 0:
        return 0
    total = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM docs_registry
            WHERE summary_status IS NULL
               OR summary_status IN ('error', 'disabled', 'stale', 'fallback-text')
               OR (
                    COALESCE(TRIM(summary_text), '') = ''
                    AND COALESCE(TRIM(summary_status), '') = ''
               )
            """
        ).fetchone()[0]
    )
    if limit < 0:
        return total
    return min(total, int(limit))


def count_pending_photo_backfill(conn: sqlite3.Connection, limit: int) -> int:
    if limit == 0:
        return 0
    total = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM photos_registry
            WHERE COALESCE(TRIM(category_primary), '') = ''
               OR COALESCE(TRIM(taxonomy), '') = ''
               OR COALESCE(TRIM(caption), '') = ''
               OR (
                    (
                      LOWER(COALESCE(TRIM(category_primary), '')) IN ('document', 'receipt')
                      OR LOWER(COALESCE(TRIM(taxonomy), '')) = 'docs'
                    )
                    AND LOWER(COALESCE(TRIM(ocr_status), '')) IN ('', 'empty')
                  )
               OR COALESCE(TRIM(analyzer_status), '') IN ('', 'error', 'disabled')
            """
        ).fetchone()[0]
    )
    if limit < 0:
        return total
    return min(total, int(limit))


def photo_registry_snapshot(conn: sqlite3.Connection, filepath: Path) -> tuple[str, ...] | None:
    row = conn.execute(
        """
        SELECT
          checksum,
          source,
          COALESCE(date_taken, ''),
          size,
          CAST(mtime AS INTEGER),
          COALESCE(category_primary, ''),
          COALESCE(category_secondary, ''),
          COALESCE(taxonomy, ''),
          COALESCE(caption, ''),
          COALESCE(analyzer_model, ''),
          COALESCE(analyzer_status, ''),
          COALESCE(analyzer_error, ''),
          COALESCE(analyzer_raw, ''),
          COALESCE(ocr_text, ''),
          COALESCE(ocr_status, ''),
          COALESCE(ocr_source, ''),
          COALESCE(dates_json, ''),
          COALESCE(primary_date, '')
        FROM photos_registry
        WHERE filepath = ?
        """,
        (str(filepath),),
    ).fetchone()
    if not row:
        return None
    return tuple(str(value or "") for value in row)


def run(cfg: Config, dry_run: bool) -> int:
    started = now_iso()
    deadline = float("inf") if cfg.max_seconds <= 0 else (time.monotonic() + cfg.max_seconds)
    budget = WorkBudget.from_max_items(cfg.max_items)

    log_verbose(cfg, "pipeline start")
    log_verbose(
        cfg,
        f"mode={'dry-run' if dry_run else 'live'} "
        f"max_seconds={cfg.max_seconds if cfg.max_seconds > 0 else 'none'} "
        f"max_items={cfg.max_items if cfg.max_items > 0 else 'none'} "
        f"skip_inbox={cfg.skip_inbox}",
    )

    counters = {
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
    }

    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)

    lock_path = cfg.db_path.parent / "vault_registry_sync.lock"
    lock_file = lock_path.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        log_verbose(cfg, "lock acquired")
    except BlockingIOError:
        print("status: skipped (another sync run is active)")
        print("docs_indexed: 0")
        print("photos_indexed: 0")
        print("mail_indexed: 0")
        print("mail_pruned: 0")
        print("mail_accounts_processed: 0")
        print("summary_updated: 0")
        print("summary_failed: 0")
        print("inbox_routed: 0")
        print("skipped: 0")
        print("errors: 0")
        return 0

    conn = connect_vault_db(cfg.db_path, timeout=30.0, ensure_parent=True)
    ensure_db(conn)
    log_verbose(cfg, "db ready")
    started_mono = time.monotonic()
    last_progress_mono = started_mono
    selected_source_kinds = _selected_source_kinds(cfg)

    inbox_files = [] if cfg.skip_inbox else list(iter_files(cfg.inbox_scanner) or [])
    docs_stage_inputs: list[tuple[Path, list[Path]]] = []
    if "docs" in selected_source_kinds:
        for root in cfg.docs_roots:
            docs_stage_inputs.append(
                (root, [f for f in list(iter_files(root) or []) if classify_ext(f) == "doc"])
            )
    photos_stage_inputs: list[tuple[Path, list[Path]]] = []
    if "photos" in selected_source_kinds:
        for root in cfg.photos_roots:
            photos_stage_inputs.append(
                (root, [f for f in list(iter_files(root) or []) if classify_ext(f) == "photo"])
            )
    docs_total = sum(len(files) for _, files in docs_stage_inputs)
    photos_total = sum(len(files) for _, files in photos_stage_inputs)
    summary_backfill_total = (
        count_pending_summary_backfill(conn, cfg.summary_reprocess_missing_limit)
        if "docs" in selected_source_kinds
        else 0
    )
    photo_backfill_total = (
        count_pending_photo_backfill(conn, cfg.photo_reprocess_missing_limit)
        if "photos" in selected_source_kinds
        else 0
    )

    chat_client: LocalOpenAIChatClient | None = None
    if cfg.summary.enabled and not dry_run:
        chat_client = LocalOpenAIChatClient(cfg.summary)

    photo_client: LocalPhotoAnalyzerClient | None = None
    if cfg.photo_analysis.enabled and not dry_run:
        photo_client = LocalPhotoAnalyzerClient(cfg.photo_analysis)

    try:
        # 1) Process inbox scanner first (route then index)
        if cfg.skip_inbox or not ({"docs", "photos"} & selected_source_kinds):
            last_progress_mono = emit_sync_progress(
                stage="1/6.inbox-route",
                stage_done=0,
                stage_total=0,
                action="skipped",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
                force=True,
            )
        else:
            last_progress_mono = emit_sync_progress(
                stage="1/6.inbox-route",
                stage_done=0,
                stage_total=len(inbox_files),
                action="start",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
                force=True,
            )
            for idx, f in enumerate(inbox_files, start=1):
                if should_stop(deadline, budget):
                    break
                action = "processing"
                last_progress_mono = emit_sync_progress(
                    stage="1/6.inbox-route",
                    stage_done=idx,
                    stage_total=len(inbox_files),
                    action=action,
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=cfg.verbose,
                    counters=counters,
                )
                try:
                    kind, route_reason = classify_inbox_kind(f, photo_client=photo_client)
                    if kind == "unknown":
                        counters["skipped"] += 1
                        action = f"skipped-unknown reason={route_reason}"
                        continue
                    if kind == "doc" and "docs" not in selected_source_kinds:
                        counters["skipped"] += 1
                        action = "skipped-doc-source-disabled"
                        continue
                    if kind == "photo" and "photos" not in selected_source_kinds:
                        counters["skipped"] += 1
                        action = "skipped-photo-source-disabled"
                        continue
                    if budget is not None and not budget.consume():
                        break
                    routed_path, kind, route_reason = route_inbox_file(
                        f,
                        cfg,
                        dry_run,
                        photo_client=photo_client,
                    )
                    counters["inbox_routed"] += 1

                    if kind == "doc":
                        doc_result = index_doc_file(
                            conn,
                            routed_path,
                            "inbox/scanner",
                            cfg.text_cap,
                            dry_run,
                            pdf_cfg=cfg.pdf_parse,
                            summary_cfg=cfg.summary,
                            chat_client=chat_client,
                            verbose=cfg.verbose,
                        )
                        if doc_result.indexed:
                            counters["docs_indexed"] += 1
                            counters["summary_updated"] += 1 if doc_result.summary_updated else 0
                            counters["summary_failed"] += 1 if doc_result.summary_failed else 0
                            if not dry_run:
                                conn.commit()
                            action = "routed-doc indexed"
                        else:
                            counters["skipped"] += 1
                            action = "routed-doc skipped-unchanged"
                    else:
                        if index_photo_file(
                            conn,
                            routed_path,
                            "inbox/scanner",
                            dry_run,
                            photo_client=photo_client,
                        ):
                            counters["photos_indexed"] += 1
                            if not dry_run:
                                conn.commit()
                            action = "routed-photo indexed"
                        else:
                            counters["skipped"] += 1
                            action = "routed-photo skipped-unchanged"
                except Exception as e:  # noqa: BLE001
                    counters["errors"] += 1
                    action = f"error type={type(e).__name__}"
                    print(f"error\tstage=inbox-route\ttype={type(e).__name__}", file=sys.stderr)
                last_progress_mono = emit_sync_progress(
                    stage="1/6.inbox-route",
                    stage_done=idx,
                    stage_total=len(inbox_files),
                    action=action,
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=cfg.verbose,
                    counters=counters,
                )

        # 2) Index existing docs roots
        docs_done = 0
        last_progress_mono = emit_sync_progress(
            stage="2/6.docs-index",
            stage_done=0,
            stage_total=docs_total,
            action="start" if "docs" in selected_source_kinds else "skipped",
            started_mono=started_mono,
            last_emit_mono=last_progress_mono,
            verbose=cfg.verbose,
            counters=counters,
            force=True,
        )
        if "docs" in selected_source_kinds:
            docs_skip_batch = 0
            docs_skip_stage_done = 0
            for root, root_files in docs_stage_inputs:
                for f in root_files:
                    if should_stop(deadline, budget):
                        break
                    docs_done += 1
                    if is_unchanged_source(conn, "docs_registry", f):
                        counters["skipped"] += 1
                        docs_skip_batch += 1
                        docs_skip_stage_done = docs_done
                        continue
                    last_progress_mono, docs_skip_batch, docs_skip_stage_done = flush_sync_skip_batch(
                        batch_count=docs_skip_batch,
                        batch_stage_done=docs_skip_stage_done,
                        stage="2/6.docs-index",
                        stage_total=docs_total,
                        started_mono=started_mono,
                        last_emit_mono=last_progress_mono,
                        verbose=cfg.verbose,
                        counters=counters,
                    )
                    if budget is not None and not budget.consume():
                        break
                    action = "processing"
                    last_progress_mono = emit_sync_progress(
                        stage="2/6.docs-index",
                        stage_done=docs_done,
                        stage_total=docs_total,
                        action=action,
                        started_mono=started_mono,
                        last_emit_mono=last_progress_mono,
                        verbose=cfg.verbose,
                        counters=counters,
                    )
                    try:
                        doc_result = index_doc_file(
                            conn,
                            f,
                            str(root),
                            cfg.text_cap,
                            dry_run,
                            pdf_cfg=cfg.pdf_parse,
                            summary_cfg=cfg.summary,
                            chat_client=chat_client,
                            verbose=cfg.verbose,
                        )
                        if doc_result.indexed:
                            counters["docs_indexed"] += 1
                            counters["summary_updated"] += 1 if doc_result.summary_updated else 0
                            counters["summary_failed"] += 1 if doc_result.summary_failed else 0
                            if not dry_run:
                                conn.commit()
                            action = "indexed"
                        else:
                            counters["skipped"] += 1
                            action = "skipped-unchanged"
                    except Exception as e:  # noqa: BLE001
                        counters["errors"] += 1
                        action = f"error type={type(e).__name__}"
                        print(f"error\tstage=docs-index\ttype={type(e).__name__}", file=sys.stderr)
                    last_progress_mono = emit_sync_progress(
                        stage="2/6.docs-index",
                        stage_done=docs_done,
                        stage_total=docs_total,
                        action=action,
                        started_mono=started_mono,
                        last_emit_mono=last_progress_mono,
                        verbose=cfg.verbose,
                        counters=counters,
                    )
                if should_stop(deadline, budget):
                    break
            last_progress_mono, docs_skip_batch, docs_skip_stage_done = flush_sync_skip_batch(
                batch_count=docs_skip_batch,
                batch_stage_done=docs_skip_stage_done,
                stage="2/6.docs-index",
                stage_total=docs_total,
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
            )

        # 3) Index existing photos roots
        if not should_stop(deadline, budget):
            photos_done = 0
            last_progress_mono = emit_sync_progress(
                stage="3/6.photos-index",
                stage_done=0,
                stage_total=photos_total,
                action="start" if "photos" in selected_source_kinds else "skipped",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
                force=True,
            )
            if "photos" in selected_source_kinds:
                photos_skip_batch = 0
                photos_skip_stage_done = 0
                for root, root_files in photos_stage_inputs:
                    for f in root_files:
                        if should_stop(deadline, budget):
                            break
                        photos_done += 1
                        if is_unchanged_source(conn, "photos_registry", f):
                            counters["skipped"] += 1
                            photos_skip_batch += 1
                            photos_skip_stage_done = photos_done
                            continue
                        (
                            last_progress_mono,
                            photos_skip_batch,
                            photos_skip_stage_done,
                        ) = flush_sync_skip_batch(
                            batch_count=photos_skip_batch,
                            batch_stage_done=photos_skip_stage_done,
                            stage="3/6.photos-index",
                            stage_total=photos_total,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=cfg.verbose,
                            counters=counters,
                        )
                        if budget is not None and not budget.consume():
                            break
                        action = "processing"
                        last_progress_mono = emit_sync_progress(
                            stage="3/6.photos-index",
                            stage_done=photos_done,
                            stage_total=photos_total,
                            action=action,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=cfg.verbose,
                            counters=counters,
                        )
                        try:
                            if index_photo_file(
                                conn,
                                f,
                                str(root),
                                dry_run,
                                photo_client=photo_client,
                            ):
                                counters["photos_indexed"] += 1
                                if not dry_run:
                                    conn.commit()
                                action = "indexed"
                            else:
                                counters["skipped"] += 1
                                action = "skipped-unchanged"
                        except Exception as e:  # noqa: BLE001
                            counters["errors"] += 1
                            action = f"error type={type(e).__name__}"
                            print(f"error\tstage=photos-index\ttype={type(e).__name__}", file=sys.stderr)
                        last_progress_mono = emit_sync_progress(
                            stage="3/6.photos-index",
                            stage_done=photos_done,
                            stage_total=photos_total,
                            action=action,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=cfg.verbose,
                            counters=counters,
                        )
                    if should_stop(deadline, budget):
                        break
                (
                    last_progress_mono,
                    photos_skip_batch,
                    photos_skip_stage_done,
                ) = flush_sync_skip_batch(
                    batch_count=photos_skip_batch,
                    batch_stage_done=photos_skip_stage_done,
                    stage="3/6.photos-index",
                    stage_total=photos_total,
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=cfg.verbose,
                    counters=counters,
                )

        # 4) Optional mail bridge sync.
        if not should_stop(deadline, budget) and "mail" in selected_source_kinds:
            try:
                m_updated, m_pruned, m_accounts = sync_mail_bridge(
                    conn,
                    mail_cfg=cfg.mail_bridge,
                    full_scan=bool(cfg.skip_inbox),
                    dry_run=dry_run,
                    deadline=deadline,
                    budget=budget,
                    verbose=cfg.verbose,
                )
                counters["mail_indexed"] += m_updated
                counters["mail_pruned"] += m_pruned
                counters["mail_accounts_processed"] += m_accounts
                if m_updated or m_pruned:
                    print(f"mail_bridge\tupdated={m_updated}\tpruned={m_pruned}\taccounts={m_accounts}")
            except Exception as exc:  # noqa: BLE001
                counters["errors"] += 1
                print(f"error\tstage=mail-sync\ttype={type(exc).__name__}", file=sys.stderr)
        else:
            last_progress_mono = emit_sync_progress(
                stage="4/6.mail-sync",
                stage_done=0,
                stage_total=0,
                action="skipped",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
                force=True,
            )

        # 5) Optional controlled summary backfill for unchanged docs.
        if (
            not should_stop(deadline, budget)
            and "docs" in selected_source_kinds
            and not dry_run
            and cfg.summary_reprocess_missing_limit != 0
        ):
            backfilled, failed = backfill_missing_summaries(
                conn,
                summary_cfg=cfg.summary,
                chat_client=chat_client,
                limit=cfg.summary_reprocess_missing_limit,
                deadline=deadline,
                budget=budget,
                verbose=cfg.verbose,
            )
            counters["summary_updated"] += backfilled
            counters["summary_failed"] += failed
            if backfilled:
                print(f"summary\tbackfilled\tcount={backfilled}\tfailed={failed}")
        else:
            last_progress_mono = emit_sync_progress(
                stage="5/6.summary-backfill",
                stage_done=0,
                stage_total=summary_backfill_total,
                action="skipped",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
                force=True,
            )

        # 6) Optional controlled photo-analysis backfill for unchanged photos.
        if (
            not should_stop(deadline, budget)
            and "photos" in selected_source_kinds
            and not dry_run
            and cfg.photo_reprocess_missing_limit != 0
        ):
            p_updated, p_failed = backfill_missing_photo_analysis(
                conn,
                photo_client=photo_client,
                limit=cfg.photo_reprocess_missing_limit,
                deadline=deadline,
                budget=budget,
                verbose=cfg.verbose,
            )
            counters["photo_backfill_updated"] += p_updated
            counters["photo_backfill_failed"] += p_failed
            if p_updated or p_failed:
                print(f"photo_backfill\tupdated={p_updated}\tfailed={p_failed}")
        else:
            last_progress_mono = emit_sync_progress(
                stage="6/6.photo-backfill",
                stage_done=0,
                stage_total=photo_backfill_total,
                action="skipped",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=cfg.verbose,
                counters=counters,
                force=True,
            )

        if not dry_run:
            conn.commit()

        finished = now_iso()
        timed_out = time.monotonic() >= deadline
        status = "timeout" if timed_out else ("bounded" if budget is not None and budget.exhausted() else "ok")
        detail = json.dumps(counters, ensure_ascii=False)
        if not dry_run:
            conn.execute(
                """
                INSERT INTO sync_runs (
                  started_at, finished_at, docs_indexed, photos_indexed,
                  inbox_routed, skipped, errors, status, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started,
                    finished,
                    counters["docs_indexed"],
                    counters["photos_indexed"],
                    counters["inbox_routed"],
                    counters["skipped"],
                    counters["errors"],
                    status,
                    detail,
                ),
            )
            conn.commit()

        print(f"status: {status}")
        for k, v in counters.items():
            print(f"{k}: {v}")
        log_verbose(cfg, f"pipeline end status={status} errors={counters['errors']}")
        return 0 if counters["errors"] == 0 else 1

    finally:
        conn.close()
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        lock_file.close()


def main() -> int:
    args = parse_args()
    summary_cfg = _resolve_summary_config(args)
    photo_cfg = _resolve_photo_analysis_config(args)
    pdf_cfg = _resolve_pdf_parse_config(args)
    mail_cfg = _resolve_mail_bridge_config(args)
    docs_roots = args.docs_root or default_docs_roots()
    photos_roots = args.photos_root or default_photos_roots()
    cfg = Config(
        db_path=Path(args.db_path),
        docs_roots=[Path(p) for p in docs_roots],
        photos_roots=[Path(p) for p in photos_roots],
        inbox_scanner=Path(args.inbox_scanner),
        docs_dest_root=Path(args.docs_dest_root),
        photos_dest_root=Path(args.photos_dest_root),
        text_cap=args.text_cap,
        max_seconds=args.max_seconds,
        max_items=int(args.max_items),
        skip_inbox=bool(args.skip_inbox),
        verbose=bool(args.verbose),
        summary=summary_cfg,
        photo_analysis=photo_cfg,
        pdf_parse=pdf_cfg,
        summary_reprocess_missing_limit=int(args.reprocess_missing_summaries),
        photo_reprocess_missing_limit=int(args.reprocess_missing_photo_analysis),
        source_selection=str(args.source or "all"),
        mail_bridge=mail_cfg,
    )
    _selected_source_kinds(cfg)
    return run(cfg, dry_run=bool(args.dry_run))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
