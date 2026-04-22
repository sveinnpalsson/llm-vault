#!/usr/bin/env python3
"""Unified vault-ops CLI.

Public commands:
  - update: ingest + incremental index update
  - repair: backfill/fix existing registry + index update
  - search: semantic search over docs/photos/mail vectors
  - status: concise ops summary
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from vault_sources import source_choices

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
REGISTRY_SYNC = SCRIPTS / "vault_registry_sync.py"
VECTOR_INDEX = SCRIPTS / "vault_vector_index.py"
DB_SUMMARY = SCRIPTS / "vault_db_summary.py"
DB_CRYPTO = SCRIPTS / "vault_db_crypto.py"
DEFAULT_REGISTRY_DB = ROOT / "state" / "vault_registry.db"
DEFAULT_VECTORS_DB = ROOT / "state" / "vault_vectors.db"
DEFAULT_INBOX_SCANNER = ROOT / "state" / "scanner_inbox"
DEFAULT_CONFIG_CANDIDATES = (
    ROOT / "vault-ops.toml",
    ROOT / ".vault-ops.toml",
)


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _print_step(prefix: str, step_no: int, total_steps: int, text: str) -> None:
    print(f"[{prefix}] [step={step_no}/{total_steps}] [action={text}]")


def _format_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(c) for c in cmd)


def _add_config_arg(parser: argparse.ArgumentParser, *, default: object | None = None) -> None:
    parser.add_argument(
        "--config",
        default=default,
        help="optional TOML config file (auto-loads vault-ops.toml when present)",
    )


def _config_section(config: dict, name: str) -> dict:
    section = config.get(name)
    return section if isinstance(section, dict) else {}


def _config_str(section: dict, key: str) -> str | None:
    value = section.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _config_int(section: dict, key: str) -> int | None:
    value = section.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _config_bool(section: dict, key: str) -> bool | None:
    value = section.get(key)
    if isinstance(value, bool):
        return value
    return None


def _config_list(section: dict, key: str) -> list[str]:
    value = section.get(key)
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _resolve_config_path(config_arg: str | None) -> Path | None:
    if config_arg:
        return Path(config_arg).expanduser().resolve()
    for candidate in DEFAULT_CONFIG_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    return None


def _load_config_data(config_path: Path | None) -> tuple[Path | None, dict]:
    if config_path is None:
        return None, {}
    try:
        with config_path.open("rb") as fh:
            raw = tomllib.load(fh)
    except FileNotFoundError:
        raise SystemExit(f"error: config file not found: {config_path}")
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(f"error: invalid config TOML in {config_path}: {exc}")
    if not isinstance(raw, dict):
        raise SystemExit(f"error: config root must be a TOML table: {config_path}")
    return config_path, raw


def _apply_if_default_path(args: argparse.Namespace, attr: str, value: str | None, default: Path) -> None:
    if not value or not hasattr(args, attr):
        return
    current = getattr(args, attr)
    if str(current) == str(default):
        setattr(args, attr, value)


def _apply_if_missing(args: argparse.Namespace, attr: str, value: str | int | None) -> None:
    if value is None or not hasattr(args, attr):
        return
    current = getattr(args, attr)
    if current is None:
        setattr(args, attr, value)


def _apply_if_empty_list(args: argparse.Namespace, attr: str, values: list[str]) -> None:
    if not values or not hasattr(args, attr):
        return
    current = getattr(args, attr)
    if isinstance(current, list) and not current:
        setattr(args, attr, list(values))


def _apply_if_false(args: argparse.Namespace, attr: str, value: bool | None) -> None:
    if value is not True or not hasattr(args, attr):
        return
    current = getattr(args, attr)
    if current is False:
        setattr(args, attr, True)


def _apply_config_defaults(args: argparse.Namespace) -> argparse.Namespace:
    config_path, config = _load_config_data(_resolve_config_path(getattr(args, "config", None)))
    setattr(args, "_config_path", config_path)
    if not config:
        return args

    paths = _config_section(config, "paths")
    summary = _config_section(config, "summary")
    embedding = _config_section(config, "embedding")
    redaction = _config_section(config, "redaction")
    photo = _config_section(config, "photo_analysis")
    pdf = _config_section(config, "pdf")
    mail = _config_section(config, "mail_bridge")
    search = _config_section(config, "search")
    runtime = _config_section(config, "runtime")

    _apply_if_default_path(args, "registry_db", _config_str(paths, "registry_db"), DEFAULT_REGISTRY_DB)
    _apply_if_default_path(args, "vectors_db", _config_str(paths, "vectors_db"), DEFAULT_VECTORS_DB)
    _apply_if_default_path(args, "inbox_scanner", _config_str(paths, "inbox_scanner"), DEFAULT_INBOX_SCANNER)
    _apply_if_empty_list(args, "docs_root", _config_list(paths, "docs_roots"))
    _apply_if_empty_list(args, "photos_root", _config_list(paths, "photos_roots"))

    _apply_if_missing(args, "summary_base_url", _config_str(summary, "base_url"))
    _apply_if_missing(args, "summary_model", _config_str(summary, "model"))
    _apply_if_missing(args, "summary_api_key", _config_str(summary, "api_key"))
    _apply_if_missing(args, "summary_timeout", _config_int(summary, "timeout"))

    _apply_if_missing(args, "embed_base_url", _config_str(embedding, "base_url"))
    _apply_if_missing(args, "embed_model", _config_str(embedding, "model"))
    _apply_if_missing(args, "embed_api_key", _config_str(embedding, "api_key"))
    _apply_if_missing(args, "embed_timeout", _config_int(embedding, "timeout"))

    _apply_if_missing(args, "redaction_base_url", _config_str(redaction, "base_url"))
    _apply_if_missing(args, "redaction_model", _config_str(redaction, "model"))
    _apply_if_missing(args, "redaction_api_key", _config_str(redaction, "api_key"))
    _apply_if_missing(args, "redaction_timeout", _config_int(redaction, "timeout"))

    _apply_if_missing(args, "photo_analysis_url", _config_str(photo, "url"))
    _apply_if_missing(args, "photo_analysis_timeout", _config_int(photo, "timeout"))
    _apply_if_false(args, "photo_analysis_force", _config_bool(photo, "force"))
    _apply_if_false(args, "disable_photo_analysis", _config_bool(photo, "disable_service"))

    _apply_if_missing(args, "pdf_parse_url", _config_str(pdf, "parse_url"))
    _apply_if_missing(args, "pdf_parse_timeout", _config_int(pdf, "timeout"))
    _apply_if_missing(args, "pdf_parse_profile", _config_str(pdf, "profile"))
    _apply_if_false(args, "disable_pdf_service", _config_bool(pdf, "disable_service"))

    _apply_if_missing(args, "search_level", _config_str(search, "search_level"))
    _apply_if_missing(args, "top_k", _config_int(search, "top_k"))

    setattr(args, "_mail_bridge_enabled", bool(_config_bool(mail, "enabled")))
    setattr(args, "_mail_bridge_db_path", _config_str(mail, "db_path") or "")
    setattr(args, "_mail_bridge_password_env", _config_str(mail, "password_env") or "")
    setattr(args, "_mail_bridge_include_accounts", _config_list(mail, "include_accounts"))
    import_summary = _config_bool(mail, "import_summary")
    setattr(args, "_mail_bridge_import_summary", True if import_summary is None else bool(import_summary))
    import_attachments = _config_bool(mail, "import_attachments")
    setattr(args, "_mail_bridge_import_attachments", True if import_attachments is None else bool(import_attachments))
    max_body_chunks = _config_int(mail, "max_body_chunks")
    setattr(args, "_mail_bridge_max_body_chunks", 12 if max_body_chunks is None else max(0, int(max_body_chunks)))

    _apply_if_missing(args, "max", _config_int(runtime, "max"))

    return args


def run_cmd(cmd: list[str], *, label: str, verbose: bool, dry_run: bool = False) -> int:
    if verbose:
        print(f"[{label}] [time={_ts()}] [action=start]")
        print(f"[{label}] [cmd={_format_cmd(cmd)}]")
    else:
        print(f"[{label}] [action=start]")

    if dry_run:
        print(f"[{label}] [action=skipped dry-run]")
        return 0

    started = time.monotonic()
    run_env = dict(os.environ)
    run_env["PYTHONUNBUFFERED"] = "1"
    if verbose:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=run_env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(f"[{label}] {line.rstrip()}")
        rc = int(proc.wait())
    else:
        rc = int(subprocess.run(cmd, env=run_env).returncode)

    elapsed = time.monotonic() - started
    print(f"[{label}] [action=done] [rc={rc}] [elapsed={elapsed:.1f}s]")
    return rc


def run_cmd_json(cmd: list[str], *, label: str, verbose: bool) -> int:
    """Run command and preserve pure JSON stdout in non-verbose mode."""

    if verbose:
        return run_cmd(cmd, label=label, verbose=True, dry_run=False)

    run_env = dict(os.environ)
    run_env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=run_env,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    return int(proc.returncode)


def _resolve_source_mode(args: argparse.Namespace) -> str:
    return str(getattr(args, "source", "all") or "all")


def _append_source_flags(cmd: list[str], source_mode: str) -> None:
    cmd += ["--source", source_mode]


def _append_common_vector_flags(cmd: list[str], args: argparse.Namespace) -> None:
    source_mode = _resolve_source_mode(args)
    _append_source_flags(cmd, source_mode)
    if getattr(args, "_mail_bridge_enabled", False):
        cmd.append("--mail-bridge-enabled")
    if getattr(args, "_mail_bridge_max_body_chunks", None) is not None:
        cmd += ["--mail-max-body-chunks", str(max(0, int(args._mail_bridge_max_body_chunks)))]
    if getattr(args, "embed_base_url", None):
        cmd += ["--embed-base-url", str(args.embed_base_url)]
    if getattr(args, "embed_model", None):
        cmd += ["--embed-model", str(args.embed_model)]
    if getattr(args, "embed_api_key", None) is not None:
        cmd += ["--embed-api-key", str(args.embed_api_key)]
    if getattr(args, "embed_timeout", None) is not None:
        cmd += ["--embed-timeout", str(args.embed_timeout)]
    if getattr(args, "embed_batch_size", None) is not None:
        cmd += ["--embed-batch-size", str(args.embed_batch_size)]
    if getattr(args, "embed_batch_tokens", None) is not None:
        cmd += ["--embed-batch-tokens", str(args.embed_batch_tokens)]
    if getattr(args, "embed_max_text_chars", None) is not None:
        cmd += ["--embed-max-text-chars", str(args.embed_max_text_chars)]
    if args.disable_redaction:
        cmd.append("--disable-redaction")
    if args.redaction_mode:
        cmd += ["--redaction-mode", str(args.redaction_mode)]
    if args.redaction_profile:
        cmd += ["--redaction-profile", str(args.redaction_profile)]
    if args.redaction_instruction:
        cmd += ["--redaction-instruction", str(args.redaction_instruction)]
    if args.redaction_base_url:
        cmd += ["--redaction-base-url", str(args.redaction_base_url)]
    if args.redaction_model:
        cmd += ["--redaction-model", str(args.redaction_model)]
    if args.redaction_api_key is not None:
        cmd += ["--redaction-api-key", str(args.redaction_api_key)]
    if args.redaction_timeout is not None:
        cmd += ["--redaction-timeout", str(args.redaction_timeout)]


def _append_registry_sync_flags(cmd: list[str], args: argparse.Namespace) -> None:
    _append_source_flags(cmd, _resolve_source_mode(args))
    docs_roots = list(getattr(args, "docs_root", []) or [])
    photos_roots = list(getattr(args, "photos_root", []) or [])
    for root in docs_roots:
        cmd += ["--docs-root", str(root)]
    for root in photos_roots:
        cmd += ["--photos-root", str(root)]

    if getattr(args, "disable_pdf_service", False):
        cmd.append("--disable-pdf-service")
    if getattr(args, "pdf_parse_url", None):
        cmd += ["--pdf-parse-url", str(args.pdf_parse_url)]
    if getattr(args, "pdf_parse_timeout", None) is not None:
        cmd += ["--pdf-parse-timeout", str(args.pdf_parse_timeout)]
    if getattr(args, "pdf_parse_profile", None):
        cmd += ["--pdf-parse-profile", str(args.pdf_parse_profile)]

    if getattr(args, "summary_base_url", None):
        cmd += ["--summary-base-url", str(args.summary_base_url)]
    if getattr(args, "summary_model", None):
        cmd += ["--summary-model", str(args.summary_model)]
    if getattr(args, "summary_api_key", None) is not None:
        cmd += ["--summary-api-key", str(args.summary_api_key)]
    if getattr(args, "summary_timeout", None) is not None:
        cmd += ["--summary-timeout", str(args.summary_timeout)]

    if getattr(args, "photo_analysis_url", None):
        cmd += ["--photo-analysis-url", str(args.photo_analysis_url)]
    if getattr(args, "photo_analysis_timeout", None) is not None:
        cmd += ["--photo-analysis-timeout", str(args.photo_analysis_timeout)]
    if getattr(args, "photo_analysis_force", False):
        cmd.append("--photo-analysis-force")
    if getattr(args, "_mail_bridge_enabled", False):
        cmd.append("--mail-bridge-enabled")
    if getattr(args, "_mail_bridge_db_path", None):
        cmd += ["--mail-bridge-db-path", str(args._mail_bridge_db_path)]
    if getattr(args, "_mail_bridge_password_env", None):
        cmd += ["--mail-bridge-password-env", str(args._mail_bridge_password_env)]
    for account in list(getattr(args, "_mail_bridge_include_accounts", []) or []):
        cmd += ["--mail-bridge-include-account", str(account)]
    if getattr(args, "_mail_bridge_import_summary", True) is False:
        cmd.append("--mail-bridge-no-import-summary")
    if getattr(args, "_mail_bridge_import_attachments", True) is False:
        cmd.append("--mail-bridge-no-import-attachments")


def _resolve_vector_update_source_mode(args: argparse.Namespace) -> str:
    source_mode = _resolve_source_mode(args)
    if (
        source_mode == "mail"
        and getattr(args, "_mail_bridge_enabled", False)
        and getattr(args, "_mail_bridge_import_attachments", True)
    ):
        # Mail bridge runs can also create mail-derived docs/photos attachment rows.
        # Vector update should pick those up without widening registry sync itself.
        return "all"
    return source_mode


def _upgrade_levels_selected(index_level: str) -> list[str]:
    level = str(index_level or "redacted").strip().lower()
    if level == "all":
        return ["redacted", "full"]
    if level in {"redacted", "full"}:
        return [level]
    return ["redacted"]


def _arg_or_default(args: argparse.Namespace, name: str, default: Path) -> Path:
    return Path(str(getattr(args, name, default)))


def cmd_update(args: argparse.Namespace) -> int:
    total_steps = 3
    registry_db = _arg_or_default(args, "registry_db", DEFAULT_REGISTRY_DB)
    vectors_db = _arg_or_default(args, "vectors_db", DEFAULT_VECTORS_DB)
    update_started_at = datetime.now(timezone.utc).isoformat()
    _print_step("update", 1, total_steps, "prepare registry sync")
    sync_cmd = [sys.executable, str(REGISTRY_SYNC), "--db-path", str(registry_db)]
    if args.max is not None and args.max > 0:
        sync_cmd += ["--max-items", str(args.max)]
    if args.disable_summary:
        sync_cmd.append("--disable-summary")
    if args.disable_photo_analysis:
        sync_cmd.append("--disable-photo-analysis")
    sync_cmd += ["--reprocess-missing-summaries", "0"]
    sync_cmd += ["--reprocess-missing-photo-analysis", "0"]
    _append_registry_sync_flags(sync_cmd, args)
    if args.dry_run:
        sync_cmd.append("--dry-run")
    if args.verbose:
        sync_cmd.append("--verbose")

    _print_step("update", 2, total_steps, "run registry sync")
    rc = run_cmd(sync_cmd, label="update:registry-sync", verbose=args.verbose)
    if rc != 0:
        return rc

    _print_step("update", 3, total_steps, "run vector update")
    if args.dry_run and not args.force_vector_update:
        print("[update:vector-index] skipped (dry-run)")
        return 0

    vector_cmd = [
        sys.executable,
        str(VECTOR_INDEX),
        "update",
        "--registry-db",
        str(registry_db),
        "--vectors-db",
        str(vectors_db),
        "--index-level",
        "redacted",
        "--updated-since",
        update_started_at,
    ]
    vector_args = argparse.Namespace(**vars(args))
    setattr(vector_args, "source", _resolve_vector_update_source_mode(args))
    _append_common_vector_flags(vector_cmd, vector_args)
    if args.verbose:
        vector_cmd.append("--verbose")
    return run_cmd(vector_cmd, label="update:vector-index", verbose=args.verbose)


def cmd_repair(args: argparse.Namespace) -> int:
    total_steps = 3
    registry_db = _arg_or_default(args, "registry_db", DEFAULT_REGISTRY_DB)
    vectors_db = _arg_or_default(args, "vectors_db", DEFAULT_VECTORS_DB)
    repair_started_at = datetime.now(timezone.utc).isoformat()
    _print_step("repair", 1, total_steps, "prepare repair sync (skip inbox)")
    sync_cmd = [
        sys.executable,
        str(REGISTRY_SYNC),
        "--db-path",
        str(registry_db),
        "--skip-inbox",
    ]
    if args.max is not None and args.max > 0:
        sync_cmd += ["--max-items", str(args.max)]
    if args.reprocess_missing_summaries != 0:
        sync_cmd += [
            "--reprocess-missing-summaries",
            str(args.reprocess_missing_summaries),
        ]

    photo_limit = int(args.reprocess_missing_photo_analysis)
    if args.photos and photo_limit == 0:
        photo_limit = -1
    if photo_limit != 0:
        sync_cmd += ["--reprocess-missing-photo-analysis", str(photo_limit)]

    if args.disable_summary:
        sync_cmd.append("--disable-summary")
    if args.disable_photo_analysis:
        sync_cmd.append("--disable-photo-analysis")
    _append_registry_sync_flags(sync_cmd, args)
    if args.dry_run:
        sync_cmd.append("--dry-run")
    if args.verbose:
        sync_cmd.append("--verbose")

    _print_step("repair", 2, total_steps, "run repair sync")
    rc = run_cmd(sync_cmd, label="repair:registry-sync", verbose=args.verbose)
    if rc != 0:
        return rc

    _print_step("repair", 3, total_steps, "run vector update / redaction repair")
    if args.no_vectors:
        print("[repair:vector-index] skipped (--no-vectors)")
        return 0
    if args.dry_run and not args.force_vector_update:
        print("[repair:vector-index] skipped (dry-run)")
        return 0

    vector_cmd = [
        sys.executable,
        str(VECTOR_INDEX),
        "update",
        "--registry-db",
        str(registry_db),
        "--vectors-db",
        str(vectors_db),
        "--index-level",
        "redacted",
        "--updated-since",
        repair_started_at,
    ]
    vector_args = argparse.Namespace(**vars(args))
    setattr(vector_args, "source", _resolve_vector_update_source_mode(args))
    _append_common_vector_flags(vector_cmd, vector_args)
    if getattr(args, "reconcile_redactions", False):
        vector_cmd.append("--reconcile-redactions")
    if args.verbose:
        vector_cmd.append("--verbose")
    return run_cmd(vector_cmd, label="repair:vector-index", verbose=args.verbose)


def cmd_search(args: argparse.Namespace) -> int:
    registry_db = _arg_or_default(args, "registry_db", DEFAULT_REGISTRY_DB)
    vectors_db = _arg_or_default(args, "vectors_db", DEFAULT_VECTORS_DB)
    query_cmd = [
        sys.executable,
        str(VECTOR_INDEX),
        "query",
        args.query,
        "--registry-db",
        str(registry_db),
        "--vectors-db",
        str(vectors_db),
    ]
    query_cmd += [
        "--top-k",
        str(args.top_k),
        "--clearance",
        str(args.clearance),
        "--search-level",
        str(getattr(args, "search_level", "auto")),
    ]
    if args.json:
        query_cmd.append("--json")
    if args.from_date:
        query_cmd += ["--from-date", str(args.from_date)]
    if args.to_date:
        query_cmd += ["--to-date", str(args.to_date)]
    if args.taxonomy:
        query_cmd += ["--taxonomy", str(args.taxonomy)]
    if args.category_primary:
        query_cmd += ["--category-primary", str(args.category_primary)]
    _append_common_vector_flags(query_cmd, args)
    if args.verbose:
        query_cmd.append("--verbose")
    if args.json:
        return run_cmd_json(query_cmd, label="search", verbose=args.verbose)
    return run_cmd(query_cmd, label="search", verbose=args.verbose)


def cmd_status(args: argparse.Namespace) -> int:
    registry_db = _arg_or_default(args, "registry_db", DEFAULT_REGISTRY_DB)
    vectors_db = _arg_or_default(args, "vectors_db", DEFAULT_VECTORS_DB)
    summary_cmd = [
        sys.executable,
        str(DB_SUMMARY),
        "--registry-db",
        str(registry_db),
        "--vectors-db",
        str(vectors_db),
        "--inbox-scanner",
        str(args.inbox_scanner),
    ]
    for root in list(getattr(args, "docs_root", []) or []):
        summary_cmd += ["--docs-root", str(root)]
    for root in list(getattr(args, "photos_root", []) or []):
        summary_cmd += ["--photos-root", str(root)]
    if getattr(args, "summary_base_url", None):
        summary_cmd += ["--summary-base-url", str(args.summary_base_url)]
    if getattr(args, "embed_base_url", None):
        summary_cmd += ["--embed-base-url", str(args.embed_base_url)]
    if getattr(args, "redaction_base_url", None):
        summary_cmd += ["--redaction-base-url", str(args.redaction_base_url)]
    if getattr(args, "disable_photo_analysis", False):
        summary_cmd.append("--disable-photo-analysis")
    if getattr(args, "photo_analysis_url", None):
        summary_cmd += ["--photo-analysis-url", str(args.photo_analysis_url)]
    if getattr(args, "disable_pdf_service", False):
        summary_cmd.append("--disable-pdf-service")
    if getattr(args, "pdf_parse_url", None):
        summary_cmd += ["--pdf-parse-url", str(args.pdf_parse_url)]
    if getattr(args, "_mail_bridge_enabled", False):
        summary_cmd.append("--mail-bridge-enabled")
    if getattr(args, "_mail_bridge_db_path", None):
        summary_cmd += ["--mail-bridge-db-path", str(args._mail_bridge_db_path)]
    if getattr(args, "_mail_bridge_password_env", None):
        summary_cmd += ["--mail-bridge-password-env", str(args._mail_bridge_password_env)]
    for account in list(getattr(args, "_mail_bridge_include_accounts", []) or []):
        summary_cmd += ["--mail-bridge-include-account", str(account)]
    if getattr(args, "_mail_bridge_import_summary", True) is False:
        summary_cmd.append("--mail-bridge-no-import-summary")
    if getattr(args, "_mail_bridge_max_body_chunks", None) is not None:
        summary_cmd += ["--mail-max-body-chunks", str(max(0, int(args._mail_bridge_max_body_chunks)))]
    if args.json:
        summary_cmd.append("--json")
    elif bool(getattr(args, "oneline", False)):
        summary_cmd.append("--oneline")
    if args.json:
        return run_cmd_json(summary_cmd, label="status", verbose=args.verbose)
    return run_cmd(summary_cmd, label="status", verbose=args.verbose)


def cmd_upgrade(args: argparse.Namespace) -> int:
    registry_db = _arg_or_default(args, "registry_db", DEFAULT_REGISTRY_DB)
    vectors_db = _arg_or_default(args, "vectors_db", DEFAULT_VECTORS_DB)
    selected_levels = _upgrade_levels_selected(args.index_level)
    upgrade_started_at = datetime.now(timezone.utc).isoformat()

    summary_cmd = [
        sys.executable,
        str(DB_SUMMARY),
        "--registry-db",
        str(registry_db),
        "--vectors-db",
        str(vectors_db),
        "--json",
    ]
    if getattr(args, "_mail_bridge_enabled", False):
        summary_cmd.append("--mail-bridge-enabled")
    if getattr(args, "_mail_bridge_db_path", None):
        summary_cmd += ["--mail-bridge-db-path", str(args._mail_bridge_db_path)]
    for account in list(getattr(args, "_mail_bridge_include_accounts", []) or []):
        summary_cmd += ["--mail-bridge-include-account", str(account)]
    if getattr(args, "_mail_bridge_import_summary", True) is False:
        summary_cmd.append("--mail-bridge-no-import-summary")
    if getattr(args, "_mail_bridge_max_body_chunks", None) is not None:
        summary_cmd += ["--mail-max-body-chunks", str(max(0, int(args._mail_bridge_max_body_chunks)))]
    summary_proc = subprocess.run(
        summary_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    if summary_proc.returncode != 0:
        if summary_proc.stdout:
            print(summary_proc.stdout, end="")
        if summary_proc.stderr:
            print(summary_proc.stderr, end="", file=sys.stderr)
        return int(summary_proc.returncode)

    try:
        current_status = json.loads(summary_proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        print(f"error: unable to parse status JSON during upgrade planning: {exc}", file=sys.stderr)
        return 2

    plan = {
        "command": "upgrade",
        "will_execute": bool(args.yes),
        "selected_index_levels": selected_levels,
        "registry_db": str(registry_db),
        "vectors_db": str(vectors_db),
        "status_before": current_status,
    }
    if not args.yes:
        plan["note"] = "dry-run only; rerun with --yes to execute upgrade actions"
        if args.json:
            print(json.dumps(plan, indent=2, ensure_ascii=False))
        else:
            print("upgrade dry-run")
            print(f"selected_index_levels={','.join(selected_levels)}")
            print("status snapshot:")
            print(json.dumps(current_status, indent=2, ensure_ascii=False))
            print("rerun with --yes to execute")
        return 0

    total_steps = 1 + len(selected_levels) + 1
    step_no = 1
    _print_step("upgrade", step_no, total_steps, "run policy-aware registry repair")
    step_no += 1
    repair_cmd = [
        sys.executable,
        str(REGISTRY_SYNC),
        "--db-path",
        str(registry_db),
        "--skip-inbox",
        "--reprocess-missing-summaries",
        str(args.reprocess_missing_summaries),
        "--reprocess-missing-photo-analysis",
        str(args.reprocess_missing_photo_analysis),
    ]
    if args.max is not None and args.max > 0:
        repair_cmd += ["--max-items", str(args.max)]
    if args.disable_summary:
        repair_cmd.append("--disable-summary")
    if args.disable_photo_analysis:
        repair_cmd.append("--disable-photo-analysis")
    _append_registry_sync_flags(repair_cmd, args)
    if args.verbose:
        repair_cmd.append("--verbose")
    rc = run_cmd(repair_cmd, label="upgrade:registry-sync", verbose=args.verbose)
    if rc != 0:
        return rc

    for level in selected_levels:
        _print_step("upgrade", step_no, total_steps, f"build {level} index level")
        step_no += 1
        vector_cmd = [
            sys.executable,
            str(VECTOR_INDEX),
            "update",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vectors_db),
            "--index-level",
            level,
            "--updated-since",
            upgrade_started_at,
        ]
        _append_common_vector_flags(vector_cmd, args)
        if args.verbose:
            vector_cmd.append("--verbose")
        rc = run_cmd(vector_cmd, label=f"upgrade:vector-index:{level}", verbose=args.verbose)
        if rc != 0:
            return rc

    _print_step("upgrade", step_no, total_steps, "final status")
    status_args = argparse.Namespace(
        json=bool(args.json),
        verbose=bool(args.verbose),
        registry_db=registry_db,
        vectors_db=vectors_db,
        inbox_scanner=DEFAULT_INBOX_SCANNER,
    )
    return cmd_status(status_args)


def cmd_migrate_encryption(args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        str(DB_CRYPTO),
        "--db-path",
        str(args.registry_db),
        "--db-path",
        str(args.vectors_db),
        "--json",
    ]
    if args.backup_suffix:
        cmd += ["--backup-suffix", str(args.backup_suffix)]
    return run_cmd(cmd, label="migrate-encryption", verbose=args.verbose)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="vault-ops unified CLI")
    _add_config_arg(p, default=None)
    sub = p.add_subparsers(dest="command", required=True)

    p_update = sub.add_parser("update", help="sync new inbox items + incremental index update")
    _add_config_arg(p_update, default=argparse.SUPPRESS)
    p_update.add_argument("--max", type=int, default=None, help="process at most N docs/photos/mail sources in this run")
    p_update.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p_update.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    p_update.add_argument("--docs-root", action="append", default=[])
    p_update.add_argument("--photos-root", action="append", default=[])
    p_update.add_argument("--source", choices=source_choices(), default="all", help="select which source kind to index")
    p_update.add_argument("--disable-summary", action="store_true")
    p_update.add_argument("--disable-photo-analysis", action="store_true")
    p_update.add_argument("--embed-base-url", default=None)
    p_update.add_argument("--embed-model", default=None)
    p_update.add_argument("--embed-api-key", default=None)
    p_update.add_argument("--embed-timeout", type=int, default=None)
    p_update.add_argument("--summary-base-url", default=None)
    p_update.add_argument("--summary-model", default=None)
    p_update.add_argument("--summary-api-key", default=None)
    p_update.add_argument("--summary-timeout", type=int, default=None)
    p_update.add_argument("--photo-analysis-url", default=None)
    p_update.add_argument("--photo-analysis-timeout", type=int, default=None)
    p_update.add_argument("--photo-analysis-force", action="store_true")
    p_update.add_argument("--disable-pdf-service", action="store_true")
    p_update.add_argument("--pdf-parse-url", default=None)
    p_update.add_argument("--pdf-parse-timeout", type=int, default=None)
    p_update.add_argument("--pdf-parse-profile", choices=["auto", "native", "ocr"], default=None)
    p_update.add_argument("--embed-batch-size", type=int, default=None)
    p_update.add_argument("--embed-batch-tokens", type=int, default=None)
    p_update.add_argument("--embed-max-text-chars", type=int, default=None)
    p_update.add_argument("--disable-redaction", action="store_true")
    p_update.add_argument("--redaction-mode", choices=["regex", "model", "hybrid"], default="hybrid")
    p_update.add_argument("--redaction-profile", default="standard")
    p_update.add_argument("--redaction-instruction", default="")
    p_update.add_argument("--redaction-base-url", default=None)
    p_update.add_argument("--redaction-model", default=None)
    p_update.add_argument("--redaction-api-key", default=None)
    p_update.add_argument("--redaction-timeout", type=int, default=None)
    p_update.add_argument("--dry-run", action="store_true", help="dry-run registry sync; skip vectors unless --force-vector-update")
    p_update.add_argument("--force-vector-update", action="store_true", help="run vector update even when --dry-run is set")
    p_update.add_argument("--verbose", action="store_true", help="print clean step-by-step progress")
    p_update.set_defaults(func=cmd_update)

    p_repair = sub.add_parser("repair", help="repair/backfill existing registry and vectors")
    _add_config_arg(p_repair, default=argparse.SUPPRESS)
    p_repair.add_argument("--max", type=int, default=None, help="process at most N docs/photos/mail sources in this run")
    p_repair.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p_repair.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    p_repair.add_argument("--docs-root", action="append", default=[])
    p_repair.add_argument("--photos-root", action="append", default=[])
    p_repair.add_argument("--reprocess-missing-summaries", type=int, default=-1, help="backfill missing/error summaries (-1 means all)")
    p_repair.add_argument("--photos", action="store_true", help="reprocess missing photo classification/caption (alias for --reprocess-missing-photo-analysis -1)")
    p_repair.add_argument("--reprocess-missing-photo-analysis", type=int, default=-1, help="backfill missing photo analysis (-1 means all)")
    p_repair.add_argument("--source", choices=source_choices(), default="all", help="select which source kind to index/query")
    p_repair.add_argument("--disable-summary", action="store_true")
    p_repair.add_argument("--disable-photo-analysis", action="store_true")
    p_repair.add_argument("--embed-base-url", default=None)
    p_repair.add_argument("--embed-model", default=None)
    p_repair.add_argument("--embed-api-key", default=None)
    p_repair.add_argument("--embed-timeout", type=int, default=None)
    p_repair.add_argument("--summary-base-url", default=None)
    p_repair.add_argument("--summary-model", default=None)
    p_repair.add_argument("--summary-api-key", default=None)
    p_repair.add_argument("--summary-timeout", type=int, default=None)
    p_repair.add_argument("--photo-analysis-url", default=None)
    p_repair.add_argument("--photo-analysis-timeout", type=int, default=None)
    p_repair.add_argument("--photo-analysis-force", action="store_true")
    p_repair.add_argument("--disable-pdf-service", action="store_true")
    p_repair.add_argument("--pdf-parse-url", default=None)
    p_repair.add_argument("--pdf-parse-timeout", type=int, default=None)
    p_repair.add_argument("--pdf-parse-profile", choices=["auto", "native", "ocr"], default=None)
    p_repair.add_argument("--embed-batch-size", type=int, default=None)
    p_repair.add_argument("--embed-batch-tokens", type=int, default=None)
    p_repair.add_argument("--embed-max-text-chars", type=int, default=None)
    p_repair.add_argument("--disable-redaction", action="store_true")
    p_repair.add_argument("--redaction-mode", choices=["regex", "model", "hybrid"], default="hybrid")
    p_repair.add_argument("--redaction-profile", default="standard")
    p_repair.add_argument("--redaction-instruction", default="")
    p_repair.add_argument("--redaction-base-url", default=None)
    p_repair.add_argument("--redaction-model", default=None)
    p_repair.add_argument("--redaction-api-key", default=None)
    p_repair.add_argument("--redaction-timeout", type=int, default=None)
    p_repair.add_argument("--no-vectors", action="store_true", help="skip vector update phase")
    p_repair.add_argument(
        "--reconcile-redactions",
        action="store_true",
        help="run explicit full-source vector redaction reconciliation after repair",
    )
    p_repair.add_argument("--dry-run", action="store_true", help="dry-run registry repair; skip vectors unless --force-vector-update")
    p_repair.add_argument("--force-vector-update", action="store_true", help="run vector update even when --dry-run is set")
    p_repair.add_argument("--verbose", action="store_true", help="print clean step-by-step progress")
    p_repair.set_defaults(func=cmd_repair)

    p_search = sub.add_parser("search", help="semantic search over docs/photos/mail vectors")
    _add_config_arg(p_search, default=argparse.SUPPRESS)
    p_search.add_argument("query")
    p_search.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p_search.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    p_search.add_argument("--top-k", type=int, default=5)
    p_search.add_argument("--source", choices=source_choices(), default="all")
    p_search.add_argument("--clearance", choices=["redacted", "full"], default="redacted")
    p_search.add_argument("--search-level", choices=["auto", "redacted", "full"], default="auto")
    p_search.add_argument("--from-date", default=None)
    p_search.add_argument("--to-date", default=None)
    p_search.add_argument("--taxonomy", default=None)
    p_search.add_argument("--category-primary", default=None)
    p_search.add_argument("--json", action="store_true", help="emit JSON output for automation")
    p_search.add_argument("--embed-base-url", default=None)
    p_search.add_argument("--embed-model", default=None)
    p_search.add_argument("--embed-api-key", default=None)
    p_search.add_argument("--embed-timeout", type=int, default=None)
    p_search.add_argument("--embed-batch-size", type=int, default=None)
    p_search.add_argument("--embed-batch-tokens", type=int, default=None)
    p_search.add_argument("--embed-max-text-chars", type=int, default=None)
    p_search.add_argument("--disable-redaction", action="store_true")
    p_search.add_argument("--redaction-mode", choices=["regex", "model", "hybrid"], default="hybrid")
    p_search.add_argument("--redaction-profile", default="standard")
    p_search.add_argument("--redaction-instruction", default="")
    p_search.add_argument("--redaction-base-url", default=None)
    p_search.add_argument("--redaction-model", default=None)
    p_search.add_argument("--redaction-api-key", default=None)
    p_search.add_argument("--redaction-timeout", type=int, default=None)
    p_search.add_argument("--verbose", action="store_true", help="print command execution details")
    p_search.set_defaults(func=cmd_search)

    p_status = sub.add_parser("status", help="show vault operational status")
    _add_config_arg(p_status, default=argparse.SUPPRESS)
    p_status.add_argument("--json", action="store_true", help="emit JSON instead of one-line")
    p_status.add_argument("--oneline", action="store_true", help="emit compact one-line status")
    p_status.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p_status.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    p_status.add_argument("--inbox-scanner", default=DEFAULT_INBOX_SCANNER)
    p_status.add_argument("--docs-root", action="append", default=[], help=argparse.SUPPRESS)
    p_status.add_argument("--photos-root", action="append", default=[], help=argparse.SUPPRESS)
    p_status.add_argument("--summary-base-url", default=None, help=argparse.SUPPRESS)
    p_status.add_argument("--embed-base-url", default=None, help=argparse.SUPPRESS)
    p_status.add_argument("--redaction-base-url", default=None, help=argparse.SUPPRESS)
    p_status.add_argument("--disable-photo-analysis", action="store_true", help=argparse.SUPPRESS)
    p_status.add_argument("--photo-analysis-url", default=None, help=argparse.SUPPRESS)
    p_status.add_argument("--disable-pdf-service", action="store_true", help=argparse.SUPPRESS)
    p_status.add_argument("--pdf-parse-url", default=None, help=argparse.SUPPRESS)
    p_status.add_argument("--verbose", action="store_true", help="print command execution details")
    p_status.set_defaults(func=cmd_status)

    p_upgrade = sub.add_parser("upgrade", help="plan/apply policy-aware index upgrades")
    _add_config_arg(p_upgrade, default=argparse.SUPPRESS)
    p_upgrade.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p_upgrade.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    p_upgrade.add_argument("--docs-root", action="append", default=[])
    p_upgrade.add_argument("--photos-root", action="append", default=[])
    p_upgrade.add_argument("--source", choices=source_choices(), default="all", help="select which source kind to index/query")
    p_upgrade.add_argument("--index-level", choices=["redacted", "full", "all"], default="redacted")
    p_upgrade.add_argument("--max", type=int, default=None)
    p_upgrade.add_argument("--reprocess-missing-summaries", type=int, default=-1)
    p_upgrade.add_argument("--reprocess-missing-photo-analysis", type=int, default=-1)
    p_upgrade.add_argument("--disable-summary", action="store_true")
    p_upgrade.add_argument("--disable-photo-analysis", action="store_true")
    p_upgrade.add_argument("--embed-base-url", default=None)
    p_upgrade.add_argument("--embed-model", default=None)
    p_upgrade.add_argument("--embed-api-key", default=None)
    p_upgrade.add_argument("--embed-timeout", type=int, default=None)
    p_upgrade.add_argument("--summary-base-url", default=None)
    p_upgrade.add_argument("--summary-model", default=None)
    p_upgrade.add_argument("--summary-api-key", default=None)
    p_upgrade.add_argument("--summary-timeout", type=int, default=None)
    p_upgrade.add_argument("--photo-analysis-url", default=None)
    p_upgrade.add_argument("--photo-analysis-timeout", type=int, default=None)
    p_upgrade.add_argument("--photo-analysis-force", action="store_true")
    p_upgrade.add_argument("--disable-pdf-service", action="store_true")
    p_upgrade.add_argument("--pdf-parse-url", default=None)
    p_upgrade.add_argument("--pdf-parse-timeout", type=int, default=None)
    p_upgrade.add_argument("--pdf-parse-profile", choices=["auto", "native", "ocr"], default=None)
    p_upgrade.add_argument("--embed-batch-size", type=int, default=None)
    p_upgrade.add_argument("--embed-batch-tokens", type=int, default=None)
    p_upgrade.add_argument("--embed-max-text-chars", type=int, default=None)
    p_upgrade.add_argument("--disable-redaction", action="store_true")
    p_upgrade.add_argument("--redaction-mode", choices=["regex", "model", "hybrid"], default="hybrid")
    p_upgrade.add_argument("--redaction-profile", default="standard")
    p_upgrade.add_argument("--redaction-instruction", default="")
    p_upgrade.add_argument("--redaction-base-url", default=None)
    p_upgrade.add_argument("--redaction-model", default=None)
    p_upgrade.add_argument("--redaction-api-key", default=None)
    p_upgrade.add_argument("--redaction-timeout", type=int, default=None)
    p_upgrade.add_argument("--json", action="store_true")
    p_upgrade.add_argument("--yes", action="store_true", help="execute upgrade actions instead of dry-run")
    p_upgrade.add_argument("--verbose", action="store_true")
    p_upgrade.set_defaults(func=cmd_upgrade)

    p_migrate = sub.add_parser(
        "migrate-encryption",
        help="migrate plaintext registry/vector DBs to SQLCipher encrypted DBs",
    )
    _add_config_arg(p_migrate, default=argparse.SUPPRESS)
    p_migrate.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p_migrate.add_argument("--vectors-db", default=DEFAULT_VECTORS_DB)
    p_migrate.add_argument("--backup-suffix", default=".plaintext.bak")
    p_migrate.add_argument("--verbose", action="store_true")
    p_migrate.set_defaults(func=cmd_migrate_encryption)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args = _apply_config_defaults(args)
    if getattr(args, "verbose", False) and getattr(args, "_config_path", None):
        print(f"[vault-ops] [config={args._config_path}]")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
