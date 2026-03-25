#!/usr/bin/env python3
"""Structured read-only agent wrapper for llm-vault."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

from vault_sources import source_choices

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VAULT_OPS = ROOT / "vault-ops"
DEFAULT_TIMEOUT_SECONDS = 120
MAX_SAFE_TOP_K = 10


class ParserError(ValueError):
    """Raised when the agent wrapper receives an invalid request."""


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ParserError(message)


def _extract_first_json(text: str) -> dict[str, Any] | list[Any] | None:
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
        if char in "{[":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if char in "}]" and depth > 0:
            depth -= 1
            if depth == 0 and start is not None:
                candidate = text[start : idx + 1]
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    start = None
                    continue
                if isinstance(parsed, (dict, list)):
                    return parsed
                start = None
    return None


def _iso_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc
    return parsed.isoformat()


def _safe_top_k(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("top_k must be an integer") from exc
    if not 1 <= parsed <= MAX_SAFE_TOP_K:
        raise argparse.ArgumentTypeError(f"top_k must be between 1 and {MAX_SAFE_TOP_K}")
    return parsed


def _run_capture(cmd: list[str], *, cwd: Path, timeout_seconds: int) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=max(1, int(timeout_seconds)),
    )
    return {
        "cmd": cmd,
        "cwd": str(cwd),
        "rc": int(proc.returncode),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _run_json(cmd: list[str], *, cwd: Path, timeout_seconds: int) -> tuple[dict[str, Any] | list[Any] | None, dict[str, Any]]:
    run = _run_capture(cmd, cwd=cwd, timeout_seconds=timeout_seconds)
    parsed = _extract_first_json(str(run.get("stdout") or ""))
    return parsed, run


def _trim_text(value: str, *, limit: int = 400) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _error_payload(
    operation: str,
    error_code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "error",
        "operation": operation,
        "errorCode": error_code,
        "message": message,
    }
    if details:
        payload["details"] = details
    return payload


def _classify_backend_error(run: dict[str, Any]) -> tuple[str, str]:
    text = "\n".join(
        part for part in [str(run.get("stderr") or "").strip(), str(run.get("stdout") or "").strip()] if part
    )
    lowered = text.lower()
    if "llm_vault_db_password" in lowered or "docs_vault_db_password" in lowered:
        return "missing_secret", "LLM_VAULT_DB_PASSWORD is required"
    if "config file not found" in lowered:
        return "config_not_found", "vault-ops config file was not found"
    if "invalid config toml" in lowered:
        return "invalid_config", "vault-ops config file is invalid"
    if "db not found" in lowered:
        return "backend_unavailable", "vault database is unavailable"
    return "backend_failed", "vault-ops command failed"


def _success_payload(
    operation: str,
    *,
    data: dict[str, Any] | list[Any],
    request: dict[str, Any] | None = None,
    enforced: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "ok",
        "operation": operation,
        "backend": "vault-ops",
        "data": data,
    }
    if request:
        payload["request"] = request
    if enforced:
        payload["enforced"] = enforced
    return payload


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _source_freshness_hint(source_payload: dict[str, Any]) -> str | None:
    for key in ("newest_file_mtime_utc", "newest_message_date"):
        value = source_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _summarize_source(
    name: str,
    source_payload: dict[str, Any],
    *,
    redacted_sources: dict[str, Any],
    full_sources: dict[str, Any],
) -> dict[str, Any]:
    approx_count = _safe_int(source_payload.get("files_total") or source_payload.get("messages_total"))
    redacted_state = redacted_sources.get(name) if isinstance(redacted_sources, dict) else {}
    full_state = full_sources.get(name) if isinstance(full_sources, dict) else {}
    redacted_indexed = _safe_int((redacted_state or {}).get("sources_indexed"))
    full_indexed = _safe_int((full_state or {}).get("sources_indexed"))

    summary: dict[str, Any] = {
        "available": approx_count > 0,
        "approx_count": approx_count,
        "freshest_at": _source_freshness_hint(source_payload),
        "redacted_indexed": redacted_indexed,
        "full_indexed": full_indexed,
    }
    if name == "mail":
        summary["enabled"] = bool(source_payload.get("bridge_enabled"))
    return summary


def _last_sync_summary(last_run: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(last_run, dict):
        return None

    detail = last_run.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    counts = {
        "docs_indexed": _safe_int(detail.get("docs_indexed")),
        "photos_indexed": _safe_int(detail.get("photos_indexed")),
        "mail_indexed": _safe_int(detail.get("mail_indexed")),
        "inbox_routed": _safe_int(detail.get("inbox_routed")),
        "errors": _safe_int(last_run.get("errors")),
    }
    return {
        "status": str(last_run.get("status") or "unknown"),
        "started_at": last_run.get("started_at"),
        "finished_at": last_run.get("finished_at"),
        "counts": counts,
    }


def _freshness_summary(
    *,
    last_sync: dict[str, Any] | None,
    newest_content_at: str | None,
    inbox_pending_files: int,
    upgrade_needed: bool,
) -> dict[str, Any]:
    status = "unknown"
    if upgrade_needed or inbox_pending_files > 0:
        status = "stale"
    elif last_sync and last_sync.get("status") == "ok":
        status = "current"
    elif newest_content_at:
        status = "needs_sync"
    reasons: list[str] = []
    if upgrade_needed:
        reasons.append("index_upgrade_needed")
    if inbox_pending_files > 0:
        reasons.append("inbox_pending")
    if last_sync and last_sync.get("status") not in {None, "", "ok"}:
        reasons.append(f"last_sync_{last_sync['status']}")
    return {
        "status": status,
        "newest_content_at": newest_content_at,
        "last_sync_finished_at": None if not last_sync else last_sync.get("finished_at"),
        "inbox_pending_files": inbox_pending_files,
        "reasons": reasons,
    }


def _agent_status_from_backend(payload: dict[str, Any]) -> dict[str, Any]:
    registry = payload.get("registry") if isinstance(payload.get("registry"), dict) else {}
    vectors = payload.get("vectors") if isinstance(payload.get("vectors"), dict) else {}
    sources = registry.get("sources") if isinstance(registry.get("sources"), dict) else {}
    levels = vectors.get("levels") if isinstance(vectors.get("levels"), dict) else {}
    redacted_level = levels.get("redacted") if isinstance(levels.get("redacted"), dict) else {}
    full_level = levels.get("full") if isinstance(levels.get("full"), dict) else {}
    redacted_sources = redacted_level.get("sources") if isinstance(redacted_level.get("sources"), dict) else {}
    full_sources = full_level.get("sources") if isinstance(full_level.get("sources"), dict) else {}
    last_sync = _last_sync_summary(registry.get("last_sync_run") if isinstance(registry.get("last_sync_run"), dict) else None)
    newest_content_at = registry.get("overall_newest_file_mtime_utc")
    newest_content_at = newest_content_at if isinstance(newest_content_at, str) else None
    inbox_pending_files = _safe_int(registry.get("inbox_pending_files"))
    upgrade_needed = bool(vectors.get("upgrade_needed"))
    redacted_available = "redacted" in list(vectors.get("available_index_levels") or [])
    full_available = bool(vectors.get("full_search_available"))
    source_summaries = {
        name: _summarize_source(
            name,
            source_payload if isinstance(source_payload, dict) else {},
            redacted_sources=redacted_sources,
            full_sources=full_sources,
        )
        for name, source_payload in sources.items()
    }
    total_items = sum(_safe_int(source.get("approx_count")) for source in source_summaries.values())

    usable = redacted_available and total_items > 0 and str(payload.get("health") or "") != "error"
    readiness = "unavailable"
    if usable:
        readiness = "ready" if str(payload.get("health") or "") == "ok" and not upgrade_needed else "degraded"

    return {
        "usable": usable,
        "readiness": readiness,
        "freshness": _freshness_summary(
            last_sync=last_sync,
            newest_content_at=newest_content_at,
            inbox_pending_files=inbox_pending_files,
            upgrade_needed=upgrade_needed,
        ),
        "sources": source_summaries,
        "counts": {
            "total_items": total_items,
            "by_source": {name: _safe_int(source.get("approx_count")) for name, source in source_summaries.items()},
        },
        "availability": {
            "redacted_search": redacted_available,
            "full_search": full_available,
            "vectors_ready": bool(vectors.get("available")),
        },
        "last_sync": last_sync,
    }


def _build_status_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [str(DEFAULT_VAULT_OPS)]
    cmd += ["status", "--json"]
    return cmd


def _build_search_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [str(DEFAULT_VAULT_OPS)]
    cmd += [
        "search",
        args.query,
        "--json",
        "--clearance",
        "redacted",
        "--search-level",
        "redacted",
        "--top-k",
        str(args.top_k),
        "--source",
        args.source,
    ]
    if args.from_date:
        cmd += ["--from-date", args.from_date]
    if args.to_date:
        cmd += ["--to-date", args.to_date]
    if args.taxonomy:
        cmd += ["--taxonomy", args.taxonomy]
    if args.category_primary:
        cmd += ["--category-primary", args.category_primary]
    return cmd


def cmd_status(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    operation = "status"
    try:
        payload, run = _run_json(_build_status_cmd(args), cwd=ROOT, timeout_seconds=args.timeout_seconds)
    except FileNotFoundError:
        return 2, _error_payload(operation, "backend_missing", "vault-ops executable was not found")
    except subprocess.TimeoutExpired:
        return 2, _error_payload(operation, "timeout", "vault-ops status timed out")

    if int(run.get("rc") or 0) != 0:
        error_code, message = _classify_backend_error(run)
        details = {
            "rc": int(run.get("rc") or 0),
            "stderr": _trim_text(str(run.get("stderr") or "")),
        }
        if isinstance(payload, (dict, list)):
            details["backendPayload"] = payload
        return 2, _error_payload(operation, error_code, message, details=details)

    if not isinstance(payload, dict):
        return 2, _error_payload(operation, "invalid_backend_output", "vault-ops status did not return JSON")

    return 0, _success_payload(operation, data=_agent_status_from_backend(payload))


def cmd_search_redacted(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    operation = "search_redacted"
    request = {
        "query": args.query,
        "source": args.source,
        "top_k": args.top_k,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "taxonomy": args.taxonomy,
        "category_primary": args.category_primary,
    }
    enforced = {"clearance": "redacted", "search_level": "redacted"}
    try:
        payload, run = _run_json(_build_search_cmd(args), cwd=ROOT, timeout_seconds=args.timeout_seconds)
    except FileNotFoundError:
        return 2, _error_payload(operation, "backend_missing", "vault-ops executable was not found")
    except subprocess.TimeoutExpired:
        return 2, _error_payload(operation, "timeout", "vault-ops search timed out", details={"request": request})

    if int(run.get("rc") or 0) != 0:
        error_code, message = _classify_backend_error(run)
        details = {
            "rc": int(run.get("rc") or 0),
            "stderr": _trim_text(str(run.get("stderr") or "")),
            "request": request,
            "enforced": enforced,
        }
        if isinstance(payload, (dict, list)):
            details["backendPayload"] = payload
        return 2, _error_payload(operation, error_code, message, details=details)

    if not isinstance(payload, dict):
        return 2, _error_payload(
            operation,
            "invalid_backend_output",
            "vault-ops search did not return JSON",
            details={"request": request, "enforced": enforced},
        )

    return 0, _success_payload(operation, data=payload, request=request, enforced=enforced)


def cmd_answer_redacted(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    return 0, {
        "status": "deferred",
        "operation": "answer_redacted",
        "message": "Phase 1 exposes status and search_redacted only; answer_redacted is deferred.",
        "reason": "not_implemented",
        "request": {
            "query": args.query,
            "source": args.source,
            "top_k": args.top_k,
        },
        "enforced": {"clearance": "redacted", "search_level": "redacted"},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Safe structured agent wrapper for llm-vault")
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=argparse.SUPPRESS,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_common = argparse.ArgumentParser(add_help=False)
    search_common.add_argument("query", help="search text")
    search_common.add_argument("--top-k", type=_safe_top_k, default=5, dest="top_k")
    search_common.add_argument("--source", choices=source_choices(), default="all")
    search_common.add_argument("--from-date", type=_iso_date, dest="from_date")
    search_common.add_argument("--to-date", type=_iso_date, dest="to_date")
    search_common.add_argument("--taxonomy")
    search_common.add_argument("--category-primary", dest="category_primary")

    p_status = subparsers.add_parser("status", help="read-only vault readiness")
    p_status.set_defaults(handler=cmd_status)

    p_search = subparsers.add_parser(
        "search-redacted",
        parents=[search_common],
        help="redacted search with enforced redacted retrieval",
    )
    p_search.set_defaults(handler=cmd_search_redacted)

    p_answer = subparsers.add_parser(
        "answer-redacted",
        parents=[search_common],
        help="reserved Phase 1 answer surface (currently deferred)",
    )
    p_answer.set_defaults(handler=cmd_answer_redacted)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except ParserError as exc:
        print(json.dumps(_error_payload("request", "invalid_request", str(exc)), indent=2, ensure_ascii=False))
        return 2

    rc, payload = args.handler(args)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
