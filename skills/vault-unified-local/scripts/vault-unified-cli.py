#!/usr/bin/env python3
"""Unified local skill CLI for inbox-vault + llm-vault."""

from __future__ import annotations

import argparse
import json
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class UnifiedConfig:
    docs_repo: Path
    inbox_repo: Path
    llm_vault_ops_cmd: str = "./vault-ops"
    docs_vector_index_cmd: str = "scripts/vault_vector_index.py"
    inbox_cli_cmd: str = "skills/inbox-vault-local/scripts/iv-cli.sh"
    default_top_k: int = 10
    default_clearance: str = "redacted"
    rrf_k: int = 60
    inbox_weight: float = 1.0
    docs_weight: float = 1.0
    photos_weight: float = 1.0
    timeout_seconds: int = 120
    enable_inbox: bool = True
    enable_docs: bool = True
    enable_photos: bool = True


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
    parsed = _extract_first_json((run.get("stdout") or ""))
    return parsed, run


def _parse_sources(raw: str) -> set[str]:
    val = str(raw or "all").strip().lower()
    if val == "all":
        return {"inbox", "docs", "photos"}
    parts = {part.strip().lower() for part in val.split(",") if part.strip()}
    valid = {"inbox", "docs", "photos"}
    return {part for part in parts if part in valid}


def _effective_sources(requested: set[str], cfg: UnifiedConfig) -> set[str]:
    enabled: set[str] = set()
    if cfg.enable_inbox:
        enabled.add("inbox")
    if cfg.enable_docs:
        enabled.add("docs")
    if cfg.enable_photos:
        enabled.add("photos")
    return requested & enabled


def _empty_sources_payload(command: str, requested: set[str], cfg: UnifiedConfig) -> dict[str, Any]:
    return {
        "command": command,
        "ok": False,
        "combined_health": "error",
        "error": "No sources selected after applying source_toggles",
        "sources_requested": sorted(requested),
        "source_toggles": {
            "inbox": cfg.enable_inbox,
            "docs": cfg.enable_docs,
            "photos": cfg.enable_photos,
        },
    }


def _title_from_inbox_content(text: str) -> str:
    for line in (text or "").splitlines():
        if line.lower().startswith("subject:"):
            return line.split(":", 1)[1].strip()
    return "email"


def _normalize_inbox_hits(payload: dict[str, Any], *, clearance: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in payload.get("results", []) or []:
        if not isinstance(item, dict):
            continue
        content = str(item.get("content") or "")
        out.append(
            {
                "source": "inbox",
                "source_id": str(item.get("msg_id") or ""),
                "score": float(item.get("score") or 0.0),
                "timestamp": item.get("date") or item.get("date_iso"),
                "title": _title_from_inbox_content(content),
                "preview": content,
                "clearance": clearance,
                "metadata": {
                    "account_email": item.get("account_email"),
                    "thread_id": item.get("thread_id"),
                    "labels": item.get("labels") or [],
                    "from_addr": item.get("from_addr"),
                    "to_addr": item.get("to_addr"),
                },
            }
        )
    return out


def _normalize_docs_hits(payload: dict[str, Any], *, clearance: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in payload.get("results", []) or []:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        table = str(item.get("source_table") or "")
        source = "docs" if table == "docs_registry" else "photos"
        filepath = str(item.get("source_filepath") or "")
        title = Path(filepath).name or source
        out.append(
            {
                "source": source,
                "source_id": str(item.get("item_id") or ""),
                "score": float(item.get("score") or 0.0),
                "timestamp": item.get("source_updated_at") or metadata.get("date_taken"),
                "title": title,
                "preview": str(item.get("preview") or ""),
                "clearance": clearance,
                "metadata": {
                    "source_table": table,
                    "source_filepath": filepath,
                    "chunk_index": item.get("chunk_index"),
                    "chunk_count": item.get("chunk_count"),
                    **metadata,
                },
            }
        )
    return out


def _weighted_rrf(
    ranked_by_source: dict[str, list[dict[str, Any]]],
    *,
    rrf_k: int,
    weights: dict[str, float],
    top_k: int,
) -> list[dict[str, Any]]:
    pooled: dict[str, dict[str, Any]] = {}
    for source, items in ranked_by_source.items():
        weight = float(weights.get(source, 1.0))
        for rank, item in enumerate(items, start=1):
            key = f"{item.get('source')}::{item.get('source_id')}"
            bucket = pooled.setdefault(
                key,
                {"item": item, "fused_score": 0.0, "channels": []},
            )
            bucket["fused_score"] += weight * (1.0 / (int(rrf_k) + rank))
            bucket["channels"].append({"source": source, "rank": rank, "weight": weight})

    merged: list[dict[str, Any]] = []
    for value in pooled.values():
        item = dict(value["item"])
        item["score"] = round(float(value["fused_score"]), 8)
        item["rank_score_rrf"] = item["score"]
        item["channels"] = value["channels"]
        merged.append(item)
    merged.sort(key=lambda x: (-float(x["score"]), str(x.get("source_id") or "")))
    return merged[: max(1, int(top_k))]


def _load_config(args: argparse.Namespace) -> UnifiedConfig:
    script_dir = Path(__file__).resolve().parent
    skill_root = script_dir.parent
    current_repo = skill_root.parent.parent
    if current_repo.name == "inbox-vault":
        inbox_repo_default = current_repo
        docs_repo_default = current_repo.parent / "llm-vault"
    else:
        docs_repo_default = current_repo
        inbox_repo_default = current_repo.parent / "inbox-vault"

    cfg = UnifiedConfig(docs_repo=docs_repo_default, inbox_repo=inbox_repo_default)
    if args.config:
        cfg_path = Path(args.config).expanduser().resolve()
        raw = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
        repos = raw.get("repos", {}) if isinstance(raw.get("repos"), dict) else {}
        commands = raw.get("commands", {}) if isinstance(raw.get("commands"), dict) else {}
        defaults = raw.get("defaults", {}) if isinstance(raw.get("defaults"), dict) else {}
        weights = raw.get("weights", {}) if isinstance(raw.get("weights"), dict) else {}
        source_toggles = (
            raw.get("source_toggles", {})
            if isinstance(raw.get("source_toggles"), dict)
            else {}
        )
        cfg.docs_repo = Path(str(repos.get("docs_repo", cfg.docs_repo))).expanduser().resolve()
        cfg.inbox_repo = Path(str(repos.get("inbox_repo", cfg.inbox_repo))).expanduser().resolve()
        cfg.llm_vault_ops_cmd = str(commands.get("llm_vault_ops_cmd", cfg.llm_vault_ops_cmd))
        cfg.docs_vector_index_cmd = str(
            commands.get("docs_vector_index_cmd", cfg.docs_vector_index_cmd)
        )
        cfg.inbox_cli_cmd = str(commands.get("inbox_cli_cmd", cfg.inbox_cli_cmd))
        cfg.default_top_k = int(defaults.get("top_k", cfg.default_top_k))
        cfg.default_clearance = str(defaults.get("clearance", cfg.default_clearance))
        cfg.rrf_k = int(defaults.get("rrf_k", cfg.rrf_k))
        cfg.timeout_seconds = int(defaults.get("timeout_seconds", cfg.timeout_seconds))
        cfg.inbox_weight = float(weights.get("inbox", cfg.inbox_weight))
        cfg.docs_weight = float(weights.get("docs", cfg.docs_weight))
        cfg.photos_weight = float(weights.get("photos", cfg.photos_weight))
        cfg.enable_inbox = bool(source_toggles.get("enable_inbox", cfg.enable_inbox))
        cfg.enable_docs = bool(source_toggles.get("enable_docs", cfg.enable_docs))
        cfg.enable_photos = bool(source_toggles.get("enable_photos", cfg.enable_photos))

    if args.docs_repo:
        cfg.docs_repo = Path(args.docs_repo).expanduser().resolve()
    if args.inbox_repo:
        cfg.inbox_repo = Path(args.inbox_repo).expanduser().resolve()
    return cfg


def _append_docs_source_flags(cmd: list[str], sources: set[str]) -> None:
    docs_selected = "docs" in sources
    photos_selected = "photos" in sources
    if docs_selected and not photos_selected:
        cmd += ["--source", "docs"]
    elif photos_selected and not docs_selected:
        cmd += ["--source", "photos"]
    elif docs_selected and photos_selected:
        cmd += ["--source", "all"]


def cmd_status(args: argparse.Namespace, cfg: UnifiedConfig) -> int:
    requested_sources = _parse_sources(args.sources)
    sources = _effective_sources(requested_sources, cfg)
    if not sources:
        print(json.dumps(_empty_sources_payload("status", requested_sources, cfg), indent=2))
        return 2
    out: dict[str, Any] = {
        "command": "status",
        "sources_requested": sorted(requested_sources),
        "sources_used": sorted(sources),
        "source_toggles": {
            "inbox": cfg.enable_inbox,
            "docs": cfg.enable_docs,
            "photos": cfg.enable_photos,
        },
        "subsystems": {},
    }

    if "inbox" in sources:
        payload, run = _run_json(
            [cfg.inbox_cli_cmd, "status", "--json"],
            cwd=cfg.inbox_repo,
            timeout_seconds=cfg.timeout_seconds,
        )
        out["subsystems"]["inbox_vault"] = {
            "ok": run["rc"] == 0 and isinstance(payload, dict),
            "rc": run["rc"],
            "status": payload if isinstance(payload, dict) else None,
            "stderr_tail": (run.get("stderr") or "").splitlines()[-5:],
        }

    if {"docs", "photos"} & sources:
        payload, run = _run_json(
            [cfg.llm_vault_ops_cmd, "status", "--json"],
            cwd=cfg.docs_repo,
            timeout_seconds=cfg.timeout_seconds,
        )
        out["subsystems"]["llm_vault"] = {
            "ok": run["rc"] == 0 and isinstance(payload, dict),
            "rc": run["rc"],
            "status": payload if isinstance(payload, dict) else None,
            "stderr_tail": (run.get("stderr") or "").splitlines()[-5:],
        }

    failures = [name for name, block in out["subsystems"].items() if not bool(block.get("ok"))]
    out["ok"] = len(failures) == 0
    out["partial_failures"] = failures
    docs_block = out["subsystems"].get("llm_vault")
    docs_health = (
        ((docs_block or {}).get("status") or {}).get("health")
        if isinstance((docs_block or {}).get("status"), dict)
        else None
    )
    if out["ok"]:
        out["combined_health"] = "ok"
    elif out["subsystems"] and len(failures) < len(out["subsystems"]):
        out["combined_health"] = "degraded"
    else:
        out["combined_health"] = "error"
    out["diagnostics"] = {"docs_health": docs_health, "failed_subsystems": failures}
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0 if out["ok"] else 1


def cmd_update_or_repair(args: argparse.Namespace, cfg: UnifiedConfig, *, command: str) -> int:
    requested_sources = _parse_sources(args.sources)
    sources = _effective_sources(requested_sources, cfg)
    if not sources:
        print(json.dumps(_empty_sources_payload(command, requested_sources, cfg), indent=2))
        return 2
    out: dict[str, Any] = {
        "command": command,
        "sources_requested": sorted(requested_sources),
        "sources_used": sorted(sources),
        "subsystems": {},
    }

    if "inbox" in sources:
        inbox_cmd = [cfg.inbox_cli_cmd, command]
        run = _run_capture(inbox_cmd, cwd=cfg.inbox_repo, timeout_seconds=cfg.timeout_seconds)
        out["subsystems"]["inbox_vault"] = {
            "rc": run["rc"],
            "ok": run["rc"] == 0,
            "stdout_tail": (run.get("stdout") or "").splitlines()[-10:],
            "stderr_tail": (run.get("stderr") or "").splitlines()[-10:],
        }

    if {"docs", "photos"} & sources:
        docs_cmd = [cfg.llm_vault_ops_cmd, command]
        _append_docs_source_flags(docs_cmd, sources)
        if args.max is not None and args.max > 0:
            docs_cmd += ["--max", str(args.max)]
        run = _run_capture(docs_cmd, cwd=cfg.docs_repo, timeout_seconds=cfg.timeout_seconds)
        out["subsystems"]["llm_vault"] = {
            "rc": run["rc"],
            "ok": run["rc"] == 0,
            "stdout_tail": (run.get("stdout") or "").splitlines()[-10:],
            "stderr_tail": (run.get("stderr") or "").splitlines()[-10:],
        }

    failures = [name for name, block in out["subsystems"].items() if not bool(block.get("ok"))]
    out["ok"] = len(failures) == 0
    out["partial_failures"] = failures
    out["combined_health"] = "ok" if out["ok"] else ("degraded" if out["subsystems"] else "error")
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0 if out["ok"] else 1


def cmd_search(args: argparse.Namespace, cfg: UnifiedConfig) -> int:
    requested_sources = _parse_sources(args.sources)
    sources = _effective_sources(requested_sources, cfg)
    if not sources:
        print(json.dumps(_empty_sources_payload("search", requested_sources, cfg), indent=2))
        return 2
    top_k = max(1, int(args.top_k or cfg.default_top_k))
    clearance = str(args.clearance or cfg.default_clearance)
    rrf_k = max(1, int(args.rrf_k or cfg.rrf_k))

    ranked_by_source: dict[str, list[dict[str, Any]]] = {}
    diagnostics: dict[str, Any] = {"raw_counts": {}, "errors": {}}

    if "inbox" in sources:
        payload, run = _run_json(
            [
                cfg.inbox_cli_cmd,
                "search",
                args.query,
                "--top-k",
                str(top_k),
                "--clearance",
                clearance,
            ],
            cwd=cfg.inbox_repo,
            timeout_seconds=cfg.timeout_seconds,
        )
        if run["rc"] == 0 and isinstance(payload, dict):
            ranked_by_source["inbox"] = _normalize_inbox_hits(payload, clearance=clearance)
            diagnostics["raw_counts"]["inbox"] = len(ranked_by_source["inbox"])
        else:
            diagnostics["errors"]["inbox"] = {
                "rc": run["rc"],
                "stderr_tail": (run.get("stderr") or "").splitlines()[-6:],
            }

    if "docs" in sources:
        payload, run = _run_json(
            [
                cfg.llm_vault_ops_cmd,
                "search",
                args.query,
                "--top-k",
                str(top_k),
                "--clearance",
                clearance,
                "--source",
                "docs",
                "--json",
            ],
            cwd=cfg.docs_repo,
            timeout_seconds=cfg.timeout_seconds,
        )
        if run["rc"] == 0 and isinstance(payload, dict):
            ranked_by_source["docs"] = _normalize_docs_hits(payload, clearance=clearance)
            diagnostics["raw_counts"]["docs"] = len(ranked_by_source["docs"])
        else:
            diagnostics["errors"]["docs"] = {
                "rc": run["rc"],
                "stderr_tail": (run.get("stderr") or "").splitlines()[-6:],
            }

    if "photos" in sources:
        payload, run = _run_json(
            [
                cfg.llm_vault_ops_cmd,
                "search",
                args.query,
                "--top-k",
                str(top_k),
                "--clearance",
                clearance,
                "--source",
                "photos",
                "--json",
            ],
            cwd=cfg.docs_repo,
            timeout_seconds=cfg.timeout_seconds,
        )
        if run["rc"] == 0 and isinstance(payload, dict):
            ranked_by_source["photos"] = _normalize_docs_hits(payload, clearance=clearance)
            diagnostics["raw_counts"]["photos"] = len(ranked_by_source["photos"])
        else:
            diagnostics["errors"]["photos"] = {
                "rc": run["rc"],
                "stderr_tail": (run.get("stderr") or "").splitlines()[-6:],
            }

    fused = _weighted_rrf(
        ranked_by_source,
        rrf_k=rrf_k,
        weights={
            "inbox": float(args.inbox_weight or cfg.inbox_weight),
            "docs": float(args.docs_weight or cfg.docs_weight),
            "photos": float(args.photos_weight or cfg.photos_weight),
        },
        top_k=top_k,
    )

    payload_out = {
        "query": args.query,
        "count": len(fused),
        "sources_requested": sorted(requested_sources),
        "sources_used": sorted(sources),
        "default_clearance": clearance,
        "results": fused,
        "diagnostics": diagnostics,
    }
    print(json.dumps(payload_out, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Unified local skill CLI for inbox-vault + llm-vault")
    p.add_argument("--config", default=None, help="Optional TOML config path")
    p.add_argument("--docs-repo", default=None, help="Override llm-vault repo path")
    p.add_argument("--inbox-repo", default=None, help="Override inbox-vault repo path")
    p.add_argument(
        "--sources",
        default="all",
        help="Comma list: inbox,docs,photos or all (default)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status")
    p_status.set_defaults(handler=cmd_status)

    p_update = sub.add_parser("update")
    p_update.add_argument("--max", type=int, default=None)
    p_update.set_defaults(handler=lambda a, c: cmd_update_or_repair(a, c, command="update"))

    p_repair = sub.add_parser("repair")
    p_repair.add_argument("--max", type=int, default=None)
    p_repair.set_defaults(handler=lambda a, c: cmd_update_or_repair(a, c, command="repair"))

    p_search = sub.add_parser("search")
    p_search.add_argument("query")
    p_search.add_argument("--top-k", type=int, default=None)
    p_search.add_argument("--clearance", choices=["redacted", "full"], default=None)
    p_search.add_argument("--rrf-k", type=int, default=None)
    p_search.add_argument("--inbox-weight", type=float, default=None)
    p_search.add_argument("--docs-weight", type=float, default=None)
    p_search.add_argument("--photos-weight", type=float, default=None)
    p_search.set_defaults(handler=cmd_search)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    cfg = _load_config(args)
    return int(args.handler(args, cfg))


if __name__ == "__main__":
    raise SystemExit(main())
