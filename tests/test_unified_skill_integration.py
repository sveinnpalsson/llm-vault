from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def _load_skill_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "skills"
        / "vault-unified-local"
        / "scripts"
        / "vault-unified-cli.py"
    )
    spec = importlib.util.spec_from_file_location("vault_unified_cli", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_weighted_rrf_is_deterministic_and_sorted() -> None:
    mod = _load_skill_module()
    ranked = {
        "inbox": [
            {
                "source": "inbox",
                "source_id": "m1",
                "score": 0.9,
                "timestamp": None,
                "title": "a",
                "preview": "a",
                "clearance": "redacted",
                "metadata": {},
            },
            {
                "source": "inbox",
                "source_id": "m2",
                "score": 0.8,
                "timestamp": None,
                "title": "b",
                "preview": "b",
                "clearance": "redacted",
                "metadata": {},
            },
        ],
        "docs": [
            {
                "source": "docs",
                "source_id": "d1",
                "score": 0.95,
                "timestamp": "2026-03-17T00:00:00+00:00",
                "title": "c",
                "preview": "c",
                "clearance": "redacted",
                "metadata": {},
            },
            {
                "source": "docs",
                "source_id": "m1",
                "score": 0.4,
                "timestamp": "2026-03-17T00:00:00+00:00",
                "title": "dup",
                "preview": "dup",
                "clearance": "redacted",
                "metadata": {},
            },
        ],
    }
    fused = mod._weighted_rrf(
        ranked,
        rrf_k=60,
        weights={"inbox": 1.0, "docs": 1.0, "photos": 1.0},
        top_k=5,
    )
    assert len(fused) >= 3
    assert fused[0]["score"] >= fused[1]["score"]
    ids = [f"{item['source']}::{item['source_id']}" for item in fused]
    assert "inbox::m1" in ids


def test_cmd_search_handles_partial_source_failures(monkeypatch, capsys) -> None:
    mod = _load_skill_module()
    cfg = mod.UnifiedConfig(
        docs_repo=Path("/tmp/llm-vault"),
        inbox_repo=Path("/tmp/inbox-vault"),
        timeout_seconds=10,
    )
    args = argparse.Namespace(
        query="tax receipt",
        sources="all",
        top_k=5,
        clearance="redacted",
        rrf_k=60,
        inbox_weight=1.0,
        docs_weight=1.0,
        photos_weight=1.0,
    )

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        cmd_str = " ".join(cmd)
        if "iv-cli.sh search" in cmd_str:
            payload = {
                "count": 1,
                "results": [
                    {
                        "score": 0.88,
                        "msg_id": "email-1",
                        "account_email": "me@example.com",
                        "thread_id": "t1",
                        "labels": ["INBOX"],
                        "from_addr": "a@example.com",
                        "to_addr": "b@example.com",
                        "content": "Subject: Tax receipt from scanner",
                    }
                ],
            }
            run = {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}
            return payload, run
        if "docs_registry" in cmd_str:
            payload = {
                "count": 1,
                "results": [
                    {
                        "score": 0.91,
                        "item_id": "docs:abc:0",
                        "source_table": "docs_registry",
                        "source_filepath": "/vault/docs/tax.txt",
                        "source_updated_at": "2026-03-16T00:00:00+00:00",
                        "chunk_index": 0,
                        "chunk_count": 1,
                        "preview": "tax receipt",
                        "metadata": {},
                    }
                ],
            }
            run = {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}
            return payload, run
        return None, {"rc": 2, "stdout": "", "stderr": "photos backend unavailable"}

    monkeypatch.setattr(mod, "_run_json", fake_run_json)
    rc = mod.cmd_search(args, cfg)
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["count"] >= 1
    assert out["query"] == "tax receipt"
    assert out["sources_used"] == ["docs", "inbox", "photos"]
    assert "photos" in out["diagnostics"]["errors"]
    hit = out["results"][0]
    for key in (
        "source",
        "source_id",
        "score",
        "timestamp",
        "title",
        "preview",
        "clearance",
        "metadata",
    ):
        assert key in hit
    assert hit["clearance"] == "redacted"


def test_cmd_status_reports_partial_failures(monkeypatch, capsys) -> None:
    mod = _load_skill_module()
    cfg = mod.UnifiedConfig(
        docs_repo=Path("/tmp/llm-vault"),
        inbox_repo=Path("/tmp/inbox-vault"),
        timeout_seconds=10,
    )
    args = argparse.Namespace(sources="all")

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        cmd_str = " ".join(cmd)
        if "iv-cli.sh status" in cmd_str:
            payload = {"health": "ok", "messages": 100}
            return payload, {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}
        payload = {"health": "degraded"}
        return payload, {"rc": 3, "stdout": json.dumps(payload), "stderr": "db unavailable"}

    monkeypatch.setattr(mod, "_run_json", fake_run_json)
    rc = mod.cmd_status(args, cfg)
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["combined_health"] == "degraded"
    assert out["ok"] is False
    assert "llm_vault" in out["partial_failures"]
    assert "inbox_vault" in out["subsystems"]
    assert "llm_vault" in out["subsystems"]


def test_cmd_search_fails_when_all_sources_disabled(capsys) -> None:
    mod = _load_skill_module()
    cfg = mod.UnifiedConfig(
        docs_repo=Path("/tmp/llm-vault"),
        inbox_repo=Path("/tmp/inbox-vault"),
        enable_inbox=False,
        enable_docs=False,
        enable_photos=False,
    )
    args = argparse.Namespace(
        query="anything",
        sources="all",
        top_k=3,
        clearance="redacted",
        rrf_k=60,
        inbox_weight=1.0,
        docs_weight=1.0,
        photos_weight=1.0,
    )
    rc = mod.cmd_search(args, cfg)
    assert rc == 2
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert "source_toggles" in out
