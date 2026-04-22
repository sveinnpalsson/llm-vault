from __future__ import annotations

import json

import pytest
import vault_agent_cli


def test_search_parser_rejects_clearance_override() -> None:
    parser = vault_agent_cli.build_parser()
    with pytest.raises(vault_agent_cli.ParserError):
        parser.parse_args(["search-redacted", "tax receipt", "--clearance", "full"])


def test_agent_parser_rejects_config_override() -> None:
    parser = vault_agent_cli.build_parser()
    with pytest.raises(vault_agent_cli.ParserError):
        parser.parse_args(["status", "--config", "vault-ops.toml"])


def test_search_redacted_builds_enforced_backend_command(monkeypatch: pytest.MonkeyPatch) -> None:
    args = vault_agent_cli.build_parser().parse_args(
        [
            "search-redacted",
            "tax receipt",
            "--source",
            "docs",
            "--top-k",
            "3",
            "--from-date",
            "2025-01-01",
            "--to-date",
            "2025-12-31",
        ]
    )
    captured: dict[str, object] = {}

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["timeout_seconds"] = timeout_seconds
        payload = {"query": "tax receipt", "count": 1, "results": [], "clearance": "redacted"}
        return payload, {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}

    monkeypatch.setattr(vault_agent_cli, "_run_json", fake_run_json)
    rc, payload = vault_agent_cli.cmd_search_redacted(args)
    assert rc == 0
    assert payload["status"] == "ok"
    cmd = captured["cmd"]
    assert isinstance(cmd, list)
    assert "--clearance" in cmd
    assert cmd[cmd.index("--clearance") + 1] == "redacted"
    assert "--search-level" in cmd
    assert cmd[cmd.index("--search-level") + 1] == "redacted"
    assert "--source" in cmd
    assert cmd[cmd.index("--source") + 1] == "docs"
    assert "--config" not in cmd


def test_search_builds_full_backend_command(monkeypatch: pytest.MonkeyPatch) -> None:
    args = vault_agent_cli.build_parser().parse_args(["search", "tax receipt", "--source", "docs", "--top-k", "3"])
    captured: dict[str, object] = {}

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        captured["cmd"] = cmd
        payload = {"query": "tax receipt", "count": 1, "results": [], "clearance": "full"}
        return payload, {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}

    monkeypatch.setattr(vault_agent_cli, "_run_json", fake_run_json)
    rc, payload = vault_agent_cli.cmd_search(args)
    assert rc == 0
    assert payload["status"] == "ok"
    cmd = captured["cmd"]
    assert isinstance(cmd, list)
    assert cmd[cmd.index("--clearance") + 1] == "full"
    assert cmd[cmd.index("--search-level") + 1] == "auto"
    assert cmd[cmd.index("--source") + 1] == "docs"


def test_status_returns_lightweight_agent_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    args = vault_agent_cli.build_parser().parse_args(["status"])

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        payload = {
            "health": "ok",
            "registry": {
                "overall_newest_file_mtime_utc": "2026-03-24T10:00:00+00:00",
                "inbox_pending_files": 0,
                "last_sync_run": {
                    "status": "ok",
                    "started_at": "2026-03-24T09:00:00+00:00",
                    "finished_at": "2026-03-24T09:05:00+00:00",
                    "errors": 0,
                    "detail": {
                        "docs_indexed": 2,
                        "photos_indexed": 1,
                        "mail_indexed": 0,
                        "inbox_routed": 1,
                    },
                },
                "sources": {
                    "docs": {
                        "files_total": 2,
                        "newest_file_mtime_utc": "2026-03-24T10:00:00+00:00",
                    },
                    "photos": {
                        "files_total": 1,
                        "newest_file_mtime_utc": "2026-03-23T15:00:00+00:00",
                    },
                    "mail": {
                        "messages_total": 4,
                        "bridge_enabled": True,
                        "newest_message_date": "2026-03-24T08:30:00+00:00",
                    },
                },
            },
            "vectors": {
                "available": True,
                "available_index_levels": ["redacted", "full"],
                "full_search_available": True,
                "upgrade_needed": False,
                "levels": {
                    "redacted": {
                        "sources": {
                            "docs": {"sources_indexed": 2},
                            "photos": {"sources_indexed": 1},
                            "mail": {"sources_indexed": 4},
                        }
                    },
                    "full": {
                        "sources": {
                            "docs": {"sources_indexed": 2},
                            "photos": {"sources_indexed": 0},
                            "mail": {"sources_indexed": 0},
                        }
                    },
                },
            },
        }
        return payload, {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}

    monkeypatch.setattr(vault_agent_cli, "_run_json", fake_run_json)
    rc, payload = vault_agent_cli.cmd_status(args)
    assert rc == 0
    data = payload["data"]
    assert payload["status"] == "ok"
    assert data["usable"] is True
    assert data["readiness"] == "ready"
    assert data["freshness"] == {
        "status": "current",
        "newest_content_at": "2026-03-24T10:00:00+00:00",
        "last_sync_finished_at": "2026-03-24T09:05:00+00:00",
        "inbox_pending_files": 0,
        "reasons": [],
    }
    assert data["sources"] == {
        "docs": {
            "available": True,
            "approx_count": 2,
            "freshest_at": "2026-03-24T10:00:00+00:00",
            "redacted_indexed": 2,
            "full_indexed": 2,
        },
        "photos": {
            "available": True,
            "approx_count": 1,
            "freshest_at": "2026-03-23T15:00:00+00:00",
            "redacted_indexed": 1,
            "full_indexed": 0,
        },
        "mail": {
            "available": True,
            "approx_count": 4,
            "freshest_at": "2026-03-24T08:30:00+00:00",
            "redacted_indexed": 4,
            "full_indexed": 0,
            "enabled": True,
        },
    }
    assert data["counts"] == {
        "total_items": 7,
        "by_source": {"docs": 2, "photos": 1, "mail": 4},
    }
    assert data["availability"] == {
        "redacted_search": True,
        "full_search": True,
        "vectors_ready": True,
    }
    assert data["last_sync"] == {
        "status": "ok",
        "started_at": "2026-03-24T09:00:00+00:00",
        "finished_at": "2026-03-24T09:05:00+00:00",
        "counts": {
            "docs_indexed": 2,
            "photos_indexed": 1,
            "mail_indexed": 0,
            "inbox_routed": 1,
            "errors": 0,
        },
    }
    assert "paths" not in json.dumps(data)
    assert "table" not in json.dumps(data)


def test_status_marks_unusable_when_redacted_search_is_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    args = vault_agent_cli.build_parser().parse_args(["status"])

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        payload = {
            "health": "degraded",
            "registry": {
                "overall_newest_file_mtime_utc": "2026-03-24T10:00:00+00:00",
                "inbox_pending_files": 3,
                "last_sync_run": {
                    "status": "timeout",
                    "started_at": "2026-03-24T09:00:00+00:00",
                    "finished_at": "2026-03-24T09:05:00+00:00",
                    "errors": 1,
                    "detail": {},
                },
                "sources": {
                    "docs": {"files_total": 1, "newest_file_mtime_utc": "2026-03-24T10:00:00+00:00"},
                },
            },
            "vectors": {
                "available": True,
                "available_index_levels": [],
                "full_search_available": False,
                "upgrade_needed": True,
                "levels": {},
            },
        }
        return payload, {"rc": 0, "stdout": json.dumps(payload), "stderr": ""}

    monkeypatch.setattr(vault_agent_cli, "_run_json", fake_run_json)
    rc, payload = vault_agent_cli.cmd_status(args)
    assert rc == 0
    assert payload["data"]["usable"] is False
    assert payload["data"]["readiness"] == "unavailable"
    assert payload["data"]["freshness"] == {
        "status": "stale",
        "newest_content_at": "2026-03-24T10:00:00+00:00",
        "last_sync_finished_at": "2026-03-24T09:05:00+00:00",
        "inbox_pending_files": 3,
        "reasons": ["index_upgrade_needed", "inbox_pending", "last_sync_timeout"],
    }


def test_search_redacted_classifies_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    args = vault_agent_cli.build_parser().parse_args(["search-redacted", "passport"])

    def fake_run_json(cmd, *, cwd, timeout_seconds):
        return None, {
            "rc": 2,
            "stdout": "",
            "stderr": "LLM_VAULT_DB_PASSWORD is required",
        }

    monkeypatch.setattr(vault_agent_cli, "_run_json", fake_run_json)
    rc, payload = vault_agent_cli.cmd_search_redacted(args)
    assert rc == 2
    assert payload["status"] == "error"
    assert payload["errorCode"] == "missing_secret"
    assert payload["details"]["enforced"] == {"clearance": "redacted", "search_level": "redacted"}


def test_answer_redacted_returns_deferred_payload() -> None:
    args = vault_agent_cli.build_parser().parse_args(
        ["answer-redacted", "What was the tax total?", "--source", "docs", "--top-k", "2"]
    )
    rc, payload = vault_agent_cli.cmd_answer_redacted(args)
    assert rc == 0
    assert payload["status"] == "deferred"
    assert payload["operation"] == "answer_redacted"
    assert payload["request"] == {"query": "What was the tax total?", "source": "docs", "top_k": 2}


def test_main_emits_json_error_for_invalid_request(capsys: pytest.CaptureFixture[str]) -> None:
    rc = vault_agent_cli.main(["search-redacted", "tax receipt", "--top-k", "50"])
    assert rc == 2
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "error"
    assert out["errorCode"] == "invalid_request"
