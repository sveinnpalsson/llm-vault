from __future__ import annotations

import argparse
from pathlib import Path

import pytest
import vault_ops_cli
from vault_service_defaults import (
    DEFAULT_LOCAL_MODEL_BASE_URL,
    DEFAULT_LOCAL_PDF_PARSE_URL,
    DEFAULT_LOCAL_PHOTO_ANALYSIS_URL,
)


def test_update_default_source_mode_is_all() -> None:
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(["update"])
    assert vault_ops_cli._resolve_source_mode(args) == "all"


def test_source_mode_switches_for_docs_photos_and_mail() -> None:
    parser = vault_ops_cli.build_parser()
    args_docs = parser.parse_args(["update", "--source", "docs"])
    args_photos = parser.parse_args(["update", "--source", "photos"])
    args_mail = parser.parse_args(["update", "--source", "mail"])
    assert vault_ops_cli._resolve_source_mode(args_docs) == "docs"
    assert vault_ops_cli._resolve_source_mode(args_photos) == "photos"
    assert vault_ops_cli._resolve_source_mode(args_mail) == "mail"


def test_removed_legacy_source_flags_fail_parse() -> None:
    parser = vault_ops_cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["update", "--docs-only"])
    with pytest.raises(SystemExit):
        parser.parse_args(["search", "tax receipt", "--source-table", "docs_registry"])


def test_removed_max_seconds_flag_fails_parse() -> None:
    parser = vault_ops_cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["update", "--max-seconds", "300"])
    with pytest.raises(SystemExit):
        parser.parse_args(["repair", "--max-seconds", "300"])


def test_search_command_defaults_to_redacted_clearance() -> None:
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(["search", "tax receipt"])
    assert args.clearance == "redacted"
    assert args.search_level == "auto"


def test_search_parser_has_json_output_flag() -> None:
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(["search", "tax receipt", "--json"])
    assert args.json is True


def test_status_json_uses_json_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        json=True,
        verbose=False,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        inbox_scanner="/tmp/scanner",
    )
    called = {"json_runner": False}

    def fake_run_cmd_json(cmd, *, label, verbose):
        called["json_runner"] = True
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd_json", fake_run_cmd_json)
    rc = vault_ops_cli.cmd_status(args)
    assert rc == 0
    assert called["json_runner"] is True


def test_status_forwards_warning_related_config_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        json=True,
        verbose=False,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        inbox_scanner="/tmp/scanner",
        docs_root=["/tmp/docs-a"],
        photos_root=["/tmp/photos-a"],
        summary_base_url="http://127.0.0.1:8080/v1",
        embed_base_url="http://127.0.0.1:8080/v1",
        redaction_base_url="http://127.0.0.1:8080/v1",
        disable_photo_analysis=False,
        photo_analysis_url="http://127.0.0.1:8081/analyze",
        disable_pdf_service=False,
        pdf_parse_url="http://127.0.0.1:8082/v1/pdf/parse",
        _mail_bridge_enabled=True,
        _mail_bridge_db_path="/tmp/inbox.db",
        _mail_bridge_password_env="INBOX_VAULT_DB_PASSWORD",
        _mail_bridge_include_accounts=[],
        _mail_bridge_import_summary=True,
        _mail_bridge_max_body_chunks=7,
    )
    calls: list[list[str]] = []

    def fake_run_cmd_json(cmd, *, label, verbose):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd_json", fake_run_cmd_json)
    rc = vault_ops_cli.cmd_status(args)
    assert rc == 0
    status_cmd = calls[0]
    assert "--docs-root" in status_cmd
    assert status_cmd[status_cmd.index("--docs-root") + 1] == "/tmp/docs-a"
    assert "--photos-root" in status_cmd
    assert status_cmd[status_cmd.index("--photos-root") + 1] == "/tmp/photos-a"
    assert "--photo-analysis-url" in status_cmd
    assert "--pdf-parse-url" in status_cmd
    assert "--mail-bridge-password-env" in status_cmd
    assert status_cmd[status_cmd.index("--mail-bridge-password-env") + 1] == "INBOX_VAULT_DB_PASSWORD"
    assert "--mail-max-body-chunks" in status_cmd
    assert status_cmd[status_cmd.index("--mail-max-body-chunks") + 1] == "7"


def test_status_defaults_to_verbose_multiline_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        json=False,
        oneline=False,
        verbose=False,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        inbox_scanner="/tmp/scanner",
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_status(args)
    assert rc == 0
    status_cmd = calls[0]
    assert "--json" not in status_cmd
    assert "--oneline" not in status_cmd


def test_status_can_explicitly_request_oneline(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        json=False,
        oneline=True,
        verbose=False,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        inbox_scanner="/tmp/scanner",
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_status(args)
    assert rc == 0
    status_cmd = calls[0]
    assert "--oneline" in status_cmd


def test_search_json_uses_json_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        query="tax",
        top_k=3,
        source="photos",
        clearance="redacted",
        search_level="auto",
        json=True,
        from_date=None,
        to_date=None,
        taxonomy=None,
        category_primary=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        verbose=False,
    )
    called = {"json_runner": False}

    def fake_run_cmd_json(cmd, *, label, verbose):
        called["json_runner"] = True
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd_json", fake_run_cmd_json)
    rc = vault_ops_cli.cmd_search(args)
    assert rc == 0
    assert called["json_runner"] is True


def test_search_forwards_source_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        query="passport",
        top_k=3,
        source="docs",
        clearance="redacted",
        search_level="auto",
        json=False,
        from_date=None,
        to_date=None,
        taxonomy=None,
        category_primary=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        verbose=False,
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_search(args)
    assert rc == 0
    search_cmd = calls[0]
    assert "--source" in search_cmd
    assert search_cmd[search_cmd.index("--source") + 1] == "docs"


def test_update_vector_command_builds_redacted_level(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        max=None,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=[],
        photos_root=[],
        source="all",
        disable_summary=False,
        disable_photo_analysis=False,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=None,
        pdf_parse_timeout=None,
        pdf_parse_profile=None,
        dry_run=False,
        force_vector_update=False,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        verbose=False,
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_update(args)
    assert rc == 0
    assert any(cmd[:3] == [vault_ops_cli.sys.executable, str(vault_ops_cli.VECTOR_INDEX), "update"] for cmd in calls)
    vector_cmd = next(cmd for cmd in calls if str(vault_ops_cli.VECTOR_INDEX) in cmd)
    assert "--index-level" in vector_cmd
    assert vector_cmd[vector_cmd.index("--index-level") + 1] == "redacted"


def test_update_forwards_max_to_registry_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        max=7,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=[],
        photos_root=[],
        source="all",
        disable_summary=False,
        disable_photo_analysis=False,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=None,
        pdf_parse_timeout=None,
        pdf_parse_profile=None,
        dry_run=False,
        force_vector_update=False,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        verbose=False,
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_update(args)
    assert rc == 0
    sync_cmd = next(cmd for cmd in calls if str(vault_ops_cli.REGISTRY_SYNC) in cmd)
    assert "--max-items" in sync_cmd
    assert sync_cmd[sync_cmd.index("--max-items") + 1] == "7"


def test_update_vector_command_forwards_hidden_mail_body_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        max=None,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=[],
        photos_root=[],
        source="mail",
        disable_summary=False,
        disable_photo_analysis=False,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=None,
        pdf_parse_timeout=None,
        pdf_parse_profile=None,
        dry_run=False,
        force_vector_update=False,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        verbose=False,
        _mail_bridge_enabled=True,
        _mail_bridge_db_path="/tmp/inbox.db",
        _mail_bridge_password_env="INBOX_VAULT_DB_PASSWORD",
        _mail_bridge_include_accounts=[],
        _mail_bridge_import_summary=True,
        _mail_bridge_max_body_chunks=7,
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_update(args)
    assert rc == 0
    vector_cmd = next(cmd for cmd in calls if str(vault_ops_cli.VECTOR_INDEX) in cmd)
    assert "--mail-max-body-chunks" in vector_cmd
    assert vector_cmd[vector_cmd.index("--mail-max-body-chunks") + 1] == "7"


def test_update_forwards_custom_roots_and_pdf_service(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        max=None,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=["/tmp/docs-a"],
        photos_root=["/tmp/photos-a"],
        source="all",
        disable_summary=False,
        disable_photo_analysis=False,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=DEFAULT_LOCAL_PDF_PARSE_URL,
        pdf_parse_timeout=120,
        pdf_parse_profile="auto",
        dry_run=False,
        force_vector_update=False,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        verbose=False,
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_update(args)
    assert rc == 0
    sync_cmd = next(cmd for cmd in calls if str(vault_ops_cli.REGISTRY_SYNC) in cmd)
    assert "--docs-root" in sync_cmd
    assert sync_cmd[sync_cmd.index("--docs-root") + 1] == "/tmp/docs-a"
    assert "--photos-root" in sync_cmd
    assert sync_cmd[sync_cmd.index("--photos-root") + 1] == "/tmp/photos-a"
    assert "--pdf-parse-url" in sync_cmd
    assert sync_cmd[sync_cmd.index("--pdf-parse-url") + 1] == DEFAULT_LOCAL_PDF_PARSE_URL


def test_repair_limits_registry_sync_and_vectors_to_this_run(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        max=5,
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=[],
        photos_root=[],
        reprocess_missing_summaries=-1,
        photos=False,
        reprocess_missing_photo_analysis=0,
        source="all",
        disable_summary=False,
        disable_photo_analysis=False,
        embed_base_url=None,
        embed_model=None,
        embed_api_key=None,
        embed_timeout=None,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=None,
        pdf_parse_timeout=None,
        pdf_parse_profile=None,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        no_vectors=False,
        dry_run=False,
        force_vector_update=False,
        verbose=False,
    )
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, label, verbose, dry_run=False):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(vault_ops_cli, "run_cmd", fake_run_cmd)
    rc = vault_ops_cli.cmd_repair(args)
    assert rc == 0
    sync_cmd = next(cmd for cmd in calls if str(vault_ops_cli.REGISTRY_SYNC) in cmd)
    vector_cmd = next(cmd for cmd in calls if str(vault_ops_cli.VECTOR_INDEX) in cmd)
    assert "--max-items" in sync_cmd
    assert sync_cmd[sync_cmd.index("--max-items") + 1] == "5"
    assert "--updated-since" in vector_cmd


def test_upgrade_dry_run_outputs_plan(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    args = argparse.Namespace(
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=[],
        photos_root=[],
        index_level="all",
        max=None,
        reprocess_missing_summaries=-1,
        reprocess_missing_photo_analysis=-1,
        disable_summary=False,
        disable_photo_analysis=False,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=None,
        pdf_parse_timeout=None,
        pdf_parse_profile=None,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        json=True,
        yes=False,
        verbose=False,
    )

    class FakeCompleted:
        def __init__(self):
            self.returncode = 0
            self.stdout = '{"vectors":{"available_index_levels":["redacted"],"upgrade_needed":true}}'
            self.stderr = ""

    monkeypatch.setattr(vault_ops_cli.subprocess, "run", lambda *a, **k: FakeCompleted())
    rc = vault_ops_cli.cmd_upgrade(args)
    assert rc == 0
    payload = capsys.readouterr().out
    assert '"will_execute": false' in payload


def test_config_file_applies_missing_endpoint_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "vault-ops.toml"
    config_path.write_text(
        f"""
[paths]
registry_db = "/tmp/config-registry.db"
vectors_db = "/tmp/config-vectors.db"
docs_roots = ["/tmp/docs-a"]
photos_roots = ["/tmp/photos-a"]

[embedding]
base_url = "{DEFAULT_LOCAL_MODEL_BASE_URL}"
model = "Qwen3-Embedding-8B"

[redaction]
base_url = "{DEFAULT_LOCAL_MODEL_BASE_URL}"
model = "qwen3-14b"

[photo_analysis]
url = "{DEFAULT_LOCAL_PHOTO_ANALYSIS_URL}"

[pdf]
parse_url = "{DEFAULT_LOCAL_PDF_PARSE_URL}"

[mail_bridge]
max_body_chunks = 7

[runtime]
max = 11
""",
        encoding="utf-8",
    )
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(["search", "tax receipt", "--config", str(config_path)])
    args = vault_ops_cli._apply_config_defaults(args)
    assert str(args.registry_db) == "/tmp/config-registry.db"
    assert str(args.vectors_db) == "/tmp/config-vectors.db"
    assert args.embed_base_url == DEFAULT_LOCAL_MODEL_BASE_URL
    assert args.embed_model == "Qwen3-Embedding-8B"
    assert args.redaction_base_url == DEFAULT_LOCAL_MODEL_BASE_URL
    assert args.redaction_model == "qwen3-14b"
    assert args._mail_bridge_max_body_chunks == 7

    args_update = parser.parse_args(["update", "--config", str(config_path)])
    args_update = vault_ops_cli._apply_config_defaults(args_update)
    assert args_update.docs_root == ["/tmp/docs-a"]
    assert args_update.photos_root == ["/tmp/photos-a"]
    assert args_update.photo_analysis_url == DEFAULT_LOCAL_PHOTO_ANALYSIS_URL
    assert args_update.pdf_parse_url == DEFAULT_LOCAL_PDF_PARSE_URL
    assert args_update.max == 11
    assert args_update._mail_bridge_max_body_chunks == 7


def test_config_file_can_disable_optional_services(tmp_path: Path) -> None:
    config_path = tmp_path / "vault-ops.toml"
    config_path.write_text(
        """
[photo_analysis]
disable_service = true

[pdf]
disable_service = true
""",
        encoding="utf-8",
    )
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(["update", "--config", str(config_path)])
    args = vault_ops_cli._apply_config_defaults(args)
    assert args.disable_photo_analysis is True
    assert args.disable_pdf_service is True


def test_cli_flags_override_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "vault-ops.toml"
    config_path.write_text(
        f"""
[embedding]
base_url = "{DEFAULT_LOCAL_MODEL_BASE_URL}"
model = "from-config"

[search]
top_k = 9
search_level = "full"
""",
        encoding="utf-8",
    )
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(
        [
            "search",
            "passport",
            "--config",
            str(config_path),
            "--embed-model",
            "explicit-model",
            "--top-k",
            "3",
            "--search-level",
            "redacted",
        ]
    )
    args = vault_ops_cli._apply_config_defaults(args)
    assert args.embed_base_url == DEFAULT_LOCAL_MODEL_BASE_URL
    assert args.embed_model == "explicit-model"
    assert args.top_k == 3
    assert args.search_level == "redacted"


def test_default_repo_config_is_auto_loaded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "vault-ops.toml"
    config_path.write_text(
        """
[paths]
registry_db = "/tmp/auto-registry.db"
vectors_db = "/tmp/auto-vectors.db"
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(vault_ops_cli, "DEFAULT_CONFIG_CANDIDATES", (config_path,))
    parser = vault_ops_cli.build_parser()
    args = parser.parse_args(["search", "passport"])
    args = vault_ops_cli._apply_config_defaults(args)
    assert str(args.registry_db) == "/tmp/auto-registry.db"
    assert str(args.vectors_db) == "/tmp/auto-vectors.db"


def test_upgrade_final_status_uses_portable_default_inbox_scanner(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        registry_db="/tmp/registry.db",
        vectors_db="/tmp/vectors.db",
        docs_root=[],
        photos_root=[],
        source="all",
        index_level="redacted",
        max=None,
        reprocess_missing_summaries=-1,
        reprocess_missing_photo_analysis=-1,
        disable_summary=False,
        disable_photo_analysis=False,
        summary_base_url=None,
        summary_model=None,
        summary_api_key=None,
        summary_timeout=None,
        photo_analysis_url=None,
        photo_analysis_timeout=None,
        photo_analysis_force=False,
        disable_pdf_service=False,
        pdf_parse_url=None,
        pdf_parse_timeout=None,
        pdf_parse_profile=None,
        embed_batch_size=None,
        embed_batch_tokens=None,
        embed_max_text_chars=None,
        disable_redaction=False,
        redaction_mode="hybrid",
        redaction_profile="standard",
        redaction_instruction="",
        redaction_base_url=None,
        redaction_model=None,
        redaction_api_key=None,
        redaction_timeout=None,
        json=False,
        yes=True,
        verbose=False,
    )

    class FakeCompleted:
        def __init__(self):
            self.returncode = 0
            self.stdout = '{"vectors":{"available_index_levels":["redacted"],"upgrade_needed":false}}'
            self.stderr = ""

    captured: dict[str, object] = {}

    monkeypatch.setattr(vault_ops_cli.subprocess, "run", lambda *a, **k: FakeCompleted())
    monkeypatch.setattr(vault_ops_cli, "run_cmd", lambda *a, **k: 0)

    def fake_cmd_status(status_args):
        captured["inbox_scanner"] = status_args.inbox_scanner
        return 0

    monkeypatch.setattr(vault_ops_cli, "cmd_status", fake_cmd_status)
    rc = vault_ops_cli.cmd_upgrade(args)
    assert rc == 0
    assert captured["inbox_scanner"] == vault_ops_cli.DEFAULT_INBOX_SCANNER
