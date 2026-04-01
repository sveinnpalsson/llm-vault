from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = ROOT / "plugins" / "llm-vault-openclaw"
PLUGIN_INDEX = str((PLUGIN_DIR / "index.js").resolve().as_posix())


def _run_node_json(snippet: str) -> dict[str, object] | list[object]:
    node = shutil.which("node")
    if not node:
        return {}

    proc = subprocess.run(
        [node, "--input-type=module", "-e", snippet],
        check=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return json.loads(proc.stdout)


def test_openclaw_plugin_manifest_declares_llm_vault_plugin() -> None:
    payload = json.loads((PLUGIN_DIR / "openclaw.plugin.json").read_text(encoding="utf-8"))
    assert payload == {
        "id": "llm-vault",
        "name": "llm-vault",
        "description": "OpenClaw plugin scaffold for safe redacted llm-vault access.",
        "configSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "repoRoot": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Absolute path to the llm-vault checkout. Defaults to this plugin checkout root.",
                },
                "vaultAgentPath": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Path to vault-agent. Relative paths resolve from repoRoot and default to ./vault-agent.",
                },
                "timeoutSeconds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 300,
                    "default": 120,
                    "description": "Timeout passed to vault-agent and enforced by the plugin wrapper.",
                },
            },
        },
    }


def test_openclaw_plugin_package_points_at_extension_module() -> None:
    payload = json.loads((PLUGIN_DIR / "package.json").read_text(encoding="utf-8"))
    assert payload["name"] == "llm-vault-openclaw"
    assert payload["type"] == "module"
    assert payload["openclaw"]["extensions"] == ["./index.js"]


def test_openclaw_plugin_docs_are_honest_about_scope() -> None:
    content = (ROOT / "docs" / "openclaw-plugin.md").read_text(encoding="utf-8")
    assert "repo-local scaffold" in content
    assert "vault-ops" in content
    assert "operator-only" in content
    assert "repoRoot" in content
    assert "vaultAgentPath" in content
    assert "manual" in content.lower()
    assert "Svenni" in content


def test_openclaw_plugin_index_keeps_safe_boundary() -> None:
    content = (PLUGIN_DIR / "index.js").read_text(encoding="utf-8")
    assert "SAFE_SURFACE" in content
    assert 'name: "vault"' in content
    assert 'runVaultAgent(["status"], rawConfig)' in content
    assert 'args.push("search-redacted"' in content
    assert "resolvePluginConfig" in content
    assert "operator-only" in content


def test_openclaw_plugin_module_exports_expected_shape() -> None:
    payload = _run_node_json(
        f"""
import plugin from {json.dumps(PLUGIN_INDEX)};
const calls = [];
plugin.register({{
  registerCommand(command) {{
    calls.push({{
      name: command.name,
      description: command.description,
      acceptsArgs: command.acceptsArgs,
      hasHandler: typeof command.handler === "function",
    }});
  }},
}});
console.log(JSON.stringify({{
  id: plugin.id,
  name: plugin.name,
  description: plugin.description,
  configSchema: plugin.configSchema,
  calls,
}}));
"""
    )
    if not payload:
        return

    assert payload["id"] == "llm-vault"
    assert payload["name"] == "llm-vault"
    assert payload["configSchema"]["properties"]["timeoutSeconds"]["default"] == 120
    assert payload["calls"] == [
        {
            "name": "vault",
            "description": "Run safe llm-vault status and redacted search commands.",
            "acceptsArgs": True,
            "hasHandler": True,
        }
    ]


def test_openclaw_plugin_config_defaults_and_overrides_are_stable(tmp_path: Path) -> None:
    repo_root = tmp_path / "alt-vault"
    payload = _run_node_json(
        f"""
import {{ buildVaultAgentInvocation, resolvePluginConfig }} from {json.dumps(PLUGIN_INDEX)};
console.log(JSON.stringify({{
  defaults: resolvePluginConfig(),
  override: resolvePluginConfig({{
    repoRoot: {json.dumps(str(repo_root))},
    vaultAgentPath: "./bin/vault-agent",
    timeoutSeconds: 45,
  }}),
  invocation: buildVaultAgentInvocation(["status"], {{
    repoRoot: {json.dumps(str(repo_root))},
    vaultAgentPath: "./bin/vault-agent",
    timeoutSeconds: 45,
  }}),
}}));
"""
    )
    if not payload:
        return

    assert payload["defaults"]["repoRoot"] == str(ROOT)
    assert payload["defaults"]["vaultAgentPath"] == str(ROOT / "vault-agent")
    assert payload["defaults"]["timeoutSeconds"] == 120
    assert payload["override"] == {
        "repoRoot": str(repo_root),
        "vaultAgentPath": str(repo_root / "bin" / "vault-agent"),
        "timeoutSeconds": 45,
    }
    assert payload["invocation"] == {
        "file": str(repo_root / "bin" / "vault-agent"),
        "args": ["--timeout-seconds", "45", "status"],
        "cwd": str(repo_root),
        "timeoutMs": 46_000,
    }


def test_openclaw_plugin_rejects_unknown_or_invalid_config() -> None:
    payload = _run_node_json(
        f"""
import {{ resolvePluginConfig }} from {json.dumps(PLUGIN_INDEX)};
const failures = [];
for (const rawConfig of [{{ unexpected: true }}, {{ timeoutSeconds: 0 }}, {{ repoRoot: 7 }}, "bad"]) {{
  try {{
    resolvePluginConfig(rawConfig);
  }} catch (error) {{
    failures.push(String(error.message || error));
  }}
}}
console.log(JSON.stringify(failures));
"""
    )
    if not payload:
        return

    assert payload == [
        "Unsupported plugin config key: unexpected",
        "timeoutSeconds must be an integer between 1 and 300.",
        "repoRoot must be a string when provided.",
        "Plugin config must be an object.",
    ]


def test_openclaw_plugin_search_parser_enforces_redacted_backend_and_safe_filters() -> None:
    payload = _run_node_json(
        f"""
import {{ parseSearchArgs, tokenizeArgs }} from {json.dumps(PLUGIN_INDEX)};
console.log(JSON.stringify({{
  tokens: tokenizeArgs('search --source docs --top-k 3 --from-date 2026-01-01 --taxonomy finance "tax receipt"'),
  parsed: parseSearchArgs([
    "--source",
    "docs",
    "--top-k",
    "3",
    "--from-date",
    "2026-01-01",
    "--taxonomy",
    "finance",
    "tax",
    "receipt",
  ]),
}}));
"""
    )
    if not payload:
        return

    assert payload["tokens"] == [
        "search",
        "--source",
        "docs",
        "--top-k",
        "3",
        "--from-date",
        "2026-01-01",
        "--taxonomy",
        "finance",
        "tax receipt",
    ]
    assert payload["parsed"] == [
        "search-redacted",
        "tax receipt",
        "--source",
        "docs",
        "--top-k",
        "3",
        "--from-date",
        "2026-01-01",
        "--taxonomy",
        "finance",
    ]
