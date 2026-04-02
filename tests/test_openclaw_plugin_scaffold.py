from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = ROOT / "plugins" / "llm-vault-openclaw"
PLUGIN_INDEX = str((PLUGIN_DIR / "index.js").resolve().as_posix())
PLUGIN_README = PLUGIN_DIR / "README.md"
PLUGIN_CONFIG_EXAMPLE = PLUGIN_DIR / "plugin-config.example.json"
SETUP_DOC = ROOT / "docs" / "openclaw-agent-setup.md"


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


def _write_fake_vault_agent(tmp_path: Path) -> Path:
    script = tmp_path / "fake-vault-agent"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "print(json.dumps({'argv': sys.argv[1:], 'cwd': os.getcwd()}))\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


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
                    "description": "Path to the llm-vault checkout. Defaults to the repo root that contains this plugin; relative paths resolve from that default root.",
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
    assert payload["private"] is True
    assert payload["type"] == "module"
    assert payload["main"] == "./index.js"
    assert payload["exports"] == {
        ".": "./index.js",
        "./openclaw.plugin.json": "./openclaw.plugin.json",
        "./plugin-config.example.json": "./plugin-config.example.json",
    }
    assert payload["files"] == [
        "README.md",
        "index.js",
        "openclaw.plugin.json",
        "package.json",
        "plugin-config.example.json",
    ]
    assert payload["openclaw"]["extensions"] == ["./index.js"]


def test_openclaw_plugin_docs_are_honest_about_scope() -> None:
    content = (ROOT / "docs" / "openclaw-plugin.md").read_text(encoding="utf-8")
    assert "repo-local plugin package" in content
    assert "vault-ops" in content
    assert "operator-only" in content
    assert "plugins.load.paths" in content
    assert "plugins.entries.llm-vault.config" in content
    assert "llm_vault_status" in content
    assert "llm_vault_search_redacted" in content
    assert "plugin-config.example.json" in content
    assert "manual and operator-run" in content


def test_openclaw_plugin_package_readme_and_example_config_cover_repo_local_enablement() -> None:
    readme = PLUGIN_README.read_text(encoding="utf-8")
    example = json.loads(PLUGIN_CONFIG_EXAMPLE.read_text(encoding="utf-8"))

    assert "repo-local OpenClaw plugin package" in readme
    assert "plugin-config.example.json" in readme
    assert "vault-agent" in readme
    assert "operator-only" in readme
    assert "plugins.load.paths" in readme
    assert "plugins.entries.llm-vault.config" in readme
    assert "llm_vault_status" in readme
    assert "llm_vault_search_redacted" in readme
    assert example == {
        "repoRoot": "/absolute/path/to/llm-vault",
        "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
        "timeoutSeconds": 120,
    }


def test_openclaw_agent_setup_doc_covers_required_inputs_and_plugin_stub() -> None:
    content = SETUP_DOC.read_text(encoding="utf-8")
    assert "operator-only" in content
    assert "vault-agent" in content
    assert "LLM_VAULT_DB_PASSWORD" in content
    assert "vault-ops.toml" in content
    assert "mkdir -p state" in content
    assert "--max 300" in content
    assert "initializes the local registry/vector backend state" in content
    assert "usable-yet-degraded" in content
    assert "plugins.load.paths" in content
    assert "plugins.entries.llm-vault.config" in content
    assert "llm_vault_search_redacted" in content
    assert "manual and operator-run" in content


def test_openclaw_plugin_index_keeps_safe_boundary() -> None:
    content = (PLUGIN_DIR / "index.js").read_text(encoding="utf-8")
    assert "SAFE_SURFACE" in content
    assert 'const COMMAND_NAME = "vault"' in content
    assert 'const TOOL_STATUS_NAME = "llm_vault_status"' in content
    assert 'const TOOL_SEARCH_REDACTED_NAME = "llm_vault_search_redacted"' in content
    assert "PLUGIN_CONFIG_RUNTIME_KEYS" in content
    assert '"meta"' in content
    assert 'api.registerTool(createStatusTool(pluginConfig), { name: TOOL_STATUS_NAME })' in content
    assert 'api.registerTool(createSearchRedactedTool(pluginConfig), { name: TOOL_SEARCH_REDACTED_NAME })' in content
    assert 'buildSearchRedactedArgs' in content
    assert "operator-only" in content


def test_openclaw_plugin_module_exports_expected_shape() -> None:
    payload = _run_node_json(
        f"""
import plugin from {json.dumps(PLUGIN_INDEX)};
const commands = [];
const tools = [];
plugin.register({{
  pluginConfig: {{}},
  registerCommand(command) {{
    commands.push({{
      name: command.name,
      description: command.description,
      acceptsArgs: command.acceptsArgs,
      hasHandler: typeof command.handler === "function",
    }});
  }},
  registerTool(tool, opts) {{
    tools.push({{
      name: tool.name,
      description: tool.description,
      label: tool.label,
      optionName: opts?.name ?? null,
      hasExecute: typeof tool.execute === "function",
      required: tool.parameters?.required ?? [],
      parameterKeys: Object.keys(tool.parameters?.properties ?? {{}}),
    }});
  }},
}});
console.log(JSON.stringify({{
  id: plugin.id,
  name: plugin.name,
  description: plugin.description,
  configSchema: plugin.configSchema,
  commands,
  tools,
}}));
"""
    )
    if not payload:
        return

    assert payload["id"] == "llm-vault"
    assert payload["name"] == "llm-vault"
    assert payload["configSchema"]["properties"]["timeoutSeconds"]["default"] == 120
    assert payload["commands"] == [
        {
            "name": "vault",
            "description": "Run safe llm-vault status and redacted search commands.",
            "acceptsArgs": True,
            "hasHandler": True,
        }
    ]
    assert payload["tools"] == [
        {
            "name": "llm_vault_status",
            "description": "Return llm-vault agent-safe status from vault-agent.",
            "label": "Vault Status",
            "optionName": "llm_vault_status",
            "hasExecute": True,
            "required": [],
            "parameterKeys": [],
        },
        {
            "name": "llm_vault_search_redacted",
            "description": "Run llm-vault redacted search through vault-agent with narrow safe filters.",
            "label": "Vault Search Redacted",
            "optionName": "llm_vault_search_redacted",
            "hasExecute": True,
            "required": ["query"],
            "parameterKeys": ["query", "source", "topK", "fromDate", "toDate", "taxonomy", "categoryPrimary"],
        },
    ]


def test_openclaw_plugin_manifest_matches_exported_config_schema() -> None:
    payload = _run_node_json(
        f"""
import plugin, {{ CONFIG_SCHEMA, PLUGIN_ID, PLUGIN_NAME }} from {json.dumps(PLUGIN_INDEX)};
console.log(JSON.stringify({{
  id: plugin.id,
  name: plugin.name,
  pluginConfigSchema: plugin.configSchema,
  exportedConfigSchema: CONFIG_SCHEMA,
  exportedId: PLUGIN_ID,
  exportedName: PLUGIN_NAME,
}}));
"""
    )
    if not payload:
        return

    manifest = json.loads((PLUGIN_DIR / "openclaw.plugin.json").read_text(encoding="utf-8"))
    assert payload["id"] == manifest["id"] == payload["exportedId"]
    assert payload["name"] == manifest["name"] == payload["exportedName"]
    assert payload["pluginConfigSchema"] == manifest["configSchema"] == payload["exportedConfigSchema"]


def test_openclaw_plugin_config_defaults_overrides_and_runtime_wrappers_are_stable(tmp_path: Path) -> None:
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
    meta: {{ runtime: "command" }},
  }}),
  wrapped: resolvePluginConfig({{
    path: "/plugins/llm-vault-openclaw",
    config: {{
      repoRoot: {json.dumps(str(repo_root))},
      vaultAgentPath: "./bin/vault-agent",
      timeoutSeconds: 45,
    }},
    meta: {{ runtime: "entry" }},
  }}),
  invocation: buildVaultAgentInvocation(["status"], {{
    repoRoot: {json.dumps(str(repo_root))},
    vaultAgentPath: "./bin/vault-agent",
    timeoutSeconds: 45,
    meta: {{ runtime: "command" }},
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
    assert payload["wrapped"] == payload["override"]
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
for (const rawConfig of [{{ unexpected: true }}, {{ timeoutSeconds: 0 }}, {{ repoRoot: 7 }}, {{ config: "bad" }}, "bad"]) {{
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
        "Plugin config wrapper key `config` must be an object when provided.",
        "Plugin config must be an object.",
    ]


def test_openclaw_plugin_search_parser_enforces_redacted_backend_and_safe_filters() -> None:
    payload = _run_node_json(
        f"""
import {{ buildSearchRedactedArgs, parseSearchArgs, tokenizeArgs }} from {json.dumps(PLUGIN_INDEX)};
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
  built: buildSearchRedactedArgs({{
    query: "tax receipt",
    source: "docs",
    topK: 3,
    fromDate: "2026-01-01",
    taxonomy: "finance",
  }}),
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
    assert payload["built"] == payload["parsed"]


def test_openclaw_command_runtime_handles_meta_wrapper_context(tmp_path: Path) -> None:
    fake_agent = _write_fake_vault_agent(tmp_path)
    payload = _run_node_json(
        f"""
import {{ handleVaultCommand }} from {json.dumps(PLUGIN_INDEX)};
const result = await handleVaultCommand("status", {{
  repoRoot: {json.dumps(str(tmp_path))},
  vaultAgentPath: {json.dumps(str(fake_agent))},
  timeoutSeconds: 23,
  meta: {{ runtime: "openclaw" }},
}});
console.log(result);
"""
    )
    if not payload:
        return

    assert payload == {
        "argv": ["--timeout-seconds", "23", "status"],
        "cwd": str(tmp_path),
    }


def test_openclaw_tool_surface_executes_redacted_backend_only(tmp_path: Path) -> None:
    fake_agent = _write_fake_vault_agent(tmp_path)
    payload = _run_node_json(
        f"""
import {{ createSearchRedactedTool }} from {json.dumps(PLUGIN_INDEX)};
const tool = createSearchRedactedTool({{
  repoRoot: {json.dumps(str(tmp_path))},
  vaultAgentPath: {json.dumps(str(fake_agent))},
  timeoutSeconds: 19,
}});
const result = await tool.execute("tool-call-1", {{
  query: "tax receipt",
  source: "docs",
  topK: 3,
  taxonomy: "finance",
}});
console.log(JSON.stringify(result));
"""
    )
    if not payload:
        return

    assert payload["details"] == {
        "backendCommand": "search-redacted",
        "forwarded": [
            "search-redacted",
            "tax receipt",
            "--source",
            "docs",
            "--top-k",
            "3",
            "--taxonomy",
            "finance",
        ],
    }
    assert len(payload["content"]) == 1
    assert payload["content"][0]["type"] == "text"
    assert json.loads(payload["content"][0]["text"]) == {
        "argv": [
            "--timeout-seconds",
            "19",
            "search-redacted",
            "tax receipt",
            "--source",
            "docs",
            "--top-k",
            "3",
            "--taxonomy",
            "finance",
        ],
        "cwd": str(tmp_path),
    }
