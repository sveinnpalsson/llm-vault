from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = ROOT / "plugins" / "llm-vault-openclaw"


def test_openclaw_plugin_manifest_declares_llm_vault_plugin() -> None:
    payload = json.loads((PLUGIN_DIR / "openclaw.plugin.json").read_text(encoding="utf-8"))
    assert payload == {
        "id": "llm-vault",
        "name": "llm-vault",
        "description": "OpenClaw plugin scaffold for safe redacted llm-vault access.",
        "configSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
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
    assert "manual" in content.lower()
    assert "Svenni" in content


def test_openclaw_plugin_index_keeps_safe_boundary() -> None:
    content = (PLUGIN_DIR / "index.js").read_text(encoding="utf-8")
    assert 'name: "vault"' in content
    assert 'runVaultAgent(["status"])' in content
    assert 'args.push("search-redacted"' in content
    assert "operator-only" in content


def test_openclaw_plugin_module_exports_expected_shape() -> None:
    node = shutil.which("node")
    if not node:
        return

    snippet = f"""
import plugin from {json.dumps(str((PLUGIN_DIR / "index.js").resolve().as_posix()))};
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
  calls,
}}));
"""
    proc = subprocess.run(
        [node, "--input-type=module", "-e", snippet],
        check=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["id"] == "llm-vault"
    assert payload["name"] == "llm-vault"
    assert payload["calls"] == [
        {
            "name": "vault",
            "description": "Run safe llm-vault status and redacted search commands.",
            "acceptsArgs": True,
            "hasHandler": True,
        }
    ]


def test_openclaw_plugin_search_parser_enforces_redacted_backend() -> None:
    node = shutil.which("node")
    if not node:
        return

    snippet = f"""
import {{ parseSearchArgs, tokenizeArgs }} from {json.dumps(str((PLUGIN_DIR / "index.js").resolve().as_posix()))};
console.log(JSON.stringify({{
  tokens: tokenizeArgs('search --source docs --top-k 3 "tax receipt"'),
  parsed: parseSearchArgs(["--source", "docs", "--top-k", "3", "tax", "receipt"]),
}}));
"""
    proc = subprocess.run(
        [node, "--input-type=module", "-e", snippet],
        check=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["tokens"] == ["search", "--source", "docs", "--top-k", "3", "tax receipt"]
    assert payload["parsed"] == ["search-redacted", "tax receipt", "--source", "docs", "--top-k", "3"]
