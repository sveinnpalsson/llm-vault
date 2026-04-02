# llm-vault OpenClaw Plugin Scaffold

This directory is the repo-local OpenClaw plugin package for `llm-vault`.

What it exposes:

- `/vault status`
- `/vault search ...`
- `/vault search-redacted ...`

Boundary:

- the plugin shells only into `vault-agent`
- `vault-ops` update, repair, upgrade, migration, and other full-clearance workflows remain operator-only
- both `/vault search` and `/vault search-redacted` are forced through the redacted `vault-agent search-redacted` backend

Repo-local install assumptions:

1. `llm-vault` is installed from the checkout with `python -m pip install -e .[dev]`
2. the same checkout has a working `vault-ops.toml` plus `LLM_VAULT_DB_PASSWORD`
3. OpenClaw can load a plugin from this directory, or from a copied directory that preserves these files:
   - `package.json`
   - `openclaw.plugin.json`
   - `index.js`
4. the runtime config points back at the checkout that owns `vault-ops.toml` and the installed `vault-agent`

Copy-paste plugin config payload:

```json
{
  "repoRoot": "/absolute/path/to/llm-vault",
  "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
  "timeoutSeconds": 120
}
```

Minimal OpenClaw config stub to adapt to the local loader shape:

```json
{
  "plugins": {
    "llm-vault": {
      "path": "/absolute/path/to/llm-vault/plugins/llm-vault-openclaw",
      "config": {
        "repoRoot": "/absolute/path/to/llm-vault",
        "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
        "timeoutSeconds": 120
      }
    }
  }
}
```

The outer OpenClaw config shape may vary by local install. The inner `config` object is the stable contract enforced by this package.

Recommended config workflow:

- copy [plugin-config.example.json](./plugin-config.example.json)
- set `repoRoot` to the checkout you want the plugin to use
- set `vaultAgentPath` explicitly if your OpenClaw process should not rely on `./vault-agent` relative to `repoRoot`

Manual status:

- this is still a repo-local scaffold, not a published standalone extension package
- fresh OpenClaw validation is still manual and operator-run
- see [OpenClaw Agent Setup Flow](../../docs/openclaw-agent-setup.md), [OpenClaw Plugin Scaffold](../../docs/openclaw-plugin.md), and [Manual OpenClaw Agent Validation](../../docs/manual-openclaw-agent-validation.md) for the full workflow and current limits
