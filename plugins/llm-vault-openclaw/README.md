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

Repo-local install assumptions in this slice:

1. `llm-vault` is installed from the checkout with `python -m pip install -e .[dev]`
2. OpenClaw can load a plugin from this directory, or from a copied directory that preserves these files:
   - `package.json`
   - `openclaw.plugin.json`
   - `index.js`
3. the runtime config points back at the checkout that owns `vault-ops.toml` and the installed `vault-agent`

Recommended config:

- copy [plugin-config.example.json](./plugin-config.example.json)
- set `repoRoot` to the checkout you want the plugin to use
- set `vaultAgentPath` explicitly if your OpenClaw process should not rely on `./vault-agent` relative to `repoRoot`

Manual status:

- this is still a repo-local scaffold, not a published standalone extension package
- fresh OpenClaw validation is still manual and operator-run
- see [OpenClaw Plugin Scaffold](../../docs/openclaw-plugin.md) and [Manual OpenClaw Agent Validation](../../docs/manual-openclaw-agent-validation.md) for the full workflow and current limits
