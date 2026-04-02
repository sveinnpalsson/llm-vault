# Manual OpenClaw Agent Validation

This checklist is intentionally manual and operator-run. It does not replace release validation, and this repo should not claim fresh OpenClaw agent coverage until Svenni runs it on a clean agent setup.

## Goal

Confirm that a fresh OpenClaw agent can install `llm-vault` from a checkout, discover the packaged `vault-ops` and `vault-agent` commands, and complete a basic redacted search path with operator-provided local config.

## Manual checklist

1. Start from a clean OpenClaw agent environment with Python 3.11 available.
2. Clone `llm-vault`, create a virtualenv, activate it, and install from the checkout:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

3. Verify the installed entry points are the ones being exercised:

```bash
command -v vault-ops
command -v vault-agent
vault-ops --help
vault-agent --help
```

4. Copy and edit local operator config:

```bash
cp vault-ops.toml.example vault-ops.toml
mkdir -p state
export LLM_VAULT_DB_PASSWORD='choose-a-strong-passphrase'
```

5. Point `vault-ops.toml` at at least one real local docs/photos root and real local-only model/service endpoints.
6. Run operator-safe checks:

```bash
vault-ops update --max 300
vault-ops status --json
vault-ops search "tax receipt" --json
```

The first `vault-ops update` initializes the local registry/vector state for that checkout. `--max` means "process at most N docs/photos/mail source items in this run", so a bounded first pass can still leave status/search usable but degraded.

7. Wire the repo-local OpenClaw plugin package from `plugins/llm-vault-openclaw/`. Use `plugins.load.paths` for repo-local discovery and `plugins.entries.llm-vault.config` for the plugin payload. Minimal example:

```json
{
  "plugins": {
    "load": {
      "paths": [
        "/absolute/path/to/llm-vault/plugins/llm-vault-openclaw"
      ]
    },
    "allow": [
      "llm-vault"
    ],
    "entries": {
      "llm-vault": {
        "enabled": true,
        "config": {
          "repoRoot": "/absolute/path/to/llm-vault",
          "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
          "timeoutSeconds": 120
        }
      }
    }
  }
}
```

8. Confirm the plugin package layout stayed intact in the location OpenClaw will load:

```bash
ls plugins/llm-vault-openclaw
```

If you copied the plugin elsewhere, confirm that copied directory still contains `package.json`, `openclaw.plugin.json`, `index.js`, and your edited config.

9. Run agent-safe checks against the constrained wrapper:

```bash
vault-agent status
vault-agent search-redacted "tax receipt" --source docs --top-k 3
```

10. From OpenClaw, verify the plugin exposes `/vault status`, the redacted `/vault search ...` command path, and the autonomous tools `llm_vault_status` and `llm_vault_search`.
11. Record the exact commands used, whether the installed entry points resolved correctly, whether the plugin found the intended checkout, and any setup friction for follow-up work.

## Notes

- Use the installed `vault-ops` and `vault-agent` commands for this check, not the repo-root `./vault-ops` or `./vault-agent` compatibility shims.
- The plugin scaffold remains agent-safe only. Do not use it to exercise operator-only `vault-ops` workflows.
- This checklist is a preparation path for Svenni's final fresh-agent validation. Passing it locally does not mean release validation is complete.
