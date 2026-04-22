# llm-vault OpenClaw Plugin

This directory is the repo-local OpenClaw plugin package for `llm-vault`.

## Exposed Surfaces

Command surface:

- `/vault status`
- `/vault list ...`
- `/vault list-redacted ...`
- `/vault search ...`
- `/vault search-redacted ...`
- `/vault fetch <source-id>`
- `/vault fetch-redacted <source-id>`

Tool surface:

- `llm_vault_status`
- `llm_vault_list`
- `llm_vault_list_redacted`
- `llm_vault_search`
- `llm_vault_search_redacted`
- `llm_vault_fetch`
- `llm_vault_fetch_redacted`

The tool surface is the intended autonomous path. Unsuffixed names are the full-access paths: `llm_vault_list`, `llm_vault_search`, and `llm_vault_fetch`. `_redacted` variants enforce redaction. The slash command remains available for manual use.

## Boundary

- the plugin shells only into `vault-agent`
- `vault-ops` update, repair, migration, and other full-clearance workflows remain operator-only
- `/vault list` runs `vault-agent list`
- `/vault list-redacted` runs `vault-agent list-redacted`
- `/vault search` runs `vault-agent search`
- `/vault search-redacted` runs `vault-agent search-redacted`
- `/vault fetch` runs `vault-agent fetch`
- `/vault fetch-redacted` runs `vault-agent fetch-redacted`
- the autonomous tool surface exposes status plus explicit full/redacted list, search, and fetch tools

## Repo-Local Wiring

`openclaw.json` uses two different plugin locations:

- discovery/load path: `plugins.load.paths`
- runtime config: `plugins.entries.llm-vault.config`

Minimal example:

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

If OpenClaw already scans a plugin directory, copy this package there intact and keep the same `plugins.entries.llm-vault.config` payload.

## Agent Allowlist

If the target agent uses tool allowlists, add:

```json
{
  "agents": {
    "list": [
      {
        "id": "my-agent",
        "tools": {
          "alsoAllow": [
            "llm_vault_status",
            "llm_vault_list",
            "llm_vault_list_redacted",
            "llm_vault_search",
            "llm_vault_search_redacted",
            "llm_vault_fetch",
            "llm_vault_fetch_redacted"
          ]
        }
      }
    ]
  }
}
```

If the agent already uses `tools.allow`, append those same tool names there instead.

## Notes

- `plugin-config.example.json` contains the exact inner payload for `plugins.entries.llm-vault.config`
- `timeoutSeconds` is enforced by the plugin wrapper process timeout; it is not forwarded as a `vault-agent` CLI flag
- OpenClaw passes `plugins.entries.llm-vault.config` to the plugin as `api.pluginConfig` during registration
- command `ctx.config` is the full OpenClaw config snapshot, not the llm-vault plugin config payload
- strict plugin config validation still applies only to the documented `repoRoot`, `vaultAgentPath`, and `timeoutSeconds` keys
- this package is still repo-local and operator-validated, not a published standalone release

See [OpenClaw Agent Setup Flow](../../docs/openclaw-agent-setup.md), [OpenClaw Plugin Contract](../../docs/openclaw-plugin.md), and [Manual OpenClaw Agent Validation](../../docs/manual-openclaw-agent-validation.md) for the full workflow.
