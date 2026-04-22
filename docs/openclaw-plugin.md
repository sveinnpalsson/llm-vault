# OpenClaw Plugin Contract

`llm-vault` includes a repo-local OpenClaw plugin package at `plugins/llm-vault-openclaw/`.

Use this document for the exact contract: routing boundary, config placement, command surface, and autonomous tool surface.

## Implemented Surface

The plugin package contains:

- `openclaw.plugin.json` manifest metadata
- `package.json` that points OpenClaw at `index.js`
- `plugin-config.example.json` for the inner runtime payload
- a manual command surface for `/vault ...`
- an agent tool surface for `llm_vault_status`, `llm_vault_list`, `llm_vault_list_redacted`, `llm_vault_search`, `llm_vault_search_redacted`, `llm_vault_fetch`, and `llm_vault_fetch_redacted`

This is a repo-local plugin package, not a published standalone release.

## Safe Boundary

This integration keeps the boundary narrow:

- `vault-ops` remains operator-only
- the plugin shells only into `vault-agent`
- the tool surface maps only to `vault-agent`
- `/vault search` maps to `vault-agent search`
- `/vault search-redacted` maps to `vault-agent search-redacted`
- `/vault list` maps to `vault-agent list`
- `/vault list-redacted` maps to `vault-agent list-redacted`
- `/vault fetch` maps to `vault-agent fetch`
- `/vault fetch-redacted` maps to `vault-agent fetch-redacted`
- the agent tool surface exposes status plus explicit full/redacted list, search, and fetch tools

`answer-redacted` remains out of scope for this plugin.

## Command Surface

```text
/vault status
/vault list --source mail --limit 3
/vault list-redacted --source docs --from-date 2026-01-01
/vault search "tax receipt" --source docs --top-k 3
/vault search-redacted "budget approval" --source mail --from-date 2026-01-01 --taxonomy work
/vault fetch 3dd3af...
/vault fetch-redacted 3dd3af...
```

Safe filters forwarded to `vault-agent search-redacted`:

- `--source all|docs|photos|mail`
- `--top-k 1-10`
- `--from-date YYYY-MM-DD`
- `--to-date YYYY-MM-DD`
- `--taxonomy <value>`
- `--category-primary <value>`

## Tool Surface

Autonomous use should go through these exact tool names:

- `llm_vault_status`
- `llm_vault_list`
- `llm_vault_list_redacted`
- `llm_vault_search`
- `llm_vault_search_redacted`
- `llm_vault_fetch`
- `llm_vault_fetch_redacted`

`llm_vault_list` and `llm_vault_list_redacted` accept:

- `source`
- `limit`
- `fromDate`
- `toDate`

`llm_vault_search` and `llm_vault_search_redacted` accept:

- `query`
- `source`
- `topK`
- `fromDate`
- `toDate`
- `taxonomy`
- `categoryPrimary`

`llm_vault_fetch` and `llm_vault_fetch_redacted` accept:

- `sourceId`

Both tools call only `vault-agent`.

`timeoutSeconds` is enforced by the plugin wrapper's child-process timeout. The plugin does not pass timeout flags into `vault-agent`.

Unsuffixed names are the full-access paths: `llm_vault_list`, `llm_vault_search`, and `llm_vault_fetch`. `_redacted` variants enforce redaction.
Search uses the built-in hybrid ranker internally: vector similarity plus a deterministic lexical boost for exact-term matches.

## Config Placement

OpenClaw uses separate locations for plugin loading and plugin config:

- repo-local discovery/load path: `plugins.load.paths`
- plugin runtime config: `plugins.entries.llm-vault.config`

Minimal `openclaw.json` example:

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

If you copy the package into a plugin directory that OpenClaw already scans, keep the package contents intact and omit only the `plugins.load.paths` override. The runtime payload still belongs under `plugins.entries.llm-vault.config`.

The inner config payload is:

```json
{
  "repoRoot": "/absolute/path/to/llm-vault",
  "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
  "timeoutSeconds": 120
}
```

`plugin-config.example.json` matches that payload exactly.

OpenClaw passes that payload to the plugin as `api.pluginConfig` during registration. The plugin resolves and validates config there.

For command execution, `ctx.config` is the current full OpenClaw config snapshot, not the llm-vault plugin config payload. The plugin does not parse command-time `ctx.config` as llm-vault config, so unrelated top-level OpenClaw keys such as `meta`, `wizard`, `apiKey`, or `defaultProvider` are ignored on the `/vault` path instead of being revalidated as plugin config.

Strict validation still applies to the actual llm-vault plugin config payload under `plugins.entries.llm-vault.config`: only `repoRoot`, `vaultAgentPath`, and `timeoutSeconds` are accepted, and wrapper metadata is never forwarded to `vault-agent`.

## Agent Allowlist

No agent-block change is required for agents that already have open tool access.

For an allowlisted agent, add the llm-vault tools explicitly:

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

If the agent already uses `tools.allow`, add those same tool names there instead of `alsoAllow`.

## Validation Status

- plugin metadata, config defaults, command/runtime config boundaries, and safe boundary are covered by automated tests
- the command surface and tool surface contracts are covered by automated tests
- package-local install docs are covered by automated tests
- live OpenClaw validation remains manual and operator-run

See [OpenClaw Agent Setup Flow](openclaw-agent-setup.md) for the install path and [Manual OpenClaw Agent Validation](manual-openclaw-agent-validation.md) for the final manual checklist.
