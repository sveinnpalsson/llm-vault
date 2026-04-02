# OpenClaw Plugin Contract

`llm-vault` includes a repo-local OpenClaw plugin package at `plugins/llm-vault-openclaw/`.

Use this document for the exact contract: safe boundary, config placement, command surface, and autonomous tool surface.

## Implemented Surface

The plugin package contains:

- `openclaw.plugin.json` manifest metadata
- `package.json` that points OpenClaw at `index.js`
- `plugin-config.example.json` for the inner runtime payload
- a manual command surface for `/vault ...`
- an agent tool surface for `llm_vault_status` and `llm_vault_search_redacted`

This is a repo-local plugin package, not a published standalone release.

## Safe Boundary

This integration keeps the boundary narrow:

- `vault-ops` remains operator-only
- the plugin shells only into `vault-agent`
- the tool surface maps only to `vault-agent`
- both `/vault search` and `/vault search-redacted` are forced through redacted search
- the agent tool surface exposes only status and redacted search

`answer-redacted` remains out of scope for this plugin.

## Command Surface

```text
/vault status
/vault search "tax receipt" --source docs --top-k 3
/vault search-redacted "budget approval" --source mail --from-date 2026-01-01 --taxonomy work
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
- `llm_vault_search_redacted`

`llm_vault_search_redacted` accepts:

- `query`
- `source`
- `topK`
- `fromDate`
- `toDate`
- `taxonomy`
- `categoryPrimary`

Both tools call only `vault-agent`.

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

## Runtime Compatibility

Real OpenClaw runtime contexts may attach wrapper metadata such as `meta` around command/tool invocation context. The plugin ignores those wrapper keys and only consumes the documented `repoRoot`, `vaultAgentPath`, and `timeoutSeconds` values.

That compatibility does not widen the backend boundary: unsupported llm-vault config keys still fail closed, and wrapper metadata is never forwarded to `vault-agent`.

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
            "llm_vault_search_redacted"
          ]
        }
      }
    ]
  }
}
```

If the agent already uses `tools.allow`, add those same tool names there instead of `alsoAllow`.

## Validation Status

- plugin metadata, config defaults, runtime compatibility, and safe boundary are covered by automated tests
- the command surface and tool surface contracts are covered by automated tests
- package-local install docs are covered by automated tests
- live OpenClaw validation remains manual and operator-run

See [OpenClaw Agent Setup Flow](openclaw-agent-setup.md) for the install path and [Manual OpenClaw Agent Validation](manual-openclaw-agent-validation.md) for the final manual checklist.
