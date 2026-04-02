# OpenClaw Plugin Scaffold

`llm-vault` includes a repo-local OpenClaw plugin scaffold at `plugins/llm-vault-openclaw/`.

Use this document for the plugin contract and safe boundary. Use [OpenClaw Agent Setup Flow](openclaw-agent-setup.md) for the straight-line install path.

## What Is Implemented

- `openclaw.plugin.json` metadata for an `llm-vault` plugin
- a package entry that points at `index.js`
- a package-local `README.md`
- `plugin-config.example.json`
- a `/vault` command surface that shells into `vault-agent`
- config validation for `repoRoot`, `vaultAgentPath`, and `timeoutSeconds`

## Safe Boundary

This slice keeps the safe surface narrow:

- `vault-ops` remains operator-only
- the plugin only calls `vault-agent`
- the plugin only exposes `status` and redacted search
- unknown config keys and unsupported search flags are rejected
- `answer-redacted` remains deferred and is not exposed by the plugin

In short: `vault-ops` remains operator-only, and the plugin is limited to agent-safe redacted access.

## Current Command Surface

```text
/vault status
/vault search "tax receipt" --source docs --top-k 3
/vault search-redacted "tax receipt" --source docs --top-k 3
/vault search "budget approval" --source mail --from-date 2026-01-01 --taxonomy work
```

Safe search filters currently forwarded to `vault-agent`:

- `--source all|docs|photos|mail`
- `--top-k 1-10`
- `--from-date YYYY-MM-DD`
- `--to-date YYYY-MM-DD`
- `--taxonomy <value>`
- `--category-primary <value>`

Both `/vault search` and `/vault search-redacted` are forced through the redacted backend.

## Config Contract

- `repoRoot`: path to the `llm-vault` checkout; defaults to the current checkout that contains this plugin scaffold, and relative paths resolve from that default repo root
- `vaultAgentPath`: path to `vault-agent`; defaults to `./vault-agent` relative to `repoRoot`
- `timeoutSeconds`: integer timeout from `1` to `300`; defaults to `120`

Exact payload:

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

The outer OpenClaw config shape may vary by local install. The inner `config` object above is the actual repo-supported plugin contract.

## Repo-Local Wiring

1. Install `llm-vault` from the checkout you want the plugin to use with `python -m pip install -e .[dev]`.
2. Keep `vault-ops.toml` and `LLM_VAULT_DB_PASSWORD` configured for that same checkout.
3. Point your local OpenClaw plugin loader at `plugins/llm-vault-openclaw/`, or copy that directory into whatever local plugin folder your OpenClaw setup already scans.
4. Keep the package contents intact when copying it. This slice assumes the loader reads `package.json`, then loads `./index.js` from the declared `openclaw.extensions` entry.
5. Copy `plugins/llm-vault-openclaw/plugin-config.example.json` into your OpenClaw plugin config and edit it for the checkout you want the plugin to use.
6. If your OpenClaw process cannot rely on the default repo-local wrapper path, configure the plugin explicitly.

If you prefer an installed entry point instead of the repo wrapper, set `vaultAgentPath` to that executable and keep `repoRoot` pointed at the checkout that owns `vault-ops.toml`.

Example repo-local enablement snippets:

```text
plugin path: /absolute/path/to/llm-vault/plugins/llm-vault-openclaw
repoRoot: /absolute/path/to/llm-vault
vaultAgentPath: /absolute/path/to/llm-vault/vault-agent
```

```text
copied plugin path: /absolute/path/to/openclaw/plugins/llm-vault-openclaw
repoRoot: /absolute/path/to/llm-vault
vaultAgentPath: /absolute/path/to/llm-vault/vault-agent
```

## What An Agent Can Reliably Infer

An OpenClaw agent skimming this repo should now be able to infer:

- the plugin path is repo-local, not a published release package
- the supported safe surface is status plus redacted search
- the plugin depends on the same checkout that owns `vault-ops.toml`
- the user still needs to supply local model endpoints, content roots, and secrets

## Current Limitations

- this is a repo-local scaffold, not a finished standalone published plugin package
- final live fresh-agent validation is still manual and operator-run
- Svenni still needs to perform the clean-agent validation before release claims should expand

## Validation Status

- plugin metadata, config defaults, package wiring, and command boundary are covered by automated tests
- package-local install docs and example config are covered by automated tests
- the agent-oriented setup flow is documented in [OpenClaw Agent Setup Flow](openclaw-agent-setup.md)
- live OpenClaw install/discovery remains covered only by the manual checklist in [Manual OpenClaw Agent Validation](manual-openclaw-agent-validation.md)
