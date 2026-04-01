# OpenClaw Plugin Scaffold

`llm-vault` now includes a first in-repo OpenClaw plugin scaffold at `plugins/llm-vault-openclaw/`.

What is implemented now:

- `openclaw.plugin.json` metadata for an `llm-vault` plugin
- an OpenClaw extension package entry that points at `index.js`
- a `/vault` command surface that shells into `vault-agent`
- only `status` and enforced `search-redacted` are exposed through the plugin
- the plugin now has a small config contract for checkout path, `vault-agent` path, and timeout defaults

Safe boundary in this slice:

- `vault-ops` remains operator-only and is still the only path for `update`, `repair`, `upgrade`, encryption migration, and any full-clearance workflow
- the plugin only calls `vault-agent`, which already enforces redacted clearance and redacted search level
- the plugin rejects unknown config keys and unsupported search flags instead of silently broadening the surface
- `answer-redacted` remains deferred and is not exposed by the plugin

In short: `vault-ops` remains operator-only, and the plugin is limited to agent-safe redacted access.

Current command surface:

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

Current config contract:

- `repoRoot`: absolute path to the `llm-vault` checkout; defaults to the current checkout that contains this plugin scaffold
- `vaultAgentPath`: path to `vault-agent`; defaults to `./vault-agent` relative to `repoRoot`
- `timeoutSeconds`: integer timeout from `1` to `300`; defaults to `120`

Repo-local wiring for OpenClaw users today:

1. Install `llm-vault` from the checkout you want the plugin to use:

   ```bash
   python -m pip install -e .[dev]
   ```

2. Keep `vault-ops.toml` and `LLM_VAULT_DB_PASSWORD` configured for that same checkout.
3. Point your local OpenClaw plugin loader at `plugins/llm-vault-openclaw/`.
4. If your OpenClaw process cannot rely on the default repo-local wrapper path, configure the plugin explicitly.

Example config payload:

```json
{
  "repoRoot": "/absolute/path/to/llm-vault",
  "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
  "timeoutSeconds": 120
}
```

If you prefer an installed entry point instead of the repo wrapper, set `vaultAgentPath` to that executable and keep `repoRoot` pointed at the checkout that owns `vault-ops.toml`.

Current limitations:

- this is a repo-local scaffold, not a finished standalone published plugin package
- final live fresh-agent validation is still manual and operator-run
- Svenni still needs to perform the clean-agent validation before release claims should expand

Validation status:

- plugin metadata, config defaults, package wiring, and command boundary are covered by automated tests
- live OpenClaw install/discovery remains covered only by the manual checklist in [Manual OpenClaw Agent Validation](manual-openclaw-agent-validation.md)
