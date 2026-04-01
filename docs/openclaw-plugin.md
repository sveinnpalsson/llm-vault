# OpenClaw Plugin Scaffold

`llm-vault` now includes a first in-repo OpenClaw plugin scaffold at `plugins/llm-vault-openclaw/`.

What is implemented now:

- `openclaw.plugin.json` metadata for an `llm-vault` plugin
- an OpenClaw extension package entry that points at `index.js`
- a `/vault` command surface that shells into `vault-agent`
- only `status` and enforced `search-redacted` are exposed through the plugin

Safe boundary in this slice:

- `vault-ops` remains operator-only and is still the only path for `update`, `repair`, `upgrade`, encryption migration, and any full-clearance workflow
- the plugin only calls `vault-agent`, which already enforces redacted clearance and redacted search level
- `answer-redacted` remains deferred and is not exposed by the plugin

In short: `vault-ops` remains operator-only, and the plugin is limited to agent-safe redacted access.

Current command surface:

```text
/vault status
/vault search "tax receipt" --source docs --top-k 3
/vault search-redacted "tax receipt" --source docs --top-k 3
```

Current limitations:

- this is a repo-local scaffold, not a finished standalone published plugin package
- final live fresh-agent validation is still manual and operator-run
- Svenni still needs to perform the clean-agent validation before release claims should expand

Validation status:

- plugin metadata, package wiring, and command boundary are covered by automated tests
- live OpenClaw install/discovery remains covered only by the manual checklist in [Manual OpenClaw Agent Validation](manual-openclaw-agent-validation.md)
