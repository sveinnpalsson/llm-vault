# OpenClaw Agent Setup Flow

This is the straight-line setup path for a local checkout that wants both:

- operator access through `vault-ops`
- agent-safe access through `vault-agent`
- OpenClaw command and tool access through the repo-local `llm-vault` plugin

Use this document when the goal is: install from a checkout, wire OpenClaw to that checkout, and know exactly what still requires operator input.

## Boundary First

Keep the surfaces separate:

- `vault-ops`: operator-only; indexing, repair, upgrade, migration, unrestricted maintenance
- `vault-agent`: status plus explicit `search`/`search-redacted` and `fetch`/`fetch-redacted`
- `plugins/llm-vault-openclaw`: OpenClaw wrapper around `vault-agent` only

Do not wire OpenClaw directly to `vault-ops`.

## Inputs Checklist

Required:

- Python 3.11
- `LLM_VAULT_DB_PASSWORD`
- at least one local docs root or photos root
- a local embedding endpoint

Recommended:

- a local summary endpoint
- a local model-backed redaction endpoint
- `pdftotext`

Optional:

- a local photo-analysis endpoint
- a local PDF parse endpoint
- `inbox-vault` DB path plus `INBOX_VAULT_DB_PASSWORD` for mail

## Step 1: Install From The Checkout

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

Verify the installed entry points:

```bash
command -v vault-ops
command -v vault-agent
vault-ops --help
vault-agent --help
```

Use the installed commands for validation. The repo-root wrappers are compatibility shims.

## Step 2: Create Local Operator Config

```bash
cp vault-ops.toml.example vault-ops.toml
mkdir -p state
export LLM_VAULT_DB_PASSWORD='choose-a-strong-passphrase'
```

Minimal useful config:

```toml
[paths]
registry_db = "state/vault_registry.db"
vectors_db = "state/vault_vectors.db"
docs_roots = ["/absolute/path/to/docs"]
photos_roots = []

[summary]
base_url = "http://127.0.0.1:8080/v1"
model = "qwen3-14b"

[embedding]
base_url = "http://127.0.0.1:8080/v1"
model = "Qwen3-Embedding-8B"

[redaction]
base_url = "http://127.0.0.1:8080/v1"
model = "qwen3-14b"

[search]
top_k = 5
search_level = "auto"
```

Before moving on:

- add at least one real `docs_roots` or `photos_roots` entry
- point `[summary]`, `[embedding]`, and `[redaction]` at reachable local-only services
- for optional `[photo_analysis]` / `[pdf]`: either set reachable local endpoints or set `disable_service = true` in those sections
- if you copy `vault-ops.toml.example`, it already includes localhost placeholder endpoints for photo/PDF; keep them only when those local services are actually running
- keep `state/` present so the first run can create the encrypted DB files there

## Step 3: Validate The Operator Path

```bash
vault-ops update --max 300
vault-ops status --json
vault-ops search "tax receipt" --json
```

The first `vault-ops update` initializes the local registry/vector backend state. `--max` means "process at most N docs/photos/mail source items in this run", so a bounded first pass can leave the system usable-yet-degraded until later runs finish the remaining corpus.

If mail is enabled:

```bash
vault-ops update --source mail
vault-ops search "budget approval" --source mail --json
```

If `[mail_bridge].import_attachments = true`, that mail update can also create mail-derived docs/photos rows for supported attachments. The update stays mail-only at registry sync time, then widens the vector refresh step so those attachment-backed docs/photos are indexed in the same run.

## Step 4: Validate The Agent-Safe CLI Path

```bash
vault-agent status
vault-agent search-redacted "tax receipt" --source docs --top-k 3
vault-agent fetch-redacted <source_id_from_search>
```

Expected boundary:

- `vault-agent` does not accept a config override
- `vault-agent` does not allow a clearance override
- routing is explicit: unsuffixed `search` and `fetch` are the full-access paths; `_redacted` variants enforce redaction

## Step 5: Wire The OpenClaw Plugin

The plugin package lives at `plugins/llm-vault-openclaw/`.

OpenClaw uses two separate config locations:

- discovery/load path: `plugins.load.paths`
- plugin runtime config: `plugins.entries.llm-vault.config`

Minimal `openclaw.json` snippet:

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

If your OpenClaw install already scans a plugin directory, copy `plugins/llm-vault-openclaw/` there intact and omit only the `plugins.load.paths` override.

The inner payload for `plugins.entries.llm-vault.config` is:

```json
{
  "repoRoot": "/absolute/path/to/llm-vault",
  "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
  "timeoutSeconds": 120
}
```

`timeoutSeconds` is enforced by the plugin wrapper around the child process. It is not forwarded into `vault-agent` as a CLI flag.

## Step 6: Allowlist The Tool Surface If Needed

The plugin exposes both a manual command surface and an autonomous tool surface.

Command surface:

- `/vault status`
- `/vault search ...`
- `/vault search-redacted ...`
- `/vault fetch ...`
- `/vault fetch-redacted ...`

Tool surface:

- `llm_vault_status`
- `llm_vault_search`
- `llm_vault_search_redacted`
- `llm_vault_fetch`
- `llm_vault_fetch_redacted`

If the target agent uses a tool allowlist, add:

```json
{
  "agents": {
    "list": [
      {
        "id": "my-agent",
        "tools": {
          "alsoAllow": [
            "llm_vault_status",
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

If the agent already uses `tools.allow`, add those same names there instead.

## Step 7: Validate The Plugin Boundary

From OpenClaw, confirm:

- `/vault status` works
- `/vault search ...` runs the unsuffixed full-search path
- `/vault fetch ...` runs the unsuffixed full-fetch path
- `llm_vault_status` is available to the agent
- `llm_vault_search`, `llm_vault_search_redacted`, `llm_vault_fetch`, and `llm_vault_fetch_redacted` are available to the agent

Config contract:

- the plugin-specific payload lives under `plugins.entries.llm-vault.config`
- OpenClaw passes that payload to the plugin as `api.pluginConfig` during registration
- command `ctx.config` is the full OpenClaw config snapshot, not the llm-vault plugin config payload
- `/vault` command execution uses the registered plugin config, so unrelated top-level OpenClaw keys such as `meta`, `wizard`, `apiKey`, or `defaultProvider` are ignored rather than parsed as plugin config

## Honest Status

This is a repo-local plugin package, not a published standalone OpenClaw plugin release. Fresh OpenClaw validation remains manual and operator-run. Passing the repo checks does not mean release validation is complete.
