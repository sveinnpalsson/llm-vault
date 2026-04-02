# OpenClaw Agent Setup Flow

This is the straight-line setup path for a local checkout that wants both:

- operator access through `vault-ops`
- agent-safe access through `vault-agent` and the repo-local OpenClaw plugin scaffold

Use this document when the goal is: "check out this repo, set it up locally, then tell me what still needs my secrets/endpoints/config."

## Outcome

At the end of this flow, an OpenClaw system agent should be able to say something close to:

> `llm-vault` is installed from this checkout. `vault-ops` and `vault-agent` are available. The OpenClaw plugin scaffold is wired to the same checkout. You still need to provide your local model endpoints, local content roots, and DB password.

## Boundary First

Before setup, distinguish the surfaces:

- `vault-ops`: operator-only; indexing, repair, upgrade, migration, and unrestricted maintenance remain here
- `vault-agent`: agent-safe; status plus redacted search
- `plugins/llm-vault-openclaw`: OpenClaw wrapper around `vault-agent` only

Do not wire OpenClaw directly to `vault-ops`.

## Inputs Checklist

Required:

- Python 3.11
- `LLM_VAULT_DB_PASSWORD`
- at least one local docs root or photos root
- local embedding endpoint

Recommended:

- local summary endpoint
- local model-backed redaction endpoint
- `pdftotext`

Optional:

- local photo-analysis endpoint
- local PDF parse endpoint
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

Use the installed `vault-ops` and `vault-agent` commands for validation. The repo-root `./vault-ops` and `./vault-agent` wrappers are compatibility shims, not the preferred validation path.

## Step 2: Create Local Operator Config

```bash
cp vault-ops.toml.example vault-ops.toml
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

Edit the example if you need:

- `photos_roots`
- `[photo_analysis]`
- `[pdf]`
- `[mail_bridge]`

All service URLs must stay local-only.

## Step 3: Validate The Operator Path

```bash
vault-ops status --json
vault-ops update --max-seconds 300
vault-ops search "tax receipt" --json
```

If mail is enabled:

```bash
vault-ops update --source mail
vault-ops search "budget approval" --source mail --json
```

## Step 4: Validate The Agent-Safe CLI Path

```bash
vault-agent status
vault-agent search-redacted "tax receipt" --source docs --top-k 3
```

Expected boundary:

- `vault-agent` does not accept a config override
- `vault-agent` does not allow a clearance override
- search is enforced as redacted

## Step 5: Wire The OpenClaw Plugin

The plugin package lives at `plugins/llm-vault-openclaw/`.

Wire it this way:

1. Point your OpenClaw plugin loader at that directory, or copy that directory intact into your local OpenClaw plugin folder.
2. Keep `package.json`, `openclaw.plugin.json`, `index.js`, and the rest of the package contents intact.
3. Point the plugin back at the checkout that owns `vault-ops.toml` and the installed `vault-agent`.

Minimal plugin config payload:

```json
{
  "repoRoot": "/absolute/path/to/llm-vault",
  "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
  "timeoutSeconds": 120
}
```

Minimal OpenClaw config stub to adapt to the local loader format:

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

The outer OpenClaw config shape may vary by local install. The inner `config` object is the stable contract implemented in this repo.

## Step 6: Validate The Plugin Boundary

From OpenClaw, confirm the plugin only exposes:

- `/vault status`
- `/vault search ...`
- `/vault search-redacted ...`

Both search commands must stay backed by `vault-agent search-redacted`.

## What The Repo Already Supports

This repo already supports:

- editable install from checkout
- installable `vault-ops` and `vault-agent` entry points
- auto-loading `vault-ops.toml`
- repo-local OpenClaw plugin scaffold metadata/package/config example
- automated tests for packaging, plugin config contract, and plugin safe boundary

## What Still Requires Human Input

- choosing real local content roots
- choosing real local model endpoints
- providing `LLM_VAULT_DB_PASSWORD`
- providing `INBOX_VAULT_DB_PASSWORD` if mail is enabled
- adapting the outer OpenClaw config container to the local OpenClaw build
- final clean-agent validation

## Honest Status

This is still a repo-local plugin scaffold, not a published standalone OpenClaw extension release. Fresh OpenClaw validation remains manual and operator-run. Passing the repo checks does not mean release validation is complete.
