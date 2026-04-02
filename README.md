# llm-vault

Privacy-first local vault for personal documents, photos, and mail-derived metadata.

`llm-vault` builds an encrypted local registry and vector index over local content, then exposes:

- `vault-ops` for operator workflows such as indexing, repair, upgrade, and maintenance
- `vault-agent` for constrained agent-safe status and redacted search
- a repo-local OpenClaw plugin scaffold that wraps only the `vault-agent` safe surface

![llm-vault architecture overview](assets/llm-vault-arch.png)

## What This Repo Is

Use this repo when you want local retrieval over private content without handing raw data to a hosted SaaS toolchain. `llm-vault` expects your compute stack to stay local: local OpenAI-compatible model endpoints for summaries, embeddings, and model-backed redaction, plus optional local services for photo analysis and scanned-PDF parsing.

This repo is installable from a checkout with `pip install -e .`. It is not yet a published standalone OpenClaw plugin release. The OpenClaw path in this slice is a repo-local plugin scaffold intended to keep the safe boundary narrow.

## Safe Surface

| Surface | Intended user | What it can do | What it must not do |
| --- | --- | --- | --- |
| `vault-ops` | operator | `status`, `update`, `repair`, `upgrade`, `search`, encryption migration | not agent-safe; can touch full-clearance and maintenance paths |
| `vault-agent` | sandboxed/local agent | `status`, `search-redacted`, deferred `answer-redacted` stub | cannot run `update`, `repair`, `upgrade`, or override clearance/config |
| `plugins/llm-vault-openclaw` | OpenClaw plugin loader | `/vault status`, `/vault search`, `/vault search-redacted` | cannot broaden beyond redacted `vault-agent` status/search |

`vault-ops` remains operator-only. The plugin shells only into `vault-agent`, and both `/vault search` and `/vault search-redacted` are forced through redacted search.

## Local Install

```bash
git clone https://github.com/sveinnpalsson/llm-vault.git
cd llm-vault
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
cp vault-ops.toml.example vault-ops.toml
mkdir -p state
export LLM_VAULT_DB_PASSWORD='choose-a-strong-passphrase'
vault-ops update --max 300
vault-ops status
```

`pip install -e .` exposes installable `vault-ops` and `vault-agent` entry points from the checkout. The repo-root `./vault-ops` and `./vault-agent` wrappers remain thin compatibility shims for existing repo-local workflows.

## Required Inputs

Required for any real local setup:

- `LLM_VAULT_DB_PASSWORD`
- at least one docs root or photos root in `vault-ops.toml`
- local embedding endpoint
- a local redaction path: either a local redaction model endpoint or an intentional operator choice to run regex-only / disabled redaction

Usually required for a useful setup:

- local summary endpoint
- `pdftotext` for native-text PDFs

Optional:

- local photo-analysis service
- local PDF parse service for scanned PDFs
- read-only `inbox-vault` bridge for mail

## Minimal `vault-ops.toml`

Start from [`vault-ops.toml.example`](vault-ops.toml.example) and trim it down to the local services you actually run:

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

Auto-loaded config paths:

- `./vault-ops.toml`
- `./.vault-ops.toml`

CLI flags override config values. All configured service URLs must stay local-only.

Before the first real run:

- copy or create `vault-ops.toml`
- add at least one `docs_roots` or `photos_roots` entry
- point `[summary]`, `[embedding]`, `[redaction]`, and any optional `[photo_analysis]` / `[pdf]` sections at local endpoints you actually run
- create `state/` if it does not exist yet
- export `LLM_VAULT_DB_PASSWORD`

The first `vault-ops update` initializes the encrypted registry/vector DB state for this checkout. Until that first update finishes, `vault-ops status` and `vault-agent status` do not have a complete backend state to read.

## Minimal Local Validation

After editing `vault-ops.toml`, creating `state/`, and exporting `LLM_VAULT_DB_PASSWORD`:

```bash
vault-ops update --max 300
vault-ops status --json
vault-ops search "tax receipt" --json
vault-agent status
vault-agent search-redacted "tax receipt" --source docs --top-k 3
```

`--max` bounds how many docs/photos/mail source items `vault-ops` will ingest or repair in that run. A first bounded pass can leave `vault-agent status` usable but degraded while the rest of the corpus is still pending.

If mail is enabled, run:

```bash
vault-ops update --source mail
vault-ops search "budget approval" --source mail --json
```

## OpenClaw Plugin Path

The repo-local plugin scaffold lives at [`plugins/llm-vault-openclaw`](plugins/llm-vault-openclaw). It is the agent-safe OpenClaw path in this repo.

High-level flow:

1. Install `llm-vault` from the checkout you want OpenClaw to use.
2. Keep `vault-ops.toml` and `LLM_VAULT_DB_PASSWORD` configured for that same checkout.
3. Point your local OpenClaw plugin loader at `plugins/llm-vault-openclaw/`, or copy that directory intact into your local plugin folder.
4. Add the plugin config payload shown below.
5. Verify `/vault status` and a redacted `/vault search`.

Plugin config payload from this repo:

```json
{
  "repoRoot": "/absolute/path/to/llm-vault",
  "vaultAgentPath": "/absolute/path/to/llm-vault/vault-agent",
  "timeoutSeconds": 120
}
```

Minimal OpenClaw config stub for humans/agents to adapt to their local loader shape:

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

The outer OpenClaw config container may vary by local setup. The inner `config` object above is the actual plugin contract implemented by this repo.

## What An OpenClaw Agent Can Infer From This Repo

A capable agent should now be able to determine:

- `llm-vault` is a local encrypted retrieval system, not a hosted service
- `vault-ops` is operator-only
- `vault-agent` and the plugin scaffold are the narrow agent-safe path
- install is `pip install -e .[dev]` from a checkout
- the user must provide `LLM_VAULT_DB_PASSWORD`, local content roots, and local compute endpoints
- the plugin needs the checkout path plus a `vault-agent` path and timeout
- fresh OpenClaw validation is still manual and operator-run

## What Remains Manual

- choosing the local model stack and endpoint URLs
- choosing content roots and mail-bridge settings
- providing `LLM_VAULT_DB_PASSWORD` and any `INBOX_VAULT_DB_PASSWORD`
- wiring the plugin into the exact local OpenClaw loader/config format
- final fresh-agent validation by Svenni or another human operator

## Docs Map

- [OpenClaw agent-oriented setup flow](docs/openclaw-agent-setup.md)
- [OpenClaw plugin scaffold details](docs/openclaw-plugin.md)
- [Infrastructure stack and config shape](docs/infrastructure-stack.md)
- [Manual OpenClaw agent validation](docs/manual-openclaw-agent-validation.md)
- [Unified local skill](skills/vault-unified-local)

## Validation

```bash
ruff check scripts tests
pytest -q
```

Optional bounded live smoke tests remain opt-in through `LLM_VAULT_RUN_LIVE_SMOKE=1`.
