# Infrastructure Stack


## Minimum install

- Python 3.11
- `pip install -e .[dev]`
- installed `vault-ops` and `vault-agent` entry points from that editable checkout
- SQLCipher-compatible runtime support
- `LLM_VAULT_DB_PASSWORD` exported before running `vault-ops`
- at least one local docs or photos root

Recommended:

- `pdftotext` for native-text PDFs
- a local `vault-ops.toml` in the working directory where you run `vault-ops`
- `~/.config/llm-vault/secrets.env` for cron or other unattended operator runs

## Config shape

Use [`vault-ops.toml.example`](../vault-ops.toml.example) as the template. Configure local paths and service endpoints there instead of repeating them on every command.

For a clean first setup:

- copy or create `vault-ops.toml`
- add at least one docs or photos root under `[paths]`
- verify the configured summary, embedding, redaction, and optional photo/PDF endpoints are local and reachable
- create `state/` if it does not exist yet
- export `LLM_VAULT_DB_PASSWORD` before running the operator CLI

Key sections:

- `[paths]` for DBs and source roots
- `[summary]`, `[embedding]`, `[redaction]` for OpenAI-compatible model endpoints
- `[photo_analysis]` for optional image enrichment
- `[pdf]` for optional HTTP PDF parsing
- `[mail_bridge]` for the read-only `inbox-vault` bridge
- `[search]` and `[runtime]` for operator defaults

Runtime defaults:

- summary, embedding, and model-redaction share one local default base URL: `http://127.0.0.1:8080/v1`
- photo-analysis and PDF parse services are optional; the template ships with localhost placeholder endpoints (`127.0.0.1:8081` and `127.0.0.1:8082`) and can be explicitly disabled via `[photo_analysis].disable_service = true` / `[pdf].disable_service = true`
- `[runtime].max` can set a default source-count cap for bounded `vault-ops update` / `repair` runs
- `vault-ops status` reports setup warnings for common local miswires (missing DB password, missing content roots, optional service unset, non-local URLs, and unreachable configured endpoints)

All configured service URLs must stay local-only (`127.0.0.1`, `localhost`, or equivalent loopback).

## Service contracts

### Embedding

- required for building or rebuilding vector items
- expects an OpenAI-compatible embeddings endpoint
- if missing, registry ingestion can still run but new vector work cannot complete

### Summary

- optional document summarization
- expects an OpenAI-compatible chat/completions endpoint
- if missing or disabled, docs still ingest and search normally

### Redaction

- used by `model` and `hybrid` redaction modes
- expects an OpenAI-compatible chat/completions endpoint
- if unavailable, use `--redaction-mode regex` or explicitly disable redaction

### Photo analysis

- optional photo caption/category/OCR enrichment
- expects a local HTTP endpoint configured at `[photo_analysis].url`
- explicit disable remains available through `--disable-photo-analysis` or `[photo_analysis].disable_service = true`
- `llm-vault` consumes analyzer output through a `sidecar` payload with fields such as `caption`, `category.primary`, and `text.raw`
- if missing or disabled, photos still ingest but enrichment and OCR-backed photo search are limited

### PDF parse service

- optional fallback for scanned or sparse PDFs
- expects a local HTTP endpoint configured at `[pdf].parse_url`
- explicit disable remains available through `--disable-pdf-service` or `[pdf].disable_service = true`
- if missing, native-text PDFs can still use `pdftotext`, but scanned PDFs may ingest with weak text

### Mail bridge

- optional read-only bridge from `inbox-vault`
- requires a local `inbox-vault` DB path and the env named by `[mail_bridge].password_env` (default: `INBOX_VAULT_DB_PASSWORD`)
- `import_summary = true` folds `inbox-vault` summaries into mail search text when those summaries exist
- `import_attachments = true` materializes supported mail attachments into `llm-vault` docs/photos rows; `false` keeps mail ingest message-only
- when disabled, `--source all` excludes mail and explicit `--source mail` errors

## Operator automation

`llm-vault` ships two cron-oriented operator scripts:

- `scripts/run_vault_update_once.sh` runs a single `vault-ops update`
- `scripts/cron_helper.sh` prints or installs a managed `llm-vault` cron block

The update runner is designed for unattended local operator use:

- resolves the repo root from the script location by default
- auto-loads `~/.config/llm-vault/secrets.env` when that file exists and required env vars are missing
- always requires `LLM_VAULT_DB_PASSWORD`
- fails clearly if `[mail_bridge].enabled = true` and the env named by `[mail_bridge].password_env` is not available
- logs UTC `START`, `OK`, and `FAIL` lines to `logs/vault-update-15m.log`

Suggested secrets file:

```bash
mkdir -p ~/.config/llm-vault
cat > ~/.config/llm-vault/secrets.env <<'EOF'
export LLM_VAULT_DB_PASSWORD='choose-a-strong-passphrase'
export INBOX_VAULT_DB_PASSWORD='choose-the-inbox-passphrase-if-mail-bridge-is-enabled'
EOF
chmod 600 ~/.config/llm-vault/secrets.env
```

If you do not use the mail bridge, omit the mail-bridge password env. If you changed `[mail_bridge].password_env`, export that name instead of `INBOX_VAULT_DB_PASSWORD`.

### Managed cron install

The helper uses a managed block pattern rather than overwriting the full crontab. `--install` removes only the existing `llm-vault` managed block, keeps unrelated entries intact, and writes the refreshed block back.

```bash
scripts/cron_helper.sh --print-only
scripts/cron_helper.sh --install
```

Defaults:

- schedule: `5,20,35,50 * * * *`
- cron log: `logs/cron.log`
- secrets file: `~/.config/llm-vault/secrets.env`

The default timing intentionally trails the common `inbox-vault` quarter-hour sync schedule by five minutes.

### Bridged two-job setup with `inbox-vault`

Use Inbox Vault when you want Gmail sync, encrypted local mail storage, and mail-side enrichment to stay separate from `llm-vault`'s docs/photos/mail retrieval layer.

Boundary:

- **Inbox Vault** owns Gmail auth, sync, repair, local enrichment, and the encrypted mail database.
- **`llm-vault`** reads from Inbox Vault through a read-only mail bridge.
- Inbox Vault does not ship the agent-facing retrieval surface for this stack; use `llm-vault` for that safe search layer.

Straight-line operator setup:

1. Install and validate `inbox-vault` first.
2. Run a first successful `inbox-vault update`.
3. Configure `[mail_bridge]` in `llm-vault` to point at the Inbox Vault DB.
4. Keep `LLM_VAULT_DB_PASSWORD` and the env named by `[mail_bridge].password_env` available to the `llm-vault` runtime.
5. Schedule the two jobs so Inbox Vault syncs first, then `llm-vault` updates a few minutes later.

Typical pattern:

```bash
# inbox-vault repo
scripts/cron_helper.sh --install

# llm-vault repo
scripts/cron_helper.sh --install
```

With the default helpers, the jobs land like this:

- `inbox-vault`: `*/15 * * * *`
- `llm-vault`: `5,20,35,50 * * * *`

That spacing gives Inbox Vault time to finish its incremental mail sync before `llm-vault` reads the bridged mail rows.

## Minimal validation

After wiring the stack:

```bash
vault-ops update --max 300
vault-ops status --json
vault-ops repair --max 300
vault-ops search "tax receipt" --json
```

The first `vault-ops update` creates the local encrypted registry/vector state. `--max` caps how many docs/photos/mail source items that run will ingest or repair. If the first pass is bounded, status can be usable but degraded until later runs finish the remaining work.

If the mail bridge is enabled:

```bash
vault-ops update --source mail
vault-ops search "budget approval" --source mail --json
```

With `import_attachments = true`, the mail registry pass can create mail-derived docs/photos rows for supported attachments. `vault-ops update --source mail` therefore widens the vector update stage to cover `all` sources in that run so those attachment-backed docs/photos are indexed immediately, without widening the registry sync beyond mail itself.
