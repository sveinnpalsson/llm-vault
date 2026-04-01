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

## Config shape

Use [`vault-ops.toml.example`](../vault-ops.toml.example) as the template. Configure local paths and service endpoints there instead of repeating them on every command.

Key sections:

- `[paths]` for DBs and source roots
- `[summary]`, `[embedding]`, `[redaction]` for OpenAI-compatible model endpoints
- `[photo_analysis]` for optional image enrichment
- `[pdf]` for optional HTTP PDF parsing
- `[mail_bridge]` for the read-only `inbox-vault` bridge
- `[search]` and `[runtime]` for operator defaults

Runtime defaults:

- summary, embedding, and model-redaction share one local default base URL: `http://127.0.0.1:8080/v1`
- photo-analysis and PDF parse services are optional and should normally be configured explicitly for the local stack that is actually running

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
- `llm-vault` consumes analyzer output through a `sidecar` payload with fields such as `caption`, `category.primary`, and `text.raw`
- if missing or disabled, photos still ingest but enrichment and OCR-backed photo search are limited

### PDF parse service

- optional fallback for scanned or sparse PDFs
- expects a local HTTP endpoint configured at `[pdf].parse_url`
- if missing, native-text PDFs can still use `pdftotext`, but scanned PDFs may ingest with weak text

### Mail bridge

- optional read-only bridge from `inbox-vault`
- requires a local `inbox-vault` DB path and `INBOX_VAULT_DB_PASSWORD`
- when disabled, `--source all` excludes mail and explicit `--source mail` errors

## Minimal validation

After wiring the stack:

```bash
vault-ops status --json
vault-ops update --max-seconds 300
vault-ops repair --max-seconds 300
vault-ops search "tax receipt" --json
```

If the mail bridge is enabled:

```bash
vault-ops update --source mail
vault-ops search "budget approval" --source mail --json
```
