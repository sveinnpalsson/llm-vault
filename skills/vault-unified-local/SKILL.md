---
name: vault-unified-local
skill_version: 1.0.0
canonical_repo: llm-vault
description: Unified local operator skill for inbox-vault + llm-vault. Use for one-call status/update/repair/search across inbox email, personal docs, and photos with redacted-by-default output.
---

# vault-unified-local

Canonical source of truth: `llm-vault/skills/vault-unified-local`.

Mirror target:
- `inbox-vault/skills/vault-unified-local` (must match `skill_version` and script content hash)

## Quick usage

From llm-vault repo root:

```bash
skills/vault-unified-local/scripts/vault-unified-cli.sh status --sources all
skills/vault-unified-local/scripts/vault-unified-cli.sh update --sources all
skills/vault-unified-local/scripts/vault-unified-cli.sh repair --sources all
skills/vault-unified-local/scripts/vault-unified-cli.sh search "tax receipt" --sources all --top-k 10
```

Defaults:
- `--sources all`
- `--clearance redacted` for search
- weighted RRF fusion for multi-source retrieval

## Config surface

Optional:

```bash
skills/vault-unified-local/scripts/vault-unified-cli.sh --config skills/vault-unified-local/config.example.toml status
```

Config controls:
- repo paths (`docs_repo`, `inbox_repo`)
- backend command paths
- default `top_k`, `clearance`, `rrf_k`, timeout
- per-source fusion weights (`inbox`, `docs`, `photos`)
- per-source enable toggles (`enable_inbox`, `enable_docs`, `enable_photos`)

## Security notes

- Redacted output is default for unified search.
- `full` clearance is explicit opt-in (`--clearance full`).
- llm-vault encrypted DB access requires `LLM_VAULT_DB_PASSWORD`.

## Sync policy

Before release, follow:
- confirm `skill_version` parity with the `inbox-vault` mirror
- confirm `scripts/vault-unified-cli.py` and `scripts/vault-unified-cli.sh` hashes match in both repos
- run smoke checks in both repos:
  - `skills/vault-unified-local/scripts/vault-unified-cli.sh status --sources all`
  - `skills/vault-unified-local/scripts/vault-unified-cli.sh search "tax receipt" --sources all --top-k 3`
