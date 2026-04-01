# llm-vault

Privacy-first local vault for personal documents, photos, and email.

`llm-vault` indexes local files into encrypted SQLite databases, builds a local vector index, and exposes a single operator CLI: `vault-ops`. `llm-vault` relies on your local compute stack to build data understanding: document summaries, photo captioning, face recognition, text recognition, and most importantly private information redaction. The vector index allows your agents to search all your documents easily and the redaction engine removes sensitive data from the results. 

**Developer note**
>I developed this project to build on top the idea I had for my [inbox-vault](https://github.com/sveinnpalsson/inbox-vault) project, expanding the idea to documents, photos, and then bridging the inbox-vault in this project so the llm-vault has access to mail through inbox-vault. I'm working on integrating inbox-vault into this project now but until then llm-vault needs inbox-vault for mail to work.

>The project is currently positioned as a skill for agents, but to be true to the goal of correctly handling agent's permission it should be a plugin tool instead. I believe that is the best way to allow sandboxed agents to access redacted information.

>This project was developed by me, heavily depending on openai/gpt-5.4 for coding and writing most of the documentation you will find in this repo. I used [openclaw](https://github.com/openclaw/openclaw) coding-agent connected to OpenAI codex + gpt-5.4. 

![llm-vault architecture overview](assets/llm-vault-arch.png)

## Quickstart

```bash
git clone https://github.com/sveinnpalsson/llm-vault.git
cd llm-vault
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
export LLM_VAULT_DB_PASSWORD='choose-a-strong-passphrase'
cp vault-ops.toml.example vault-ops.toml
vault-ops status
```

`pip install -e .` now exposes installable `vault-ops` and `vault-agent` commands from the checkout. The repo-root `./vault-ops` and `./vault-agent` wrappers remain thin compatibility shims for people who still run directly from the repo root.

Current scope: this is the first real install foundation for `llm-vault`, not the final standalone OpenClaw plugin artifact. Fresh-agent validation is still a manual operator-run path and is documented separately in [Manual OpenClaw Agent Validation](docs/manual-openclaw-agent-validation.md).

## Config

Use `vault-ops.toml` for stable local settings:

- encrypted DB paths
- docs/photos roots
- optional mail bridge settings
- summary, embedding, redaction, photo-analysis, and PDF parse endpoints
- default search knobs

Auto-loaded locations:

- `./vault-ops.toml`
- `./.vault-ops.toml`

CLI flags override config values.

The runtime defaults are intentionally small:

- summary, embedding, and model-backed redaction default to one local OpenAI-compatible base URL: `http://127.0.0.1:8080/v1`
- photo analysis and PDF parsing should normally be set in `vault-ops.toml` or env for your local stack

See [Infrastructure Stack](docs/infrastructure-stack.md) for the supported config shape and degraded behavior when optional services are missing.

## Core Commands

```bash
vault-ops status
vault-ops update
vault-ops repair
vault-ops search "tax receipt"
vault-ops upgrade --index-level redacted
```

Useful variants:

```bash
vault-ops update --source docs
vault-ops update --source photos
vault-ops update --source mail
vault-ops repair --reprocess-missing-summaries 200
vault-ops repair --reprocess-missing-photo-analysis 100
vault-ops search "passport" --source docs --json
vault-ops search "beach trip" --source photos --taxonomy personal
vault-ops search "budget approval" --source mail --json
```

Defaults:

- source selection uses `--source all|docs|photos|mail`
- docs and photos are included by default
- mail is available only when `[mail_bridge].enabled = true`
- redaction mode defaults to `hybrid`
- search defaults to redacted output

## Mail Bridge

`llm-vault` does not sync Gmail directly. Mail ingestion stays in `inbox-vault`; this repo only supports a read-only bridge into the `mail` source.

Example:

```toml
[mail_bridge]
enabled = true
db_path = "/path/to/inbox-vault/state/inbox.db"
password_env = "INBOX_VAULT_DB_PASSWORD"
include_accounts = []
import_summary = true
max_body_chunks = 12
```

Typical flow:

```bash
# in inbox-vault
inbox-vault update

# in llm-vault
vault-ops update --source mail
vault-ops search "budget approval" --source mail
```

## Agent Wrapper

For constrained read-only agent access, use `vault-agent` instead of raw `vault-ops`:

```bash
vault-agent status
vault-agent search-redacted "tax receipt" --source docs --top-k 3
```

## Encryption And Privacy

- SQLCipher-backed runtime DBs require `LLM_VAULT_DB_PASSWORD`
- local-only endpoint checks are enforced for summary, redaction, photo-analysis, and PDF parse services
- indexed text is redacted before embeddings unless redaction is explicitly disabled

To migrate older plaintext DBs:

```bash
vault-ops migrate-encryption
```

## Unified Skill

The unified local skill lives in [skills/vault-unified-local](skills/vault-unified-local).

## Validation

```bash
ruff check scripts tests
pytest -q
```

Optional bounded live smoke tests remain opt-in through `LLM_VAULT_RUN_LIVE_SMOKE=1`.

For install-surface coverage specifically, `tests/test_packaging_install.py` verifies the declared console entry points and smoke-tests `pip install -e . --no-deps --no-build-isolation` in a temporary virtualenv.
