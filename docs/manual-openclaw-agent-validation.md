# Manual OpenClaw Agent Validation

This checklist is intentionally manual and operator-run. It does not replace release validation, and this repo should not claim fresh OpenClaw agent coverage until Svenni runs it on a clean agent setup.

## Goal

Confirm that a fresh OpenClaw agent can install `llm-vault` from a checkout, discover the packaged `vault-ops` and `vault-agent` commands, and complete a basic redacted search path with operator-provided local config.

## Manual checklist

1. Start from a clean OpenClaw agent environment with Python 3.11 available.
2. Clone `llm-vault`, create a virtualenv, activate it, and install from the checkout:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

3. Verify the installed entry points are the ones being exercised:

```bash
command -v vault-ops
command -v vault-agent
vault-ops --help
vault-agent --help
```

4. Copy and edit local operator config:

```bash
cp vault-ops.toml.example vault-ops.toml
export LLM_VAULT_DB_PASSWORD='choose-a-strong-passphrase'
```

5. Point `vault-ops.toml` at real local content roots and local-only model services.
6. Run operator-safe checks:

```bash
vault-ops status --json
vault-ops update --max-seconds 300
vault-ops search "tax receipt" --json
```

7. Run agent-safe checks against the constrained wrapper:

```bash
vault-agent status
vault-agent search-redacted "tax receipt" --source docs --top-k 3
```

8. Record the exact commands used, whether the installed entry points resolved correctly, and any setup friction for follow-up work.

## Notes

- Use the installed `vault-ops` and `vault-agent` commands for this check, not the repo-root `./vault-ops` or `./vault-agent` compatibility shims.
- This checklist is a preparation path for Svenni's final fresh-agent validation. Passing it locally does not mean release validation is complete.
