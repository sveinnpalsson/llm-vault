# llm-vault TODO

## Near-term

1. Improve `.doc/.docx` extraction quality and add regression fixtures.
2. Add richer OCR-noise synthetic fixtures for parser + retrieval evaluation.
3. Expand query filters (for example per-root/source labels) if agent workloads require it.
4. Expand the new in-repo OpenClaw plugin scaffold into a standalone install path instead of relying on a repo-local plugin folder.
5. Add a fresh-agent setup path that is documented end to end and validated from a clean OpenClaw agent install.
6. Keep the packaged `vault-ops` and `vault-agent` entry points compatible as the plugin path grows.
7. Add plugin/install smoke checks that prove a fresh agent can install, configure, discover the plugin, and run redacted search without repo-specific knowledge.
8. Add a redaction benchmark harness with a pinned evaluation slice, reproducible run command, and reportable metrics.
9. Planned OpenClaw direction: keep a single `llm_vault_search` tool name and derive the effective search clearance/level from config or policy, potentially per-agent or per-plugin. Until that lands, `llm_vault_search` stays redacted-only and safe by default.

## Release operations

1. Keep unified skill mirror parity with `inbox-vault`.
2. Run unified-skill sync checks before any release.
3. Keep security review docs updated whenever trust boundaries or endpoint policy changes.
