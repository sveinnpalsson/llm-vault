# llm-vault TODO

## Near-term

1. Improve `.doc/.docx` extraction quality and add regression fixtures.
2. Add richer OCR-noise synthetic fixtures for parser + retrieval evaluation.
3. Expand query filters (for example per-root/source labels) if agent workloads require it.
4. Make `llm-vault` installable as an OpenClaw plugin/tool instead of relying on repo-local wrappers and skill-style setup.
5. Add a fresh-agent setup path that is documented end to end and validated from a clean OpenClaw agent install.
6. Package installable CLI entry points for `vault-ops` and `vault-agent`, then keep repo-root wrappers as thin compatibility shims if needed.
7. Add plugin/install smoke checks that prove a fresh agent can install, configure, and run redacted search without repo-specific knowledge.
8. Add a redaction benchmark harness with a pinned evaluation slice, reproducible run command, and reportable metrics.

## Release operations

1. Keep unified skill mirror parity with `inbox-vault`.
2. Run unified-skill sync checks before any release.
3. Keep security review docs updated whenever trust boundaries or endpoint policy changes.
