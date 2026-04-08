# Redaction evaluation ownership

`llm-vault` is the canonical home for the shared redaction contract and benchmark story across docs, photos, and bridged mail.

The release-readable benchmark entrypoint now lives at [`eval/redaction/README.md`](../eval/redaction/README.md). That subdirectory owns the repo-visible fixtures, tracked summary artifacts, and the exact local compare commands for the current redaction benchmark surface.

## Why this repo owns it

`llm-vault` is where the strongest public claim lives: safe, redacted retrieval across docs, photos, and bridged mail through one local-first retrieval layer. Because of that, this repo should own:

- redaction policy/versioning
- placeholder semantics and retrieval-safe output expectations
- benchmark definitions, harness, and reportable quality metrics
- cross-modality evaluation for docs, OCR-heavy photos, screenshots, and bridged mail retrieval

## Relationship to `inbox-vault`

`inbox-vault` still owns:

- Gmail auth and sync
- encrypted mail storage
- mail-side enrichment/profile logic
- mail-specific integration tests and bridge validation

But `inbox-vault` should not become a second canonical benchmark owner. If it needs redaction validation, treat that as mail-specific validation against the `llm-vault` contract rather than a separate benchmark program.

## Near-term evaluation plan

### Phase A: canonical text benchmark

Build the first reproducible benchmark here, likely from a pinned slice of `ai4privacy/pii-masking-300k` or an equivalent labeled text set.

Track at least:

- strict label-aware precision / recall / F1
- exact binary hide-vs-leak counts from harness output:
  `hidden_any_label`, `leaked_visible`, `mislabeled_but_hidden`, and `binary_over_redaction_count`
- binary hide rate (`hidden_any_label / expected_sensitive_values_total`)
- leakage examples / failure modes

Do not publish "possibly hidden" or other bounds language when the harness has per-case outputs available. If exact binary counts cannot be regenerated for a tracked public report, say that explicitly and rerun the report locally before updating the public page.

The current tracked AI4Privacy validation/train summaries in [`eval/redaction/reports/`](../eval/redaction/reports/) now include those exact full-split binary metrics.

### Phase B: vault-specific retrieval benchmark

Add `llm-vault`-specific evaluation that reflects how the product actually behaves:

- OCR-heavy photos and screenshots
- scanned PDFs / weak text extraction
- bridged mail records
- retrieval usefulness after redaction, not just span masking quality

## Division of work

### `llm-vault` should own now

- the written redaction contract
- benchmark harness code and benchmark fixtures
- reportable quality metrics and release-facing claims
- cross-modality validation across docs, OCR-heavy photos, screenshots, and bridged mail retrieval
- the decision about what counts as safe redacted retrieval for agent-facing use

### `inbox-vault` should own now

- mail-specific operational validation
- bridge-contract validation into `llm-vault`
- mail-specific failure examples that should be added to the canonical benchmark later
- mail-side implementation changes needed to stay compatible with the shared contract

### What should wait

Do not extract a shared library first.

First align:

1. the contract
2. the benchmark definitions
3. the output semantics
4. the quality bar

Only after that should we decide whether the redaction implementation should stay duplicated, be vendored, or move into a shared internal module.

## Suggested execution order

1. Write a small benchmark spec in this repo that names the first dataset slice, metrics, and expected outputs.
2. Build the Phase A text harness here and produce one baseline report.
3. Add Phase B vault-specific evaluation here for OCR-heavy docs/photos/mail retrieval behavior.
4. Add only mail-specific validation hooks in `inbox-vault`, pointing back to this benchmark contract.
5. Revisit code-sharing only after the benchmark and contract stabilize.
