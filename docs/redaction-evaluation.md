# Redaction evaluation ownership

`llm-vault` is the canonical home for the shared redaction contract and benchmark story across docs, photos, and bridged mail.

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

- precision
- recall
- F1
- over-redaction rate
- leakage examples / failure modes

### Phase B: vault-specific retrieval benchmark

Add `llm-vault`-specific evaluation that reflects how the product actually behaves:

- OCR-heavy photos and screenshots
- scanned PDFs / weak text extraction
- bridged mail records
- retrieval usefulness after redaction, not just span masking quality

## Implementation stance

Do not extract a shared library first.

First align:

1. the contract
2. the benchmark definitions
3. the output semantics
4. the quality bar

Only after that should we decide whether the redaction implementation should stay duplicated, be vendored, or move into a shared internal module.
