# Redaction Evaluation

This directory holds the current benchmark results and runner for the `llm-vault` redaction pipeline.

Tracked here:

- small checked-in fixtures for quick benchmark checks: [`fixtures/redaction_eval_phase_a.jsonl`](fixtures/redaction_eval_phase_a.jsonl) and [`fixtures/redaction_eval_hybrid_smoke.jsonl`](fixtures/redaction_eval_hybrid_smoke.jsonl)
- tracked summary artifacts derived from the latest local full compares:
  [`reports/ai4privacy-validation-regex-vs-hybrid.summary.json`](reports/ai4privacy-validation-regex-vs-hybrid.summary.json)
  and [`reports/ai4privacy-train-regex-vs-hybrid.summary.json`](reports/ai4privacy-train-regex-vs-hybrid.summary.json)
- the runnable harness entrypoint: `./redaction-eval` or the installed `redaction-eval` console script

Not tracked here:

- the downloaded public dataset
- the large prepared validation/train fixtures under `tmp/redaction-eval/fixtures/`
- the large raw per-case compare outputs under `tmp/redaction-eval/reports/`

Those stay local and are kept out of git by `.gitignore`.

## What Was Benchmarked

Current full compares use operator-prepared local fixtures derived from the English split of `ai4privacy/pii-masking-300k` and score placeholder-key output from the existing `llm-vault` redaction pipeline in two modes:

- `regex`
- `hybrid`

The committed fixtures in this directory are small quick-check fixtures. The full validation/train fixtures remain local-only:

- `tmp/redaction-eval/fixtures/ai4privacy-validation-full.jsonl`
- `tmp/redaction-eval/fixtures/ai4privacy-train-full.jsonl`

## Exact Reproduction Commands

Small regex sanity check:

```bash
mkdir -p tmp/redaction-eval/reports
./redaction-eval \
  --fixture eval/redaction/fixtures/redaction_eval_phase_a.jsonl \
  --mode regex \
  --output tmp/redaction-eval/reports/seed-regex.json
```

Small hybrid check that requires model-backed candidates:

```bash
mkdir -p tmp/redaction-eval/reports
./redaction-eval \
  --fixture eval/redaction/fixtures/redaction_eval_hybrid_smoke.jsonl \
  --compare-mode regex \
  --compare-mode hybrid \
  --config vault-ops.toml \
  --require-llm-candidates \
  --output tmp/redaction-eval/reports/hybrid-smoke-regex-vs-hybrid.json
```

Public-dataset preparation is still local/operator-run. The downloaded dataset should be placed under `local/benchmark-data/redaction/ai4privacy/pii-masking-300k/` and preflighted before any full compare:

```bash
./redaction-eval \
  --dataset-format ai4privacy-pii-masking-300k \
  --dataset-root local/benchmark-data/redaction/ai4privacy/pii-masking-300k \
  --dataset-file english_openpii_38k.jsonl \
  --check-dataset
```

The exact commands used to reproduce the current local validation and train compares are:

```bash
mkdir -p tmp/redaction-eval/reports
./redaction-eval \
  --fixture tmp/redaction-eval/fixtures/ai4privacy-validation-full.jsonl \
  --compare-mode regex \
  --compare-mode hybrid \
  --config vault-ops.toml \
  --output tmp/redaction-eval/reports/ai4privacy-validation-regex-vs-hybrid.json

./redaction-eval \
  --fixture tmp/redaction-eval/fixtures/ai4privacy-train-full.jsonl \
  --compare-mode regex \
  --compare-mode hybrid \
  --config vault-ops.toml \
  --output tmp/redaction-eval/reports/ai4privacy-train-regex-vs-hybrid.json
```

The harness writes append-only checkpoint sidecars beside `--output` and automatically resumes from them on rerun.

## Current Results

These runs compare `regex` against `hybrid`. The hybrid run used `qwen3-14b` for model-backed detection.

Read the results in two ways:

- **Strict label-aware metrics** ask whether the system hid the value and used the expected placeholder label.
- **Exact binary hide-vs-leak metrics** ask the simpler question: was the sensitive value fully hidden at all, regardless of label?

The harness now emits exact binary counts directly from each case by aligning the source text against both the expected and actual redacted outputs and scoring source-span coverage. That produces these exact fields:

- `hidden_any_label`: expected sensitive values that were fully hidden by some placeholder, even if the placeholder label was wrong
- `leaked_visible`: expected sensitive values that remained visible because no actual redaction fully covered them
- `mislabeled_but_hidden`: expected sensitive values that were fully hidden, but only under the wrong placeholder label
- `binary_over_redaction_count`: actual redaction spans that do not overlap any expected sensitive span

These are exact counts, not bounds. The harness also reports `binary_hide_rate`, which is the expected-value hide rate:

```text
binary_hide_rate = hidden_any_label / expected_sensitive_values_total
```

Binary precision/F1 are intentionally not reported. The harness can score exact hide coverage on the expected-value axis, but there is no equally clean one-to-one "predicted positive" unit once a single actual placeholder can cover multiple expected values or an oversized actual span can cover one expected value plus extra text.

### Strict Label-Aware Summary

| Split | Precision | Recall | F1 | F2 | Fewer mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| Validation | +0.0262 | +0.1757 | +0.1999 | +0.1873 | 77 fewer |
| Train | +0.0294 | +0.1665 | +0.1925 | +0.1784 | 288 fewer |

### Strict Label-Aware Full Comparison

| Split | Mode | Precision | Recall | F1 | F2 |
| --- | --- | ---: | ---: | ---: | ---: |
| Validation | `regex` | 0.7150 | 0.1619 | 0.2640 | 0.1915 |
| Validation | `hybrid` | 0.7412 | 0.3375 | 0.4639 | 0.3788 |
| Train | `regex` | 0.7039 | 0.1564 | 0.2559 | 0.1852 |
| Train | `hybrid` | 0.7333 | 0.3229 | 0.4484 | 0.3636 |

### Strict Label-Aware Counts

| Split | Mode | Should have been redacted | Redacted with the expected label | Strict misses | Unexpected redactions |
| --- | --- | ---: | ---: | ---: | ---: |
| Validation | `regex` | 4201 | 680 | 3521 | 271 |
| Validation | `hybrid` | 4201 | 1418 | 2783 | 495 |
| Train | `regex` | 16148 | 2525 | 13623 | 1062 |
| Train | `hybrid` | 16148 | 5214 | 10934 | 1896 |

Here, **"redacted with the expected label"** means the system both hid the value and used the expected placeholder category in this benchmark. A wrong-label redaction is still useful in practice because the value is hidden, but the strict benchmark counts it as a miss plus an unexpected redaction.

### Exact Binary Hide-vs-Leak View

The checked-in full validation/train summaries in [`reports/`](reports) were generated before the harness started emitting exact binary fields, and the local prepared fixtures plus raw compare outputs they reference are not present in this worktree. Because of that, this repo copy cannot publish refreshed exact full-split binary numbers yet.

To regenerate exact full-split binary metrics honestly, rerun the same local compare commands shown above and then refresh the tracked summaries from the resulting `tmp/redaction-eval/reports/*.json` outputs. Once regenerated, the public page should report the exact `hidden_any_label`, `leaked_visible`, `mislabeled_but_hidden`, `binary_over_redaction_count`, and `binary_hide_rate` values from those reports directly.

The exact per-run summaries are tracked in [`reports/`](reports).

## Where It Still Struggles

Across both full splits, the biggest remaining weak spots are:

- **Account and ID-like fields** remain the largest miss bucket.
- **Custom handles and usernames** improve very little under hybrid.
- **Addresses** improve, but still leak too often.
- **Person names** still miss often enough to matter.
- **Phone numbers** are the biggest over-redaction bucket.

## Scope

- This benchmark scores placeholder keys, not span overlap.
- It does not yet measure retrieval quality after redaction across vault search workflows.
- It does not yet cover photo, screenshot, OCR-heavy document, scanned-PDF, or bridged-mail behavior.
- Different local models or endpoint wiring can change the numbers materially.
