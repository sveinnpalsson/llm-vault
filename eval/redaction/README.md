# Redaction Evaluation

This directory is the release-readable benchmark surface for the current `llm-vault` redaction pipeline.

What is tracked here:

- repo-owned seed fixtures for deterministic smoke coverage: [`fixtures/redaction_eval_phase_a.jsonl`](fixtures/redaction_eval_phase_a.jsonl) and [`fixtures/redaction_eval_hybrid_smoke.jsonl`](fixtures/redaction_eval_hybrid_smoke.jsonl)
- tracked summary artifacts derived from the latest local full compares:
  [`reports/ai4privacy-validation-regex-vs-hybrid.summary.json`](reports/ai4privacy-validation-regex-vs-hybrid.summary.json)
  and [`reports/ai4privacy-train-regex-vs-hybrid.summary.json`](reports/ai4privacy-train-regex-vs-hybrid.summary.json)
- the runnable harness entrypoint: `./redaction-eval` or the installed `redaction-eval` console script

What is intentionally not tracked here:

- the downloaded public dataset
- the large prepared validation/train fixtures under `tmp/redaction-eval/fixtures/`
- the large raw per-case compare outputs under `tmp/redaction-eval/reports/`

Those remain local/operator-run only and are kept out of git by `.gitignore`.

## What Was Benchmarked

Current full compares use operator-prepared local fixtures derived from the English split of `ai4privacy/pii-masking-300k` and score placeholder-key output from the existing `llm-vault` redaction pipeline in two modes:

- `regex`
- `hybrid`

The committed repo-owned fixtures in this directory are smaller smoke fixtures. The full validation/train fixtures remain local-only:

- `tmp/redaction-eval/fixtures/ai4privacy-validation-full.jsonl`
- `tmp/redaction-eval/fixtures/ai4privacy-train-full.jsonl`

## Exact Reproduction Commands

Repo-owned seed smoke:

```bash
mkdir -p tmp/redaction-eval/reports
./redaction-eval \
  --fixture eval/redaction/fixtures/redaction_eval_phase_a.jsonl \
  --mode regex \
  --output tmp/redaction-eval/reports/seed-regex.json
```

Hybrid smoke that requires model-backed candidates:

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

Tested setup from the tracked summary artifacts: `vault-ops.toml`, `http://127.0.0.1:18080/v1`, model `qwen3-14b`, profile `standard`, timeout `45s`. These numbers are for that local setup.

### Hybrid vs. Regex Delta

| Split | Precision | Recall | F1 | F2 | Mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| Validation | +0.0262 | +0.1757 | +0.1999 | +0.1873 | -77 |
| Train | +0.0294 | +0.1665 | +0.1925 | +0.1784 | -288 |

### Absolute Metrics

| Split | Mode | Precision | Recall | F1 | F2 |
| --- | --- | ---: | ---: | ---: | ---: |
| Validation | `regex` | 0.7150 | 0.1619 | 0.2640 | 0.1915 |
| Validation | `hybrid` | 0.7412 | 0.3375 | 0.4639 | 0.3788 |
| Train | `regex` | 0.7039 | 0.1564 | 0.2559 | 0.1852 |
| Train | `hybrid` | 0.7333 | 0.3229 | 0.4484 | 0.3636 |

### Leak Framing on the Hybrid Run

| Split | Expected placeholders | Correctly redacted | Leaked | Unexpected |
| --- | ---: | ---: | ---: | ---: |
| Validation | 4201 | 1418 | 2783 | 495 |
| Train | 16148 | 5214 | 10934 | 1896 |

On both tracked splits, `hybrid` materially improves recall and F-scores over `regex`, but it still leaves a large number of expected placeholders unredacted.

The exact per-run summaries and weak-category counts are captured in the tracked JSON summaries in [`reports/`](reports).

## Current Weak Categories

Across both full splits, the main remaining weak areas are concentrated and visible:

- `ACCOUNT` is still the largest remaining miss bucket after hybrid: `901` missing on validation and `3353` on train.
- `CUSTOM` remains effectively unimproved by hybrid on these local compares: `751` missing on validation and `3061` on train in both modes.
- `ADDRESS` improves only modestly relative to regex: `777 -> 704` missing on validation and `3296 -> 2954` on train.
- `PERSON` still misses often: `346` missing on validation and `1273` on train after hybrid.
- Over-redaction pressure is concentrated in `PHONE`, then `ACCOUNT` and `PERSON`: validation hybrid unexpected counts are `PHONE=264`, `PERSON=126`, `ACCOUNT=88`; train hybrid unexpected counts are `PHONE=1029`, `ACCOUNT=437`, `PERSON=304`.

## Scope

- This benchmark scores placeholder keys, not span overlap.
- It does not yet measure retrieval quality after redaction across vault search workflows.
- It does not yet cover photo, screenshot, OCR-heavy document, scanned-PDF, or bridged-mail behavior.
- Different local models or endpoint wiring can change the numbers materially.
