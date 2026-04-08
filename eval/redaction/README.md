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
- **Binary hide-vs-leak framing** asks the simpler question: was the sensitive value hidden at all, regardless of label?

The tracked summaries in [`reports/`](reports) give exact strict metrics. They do **not** preserve enough detail to recover exact binary point counts, because a wrong-label redaction is counted as both:

- one strict miss (`fn`)
- one unexpected redaction (`fp`)

So the binary section below reports exact **bounds** from the tracked artifacts. Exact binary point counts would require the local raw per-case reports under `tmp/redaction-eval/reports/`, which are not present in this worktree.

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

### Binary Hide-vs-Leak View

This view asks only whether the sensitive value was hidden at all.

From the tracked summaries we can say:

- **guaranteed hidden** = values hidden with the expected label (`tp`)
- **possibly hidden under the wrong label** = at most the number of unexpected redactions (`fp`)
- **leaked** therefore falls into a range, not a single exact count
- **binary over-redaction** also falls into a range, because some unexpected redactions may actually be wrong-label hides rather than true extra redactions

| Split | Mode | Sensitive values total | Guaranteed hidden | Possibly hidden under wrong label | Hidden range | Leaked range | Binary over-redaction range |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Validation | `regex` | 4201 | 680 | 0 to 271 | 680 to 951 | 3250 to 3521 | 0 to 271 |
| Validation | `hybrid` | 4201 | 1418 | 0 to 495 | 1418 to 1913 | 2288 to 2783 | 0 to 495 |
| Train | `regex` | 16148 | 2525 | 0 to 1062 | 2525 to 3587 | 12561 to 13623 | 0 to 1062 |
| Train | `hybrid` | 16148 | 5214 | 0 to 1896 | 5214 to 7110 | 9038 to 10934 | 0 to 1896 |

Plain-English readout:

- On the validation split, `hybrid` definitely hides **1418** values and could hide as many as **1913** if every wrong-label event still hid the underlying value. That means the number of real leaks is somewhere between **2288** and **2783**.
- On the train split, `hybrid` definitely hides **5214** values and could hide as many as **7110**. That puts real leaks somewhere between **9038** and **10934**.
- `hybrid` is clearly better than `regex` under both views. It gets many more exact labels right, and it also improves the best-case and worst-case binary hide counts.
- Even with that improvement, the benchmark still shows a lot of leakage. These numbers are progress numbers, not finish-line numbers.

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
