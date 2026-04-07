# Redaction Evaluation

This directory is the release-readable surface for the current `llm-vault` redaction benchmark work.

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

## Current Local Setup

The tracked summary artifacts reflect one local operator run with:

- config path: `vault-ops.toml`
- redaction base URL: `http://127.0.0.1:18080/v1`
- model: `qwen3-14b`
- profile: `standard`
- timeout: `45` seconds

These numbers should be read as results for that local setup, not as a general claim about every deployment.

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

## Current Result Summary

Validation split:

- `regex`: `cases_total=1065`, `cases_with_mismatch=1000`, `tp=680`, `fp=271`, `fn=3521`, `precision=0.7150368033648791`, `recall=0.16186622232801715`, `f1=0.2639751552795031`, `f2=0.1914953534215714`, `llm_candidate_cases=0`, `llm_candidates_total=0`
- `hybrid`: `cases_total=1065`, `cases_with_mismatch=923`, `tp=1418`, `fp=495`, `fn=2783`, `precision=0.7412441191845269`, `recall=0.33753868126636516`, `f1=0.46385345109584564`, `f2=0.37880002137094626`, `llm_candidate_cases=676`, `llm_candidates_total=1775`
- delta vs `regex`: `precision=+0.02620731581964786`, `recall=+0.17567245893834801`, `f1=+0.19987829581634253`, `f2=+0.18730466794937486`, `cases_with_mismatch=-77`

Train split:

- `regex`: `cases_total=3817`, `cases_with_mismatch=3666`, `tp=2525`, `fp=1062`, `fn=13623`, `precision=0.7039308614441037`, `recall=0.15636611345058213`, `f1=0.2558905497846466`, `f2=0.1851743205385823`, `llm_candidate_cases=0`, `llm_candidates_total=0`
- `hybrid`: `cases_total=3817`, `cases_with_mismatch=3378`, `tp=5214`, `fp=1896`, `fn=10934`, `precision=0.7333333333333333`, `recall=0.3228882833787466`, `f1=0.4483618539857253`, `f2=0.36358818443000196`, `llm_candidate_cases=2544`, `llm_candidates_total=6549`
- delta vs `regex`: `precision=+0.02940247188922962`, `recall=+0.16652216992816446`, `f1=+0.19247130420107872`, `f2=+0.17841386389141967`, `cases_with_mismatch=-288`

The exact per-run summaries and weak-category counts are captured in the tracked JSON summaries in [`reports/`](reports).

## What This Proves

- The current `hybrid` path materially outperforms `regex` alone on the operator-prepared AI4Privacy validation and train fixtures for this local setup.
- The benchmark harness can compare modes, emit machine-readable summaries, and resume long local runs.
- Model-backed candidate detection is active in the current local setup: `llm_candidates_total=1775` on validation and `6549` on train.

## What This Does Not Yet Prove

- It does not prove span-level accuracy; the current scaffold scores placeholder keys, not token overlap.
- It does not prove retrieval quality after redaction across vault search workflows.
- It does not prove photo, screenshot, OCR-heavy document, or bridged-mail retrieval behavior yet.
- It does not prove that every local model or every endpoint wiring will reproduce the same numbers.
- It does not prove a fully automated public-dataset preparation flow inside git; preparation is still operator-run and local-only.

## Current Weak Categories

Across both full splits, the main remaining weak areas are concentrated and visible:

- `ACCOUNT` is still the largest remaining miss bucket after hybrid: `901` missing on validation and `3353` on train.
- `CUSTOM` remains effectively unimproved by hybrid on these local compares: `751` missing on validation and `3061` on train in both modes.
- `ADDRESS` improves only modestly relative to regex: `777 -> 704` missing on validation and `3296 -> 2954` on train.
- `PERSON` still misses often: `346` missing on validation and `1273` on train after hybrid.
- Over-redaction pressure is concentrated in `PHONE`, then `ACCOUNT` and `PERSON`: validation hybrid unexpected counts are `PHONE=264`, `PERSON=126`, `ACCOUNT=88`; train hybrid unexpected counts are `PHONE=1029`, `ACCOUNT=437`, `PERSON=304`.

## Follow-On Work

- Add a pinned, reproducible local manifest for the validation/train fixture preparation path instead of relying on operator-held prepared JSONL files.
- Add retrieval-facing benchmark slices for OCR-heavy docs, screenshots, scanned PDFs, and bridged mail.
- Add span-level scoring only after the placeholder-key benchmark and label mapping stabilize.
