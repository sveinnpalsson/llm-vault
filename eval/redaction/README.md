# Redaction Evaluation

Redaction is a core part of `llm-vault`. If the system fails to hide sensitive information reliably, retrieval quality does not matter. This directory contains the redaction evaluation code and the current checked-in result summaries.

The goal of this evaluation work is straightforward:

- measure how well the redaction system hides sensitive content
- measure how often it hides the **right kind** of content
- measure the tradeoff between missed redactions and over-redaction
- make it easy to compare a simple baseline (`regex`) against model-assisted redaction (`hybrid`)

At the moment, the validated checked-in model-backed results use **`gemma4-26b`** in hybrid mode.

## Method

The current validated comparison uses a locally prepared fixture derived from the public AI4Privacy dataset:

- dataset: [`ai4privacy/pii-masking-300k`](https://huggingface.co/datasets/ai4privacy/pii-masking-300k)
- split used here: validation
- modes compared: `regex` and `hybrid`

We report the benchmark in two ways.

### 1) Label-aware scoring

This is the strict view. A case only counts as fully correct when the sensitive value is hidden **with the expected placeholder label**.

This view is useful when operators care about the exact class of redaction, not just whether something disappeared from the text.

Reported metrics:

- precision
- recall
- F1
- F2

### 2) Binary hide-vs-visible scoring

This is the simpler privacy view. It asks whether the sensitive value was hidden at all, regardless of whether the placeholder label was exactly right.

This view is useful when operators mainly care about leakage and are willing to tolerate some label mistakes if the underlying value is still hidden.

Reported counts:

- **Hidden any label**: expected sensitive values that were hidden by some placeholder
- **Still visible**: expected sensitive values that remained visible
- **Hidden under the wrong label**: values that were hidden, but classified under the wrong placeholder type
- **Over-redacted**: redaction spans that do not overlap an expected sensitive span
- **Hide rate**: hidden values divided by expected sensitive values


## Current Results

The current checked-in validated result compares `regex` against `hybrid` on the validation fixture in `tmp/redaction-eval/reports/ai4privacy-validation-map-downstream-pass-regex-vs-hybrid.json`.

### Label-aware results

| Split | Mode | Precision | Recall | F1 | F2 |
| --- | --- | ---: | ---: | ---: | ---: |
| Validation | Regex | 0.7150 | 0.1619 | 0.2640 | 0.1915 |
| Validation | Hybrid | 0.8332 | 0.7253 | 0.7755 | 0.7446 |

### Binary results

| Split | Mode | Hidden any label | Still visible | Hidden under wrong label | Over-redacted | Hide rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Validation | Regex | 929 | 3272 | 314 | 50 | 0.2211 |
| Validation | Hybrid | 3546 | 655 | 483 | 227 | 0.8441 |

## Summary

The short version is:

- **Hybrid is clearly better than regex**.
- The biggest gain is in **recall** and **binary hide rate**.
- Hybrid hides much more sensitive content than regex.
- Even so, the current system is **not close to privacy-complete** on this benchmark.

Validation:

- F1 improves from **0.2640** to **0.7755**
- Recall improves from **0.1619** to **0.7253**
- Hide rate improves from **0.2211** to **0.8441**
- Visible leaks drop from **3272** to **655**

That is a much stronger result than the earlier reported numbers, but it still leaves visible leakage and materially higher over-redaction than the regex baseline.

## Where the system still struggles

The main weak spots in the validated validation run are:

- **Custom handles and usernames** still leak in a noticeable share of cases.
- **Account and ID-like fields** remain sensitive to labeling and field-context errors.
- **Over-redaction increases** in hybrid mode even while leakage drops sharply.

So the current story is not “problem solved.” It is:

- regex is too weak
- hybrid is materially better on the validated validation fixture
- hybrid still is not a finished privacy solution

## Running the benchmark yourself

From the repo root:

### 1) Install the local command

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

### 2) Put the AI4Privacy dataset in a local folder

Expected root:

```text
local/benchmark-data/redaction/ai4privacy/pii-masking-300k/
```

### 3) Check the dataset files

```bash
./redaction-eval \
  --dataset-format ai4privacy-pii-masking-300k \
  --dataset-root local/benchmark-data/redaction/ai4privacy/pii-masking-300k \
  --dataset-file data/validation/1english_openpii_8k.jsonl \
  --check-dataset

./redaction-eval \
  --dataset-format ai4privacy-pii-masking-300k \
  --dataset-root local/benchmark-data/redaction/ai4privacy/pii-masking-300k \
  --dataset-file data/train/1english_openpii_30k.jsonl \
  --check-dataset
```

### 4) Prepare local full fixtures

```bash
mkdir -p tmp/redaction-eval/fixtures tmp/redaction-eval/reports

./redaction-eval \
  --dataset-format ai4privacy-pii-masking-300k \
  --dataset-root local/benchmark-data/redaction/ai4privacy/pii-masking-300k \
  --dataset-file data/validation/1english_openpii_8k.jsonl \
  --prepare-output tmp/redaction-eval/fixtures/ai4privacy-validation-full.jsonl \
  --max-cases 1065

./redaction-eval \
  --dataset-format ai4privacy-pii-masking-300k \
  --dataset-root local/benchmark-data/redaction/ai4privacy/pii-masking-300k \
  --dataset-file data/train/1english_openpii_30k.jsonl \
  --prepare-output tmp/redaction-eval/fixtures/ai4privacy-train-full.jsonl \
  --max-cases 3817
```

### 5) Run the compares

```bash
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

### 6) If you want a clean rerun

The command automatically resumes from checkpoint sidecars if you reuse the same `--output` path. If you want a fresh run, delete the report JSON and its matching `.cases.jsonl` sidecars first.


## Discussion

This benchmark is useful because it separates two different questions:

1. **Did the system hide the sensitive value at all?**
2. **Did it hide it under the expected category?**

Those are not the same thing.

For some operators, wrong-label redaction is still acceptable if the value is hidden. For others, category quality matters because the downstream system needs reliable placeholder semantics.

That is why both views are tracked here.

Right now the benchmark says:

- `llm-vault` redaction is better in `hybrid` mode than in `regex`
- the gain is real and measurable
- the remaining leak rate is still too high
- future work should focus first on the weak categories that dominate leakage, especially account-like fields, custom identifiers, and addresses
- future work should focus on improving the redaction flow, for example via prompt engineering for the redaction model.
