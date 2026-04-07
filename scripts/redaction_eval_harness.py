#!/usr/bin/env python3
"""Redaction benchmark harness for llm-vault."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import tomllib
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TextIO

from vault_redaction import (
    PersistentRedactionMap,
    RedactionConfig,
    redact_chunks_with_persistent_map,
)

_PLACEHOLDER_PATTERN = re.compile(r"<REDACTED_([A-Z]+)(?:_[A-Z]+)?>")
_TARGET_LABEL_PATTERN = re.compile(r"\[([A-Z0-9_]+)\]")

DEFAULT_FIXTURE_PATH = Path("eval/redaction/fixtures/redaction_eval_phase_a.jsonl")
DEFAULT_DATASET_ROOT = Path("local/benchmark-data/redaction/ai4privacy/pii-masking-300k")
DEFAULT_DATASET_FILE = "english_openpii_38k.jsonl"
DEFAULT_PREPARED_FIXTURE_PATH = Path(
    "tmp/redaction-eval/fixtures/ai4privacy-pii-masking-300k-english.jsonl"
)
DEFAULT_REPORT_PATH = Path("tmp/redaction-eval/reports/redaction-eval-report.json")
SUPPORTED_DATASET_FORMATS = ("ai4privacy-pii-masking-300k",)
AI4PRIVACY_LABEL_MAP = {
    "ACCOUNT": "ACCOUNT",
    "ACCOUNTNUMBER": "ACCOUNT",
    "BANKACCOUNT": "ACCOUNT",
    "CARDNUMBER": "ACCOUNT",
    "CREDITCARDNUMBER": "ACCOUNT",
    "IBAN": "ACCOUNT",
    "IDCARD": "ACCOUNT",
    "PASSPORT": "ACCOUNT",
    "SOCIALNUMBER": "ACCOUNT",
    "SSN": "ACCOUNT",
    "EMAIL": "EMAIL",
    "EMAILADDRESS": "EMAIL",
    "FAX": "PHONE",
    "PHONE": "PHONE",
    "PHONENUMBER": "PHONE",
    "TEL": "PHONE",
    "MOBILEPHONENUMBER": "PHONE",
    "URL": "URL",
    "WEBSITE": "URL",
    "ADDRESS": "ADDRESS",
    "STREET": "ADDRESS",
    "BUILDING": "ADDRESS",
    "CITY": "ADDRESS",
    "STATE": "ADDRESS",
    "POSTCODE": "ADDRESS",
    "ZIPCODE": "ADDRESS",
    "ZIP": "ADDRESS",
    "COUNTRY": "ADDRESS",
    "FIRSTNAME": "PERSON",
    "LASTNAME": "PERSON",
    "LASTNAME1": "PERSON",
    "LASTNAME2": "PERSON",
    "MIDDLENAME": "PERSON",
    "FULLNAME": "PERSON",
    "PERSON": "PERSON",
    "USERNAME": "CUSTOM",
}


@dataclass(slots=True)
class RedactionEvalCase:
    case_id: str
    source_type: str
    text: str
    expected_redacted_text: str
    expected_placeholders: list[str]


@dataclass(slots=True)
class RedactionEvalCaseResult:
    case_id: str
    source_type: str
    expected_redacted_text: str
    actual_redacted_text: str
    expected_placeholders: list[str]
    actual_placeholders: list[str]
    missing_placeholders: list[str]
    unexpected_placeholders: list[str]
    text_mismatch: bool
    candidate_sources: dict[str, int]
    llm_candidates_detected: int


@dataclass(slots=True)
class RedactionEvalSummary:
    cases_total: int
    cases_with_mismatch: int
    tp: int
    fp: int
    fn: int
    precision: float
    recall: float
    f1: float
    f2: float
    over_redaction_rate: float
    leakage_rate: float
    candidate_sources: dict[str, int]
    llm_candidate_cases: int
    llm_candidates_total: int


@dataclass(slots=True)
class RedactionEvalReport:
    fixture_path: str
    mode: str
    profile: str
    base_url: str
    model: str
    timeout_seconds: int
    summary: RedactionEvalSummary
    cases: list[RedactionEvalCaseResult]


@dataclass(slots=True)
class RedactionEvalCheckpointState:
    checkpoint_path: str
    resumed_cases: int


@dataclass(slots=True)
class DatasetCheckResult:
    dataset_format: str
    dataset_root: str
    dataset_file: str
    dataset_path: str
    dataset_root_exists: bool
    dataset_file_exists: bool
    schema_ok: bool
    schema_fields: list[str]


@dataclass(slots=True)
class PreparedFixtureSummary:
    dataset_format: str
    dataset_path: str
    output_path: str
    rows_seen: int
    cases_written: int
    rows_skipped: int
    max_cases: int | None


@dataclass(slots=True)
class RedactionEvalComparisonDelta:
    baseline_mode: str
    mode: str
    precision_delta: float
    recall_delta: float
    f1_delta: float
    f2_delta: float
    mismatch_delta: int
    llm_candidates_delta: int


class ProgressReporter:
    def __init__(
        self,
        *,
        mode: str,
        total_cases: int,
        stream: TextIO | None = None,
        min_interval_seconds: float = 1.0,
    ) -> None:
        self.mode = mode
        self.total_cases = total_cases
        self.stream = stream or sys.stderr
        self.min_interval_seconds = min_interval_seconds
        self.start = time.monotonic()
        self.last_emit = 0.0
        self.last_completed = 0

    def emit_resume(self, completed_cases: int) -> None:
        if completed_cases <= 0:
            return
        self._emit(
            completed_cases,
            mismatches=None,
            force=True,
            prefix=f"resume [{self.mode}]",
        )
        self.last_completed = completed_cases

    def emit_progress(self, completed_cases: int, *, mismatches: int, force: bool = False) -> None:
        self._emit(
            completed_cases,
            mismatches=mismatches,
            force=force,
            prefix=f"progress [{self.mode}]",
        )
        self.last_completed = completed_cases

    def emit_final(self, completed_cases: int, *, mismatches: int) -> None:
        self.emit_progress(completed_cases, mismatches=mismatches, force=True)

    def _emit(
        self,
        completed_cases: int,
        *,
        mismatches: int | None,
        force: bool,
        prefix: str,
    ) -> None:
        now = time.monotonic()
        if not force and completed_cases < self.total_cases:
            if completed_cases == self.last_completed and completed_cases > 0:
                return
            if (now - self.last_emit) < self.min_interval_seconds:
                return
        elapsed = max(now - self.start, 1e-6)
        rate = completed_cases / elapsed if completed_cases else 0.0
        remaining = max(self.total_cases - completed_cases, 0)
        eta = (remaining / rate) if rate > 0 else None
        percent = (completed_cases / self.total_cases * 100.0) if self.total_cases else 100.0
        message = (
            f"{prefix}: {completed_cases}/{self.total_cases} "
            f"({percent:5.1f}%) elapsed={_format_duration(elapsed)} rate={rate:0.2f}/s"
        )
        if eta is not None:
            message += f" eta={_format_duration(eta)}"
        if mismatches is not None:
            message += f" mismatches={mismatches}"
        print(message, file=self.stream, flush=True)
        self.last_emit = now


def _format_duration(seconds: float) -> str:
    rounded = int(max(seconds, 0))
    minutes, secs = divmod(rounded, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _require_str(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"benchmark case requires non-empty string field: {key}")
    return value


def _require_placeholder_list(raw: dict[str, Any]) -> list[str]:
    value = raw.get("expected_placeholders")
    if not isinstance(value, list) or not value:
        raise ValueError("benchmark case requires non-empty list field: expected_placeholders")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("expected_placeholders must contain non-empty strings")
        out.append(item.strip().upper())
    return out


def load_eval_cases(path: Path) -> list[RedactionEvalCase]:
    cases: list[RedactionEvalCase] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise ValueError(f"{path}:{line_no}: benchmark line must be a JSON object")
        cases.append(
            RedactionEvalCase(
                case_id=_require_str(raw, "case_id"),
                source_type=_require_str(raw, "source_type"),
                text=_require_str(raw, "text"),
                expected_redacted_text=_require_str(raw, "expected_redacted_text"),
                expected_placeholders=_require_placeholder_list(raw),
            )
        )
    if not cases:
        raise ValueError(f"{path}: benchmark fixture is empty")
    return cases


def extract_placeholder_keys(text: str) -> list[str]:
    return [match.group(1) for match in _PLACEHOLDER_PATTERN.finditer(text)]


def _alpha_token(index: int) -> str:
    if index < 1:
        raise ValueError("placeholder index must be positive")
    out = ""
    current = index
    while current > 0:
        current -= 1
        out = chr(ord("A") + (current % 26)) + out
        current //= 26
    return out


def _build_placeholder(key_name: str, ordinal: int) -> str:
    return f"<REDACTED_{key_name}_{_alpha_token(ordinal)}>"


def resolve_dataset_path(dataset_root: Path, dataset_file: str) -> Path:
    dataset_path = (dataset_root / dataset_file).resolve()
    root_path = dataset_root.resolve()
    try:
        dataset_path.relative_to(root_path)
    except ValueError as exc:
        raise ValueError("dataset_file must stay within dataset_root") from exc
    return dataset_path


def _load_first_json_line(path: Path) -> dict[str, Any]:
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise ValueError(f"{path}:{line_no}: dataset line must be a JSON object")
        return raw
    raise ValueError(f"{path}: dataset file is empty")


def check_dataset_inputs(dataset_format: str, dataset_root: Path, dataset_file: str) -> DatasetCheckResult:
    if dataset_format not in SUPPORTED_DATASET_FORMATS:
        raise ValueError(f"unsupported dataset format: {dataset_format}")
    dataset_path = resolve_dataset_path(dataset_root, dataset_file)
    root_exists = dataset_root.exists()
    file_exists = dataset_path.exists()
    schema_ok = False
    schema_fields: list[str] = []
    if file_exists:
        sample = _load_first_json_line(dataset_path)
        schema_fields = sorted(sample.keys())
        required = {"source_text", "target_text", "privacy_mask"}
        schema_ok = required.issubset(sample.keys())
    return DatasetCheckResult(
        dataset_format=dataset_format,
        dataset_root=str(dataset_root),
        dataset_file=dataset_file,
        dataset_path=str(dataset_path),
        dataset_root_exists=root_exists,
        dataset_file_exists=file_exists,
        schema_ok=schema_ok,
        schema_fields=schema_fields,
    )


def _map_ai4privacy_label(label: str) -> str | None:
    return AI4PRIVACY_LABEL_MAP.get(label.strip().upper())


def _normalize_ai4privacy_case(raw: dict[str, Any], row_index: int) -> RedactionEvalCase | None:
    source_text = raw.get("source_text")
    target_text = raw.get("target_text")
    privacy_mask = raw.get("privacy_mask")
    raw_id = raw.get("id")
    if not isinstance(source_text, str) or not source_text.strip():
        return None
    if not isinstance(target_text, str) or not target_text.strip():
        return None
    if not isinstance(privacy_mask, list) or not privacy_mask:
        return None

    entities: list[dict[str, Any]] = []
    for item in privacy_mask:
        if not isinstance(item, dict):
            return None
        label = item.get("label")
        value = item.get("value")
        start = item.get("start")
        end = item.get("end")
        if not isinstance(label, str) or not label.strip():
            return None
        if not isinstance(value, str) or not value.strip():
            return None
        if not isinstance(start, int) or not isinstance(end, int):
            return None
        mapped_key = _map_ai4privacy_label(label)
        if mapped_key is None:
            return None
        entities.append(
            {
                "label": label.strip().upper(),
                "value": value,
                "mapped_key": mapped_key,
                "start": start,
                "end": end,
            }
        )

    entities.sort(key=lambda item: (int(item["start"]), int(item["end"])))
    label_buckets: dict[str, list[dict[str, Any]]] = {}
    for entity in entities:
        label_buckets.setdefault(str(entity["label"]), []).append(entity)

    key_counts: Counter[str] = Counter()
    value_to_placeholder: dict[tuple[str, str], str] = {}
    rebuilt: list[str] = []
    cursor = 0
    for match in _TARGET_LABEL_PATTERN.finditer(target_text):
        label = match.group(1).strip().upper()
        bucket = label_buckets.get(label)
        if not bucket:
            return None
        entity = bucket.pop(0)
        mapped_key = str(entity["mapped_key"])
        value_key = (mapped_key, str(entity["value"]).strip().lower())
        placeholder = value_to_placeholder.get(value_key)
        if placeholder is None:
            key_counts[mapped_key] += 1
            placeholder = _build_placeholder(mapped_key, key_counts[mapped_key])
            value_to_placeholder[value_key] = placeholder
        rebuilt.append(target_text[cursor : match.start()])
        rebuilt.append(placeholder)
        cursor = match.end()
    rebuilt.append(target_text[cursor:])
    if any(bucket for bucket in label_buckets.values()):
        return None

    expected_redacted_text = "".join(rebuilt)
    expected_placeholders = extract_placeholder_keys(expected_redacted_text)
    if not expected_placeholders:
        return None
    case_suffix = str(raw_id).strip() if isinstance(raw_id, str) and raw_id.strip() else str(row_index)
    return RedactionEvalCase(
        case_id=f"ai4privacy-{case_suffix}",
        source_type="docs",
        text=source_text,
        expected_redacted_text=expected_redacted_text,
        expected_placeholders=expected_placeholders,
    )


def prepare_ai4privacy_fixture(
    dataset_path: Path,
    output_path: Path,
    *,
    max_cases: int | None = None,
) -> PreparedFixtureSummary:
    rows_seen = 0
    rows_skipped = 0
    cases: list[RedactionEvalCase] = []
    for line_no, line in enumerate(dataset_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        rows_seen += 1
        raw = json.loads(line)
        if not isinstance(raw, dict):
            rows_skipped += 1
            continue
        case = _normalize_ai4privacy_case(raw, row_index=line_no)
        if case is None:
            rows_skipped += 1
            continue
        cases.append(case)
        if max_cases is not None and len(cases) >= max_cases:
            break

    if not cases:
        raise ValueError(f"{dataset_path}: no benchmark-compatible rows found for ai4privacy adapter")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = [
        json.dumps(
            {
                "case_id": case.case_id,
                "source_type": case.source_type,
                "text": case.text,
                "expected_redacted_text": case.expected_redacted_text,
                "expected_placeholders": case.expected_placeholders,
            },
            sort_keys=True,
        )
        for case in cases
    ]
    output_path.write_text("\n".join(serialized) + "\n", encoding="utf-8")
    return PreparedFixtureSummary(
        dataset_format="ai4privacy-pii-masking-300k",
        dataset_path=str(dataset_path),
        output_path=str(output_path),
        rows_seen=rows_seen,
        cases_written=len(cases),
        rows_skipped=rows_skipped,
        max_cases=max_cases,
    )


def _counter_diff(left: Counter[str], right: Counter[str]) -> list[str]:
    diff = left - right
    out: list[str] = []
    for key, count in sorted(diff.items()):
        out.extend([key] * count)
    return out


def _score_from_counts(tp: int, fp: int, fn: int, cases_total: int) -> RedactionEvalSummary:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    f2 = (5 * precision * recall / ((4 * precision) + recall)) if ((4 * precision) + recall) else 0.0
    return RedactionEvalSummary(
        cases_total=cases_total,
        cases_with_mismatch=0,
        tp=tp,
        fp=fp,
        fn=fn,
        precision=precision,
        recall=recall,
        f1=f1,
        f2=f2,
        over_redaction_rate=(fp / cases_total) if cases_total else 0.0,
        leakage_rate=(fn / cases_total) if cases_total else 0.0,
        candidate_sources={},
        llm_candidate_cases=0,
        llm_candidates_total=0,
    )


def build_report_from_case_results(
    results: list[RedactionEvalCaseResult],
    *,
    cfg: RedactionConfig,
    fixture_path: Path,
) -> RedactionEvalReport:
    tp = 0
    fp = 0
    fn = 0
    mismatches = 0
    candidate_sources: Counter[str] = Counter()
    llm_candidate_cases = 0
    llm_candidates_total = 0

    for result in results:
        expected_counter = Counter(result.expected_placeholders)
        actual_counter = Counter(result.actual_placeholders)
        matched_counter = expected_counter & actual_counter
        tp += sum(matched_counter.values())
        fp += sum((actual_counter - expected_counter).values())
        fn += sum((expected_counter - actual_counter).values())
        candidate_sources.update(result.candidate_sources)
        llm_candidates_total += result.llm_candidates_detected
        if result.llm_candidates_detected > 0:
            llm_candidate_cases += 1
        if result.text_mismatch or result.missing_placeholders or result.unexpected_placeholders:
            mismatches += 1

    summary = _score_from_counts(tp=tp, fp=fp, fn=fn, cases_total=len(results))
    summary.cases_with_mismatch = mismatches
    summary.candidate_sources = dict(sorted(candidate_sources.items()))
    summary.llm_candidate_cases = llm_candidate_cases
    summary.llm_candidates_total = llm_candidates_total
    return RedactionEvalReport(
        fixture_path=str(fixture_path),
        mode=cfg.mode,
        profile=cfg.profile,
        base_url=cfg.base_url,
        model=cfg.model,
        timeout_seconds=cfg.timeout_seconds,
        summary=summary,
        cases=list(results),
    )


def _load_redaction_settings_from_config(path: Path) -> dict[str, Any]:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    section = raw.get("redaction")
    if not isinstance(section, dict):
        return {}
    out: dict[str, Any] = {}
    for key in ("base_url", "model", "api_key", "timeout_seconds"):
        value = section.get(key)
        if isinstance(value, str) and value.strip():
            out[key] = value.strip()
        elif isinstance(value, int):
            out[key] = value
    return out


def resolve_redaction_config(args: argparse.Namespace) -> tuple[RedactionConfig, str | None]:
    loaded_from: str | None = None
    resolved: dict[str, Any] = {}
    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            raise ValueError(f"config file does not exist: {config_path}")
        resolved.update(_load_redaction_settings_from_config(config_path))
        loaded_from = str(config_path)

    if args.redaction_base_url:
        resolved["base_url"] = args.redaction_base_url
    if args.redaction_model:
        resolved["model"] = args.redaction_model
    if args.redaction_api_key:
        resolved["api_key"] = args.redaction_api_key
    if args.timeout_seconds is not None:
        resolved["timeout_seconds"] = args.timeout_seconds

    cfg = RedactionConfig(
        mode=args.mode,
        enabled=True,
        profile=args.profile,
        instruction=args.instruction or "",
        base_url=str(resolved.get("base_url") or RedactionConfig.base_url),
        model=str(resolved.get("model") or RedactionConfig.model),
        api_key=str(resolved.get("api_key") or RedactionConfig.api_key),
        timeout_seconds=int(resolved.get("timeout_seconds") or RedactionConfig.timeout_seconds),
    )
    return cfg, loaded_from


def _build_mode_config(base_cfg: RedactionConfig, mode: str) -> RedactionConfig:
    return RedactionConfig(
        mode=mode,
        profile=base_cfg.profile,
        instruction=base_cfg.instruction,
        enabled=base_cfg.enabled,
        base_url=base_cfg.base_url,
        model=base_cfg.model,
        api_key=base_cfg.api_key,
        timeout_seconds=base_cfg.timeout_seconds,
    )


def run_eval_cases(
    cases: list[RedactionEvalCase],
    *,
    cfg: RedactionConfig,
    fixture_path: Path,
    existing_results: list[RedactionEvalCaseResult] | None = None,
    progress_reporter: ProgressReporter | None = None,
    case_result_callback: Callable[[RedactionEvalCaseResult, list[RedactionEvalCaseResult]], None] | None = None,
) -> RedactionEvalReport:
    results: list[RedactionEvalCaseResult] = list(existing_results or [])
    completed_case_ids = {result.case_id for result in results}
    if progress_reporter is not None:
        progress_reporter.emit_resume(len(results))
        if len(results) >= len(cases):
            final_report = build_report_from_case_results(results, cfg=cfg, fixture_path=fixture_path)
            progress_reporter.emit_final(
                len(final_report.cases),
                mismatches=final_report.summary.cases_with_mismatch,
            )
            return final_report

    for case in cases:
        if case.case_id in completed_case_ids:
            continue
        # Each benchmark case runs in isolation so expected placeholder output
        # stays deterministic regardless of fixture ordering.
        table = PersistentRedactionMap()
        run = redact_chunks_with_persistent_map([case.text], mode=cfg.mode, table=table, cfg=cfg)
        actual_redacted_text = run.chunk_text_redacted[0]
        expected_counter = Counter(case.expected_placeholders)
        actual_counter = Counter(extract_placeholder_keys(actual_redacted_text))
        llm_hits = int(run.candidate_sources.get("llm_chunk", 0))
        missing = _counter_diff(expected_counter, actual_counter)
        unexpected = _counter_diff(actual_counter, expected_counter)
        text_mismatch = actual_redacted_text != case.expected_redacted_text
        case_result = RedactionEvalCaseResult(
            case_id=case.case_id,
            source_type=case.source_type,
            expected_redacted_text=case.expected_redacted_text,
            actual_redacted_text=actual_redacted_text,
            expected_placeholders=case.expected_placeholders,
            actual_placeholders=extract_placeholder_keys(actual_redacted_text),
            missing_placeholders=missing,
            unexpected_placeholders=unexpected,
            text_mismatch=text_mismatch,
            candidate_sources=dict(sorted(run.candidate_sources.items())),
            llm_candidates_detected=llm_hits,
        )
        results.append(case_result)
        completed_case_ids.add(case.case_id)
        current_report = build_report_from_case_results(results, cfg=cfg, fixture_path=fixture_path)
        if case_result_callback is not None:
            case_result_callback(case_result, results)
        if progress_reporter is not None:
            progress_reporter.emit_progress(
                len(results),
                mismatches=current_report.summary.cases_with_mismatch,
                force=(len(results) == len(cases)),
            )

    final_report = build_report_from_case_results(results, cfg=cfg, fixture_path=fixture_path)
    if progress_reporter is not None and len(results) < len(cases):
        progress_reporter.emit_final(
            len(final_report.cases),
            mismatches=final_report.summary.cases_with_mismatch,
        )
    return final_report


def report_to_dict(report: RedactionEvalReport) -> dict[str, Any]:
    return {
        "fixture_path": report.fixture_path,
        "mode": report.mode,
        "profile": report.profile,
        "summary": asdict(report.summary),
        "cases": [asdict(case) for case in report.cases],
    }


def _checkpoint_path_for_mode(output_path: Path, mode: str) -> Path:
    return Path(f"{output_path}.{mode}.cases.jsonl")


def _checkpoint_metadata(*, fixture_path: Path, cfg: RedactionConfig) -> dict[str, Any]:
    return {
        "record_type": "meta",
        "fixture_path": str(fixture_path),
        "mode": cfg.mode,
        "profile": cfg.profile,
        "base_url": cfg.base_url,
        "model": cfg.model,
        "timeout_seconds": cfg.timeout_seconds,
    }


def _write_checkpoint_header(checkpoint_path: Path, *, fixture_path: Path, cfg: RedactionConfig) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps(_checkpoint_metadata(fixture_path=fixture_path, cfg=cfg), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _append_checkpoint_result(
    checkpoint_path: Path,
    *,
    fixture_path: Path,
    cfg: RedactionConfig,
    result: RedactionEvalCaseResult,
) -> None:
    if not checkpoint_path.exists():
        _write_checkpoint_header(checkpoint_path, fixture_path=fixture_path, cfg=cfg)
    with checkpoint_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "record_type": "case",
                    "case": asdict(result),
                },
                sort_keys=True,
            )
            + "\n"
        )


def load_checkpoint_results(
    checkpoint_path: Path,
    *,
    fixture_path: Path,
    cfg: RedactionConfig,
) -> list[RedactionEvalCaseResult]:
    if not checkpoint_path.exists():
        return []
    lines = checkpoint_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return []
    metadata = json.loads(lines[0])
    if metadata != _checkpoint_metadata(fixture_path=fixture_path, cfg=cfg):
        raise ValueError(f"checkpoint metadata mismatch for {checkpoint_path}")
    results: list[RedactionEvalCaseResult] = []
    seen_case_ids: set[str] = set()
    for line_no, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict) or record.get("record_type") != "case":
            raise ValueError(f"{checkpoint_path}:{line_no}: invalid checkpoint record")
        case_payload = record.get("case")
        if not isinstance(case_payload, dict):
            raise ValueError(f"{checkpoint_path}:{line_no}: missing checkpoint case payload")
        result = RedactionEvalCaseResult(**case_payload)
        if result.case_id in seen_case_ids:
            raise ValueError(f"{checkpoint_path}:{line_no}: duplicate checkpoint case_id {result.case_id}")
        seen_case_ids.add(result.case_id)
        results.append(result)
    return results


def _write_json_output(output_path: Path, payload_obj: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _build_payload(
    reports: list[RedactionEvalReport],
    *,
    fixture_path: Path,
    cfg: RedactionConfig,
    config_path: str | None,
    run_modes: list[str],
    checkpoint_states: dict[str, RedactionEvalCheckpointState] | None = None,
    current_status_by_mode: dict[str, str] | None = None,
) -> dict[str, Any]:
    checkpoint_states = checkpoint_states or {}
    current_status_by_mode = current_status_by_mode or {}
    if len(run_modes) == 1 and len(reports) == 1:
        run_payload = report_to_dict(reports[0])
        run_payload["status"] = current_status_by_mode.get(reports[0].mode, "complete")
        checkpoint_state = checkpoint_states.get(reports[0].mode)
        if checkpoint_state is not None:
            run_payload["checkpoint_path"] = checkpoint_state.checkpoint_path
            run_payload["resumed_cases"] = checkpoint_state.resumed_cases
        return run_payload

    runs_payload: list[dict[str, Any]] = []
    for report in reports:
        run_payload = report_to_dict(report)
        run_payload["status"] = current_status_by_mode.get(report.mode, "complete")
        checkpoint_state = checkpoint_states.get(report.mode)
        if checkpoint_state is not None:
            run_payload["checkpoint_path"] = checkpoint_state.checkpoint_path
            run_payload["resumed_cases"] = checkpoint_state.resumed_cases
        runs_payload.append(run_payload)

    payload: dict[str, Any] = {
        "fixture_path": str(fixture_path),
        "profile": cfg.profile,
        "redaction": {
            "config_path": config_path,
            "base_url": cfg.base_url,
            "model": cfg.model,
            "timeout_seconds": cfg.timeout_seconds,
        },
        "runs": runs_payload,
        "pending_modes": [mode for mode in run_modes if mode not in {report.mode for report in reports}],
    }
    if reports:
        baseline = reports[0]
        deltas = [
            asdict(
                RedactionEvalComparisonDelta(
                    baseline_mode=baseline.mode,
                    mode=report.mode,
                    precision_delta=report.summary.precision - baseline.summary.precision,
                    recall_delta=report.summary.recall - baseline.summary.recall,
                    f1_delta=report.summary.f1 - baseline.summary.f1,
                    f2_delta=report.summary.f2 - baseline.summary.f2,
                    mismatch_delta=report.summary.cases_with_mismatch - baseline.summary.cases_with_mismatch,
                    llm_candidates_delta=report.summary.llm_candidates_total
                    - baseline.summary.llm_candidates_total,
                )
            )
            for report in reports[1:]
        ]
        payload["comparison"] = {
            "baseline_mode": baseline.mode,
            "deltas": deltas,
        }
    return payload


def _dry_run_payload(
    *,
    fixture_path: Path | None,
    mode: str,
    profile: str,
    redaction_cfg: RedactionConfig,
    config_path: str | None,
    dataset_check: DatasetCheckResult | None = None,
    prepare_output: Path | None = None,
    max_cases: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "mode": mode,
        "profile": profile,
        "dry_run": True,
        "redaction": {
            "config_path": config_path,
            "base_url": redaction_cfg.base_url,
            "model": redaction_cfg.model,
            "timeout_seconds": redaction_cfg.timeout_seconds,
        },
    }
    if fixture_path is not None:
        cases = load_eval_cases(fixture_path)
        payload["fixture"] = {
            "path": str(fixture_path),
            "exists": fixture_path.exists(),
            "cases_total": len(cases),
        }
    if dataset_check is not None:
        payload["dataset"] = asdict(dataset_check)
    if prepare_output is not None:
        payload["prepare_output"] = str(prepare_output)
    if max_cases is not None:
        payload["max_cases"] = max_cases
    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the llm-vault redaction evaluation harness.")
    parser.add_argument(
        "--fixture",
        default=str(DEFAULT_FIXTURE_PATH),
        help="Path to benchmark fixture JSONL file.",
    )
    parser.add_argument(
        "--mode",
        default="regex",
        choices=("regex", "model", "hybrid"),
        help="Redaction mode to execute.",
    )
    parser.add_argument(
        "--profile",
        default="standard",
        help="Redaction profile label to record in the report.",
    )
    parser.add_argument(
        "--instruction",
        default="",
        help="Optional extra model instruction passed through to model/hybrid redaction.",
    )
    parser.add_argument(
        "--output",
        help=(
            "Path to write the JSON report. The harness also writes append-only per-mode "
            "checkpoint sidecars beside this file and automatically resumes from them."
        ),
    )
    parser.add_argument(
        "--config",
        help="Explicit vault-ops TOML path to load [redaction] settings from.",
    )
    parser.add_argument(
        "--redaction-base-url",
        help="Explicit OpenAI-compatible local base URL for model/hybrid redaction.",
    )
    parser.add_argument(
        "--redaction-model",
        help="Explicit model name for model/hybrid redaction.",
    )
    parser.add_argument(
        "--redaction-api-key",
        help="Explicit API key for model/hybrid redaction. Defaults to local if omitted.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        help="Timeout for model-backed redaction calls.",
    )
    parser.add_argument(
        "--compare-mode",
        action="append",
        choices=("regex", "model", "hybrid"),
        help=(
            "Run multiple modes on the same fixture and emit a comparison report. Repeat to compare "
            "more than one mode. Progress is shown per mode."
        ),
    )
    parser.add_argument(
        "--require-llm-candidates",
        action="store_true",
        help="Fail if a model/hybrid run produces zero llm_chunk candidate detections.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and print resolved paths without running the benchmark.",
    )
    parser.add_argument(
        "--dataset-format",
        choices=SUPPORTED_DATASET_FORMATS,
        help="Prepare or validate a supported public dataset adapter.",
    )
    parser.add_argument(
        "--dataset-root",
        default=str(DEFAULT_DATASET_ROOT),
        help="Local-only directory where the downloaded public dataset should live.",
    )
    parser.add_argument(
        "--dataset-file",
        default=DEFAULT_DATASET_FILE,
        help="Dataset file relative to --dataset-root for adapter preparation or checks.",
    )
    parser.add_argument(
        "--check-dataset",
        action="store_true",
        help="Validate that the expected local dataset directory/file exists and matches the adapter schema.",
    )
    parser.add_argument(
        "--prepare-output",
        help="Write a normalized benchmark fixture from the dataset adapter to this JSONL path.",
    )
    parser.add_argument(
        "--max-cases",
        type=int,
        help="Maximum compatible dataset rows to write when preparing a fixture.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    fixture_path = Path(args.fixture)
    output_path = Path(args.output) if args.output else None
    prepare_output = Path(args.prepare_output) if args.prepare_output else None
    dataset_check: DatasetCheckResult | None = None
    cfg, config_path = resolve_redaction_config(args)
    compare_modes = args.compare_mode or []
    run_modes = compare_modes or [args.mode]

    if args.max_cases is not None and args.max_cases <= 0:
        raise ValueError("--max-cases must be a positive integer")
    if args.timeout_seconds is not None and args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be a positive integer")
    if args.require_llm_candidates and not any(mode in {"model", "hybrid"} for mode in run_modes):
        raise ValueError("--require-llm-candidates requires at least one model or hybrid run")

    if args.dataset_format:
        dataset_root = Path(args.dataset_root)
        dataset_check = check_dataset_inputs(args.dataset_format, dataset_root, args.dataset_file)
        if args.check_dataset or args.dry_run:
            payload = json.dumps(
                _dry_run_payload(
                    fixture_path=None if args.dataset_format else fixture_path,
                    mode=args.mode,
                    profile=args.profile,
                    redaction_cfg=cfg,
                    config_path=config_path,
                    dataset_check=dataset_check,
                    prepare_output=prepare_output,
                    max_cases=args.max_cases,
                ),
                indent=2,
                sort_keys=True,
            )
            print(payload)
            return 0
        if prepare_output is not None:
            if not dataset_check.dataset_file_exists:
                raise ValueError(f"dataset file is missing: {dataset_check.dataset_path}")
            if not dataset_check.schema_ok:
                raise ValueError(
                    "dataset schema check failed; expected ai4privacy fields source_text, target_text, privacy_mask"
                )
            summary = prepare_ai4privacy_fixture(
                Path(dataset_check.dataset_path),
                prepare_output,
                max_cases=args.max_cases,
            )
            print(json.dumps(asdict(summary), indent=2, sort_keys=True))
            return 0
        raise ValueError(
            "dataset adapter mode requires either --check-dataset, --dry-run, or --prepare-output"
        )

    if args.dry_run:
        payload = json.dumps(
            _dry_run_payload(
                fixture_path=fixture_path,
                mode=args.mode,
                profile=args.profile,
                redaction_cfg=cfg,
                config_path=config_path,
                prepare_output=output_path,
            ),
            indent=2,
            sort_keys=True,
        )
        print(payload)
        return 0

    cases = load_eval_cases(fixture_path)
    reports: list[RedactionEvalReport] = []
    checkpoint_states: dict[str, RedactionEvalCheckpointState] = {}
    current_status_by_mode: dict[str, str] = {}
    try:
        for mode in run_modes:
            mode_cfg = _build_mode_config(cfg, mode)
            existing_results: list[RedactionEvalCaseResult] = []
            checkpoint_path: Path | None = None
            if output_path is not None:
                checkpoint_path = _checkpoint_path_for_mode(output_path, mode)
                existing_results = load_checkpoint_results(
                    checkpoint_path,
                    fixture_path=fixture_path,
                    cfg=mode_cfg,
                )
                checkpoint_states[mode] = RedactionEvalCheckpointState(
                    checkpoint_path=str(checkpoint_path),
                    resumed_cases=len(existing_results),
                )
            current_status_by_mode[mode] = "partial"

            def _on_case_result(
                case_result: RedactionEvalCaseResult,
                mode_results: list[RedactionEvalCaseResult],
                *,
                mode_cfg: RedactionConfig = mode_cfg,
                mode: str = mode,
                checkpoint_path: Path | None = checkpoint_path,
            ) -> None:
                if checkpoint_path is not None:
                    _append_checkpoint_result(
                        checkpoint_path,
                        fixture_path=fixture_path,
                        cfg=mode_cfg,
                        result=case_result,
                    )
                    partial_report = build_report_from_case_results(
                        mode_results,
                        cfg=mode_cfg,
                        fixture_path=fixture_path,
                    )
                    partial_payload = _build_payload(
                        reports + [partial_report],
                        fixture_path=fixture_path,
                        cfg=cfg,
                        config_path=config_path,
                        run_modes=run_modes,
                        checkpoint_states=checkpoint_states,
                        current_status_by_mode=current_status_by_mode,
                    )
                    _write_json_output(output_path, partial_payload)

            report = run_eval_cases(
                cases,
                cfg=mode_cfg,
                fixture_path=fixture_path,
                existing_results=existing_results,
                progress_reporter=ProgressReporter(mode=mode, total_cases=len(cases)),
                case_result_callback=_on_case_result,
            )
            current_status_by_mode[mode] = "complete"
            reports.append(report)
            if output_path is not None:
                _write_json_output(
                    output_path,
                    _build_payload(
                        reports,
                        fixture_path=fixture_path,
                        cfg=cfg,
                        config_path=config_path,
                        run_modes=run_modes,
                        checkpoint_states=checkpoint_states,
                        current_status_by_mode=current_status_by_mode,
                    ),
                )
    except KeyboardInterrupt:
        if output_path is not None and reports:
            _write_json_output(
                output_path,
                _build_payload(
                    reports,
                    fixture_path=fixture_path,
                    cfg=cfg,
                    config_path=config_path,
                    run_modes=run_modes,
                    checkpoint_states=checkpoint_states,
                    current_status_by_mode=current_status_by_mode,
                ),
            )
        print("interrupted: partial results remain on disk and will auto-resume on the next run", file=sys.stderr)
        return 130
    if args.require_llm_candidates:
        failing_modes = [
            report.mode
            for report in reports
            if report.mode in {"model", "hybrid"} and report.summary.llm_candidates_total <= 0
        ]
        if failing_modes:
            raise ValueError(
                "model-backed redaction did not report any llm_chunk candidates for modes: "
                + ", ".join(failing_modes)
            )
    payload_obj = _build_payload(
        reports,
        fixture_path=fixture_path,
        cfg=cfg,
        config_path=config_path,
        run_modes=run_modes,
        checkpoint_states=checkpoint_states,
        current_status_by_mode=current_status_by_mode,
    )
    payload = json.dumps(payload_obj, indent=2, sort_keys=True)
    if output_path:
        _write_json_output(output_path, payload_obj)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
