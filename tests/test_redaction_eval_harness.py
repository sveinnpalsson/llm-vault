from __future__ import annotations

import json
import sys
from pathlib import Path

from redaction_eval_harness import (
    ProgressReporter,
    build_arg_parser,
    check_dataset_inputs,
    extract_placeholder_keys,
    load_checkpoint_results,
    load_eval_cases,
    main,
    prepare_ai4privacy_fixture,
    report_to_dict,
    resolve_redaction_config,
    run_eval_cases,
)
from vault_redaction import RedactionConfig

FIXTURES = Path("eval/redaction/fixtures")


def test_load_eval_cases_requires_expected_fields(tmp_path: Path) -> None:
    fixture = tmp_path / "bad.jsonl"
    fixture.write_text('{"case_id":"missing-fields"}\n', encoding="utf-8")

    try:
        load_eval_cases(fixture)
    except ValueError as exc:
        assert "source_type" in str(exc)
    else:
        raise AssertionError("expected invalid benchmark fixture to raise ValueError")


def test_extract_placeholder_keys_preserves_occurrence_count() -> None:
    text = "A <REDACTED_EMAIL_A> B <REDACTED_PHONE_A> C <REDACTED_EMAIL_B>"
    assert extract_placeholder_keys(text) == ["EMAIL", "PHONE", "EMAIL"]


def test_run_eval_cases_matches_seed_fixture() -> None:
    fixture = FIXTURES / "redaction_eval_phase_a.jsonl"
    cases = load_eval_cases(fixture)

    report = run_eval_cases(
        cases,
        cfg=RedactionConfig(mode="regex", enabled=True),
        fixture_path=fixture,
    )

    assert report.summary.cases_total == 3
    assert report.summary.cases_with_mismatch == 0
    assert report.summary.tp == 7
    assert report.summary.fp == 0
    assert report.summary.fn == 0
    assert report.summary.precision == 1.0
    assert report.summary.recall == 1.0
    assert report.summary.f1 == 1.0
    assert report.summary.f2 == 1.0
    assert report.summary.over_redaction_rate == 0.0
    assert report.summary.leakage_rate == 0.0
    assert report.summary.candidate_sources == {"regex": 7}
    assert report.summary.llm_candidate_cases == 0
    assert report.summary.llm_candidates_total == 0
    assert all(not case.text_mismatch for case in report.cases)

    payload = report_to_dict(report)
    assert payload["summary"]["cases_total"] == 3
    assert payload["cases"][1]["actual_placeholders"] == ["PERSON", "ACCOUNT", "EMAIL"]
    assert payload["cases"][1]["candidate_sources"] == {"regex": 3}
    assert payload["cases"][1]["llm_candidates_detected"] == 0


def test_run_eval_cases_regex_baseline_leaks_hybrid_smoke_fixture() -> None:
    fixture = FIXTURES / "redaction_eval_hybrid_smoke.jsonl"
    cases = load_eval_cases(fixture)

    report = run_eval_cases(
        cases,
        cfg=RedactionConfig(mode="regex", enabled=True),
        fixture_path=fixture,
    )

    assert report.summary.cases_total == 1
    assert report.summary.cases_with_mismatch == 1
    assert report.summary.tp == 1
    assert report.summary.fp == 0
    assert report.summary.fn == 1
    assert report.summary.precision == 1.0
    assert report.summary.recall == 0.5
    assert round(report.summary.f1, 4) == 0.6667
    assert round(report.summary.f2, 4) == 0.5556
    assert report.summary.candidate_sources == {"regex": 1}
    assert report.summary.llm_candidate_cases == 0
    assert report.summary.llm_candidates_total == 0
    assert report.cases[0].missing_placeholders == ["ADDRESS"]
    assert report.cases[0].candidate_sources == {"regex": 1}
    assert report.cases[0].llm_candidates_detected == 0


def test_run_eval_cases_emits_visible_progress(capsys) -> None:
    fixture = FIXTURES / "redaction_eval_phase_a.jsonl"
    cases = load_eval_cases(fixture)

    report = run_eval_cases(
        cases,
        cfg=RedactionConfig(mode="regex", enabled=True),
        fixture_path=fixture,
        progress_reporter=ProgressReporter(mode="regex", total_cases=len(cases), min_interval_seconds=0.0),
    )

    captured = capsys.readouterr()
    assert report.summary.cases_total == 3
    assert "progress [regex]:" in captured.err
    assert "3/3" in captured.err


def test_check_dataset_inputs_reports_schema_for_ai4privacy_jsonl(tmp_path: Path) -> None:
    dataset_root = tmp_path / "local-benchmark-data"
    dataset_root.mkdir()
    dataset_file = dataset_root / "english_openpii_38k.jsonl"
    dataset_file.write_text(
        '{"id":"40768A","source_text":"Email: bballoi@yahoo.com","target_text":"Email: [EMAIL]","privacy_mask":[{"value":"bballoi@yahoo.com","start":7,"end":24,"label":"EMAIL"}]}\n',
        encoding="utf-8",
    )

    result = check_dataset_inputs(
        "ai4privacy-pii-masking-300k",
        dataset_root,
        "english_openpii_38k.jsonl",
    )

    assert result.dataset_root_exists is True
    assert result.dataset_file_exists is True
    assert result.schema_ok is True
    assert "privacy_mask" in result.schema_fields


def test_prepare_ai4privacy_fixture_writes_benchmark_cases(tmp_path: Path) -> None:
    dataset_path = tmp_path / "english_openpii_38k.jsonl"
    dataset_path.write_text(
        "\n".join(
            [
                '{"id":"40768A","source_text":"Email: bballoi@yahoo.com","target_text":"Email: [EMAIL]","privacy_mask":[{"value":"bballoi@yahoo.com","start":7,"end":24,"label":"EMAIL"}]}',
                '{"id":"40768B","source_text":"Applicant: Balloi Eckrich\\nEmail: bballoi@yahoo.com","target_text":"Applicant: [LASTNAME1] [LASTNAME2]\\nEmail: [EMAIL]","privacy_mask":[{"value":"Balloi","start":11,"end":17,"label":"LASTNAME1"},{"value":"Eckrich","start":18,"end":25,"label":"LASTNAME2"},{"value":"bballoi@yahoo.com","start":33,"end":50,"label":"EMAIL"}]}',
                '{"id":"unsupported","source_text":"Meeting at 10:20am","target_text":"Meeting at [TIME]","privacy_mask":[{"value":"10:20am","start":11,"end":18,"label":"TIME"}]}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "prepared.jsonl"

    summary = prepare_ai4privacy_fixture(dataset_path, output_path, max_cases=2)

    assert summary.rows_seen == 2
    assert summary.cases_written == 2
    assert summary.rows_skipped == 0
    cases = load_eval_cases(output_path)
    assert [case.case_id for case in cases] == ["ai4privacy-40768A", "ai4privacy-40768B"]
    assert cases[0].expected_redacted_text == "Email: <REDACTED_EMAIL_A>"
    assert cases[1].expected_redacted_text == (
        "Applicant: <REDACTED_PERSON_A> <REDACTED_PERSON_B>\nEmail: <REDACTED_EMAIL_A>"
    )


def test_resolve_redaction_config_reads_explicit_toml_and_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "vault-ops.toml"
    config_path.write_text(
        "\n".join(
            [
                "[redaction]",
                'base_url = "http://127.0.0.1:8090/v1"',
                'model = "from-config"',
                'api_key = "from-config-key"',
                "timeout_seconds = 23",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    args = build_arg_parser().parse_args(
        [
            "--config",
            str(config_path),
            "--redaction-model",
            "explicit-model",
            "--timeout-seconds",
            "17",
        ]
    )

    cfg, loaded_from = resolve_redaction_config(args)

    assert loaded_from == str(config_path)
    assert cfg.base_url == "http://127.0.0.1:8090/v1"
    assert cfg.model == "explicit-model"
    assert cfg.api_key == "from-config-key"
    assert cfg.timeout_seconds == 17


def test_main_compare_mode_writes_comparison_payload(tmp_path: Path, monkeypatch) -> None:
    fixture = FIXTURES / "redaction_eval_phase_a.jsonl"
    output_path = tmp_path / "comparison.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "redaction-eval",
            "--fixture",
            str(fixture),
            "--compare-mode",
            "regex",
            "--compare-mode",
            "hybrid",
            "--output",
            str(output_path),
        ],
    )

    exit_code = main()

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["comparison"]["baseline_mode"] == "regex"
    assert [run["mode"] for run in payload["runs"]] == ["regex", "hybrid"]
    assert payload["comparison"]["deltas"][0]["mode"] == "hybrid"


def test_main_writes_partial_report_and_auto_resumes(tmp_path: Path, monkeypatch) -> None:
    fixture = FIXTURES / "redaction_eval_phase_a.jsonl"
    output_path = tmp_path / "resume-report.json"
    real_redact = sys.modules["redaction_eval_harness"].redact_chunks_with_persistent_map
    interrupted_call_count = 0
    resumed_call_count = 0

    def interrupt_after_two_calls(*args, **kwargs):
        nonlocal interrupted_call_count
        interrupted_call_count += 1
        if interrupted_call_count == 3:
            raise KeyboardInterrupt
        return real_redact(*args, **kwargs)

    def count_resumed_calls(*args, **kwargs):
        nonlocal resumed_call_count
        resumed_call_count += 1
        return real_redact(*args, **kwargs)

    monkeypatch.setattr(
        sys.modules["redaction_eval_harness"],
        "redact_chunks_with_persistent_map",
        interrupt_after_two_calls,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "redaction-eval",
            "--fixture",
            str(fixture),
            "--mode",
            "regex",
            "--output",
            str(output_path),
        ],
    )

    exit_code = main()

    assert exit_code == 130
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "partial"
    assert payload["summary"]["cases_total"] == 2
    checkpoint_path = Path(payload["checkpoint_path"])
    checkpoint_results = load_checkpoint_results(
        checkpoint_path,
        fixture_path=fixture,
        cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert len(checkpoint_results) == 2

    monkeypatch.setattr(
        sys.modules["redaction_eval_harness"],
        "redact_chunks_with_persistent_map",
        count_resumed_calls,
    )

    exit_code = main()

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "complete"
    assert payload["summary"]["cases_total"] == 3
    assert payload["resumed_cases"] == 2
    assert interrupted_call_count == 3
    assert resumed_call_count == 1


def test_main_require_llm_candidates_rejects_regex_only(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "redaction-eval",
            "--fixture",
            "eval/redaction/fixtures/redaction_eval_phase_a.jsonl",
            "--mode",
            "regex",
            "--require-llm-candidates",
        ],
    )

    try:
        main()
    except ValueError as exc:
        assert "--require-llm-candidates requires at least one model or hybrid run" in str(exc)
    else:
        raise AssertionError("expected regex-only llm requirement to fail")
