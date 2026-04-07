from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import uuid4

import pytest
from redaction_eval_harness import load_eval_cases, run_eval_cases
from vault_redaction import RedactionConfig
from vault_service_defaults import DEFAULT_LOCAL_MODEL_BASE_URL

ROOT = Path(__file__).resolve().parents[1]


def _enabled() -> bool:
    return str(os.getenv("LLM_VAULT_RUN_LIVE_SMOKE", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _is_local_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").strip().lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


def _post_json(url: str, payload: dict, *, timeout: int = 30) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _post_file(url: str, file_path: Path, *, timeout: int = 45) -> dict:
    boundary = f"----llm-vault-live-smoke-{uuid4().hex}"
    body = b"".join(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="file"; filename="{file_path.name}"\r\n'
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode("utf-8"),
            file_path.read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


@pytest.mark.live_llm
def test_live_embedding_smoke_two_calls_max() -> None:
    if not _enabled():
        pytest.skip("set LLM_VAULT_RUN_LIVE_SMOKE=1 to enable")

    base = str(os.getenv("VAULT_EMBED_BASE_URL", DEFAULT_LOCAL_MODEL_BASE_URL)).strip().rstrip("/")
    model = str(os.getenv("VAULT_EMBED_MODEL", "Qwen3-Embedding-8B")).strip()
    if not _is_local_url(base):
        pytest.skip("live smoke enforces local-only endpoints")

    try:
        # Bounded live calls: exactly 2 embed inputs in a single request.
        payload = _post_json(
            f"{base}/embeddings",
            {"model": model, "input": ["tax receipt", "family beach photo"]},
            timeout=30,
        )
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
        pytest.skip("embedding endpoint unavailable")

    data = payload.get("data")
    assert isinstance(data, list)
    assert len(data) == 2
    assert isinstance(data[0].get("embedding"), list)


@pytest.mark.live_llm
def test_live_summary_smoke_two_calls_max() -> None:
    if not _enabled():
        pytest.skip("set LLM_VAULT_RUN_LIVE_SMOKE=1 to enable")

    base = str(os.getenv("VAULT_SUMMARY_BASE_URL", DEFAULT_LOCAL_MODEL_BASE_URL)).strip().rstrip("/")
    model = str(os.getenv("VAULT_SUMMARY_MODEL", "qwen3-14b")).strip()
    if not _is_local_url(base):
        pytest.skip("live smoke enforces local-only endpoints")

    prompts = [
        "Summarize in one line: tax receipt for scanner upload.",
        "Summarize in one line: family beach photo from summer trip.",
    ]
    for prompt in prompts:
        try:
            payload = _post_json(
                f"{base}/chat/completions",
                {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "Return concise plain text."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 80,
                },
                timeout=35,
            )
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
            pytest.skip("summary endpoint unavailable")
        choices = payload.get("choices")
        assert isinstance(choices, list) and len(choices) >= 1


@pytest.mark.live_llm
def test_live_redaction_eval_hybrid_smoke_one_case() -> None:
    if not _enabled():
        pytest.skip("set LLM_VAULT_RUN_LIVE_SMOKE=1 to enable")

    base = str(os.getenv("VAULT_REDACTION_BASE_URL", DEFAULT_LOCAL_MODEL_BASE_URL)).strip().rstrip("/")
    model = str(os.getenv("VAULT_REDACTION_MODEL", "qwen3-14b")).strip()
    if not _is_local_url(base):
        pytest.skip("live smoke enforces local-only endpoints")

    fixture = ROOT / "eval" / "redaction" / "fixtures" / "redaction_eval_hybrid_smoke.jsonl"
    case = load_eval_cases(fixture)[0]
    try:
        report = run_eval_cases(
            [case],
            cfg=RedactionConfig(
                mode="hybrid",
                enabled=True,
                base_url=base,
                model=model,
                api_key=str(os.getenv("VAULT_REDACTION_API_KEY", "local")).strip() or "local",
            ),
            fixture_path=fixture,
        )
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
        pytest.skip("redaction endpoint unavailable")

    assert report.summary.llm_candidate_cases >= 1
    assert report.summary.llm_candidates_total >= 1
    assert report.cases[0].candidate_sources.get("llm_chunk", 0) >= 1
