from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))


@pytest.fixture(autouse=True)
def _default_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_VAULT_ALLOW_PLAINTEXT_FOR_TESTS", "1")
    monkeypatch.setenv("LLM_VAULT_DB_PASSWORD", "test-password")
