from __future__ import annotations

import json
import urllib.error
from pathlib import Path

from vault_db import connect_vault_db
from vault_db_summary import redaction_stats
from vault_redaction import (
    PersistentRedactionMap,
    RedactionConfig,
    _regex_detect_candidates,
    is_redaction_value_allowed,
    redact_chunks_with_persistent_map,
)
from vault_vector_index import (
    ensure_redaction_table,
    prune_invalid_redaction_entries,
    upsert_redaction_entries,
)


def test_redaction_placeholder_reuse_across_chunks() -> None:
    table = PersistentRedactionMap()
    cfg = RedactionConfig(mode="regex", enabled=True)
    chunks = [
        "Contact me at jane.doe@example.com for the tax receipt.",
        "Forward all notes to jane.doe@example.com and call 212-555-1234.",
    ]
    out = redact_chunks_with_persistent_map(chunks, mode="regex", table=table, cfg=cfg)

    assert len(out.chunk_text_redacted) == 2
    assert out.items_redacted >= 2
    assert out.entries_total >= 2
    assert len(out.inserted_entries) >= 2
    assert out.candidate_sources["regex"] >= 2

    first = out.chunk_text_redacted[0]
    second = out.chunk_text_redacted[1]
    assert "<REDACTED_EMAIL_A>" in first
    assert "<REDACTED_EMAIL_A>" in second
    assert "<REDACTED_PHONE_A>" in second


def test_redaction_entries_upsert_increments_hit_count(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_redaction_table(conn)
        entries = [
            {
                "key_name": "EMAIL",
                "placeholder": "<REDACTED_EMAIL_A>",
                "value_norm": "amy@example.com",
                "original_value": "amy@example.com",
                "source_mode": "regex",
            }
        ]
        total_1 = upsert_redaction_entries(
            conn,
            scope_type="vault",
            scope_id="global",
            entries=entries,
        )
        total_2 = upsert_redaction_entries(
            conn,
            scope_type="vault",
            scope_id="global",
            entries=entries,
        )
        assert total_1 == 1
        assert total_2 == 1
        row = conn.execute(
            """
            SELECT placeholder, hit_count
            FROM redaction_entries
            WHERE scope_type=? AND scope_id=? AND value_norm=?
            """,
            ("vault", "global", "amy@example.com"),
        ).fetchone()
        assert row is not None
        assert str(row[0]) == "<REDACTED_EMAIL_A>"
        assert int(row[1]) == 2
    finally:
        conn.close()


def test_redaction_value_filters_reject_common_false_positives() -> None:
    assert not is_redaction_value_allowed("ACCOUNT", "24")
    assert not is_redaction_value_allowed("ACCOUNT", "20")
    assert not is_redaction_value_allowed("ADDRESS", "CA")
    assert not is_redaction_value_allowed("PERSON", "LAST NAME")
    assert not is_redaction_value_allowed("PERSON", "name")
    assert not is_redaction_value_allowed("PERSON", "Two individuals")
    assert not is_redaction_value_allowed("CUSTOM", "employee id")
    assert not is_redaction_value_allowed("CUSTOM", "username")
    assert not is_redaction_value_allowed("CUSTOM", "abcdef")

    assert is_redaction_value_allowed("EMAIL", "amy@example.com")
    assert is_redaction_value_allowed("PHONE", "617-555-1212")
    assert is_redaction_value_allowed("PERSON", "Amy Doe")
    assert is_redaction_value_allowed("ADDRESS", "123 Main St")
    assert is_redaction_value_allowed("ADDRESS", "ENG", source_text='State: "ENG"')
    assert is_redaction_value_allowed("ADDRESS", "58", source_text='Building Number: "58"')
    assert is_redaction_value_allowed("CUSTOM", "@amy.doe-77")
    assert is_redaction_value_allowed("CUSTOM", "amy_doe")
    assert is_redaction_value_allowed("CUSTOM", "amy.doe-77")
    assert is_redaction_value_allowed("CUSTOM", "neo-43CU")
    assert is_redaction_value_allowed("CUSTOM", "cust8472")
    assert is_redaction_value_allowed("CUSTOM", "43CU")

    assert not is_redaction_value_allowed("CUSTOM", "speaker")
    assert not is_redaction_value_allowed("CUSTOM", "participant")
    assert not is_redaction_value_allowed("CUSTOM", "employee id")


def test_redaction_map_ignores_invalid_persisted_entries() -> None:
    table = PersistentRedactionMap.from_rows(
        [
            ("ADDRESS", "<REDACTED_ADDRESS_A>", "ca", "CA"),
            ("PERSON", "<REDACTED_PERSON_A>", "last name", "LAST NAME"),
            ("EMAIL", "<REDACTED_EMAIL_A>", "amy@example.com", "amy@example.com"),
        ]
    )

    assert "<REDACTED_EMAIL_A>" in table.placeholder_to_value
    assert "<REDACTED_ADDRESS_A>" not in table.placeholder_to_value
    assert "<REDACTED_PERSON_A>" not in table.placeholder_to_value


def test_hybrid_redaction_filters_weak_model_candidates(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(key_name="PERSON", value="LAST NAME", source=source),
            RedactionCandidate(key_name="ADDRESS", value="CA", source=source),
            RedactionCandidate(key_name="PERSON", value="Amy Doe", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        ["Amy Doe lives here. The form says LAST NAME and CA."],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    assert any(entry["original_value"] == "Amy Doe" for entry in out.inserted_entries)
    assert all(entry["original_value"] != "LAST NAME" for entry in out.inserted_entries)
    assert all(entry["original_value"] != "CA" for entry in out.inserted_entries)
    assert out.candidate_sources["llm_chunk"] == 3


def test_hybrid_redaction_expands_composed_address_model_candidates(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(
                key_name="ADDRESS",
                value="58 Kings Lane, Norwich, ENG, NR1 3PS",
                source=source,
            ),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    text = (
        'Participant Information:\n'
        '- Building Number: "58"\n'
        '- Street: "Kings Lane"\n'
        '- City: "Norwich"\n'
        '- State: "ENG"\n'
        '- Postcode: "NR1 3PS"\n'
    )
    out = redact_chunks_with_persistent_map(
        [text],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    originals = {entry["original_value"] for entry in out.inserted_entries}
    assert {"58", "Kings Lane", "Norwich", "ENG", "NR1 3PS"}.issubset(originals)


def test_hybrid_redaction_keeps_strong_custom_model_candidates(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(key_name="CUSTOM", value="employee id", source=source),
            RedactionCandidate(key_name="CUSTOM", value="amy_doe", source=source),
            RedactionCandidate(key_name="CUSTOM", value="cust8472", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        ["Use amy_doe for chat and cust8472 for the portal. The field label says employee id."],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    kept_values = {entry["original_value"] for entry in out.inserted_entries}
    assert "amy_doe" in kept_values
    assert "cust8472" in kept_values
    assert "employee id" not in kept_values
    assert out.candidate_sources["llm_chunk"] == 3


def test_hybrid_redaction_remaps_person_handle_to_custom(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(key_name="PERSON", value="neo-43CU", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        ["User neo-43CU joined the private room."],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    assert out.inserted_entries == [
        {
            "key_name": "CUSTOM",
            "placeholder": "<REDACTED_CUSTOM_A>",
            "value_norm": "neo-43cu",
            "original_value": "neo-43CU",
            "source_mode": "llm_chunk",
        }
    ]


def test_hybrid_redaction_remaps_custom_to_canonical_labels(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(key_name="CUSTOM", value="amy@example.com", source=source),
            RedactionCandidate(key_name="CUSTOM", value="617-555-1212", source=source),
            RedactionCandidate(key_name="CUSTOM", value="https://vault.example.com/u/amy", source=source),
            RedactionCandidate(key_name="CUSTOM", value="ZXCV99887766", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        [
            "Contact amy@example.com or 617-555-1212, visit https://vault.example.com/u/amy "
            "and reference ZXCV99887766."
        ],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    assert [(entry["key_name"], entry["original_value"]) for entry in out.inserted_entries] == [
        ("EMAIL", "amy@example.com"),
        ("PHONE", "617-555-1212"),
        ("URL", "https://vault.example.com/u/amy"),
        ("ACCOUNT", "ZXCV99887766"),
    ]


def test_hybrid_redaction_keeps_handle_like_customs_out_of_account(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(key_name="CUSTOM", value="2005zheng.monckton", source=source),
            RedactionCandidate(key_name="CUSTOM", value="wsfdkmi9214", source=source),
            RedactionCandidate(key_name="CUSTOM", value="maugeon1942", source=source),
            RedactionCandidate(key_name="CUSTOM", value="ZXCV99887766", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        [
            "Thread participants 2005zheng.monckton, wsfdkmi9214, and maugeon1942 "
            "referenced token ZXCV99887766."
        ],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    assert [(entry["key_name"], entry["original_value"]) for entry in out.inserted_entries] == [
        ("CUSTOM", "2005zheng.monckton"),
        ("CUSTOM", "wsfdkmi9214"),
        ("CUSTOM", "maugeon1942"),
        ("ACCOUNT", "ZXCV99887766"),
    ]


def test_hybrid_redaction_keeps_last_name_fields_as_person(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    text = (
        "\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\n"
        "<StudentProgressReport>\n"
        "    <Student>\n"
        "        <LastName id=\\\"Giazzi\\\"></LastName>\n"
        "    </Student>\n"
        "    <Student>\n"
        "        <LastName id=\\\"Wurzel\\\" id2=\\\"Armbrüster\\\"></LastName>\n"
        "    </Student>\n"
        "    <Student>\n"
        "        <LastName id=\\\"Schulze-Rouat\\\"></LastName>\n"
        "    </Student>\n"
        "    <Student>\n"
        "        <LastName id=\\\"Dragomirova\\\"></LastName>\n"
        "    </Student>\n"
    )

    def fake_model_detect_candidates(chunk: str, *, cfg: RedactionConfig, source: str):
        assert chunk == text
        return [
            RedactionCandidate(key_name="PERSON", value="Giazzi", source=source),
            RedactionCandidate(key_name="PERSON", value="Wurzel", source=source),
            RedactionCandidate(key_name="PERSON", value="Armbrüster", source=source),
            RedactionCandidate(key_name="PERSON", value="Schulze-Rouat", source=source),
            RedactionCandidate(key_name="PERSON", value="Dragomirova", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        [text],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    assert [entry["key_name"] for entry in out.inserted_entries] == [
        "PERSON",
        "PERSON",
        "PERSON",
        "PERSON",
        "PERSON",
    ]
    assert out.chunk_text_redacted[0] == (
        "\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\n"
        "<StudentProgressReport>\n"
        "    <Student>\n"
        "        <LastName id=\\\"<REDACTED_PERSON_A>\\\"></LastName>\n"
        "    </Student>\n"
        "    <Student>\n"
        "        <LastName id=\\\"<REDACTED_PERSON_B>\\\" id2=\\\"<REDACTED_PERSON_C>\\\"></LastName>\n"
        "    </Student>\n"
        "    <Student>\n"
        "        <LastName id=\\\"<REDACTED_PERSON_D>\\\"></LastName>\n"
        "    </Student>\n"
        "    <Student>\n"
        "        <LastName id=\\\"<REDACTED_PERSON_E>\\\"></LastName>\n"
        "    </Student>\n"
    )


def test_model_detect_candidates_disables_thinking_for_local_qwen(monkeypatch) -> None:
    from vault_redaction import _model_detect_candidates

    captured_payloads: list[dict] = []

    class FakeResponse:
        def __init__(self, payload: dict):
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_urlopen(req, timeout):  # noqa: ANN001
        captured_payloads.append(json.loads(req.data.decode("utf-8")))
        return FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "redactions": [
                                        {
                                            "key_name": "ADDRESS",
                                            "values": ["44 West 81st Street, Apt 5B, New York, NY 10024"],
                                        }
                                    ]
                                }
                            )
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    candidates = _model_detect_candidates(
        "Ship the reimbursement packet to 44 West 81st Street, Apt 5B, New York, NY 10024.",
        cfg=RedactionConfig(mode="hybrid", enabled=True),
        source="llm_chunk",
    )

    assert [candidate.value for candidate in candidates] == [
        "44 West 81st Street, Apt 5B, New York, NY 10024"
    ]
    assert captured_payloads[0]["chat_template_kwargs"] == {"enable_thinking": False}
    assert captured_payloads[0]["response_format"] == {"type": "json_object"}


def test_model_detect_candidates_retries_without_response_format_then_template_kwargs(monkeypatch) -> None:
    from vault_redaction import _model_detect_candidates

    captured_payloads: list[dict] = []
    calls = 0

    class FakeResponse:
        def __init__(self, payload: dict):
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_http_error(req, timeout):  # noqa: ANN001
        nonlocal calls
        calls += 1
        payload = json.loads(req.data.decode("utf-8"))
        captured_payloads.append(payload)
        if calls < 3:
            raise urllib.error.HTTPError(
                req.full_url,
                400,
                "bad request",
                hdrs=None,
                fp=None,
            )
        return FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "redactions": [
                                        {"key_name": "EMAIL", "values": ["jane.doe@example.com"]}
                                    ]
                                }
                            )
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_http_error)

    candidates = _model_detect_candidates(
        "Send confirmation to jane.doe@example.com.",
        cfg=RedactionConfig(mode="hybrid", enabled=True),
        source="llm_chunk",
    )

    assert [candidate.value for candidate in candidates] == ["jane.doe@example.com"]
    assert "response_format" in captured_payloads[0]
    assert "chat_template_kwargs" in captured_payloads[0]
    assert "response_format" not in captured_payloads[1]
    assert "chat_template_kwargs" in captured_payloads[1]
    assert "response_format" not in captured_payloads[2]
    assert "chat_template_kwargs" not in captured_payloads[2]


def test_model_detect_candidates_strips_wrapping_punctuation_from_values(monkeypatch) -> None:
    from vault_redaction import _model_detect_candidates

    class FakeResponse:
        def __init__(self, payload: dict):
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_urlopen(req, timeout):  # noqa: ANN001
        return FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "redactions": [
                                        {"key_name": "CUSTOM", "values": ["[rglfqmdcyfhwo87]"]}
                                    ]
                                }
                            )
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    candidates = _model_detect_candidates(
        "Best regards,\n\n[rglfqmdcyfhwo87]\nStudent Support Services Department",
        cfg=RedactionConfig(mode="hybrid", enabled=True),
        source="llm_chunk",
    )

    assert [(candidate.key_name, candidate.value) for candidate in candidates] == [
        ("CUSTOM", "rglfqmdcyfhwo87")
    ]


def test_hybrid_redaction_preserves_brackets_around_cleaned_custom(monkeypatch) -> None:
    from vault_redaction import RedactionCandidate

    def fake_model_detect_candidates(text: str, *, cfg: RedactionConfig, source: str):
        return [
            RedactionCandidate(key_name="CUSTOM", value="rglfqmdcyfhwo87", source=source),
        ]

    monkeypatch.setattr("vault_redaction._model_detect_candidates", fake_model_detect_candidates)
    table = PersistentRedactionMap()
    out = redact_chunks_with_persistent_map(
        [
            "Best regards,\n\n[rglfqmdcyfhwo87]\nStudent Support Services Department",
        ],
        mode="hybrid",
        table=table,
        cfg=RedactionConfig(mode="hybrid", enabled=True),
    )

    assert out.chunk_text_redacted[0] == (
        "Best regards,\n\n[<REDACTED_CUSTOM_A>]\nStudent Support Services Department"
    )


def test_labeled_account_detection_beats_phone_detection() -> None:
    text = "Account number: 9876543210\nPhone: 617-555-1212"
    candidates = _regex_detect_candidates(text)
    seen = {(cand.key_name, cand.value) for cand in candidates}
    assert ("ACCOUNT", "9876543210") in seen
    assert ("PHONE", "617-555-1212") in seen
    assert ("PHONE", "9876543210") not in seen


def test_labeled_person_detection_catches_strong_name_context() -> None:
    text = "Billing contact: Jane Doe\nPhone: 617-555-1212"
    candidates = _regex_detect_candidates(text)
    seen = {(cand.key_name, cand.value) for cand in candidates}
    assert ("PERSON", "Jane Doe") in seen
    assert ("PHONE", "617-555-1212") in seen


def test_labeled_person_detection_rejects_form_label_noise() -> None:
    text = "Name: LAST NAME\nAlternate contact: FIRST NAME"
    candidates = _regex_detect_candidates(text)
    assert not candidates
    table = PersistentRedactionMap()
    run = redact_chunks_with_persistent_map(
        [text],
        mode="regex",
        table=table,
        cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert not run.inserted_entries
    assert run.chunk_text_redacted[0] == text


def test_scientific_volume_noise_is_not_detected_as_phone_or_account() -> None:
    text = "Scientific Reports | Vol.:(0123456789) (2022) 12:19744"
    candidates = _regex_detect_candidates(text)
    seen = {(cand.key_name, cand.value) for cand in candidates}
    assert ("PHONE", "0123456789") not in seen
    assert ("ACCOUNT", "0123456789") not in seen


def test_regex_redaction_uses_persisted_account_placeholder_for_labeled_account() -> None:
    table = PersistentRedactionMap()
    text = "Account number: 9876543210\nPhone: 617-555-1212"
    run = redact_chunks_with_persistent_map(
        [text],
        mode="regex",
        table=table,
        cfg=RedactionConfig(mode="regex", enabled=True),
    )
    redacted = run.chunk_text_redacted[0]
    assert "<REDACTED_ACCOUNT_A>" in redacted
    assert "<REDACTED_PHONE_A>" in redacted
    assert "<REDACTED_ACCOUNT>:" not in redacted


def test_persisted_person_redaction_matches_newline_variant() -> None:
    table = PersistentRedactionMap.from_rows(
        [("PERSON", "<REDACTED_PERSON_A>", "sveinn palsson", "Sveinn Palsson")]
    )
    text = "Signer: Sveinn\nPalsson"
    run = redact_chunks_with_persistent_map(
        [text],
        mode="regex",
        table=table,
        cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert run.chunk_text_redacted[0] == "Signer: <REDACTED_PERSON_A>"
    assert not run.inserted_entries


def test_persisted_person_redaction_matches_multi_space_variant() -> None:
    table = PersistentRedactionMap.from_rows(
        [("PERSON", "<REDACTED_PERSON_A>", "sveinn palsson", "Sveinn Palsson")]
    )
    text = "Signer: Sveinn   Palsson"
    run = redact_chunks_with_persistent_map(
        [text],
        mode="regex",
        table=table,
        cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert run.chunk_text_redacted[0] == "Signer: <REDACTED_PERSON_A>"
    assert not run.inserted_entries


def test_prune_invalid_redaction_entries_removes_false_positives(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_redaction_table(conn)
        conn.executemany(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm,
              original_value, source_mode, first_seen_at, last_seen_at, hit_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "vault",
                    "global",
                    "ADDRESS",
                    "<REDACTED_ADDRESS_A>",
                    "ca",
                    "CA",
                    "llm_chunk",
                    "2026-03-22T00:00:00+00:00",
                    "2026-03-22T00:00:00+00:00",
                    1,
                ),
                (
                    "vault",
                    "global",
                    "EMAIL",
                    "<REDACTED_EMAIL_A>",
                    "amy@example.com",
                    "amy@example.com",
                    "regex",
                    "2026-03-22T00:00:00+00:00",
                    "2026-03-22T00:00:00+00:00",
                    1,
                ),
            ],
        )
        conn.commit()

        removed = prune_invalid_redaction_entries(conn, scope_type="vault", scope_id="global")
        assert removed == 1

        rows = conn.execute(
            "SELECT key_name, original_value, COALESCE(status, 'active') FROM redaction_entries ORDER BY key_name"
        ).fetchall()
        assert [(str(row[0]), str(row[1]), str(row[2])) for row in rows] == [
            ("ADDRESS", "CA", "rejected"),
            ("EMAIL", "amy@example.com", "active"),
        ]
    finally:
        conn.close()


def test_redaction_stats_supports_legacy_entries_without_status_column(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        conn.execute(
            """
            CREATE TABLE redaction_entries (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scope_type TEXT NOT NULL,
              scope_id TEXT NOT NULL,
              key_name TEXT NOT NULL,
              placeholder TEXT NOT NULL,
              value_norm TEXT NOT NULL,
              original_value TEXT NOT NULL,
              source_mode TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              hit_count INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm,
              original_value, source_mode, first_seen_at, last_seen_at, hit_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "vault",
                "global",
                "EMAIL",
                "<REDACTED_EMAIL_A>",
                "amy@example.com",
                "amy@example.com",
                "regex",
                "2026-03-22T00:00:00+00:00",
                "2026-03-22T00:00:00+00:00",
                2,
            ),
        )
        conn.commit()

        stats = redaction_stats(conn)
        assert stats["redaction_entries_total"] == 1
        assert stats["redaction_hit_count_total"] == 2
        assert stats["redaction_entries_rejected"] == 0
    finally:
        conn.close()
