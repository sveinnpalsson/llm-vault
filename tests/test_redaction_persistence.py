from __future__ import annotations

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

    assert is_redaction_value_allowed("EMAIL", "amy@example.com")
    assert is_redaction_value_allowed("PHONE", "617-555-1212")
    assert is_redaction_value_allowed("PERSON", "Amy Doe")
    assert is_redaction_value_allowed("ADDRESS", "123 Main St")


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
