from __future__ import annotations

import json
from pathlib import Path

from vault_db import connect_vault_db
from vault_fetch import fetch_source, list_sources
from vault_registry_sync import ensure_db
from vault_vector_index import _stable_source_id


def _seed_fetch_registry(registry_db: Path) -> dict[str, str]:
    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.execute(
            """
            INSERT INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/docs/fetch-doc.txt",
                "fetch-doc-1",
                "generated",
                "Call Jane Doe at jane.doe@example.com about the invoice.",
                "plain",
                128,
                1735689600.0,
                "2026-03-24T10:00:00+00:00",
                "Invoice follow-up for Jane Doe.",
                "local-test",
                "doc-hash",
                "ok",
                "2026-03-24T10:00:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2026-03-20T00:00:00+00:00",
                            "kind": "document_date",
                            "source": "regex_text",
                        }
                    ]
                ),
                "2026-03-20T00:00:00+00:00",
            ),
        )
        reg.execute(
            """
            INSERT INTO mail_registry (
              filepath, checksum, source, msg_id, account_email, thread_id,
              date_iso, from_addr, to_addr, subject, snippet, body_text,
              labels_json, summary_text, primary_date, dates_json, indexed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "mail://message/fetch-mail-1",
                "fetch-mail-1",
                "inbox-vault",
                "fetch-mail-1",
                "acct@example.com",
                "thread-1",
                "2026-03-24T11:00:00+00:00",
                "boss@example.com",
                "acct@example.com",
                "Budget approval",
                "Need approval from Jane Doe",
                "Please send the approval to jane.doe@example.com today.",
                json.dumps(["INBOX", "IMPORTANT"]),
                "Jane Doe approval summary.",
                "2026-03-24T11:00:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2026-03-24T11:00:00+00:00",
                            "kind": "message_date",
                            "source": "date_iso",
                        }
                    ]
                ),
                "2026-03-24T11:05:00+00:00",
                "2026-03-24T11:05:00+00:00",
            ),
        )
        reg.execute(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm, original_value,
              source_mode, first_seen_at, last_seen_at, hit_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "vault",
                "global",
                "email",
                "<REDACTED_EMAIL_1>",
                "jane.doe@example.com",
                "jane.doe@example.com",
                "regex",
                "2026-03-24T11:05:00+00:00",
                "2026-03-24T11:05:00+00:00",
                1,
            ),
        )
        reg.commit()
    finally:
        reg.close()
    return {
        "doc": _stable_source_id("docs_registry", "/vault/docs/fetch-doc.txt"),
        "mail": _stable_source_id("mail_registry", "mail://message/fetch-mail-1"),
    }


def test_fetch_source_full_returns_filepath_and_unredacted_content(tmp_path: Path) -> None:
    ids = _seed_fetch_registry(tmp_path / "state" / "vault_registry.db")

    payload = fetch_source(tmp_path / "state" / "vault_registry.db", ids["doc"], clearance="full")

    assert payload["source_id"] == ids["doc"]
    assert payload["source_kind"] == "docs"
    assert payload["source_filepath"] == "/vault/docs/fetch-doc.txt"
    assert "jane.doe@example.com" in payload["content"].lower()
    assert payload["metadata"]["parser"] == "plain"


def test_fetch_source_redacted_hides_filepath_and_redacts_mail_content(tmp_path: Path) -> None:
    ids = _seed_fetch_registry(tmp_path / "state" / "vault_registry.db")

    payload = fetch_source(tmp_path / "state" / "vault_registry.db", ids["mail"], clearance="redacted")

    assert payload["source_id"] == ids["mail"]
    assert payload["source_kind"] == "mail"
    assert payload["source_filepath"] is None
    assert "<REDACTED_EMAIL_1>" in payload["content"]
    assert "account_email" not in payload["metadata"]
    assert "msg_id" not in payload["metadata"]


def test_fetch_source_redacted_clips_after_redaction_for_repeated_urls(tmp_path: Path) -> None:
    ids = _seed_fetch_registry(tmp_path / "state" / "vault_registry.db")
    reg = connect_vault_db(tmp_path / "state" / "vault_registry.db", ensure_parent=True)
    try:
        long_url = "https://app.mgbcareconnect.org/app/link/tab/visits?session_id=01KNYFZN5M4BESFJBQPGMPV642&utm_medium=email&utm_campaign=After_Visit_Summary_EmailPush&utm_content=After_Visit_Summary_EM&utm_source=braze"
        body_text = (
            "Please review your after visit summary.\n"
            f"View after visit summary ({long_url})\n"
            f"View after visit summary ({long_url})\n"
            + ("Extra filler text. " * 80)
        )
        reg.execute(
            "UPDATE mail_registry SET body_text = ?, snippet = ?, summary_text = ? WHERE filepath = ?",
            (
                body_text,
                "Please review your after visit summary.",
                "After visit summary available.",
                "mail://message/fetch-mail-1",
            ),
        )
        reg.execute(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm, original_value,
              source_mode, first_seen_at, last_seen_at, hit_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "vault",
                "global",
                "url",
                "<REDACTED_URL_1>",
                long_url,
                long_url,
                "regex",
                "2026-03-24T11:05:00+00:00",
                "2026-03-24T11:05:00+00:00",
                2,
            ),
        )
        reg.commit()
    finally:
        reg.close()

    payload = fetch_source(tmp_path / "state" / "vault_registry.db", ids["mail"], clearance="redacted")

    assert "<REDACTED_URL_1>" in payload["content"]
    assert "https://app.mgbcareconnect.org" not in payload["content"]


def test_list_sources_returns_newest_first_compact_rows(tmp_path: Path) -> None:
    ids = _seed_fetch_registry(tmp_path / "state" / "vault_registry.db")

    payload = list_sources(tmp_path / "state" / "vault_registry.db", source="all", limit=5, clearance="full")

    assert payload["source"] == "all"
    assert payload["limit"] == 5
    assert payload["count"] == 2
    assert [item["source_id"] for item in payload["results"]] == [ids["mail"], ids["doc"]]
    assert payload["results"][0]["source_kind"] == "mail"
    assert payload["results"][0]["preview"] == "Budget approval"
    assert payload["results"][1]["source_filepath"] == "/vault/docs/fetch-doc.txt"


def test_list_sources_redacted_filters_dates_and_hides_full_fields(tmp_path: Path) -> None:
    ids = _seed_fetch_registry(tmp_path / "state" / "vault_registry.db")

    payload = list_sources(
        tmp_path / "state" / "vault_registry.db",
        source="mail",
        from_date="2026-03-24",
        to_date="2026-03-24",
        limit=5,
        clearance="redacted",
    )

    assert payload["count"] == 1
    assert payload["results"][0]["source_id"] == ids["mail"]
    assert payload["results"][0]["source_filepath"] is None
    assert payload["results"][0]["preview"] == "Budget approval"
