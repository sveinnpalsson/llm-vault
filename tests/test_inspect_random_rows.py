from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path

import inspect_random_rows
from inspect_random_rows import _load_redaction_rows, _prepare_photo
from vault_db import connect_vault_db
from vault_redaction import RedactionConfig
from vault_registry_sync import ensure_db
from vault_vector_index import update_index


class StubEmbeddingClient:
    def __init__(self, dim: int = 8):
        self.dim = dim

    def _embed_one(self, text: str) -> list[float]:
        out = [0.0] * self.dim
        for token in str(text or "").lower().split():
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            idx = int.from_bytes(digest[:4], "big") % self.dim
            sign = 1.0 if (digest[4] % 2 == 0) else -1.0
            out[idx] += sign
        norm = math.sqrt(sum(v * v for v in out))
        if norm > 0:
            out = [v / norm for v in out]
        return out

    def embed_texts(self, texts: list[str]) -> tuple[list[bytes], int]:
        import array

        blobs: list[bytes] = []
        for text in texts:
            blobs.append(array.array("f", self._embed_one(text)).tobytes())
        return blobs, self.dim


def test_prepare_photo_surfaces_ocr_fields_and_indexed_channels(tmp_path: Path) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.execute(
            """
            INSERT INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, indexed_at, updated_at, notes,
              category_primary, category_secondary, taxonomy, caption,
              analyzer_model, analyzer_status, analyzer_error, analyzer_raw, analyzed_at,
              ocr_text, ocr_status, ocr_source, ocr_updated_at,
              dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/photos/shipping-label.jpg",
                "photo-inspect-1",
                "generated",
                "2025-02-11T09:15:00+00:00",
                240,
                1735689602.0,
                "2026-03-17T00:10:00+00:00",
                "2026-03-17T00:10:00+00:00",
                "package intake notes",
                "document",
                "label",
                "docs",
                "shipping label on parcel",
                "test-photo-model",
                "ok",
                "",
                '{"caption":"shipping label on parcel","text_raw":"Contact jane.doe@example.com for waybill intake"}',
                "2026-03-17T00:10:00+00:00",
                "Contact jane.doe@example.com for waybill intake",
                "ok",
                "analyzer:text_raw",
                "2026-03-17T00:10:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2025-02-11T09:15:00+00:00",
                            "kind": "photo_taken",
                            "source": "metadata",
                            "confidence": 1.0,
                        }
                    ]
                ),
                "2025-02-11T09:15:00+00:00",
            ),
        )
        reg.commit()
    finally:
        reg.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="photos",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    reg = connect_vault_db(registry_db)
    vec = connect_vault_db(vector_db)
    try:
        row = reg.execute(
            "SELECT * FROM photos_registry WHERE filepath = ?",
            ("/vault/photos/shipping-label.jpg",),
        ).fetchone()
        assert row is not None
        payload = _prepare_photo(
            dict(row),
            vec,
            _load_redaction_rows(reg),
            limit=120,
        )
    finally:
        reg.close()
        vec.close()

    assert payload["ocr_status"] == "ok"
    assert payload["ocr_source"] == "analyzer:text_raw"
    assert payload["ocr_updated_at"]
    assert "Contact jane.doe@example.com" in payload["ocr_text"]
    assert "<REDACTED_EMAIL" in payload["ocr_text_redacted"]
    assert payload["embedding_present"] is True
    assert "redacted" in payload["photo_channels_by_level"]
    assert "ocr" in payload["photo_channels_by_level"]["redacted"]


def test_inspect_main_outputs_rows_by_registered_source(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.execute(
            """
            INSERT INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, indexed_at, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at, dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/docs/passport.txt",
                "doc-inspect-1",
                "generated",
                "Passport copy for Jane Doe",
                "plain",
                100,
                1735689600.0,
                "2026-03-17T00:00:00+00:00",
                "2026-03-17T00:00:00+00:00",
                "Passport copy summary",
                "local-test",
                "hash-doc",
                "ok",
                "2026-03-17T00:00:00+00:00",
                "[]",
                "2026-03-17T00:00:00+00:00",
            ),
        )
        reg.execute(
            """
            INSERT INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, indexed_at, updated_at, notes,
              category_primary, category_secondary, taxonomy, caption,
              analyzer_model, analyzer_status, analyzer_error, analyzer_raw, analyzed_at,
              ocr_text, ocr_status, ocr_source, ocr_updated_at,
              dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/photos/passport-card.jpg",
                "photo-inspect-2",
                "generated",
                "2025-02-11T09:15:00+00:00",
                240,
                1735689602.0,
                "2026-03-17T00:10:00+00:00",
                "2026-03-17T00:10:00+00:00",
                "passport card intake",
                "document",
                "",
                "docs",
                "passport card on desk",
                "test-photo-model",
                "ok",
                "",
                '{"caption":"passport card on desk","text_raw":"Renfei passport identifier"}',
                "2026-03-17T00:10:00+00:00",
                "Renfei passport identifier",
                "ok",
                "analyzer:text_raw",
                "2026-03-17T00:10:00+00:00",
                "[]",
                "2025-02-11T09:15:00+00:00",
            ),
        )
        reg.commit()
    finally:
        reg.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="all",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "inspect_random_rows.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--text-limit",
            "120",
        ],
    )
    rc = inspect_random_rows.main()
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    assert sorted(payload["sources"]) == ["docs", "mail", "photos"]
    assert payload["sources"]["docs"]["source_kind"] == "docs"
    assert payload["sources"]["photos"]["source_kind"] == "photos"
    assert payload["sources"]["photos"]["source_table"] == "photos_registry"
    assert payload["sources"]["mail"] is None
    assert "ocr" in payload["sources"]["photos"]["photo_channels_by_level"]["redacted"]


def test_inspect_main_surfaces_mail_fields_and_channels(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"

    reg = connect_vault_db(registry_db, ensure_parent=True)
    try:
        ensure_db(reg)
        reg.execute(
            """
            INSERT INTO mail_registry (
              filepath, checksum, source, msg_id, account_email, thread_id,
              date_iso, from_addr, to_addr, subject, snippet, body_text,
              labels_json, summary_text, primary_date, dates_json, indexed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "mail://message/msg-inspect-1",
                "mail-inspect-1",
                "inbox-vault",
                "msg-inspect-1",
                "acct@example.com",
                "thread-1",
                "2026-03-24T10:00:00+00:00",
                "boss@example.com",
                "acct@example.com",
                "Renfei budget approval",
                "Need Renfei approval by Friday",
                "Contact renfei@example.com for the budget approval packet.",
                json.dumps(["INBOX", "IMPORTANT"]),
                "Renfei approval summary for finance review.",
                "2026-03-24T10:00:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2026-03-24T10:00:00+00:00",
                            "kind": "message_date",
                            "source": "date_iso",
                        }
                    ]
                ),
                "2026-03-24T11:00:00+00:00",
                "2026-03-24T11:00:00+00:00",
            ),
        )
        reg.commit()
    finally:
        reg.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=StubEmbeddingClient(dim=8),
        source_selection="mail",
        mail_bridge_enabled=True,
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0
    _ = capsys.readouterr()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "inspect_random_rows.py",
            "--registry-db",
            str(registry_db),
            "--vectors-db",
            str(vector_db),
            "--text-limit",
            "120",
            "--mail-bridge-enabled",
        ],
    )
    rc = inspect_random_rows.main()
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    mail = payload["sources"]["mail"]
    assert mail["source_kind"] == "mail"
    assert mail["source_table"] == "mail_registry"
    assert mail["bridge_enabled"] is True
    assert mail["subject"] == "Renfei budget approval"
    assert "<REDACTED_EMAIL" in mail["body_text_redacted"]
    assert "redacted" in mail["mail_channels_by_level"]
    assert "summary" in mail["mail_channels_by_level"]["redacted"]
