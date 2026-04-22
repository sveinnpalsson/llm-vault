from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import vault_redaction
from vault_db import connect_vault_db
from vault_redaction import RedactionConfig
from vault_vector_index import _doc_summary_backfill_pending, query_index, update_index


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
            vec = self._embed_one(text)
            blobs.append(array.array("f", vec).tobytes())
        return blobs, self.dim


class FlatEmbeddingClient:
    def __init__(self, dim: int = 8):
        self.dim = dim

    def embed_texts(self, texts: list[str]) -> tuple[list[bytes], int]:
        import array

        vec = [0.0] * self.dim
        vec[0] = 1.0
        blob = array.array("f", vec).tobytes()
        return [blob for _ in texts], self.dim


def _seed_registry_db(registry_db: Path) -> None:
    conn = connect_vault_db(registry_db, ensure_parent=True)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              text_content TEXT,
              parser TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              summary_text TEXT,
              summary_model TEXT,
              summary_hash TEXT,
              summary_status TEXT,
              summary_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS photos_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              date_taken TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              notes TEXT,
              category_primary TEXT,
              category_secondary TEXT,
              taxonomy TEXT,
              caption TEXT,
              analyzer_status TEXT,
              ocr_text TEXT,
              ocr_status TEXT,
              ocr_source TEXT,
              ocr_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/docs/tax-receipt-2025.txt",
                "doc-check-1",
                "generated",
                (
                    "Tax receipt for scanner upload from Jane Doe. "
                    "Invoice number INV-4312. Contact jane.doe@example.com."
                ),
                "plain",
                100,
                1735689600.0,
                "2026-03-17T00:00:00+00:00",
                "Tax receipt for reimbursement and contact follow-up.",
                "local-test",
                "hash",
                "ok",
                "2026-03-17T00:00:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2025-01-15T00:00:00+00:00",
                            "kind": "billing_date",
                            "source": "regex_text",
                            "confidence": 0.92,
                        }
                    ]
                ),
                "2025-01-15T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/docs/t4x-r3ceipt-ocr-noise.txt",
                "doc-check-2",
                "generated",
                (
                    "T4X reciept scanned copy. Contact j4ne.doe@example.com "
                    "for reimbursement and route to finance."
                ),
                "plain",
                120,
                1735689601.0,
                "2026-03-17T00:05:00+00:00",
                "Scanned tax receipt copy for reimbursement and finance routing.",
                "local-test",
                "hash-2",
                "ok",
                "2026-03-17T00:05:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2025-01-18T00:00:00+00:00",
                            "kind": "document_date",
                            "source": "regex_text",
                            "confidence": 0.88,
                        }
                    ]
                ),
                "2025-01-18T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, updated_at,
              notes, category_primary, category_secondary, taxonomy, caption, analyzer_status,
              ocr_text, ocr_status, ocr_source, ocr_updated_at, dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/photos/beach-trip.jpg",
                "photo-check-1",
                "generated",
                "2025-07-01T11:00:00+00:00",
                200,
                1735689600.0,
                "2026-03-17T00:00:00+00:00",
                "family beach trip",
                "group_photo",
                "",
                "personal",
                "Jane Doe at the beach house",
                "ok",
                "",
                "not_applicable",
                "",
                "2026-03-17T00:00:00+00:00",
                json.dumps(
                    [
                        {
                            "value": "2025-07-01T11:00:00+00:00",
                            "kind": "photo_taken",
                            "source": "metadata",
                            "confidence": 1.0,
                        }
                    ]
                ),
                "2025-07-01T11:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO photos_registry (
              filepath, checksum, source, date_taken, size, mtime, updated_at,
              notes, category_primary, category_secondary, taxonomy, caption, analyzer_status,
              ocr_text, ocr_status, ocr_source, ocr_updated_at, dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/photos/shipping-label.jpg",
                "photo-check-2",
                "generated",
                "2025-02-11T09:15:00+00:00",
                240,
                1735689602.0,
                "2026-03-17T00:10:00+00:00",
                "package dropoff record",
                "document",
                "label",
                "docs",
                "shipping label on parcel",
                "ok",
                "waybill locator for reimbursement intake",
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
        conn.commit()
    finally:
        conn.close()


def _seed_consistency_registry_db(registry_db: Path) -> None:
    conn = connect_vault_db(registry_db, ensure_parent=True)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              text_content TEXT,
              parser TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              summary_text TEXT,
              summary_model TEXT,
              summary_hash TEXT,
              summary_status TEXT,
              summary_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS photos_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              date_taken TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              notes TEXT,
              category_primary TEXT,
              category_secondary TEXT,
              taxonomy TEXT,
              caption TEXT,
              analyzer_status TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        rows = [
            (
                "/vault/docs/private-letter.txt",
                "doc-private",
                "generated",
                (
                    "Employee onboarding notes for Jane Doe. "
                    "Email: jane.doe@example.com Phone: 617-555-1212 "
                    "Address: 123 Main St, Boston, MA 02110. "
                    "Account number: 9876543210. Respond by 2026-03-31."
                ),
                "plain",
                100,
                1735689600.0,
                "2026-03-17T00:00:00+00:00",
                "Employee onboarding notes for Jane Doe and follow-up actions.",
                "local-test",
                "hash-private",
                "ok",
                "2026-03-17T00:00:00+00:00",
                "[]",
                "",
            ),
            (
                "/vault/docs/travel-note.txt",
                "doc-travel",
                "generated",
                (
                    "Passport renewal checklist for Jane Doe. "
                    "Send copies to jane.doe@example.com and call 617-555-1212. "
                    "Mailing address remains 123 Main St, Boston, MA 02110."
                ),
                "plain",
                120,
                1735689601.0,
                "2026-03-17T00:05:00+00:00",
                "Passport renewal checklist for Jane Doe.",
                "local-test",
                "hash-travel",
                "ok",
                "2026-03-17T00:05:00+00:00",
                "[]",
                "",
            ),
        ]
        conn.executemany(
            """
            INSERT OR REPLACE INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _insert_generated_doc(
    registry_db: Path,
    *,
    filepath: str,
    checksum: str,
    updated_at: str,
    text_content: str,
    summary_text: str,
) -> None:
    conn = connect_vault_db(registry_db, ensure_parent=True)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              text_content TEXT,
              parser TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              summary_text TEXT,
              summary_model TEXT,
              summary_hash TEXT,
              summary_status TEXT,
              summary_updated_at TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS photos_registry (
              filepath TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              source TEXT,
              date_taken TEXT,
              size INTEGER,
              mtime REAL,
              updated_at TEXT,
              notes TEXT,
              category_primary TEXT,
              category_secondary TEXT,
              taxonomy TEXT,
              caption TEXT,
              analyzer_status TEXT,
              dates_json TEXT,
              primary_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                filepath,
                checksum,
                "generated",
                text_content,
                "plain",
                len(text_content),
                1735689600.0,
                updated_at,
                summary_text,
                "local-test",
                hashlib.sha1(summary_text.encode("utf-8")).hexdigest(),
                "ok",
                updated_at,
                "[]",
                "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_doc_summary_backfill_pending_excludes_empty_source() -> None:
    assert _doc_summary_backfill_pending({"summary_text": "", "summary_status": ""}) is True
    assert _doc_summary_backfill_pending({"summary_text": "", "summary_status": "error"}) is True
    assert _doc_summary_backfill_pending({"summary_text": "", "summary_status": "fallback-text"}) is True
    assert _doc_summary_backfill_pending({"summary_text": "", "summary_status": "empty-source"}) is False
    assert _doc_summary_backfill_pending({"summary_text": "ready", "summary_status": "ok"}) is False


def test_update_index_does_not_count_empty_source_docs_as_waiting_summary(capsys, tmp_path: Path) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_registry_db(registry_db)

    conn = connect_vault_db(registry_db)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO docs_registry (
              filepath, checksum, source, text_content, parser, size, mtime, updated_at,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
              dates_json, primary_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "/vault/docs/image-only-scan.txt",
                "doc-check-empty-source",
                "generated",
                "",
                "plain",
                1,
                1735689603.0,
                "2026-03-17T00:20:00+00:00",
                "",
                "local-test",
                "",
                "empty-source",
                "2026-03-17T00:20:00+00:00",
                "[]",
                "",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    stdout = capsys.readouterr().out
    assert 'source_stats={"docs":' in stdout
    assert '"waiting":0' in stdout


def test_generated_corpus_index_and_query(capsys, tmp_path: Path) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_registry_db(registry_db)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="all",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    _ = capsys.readouterr()
    rc_query = query_index(
        registry_db,
        vector_db,
        "tax receipt scanner",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="redacted",
        as_json=True,
    )
    assert rc_query == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] >= 1
    assert all(result["source_kind"] == "docs" for result in payload["results"])
    assert all(result["source_table"] == "docs_registry" for result in payload["results"])
    assert any("REDACTED" in result["preview"] for result in payload["results"])
    assert all(result["source_filepath"] is None for result in payload["results"])
    assert all(result["source_id"] for result in payload["results"])
    assert all("filepath" not in result["metadata"] for result in payload["results"])

    rc_query_full = query_index(
        registry_db,
        vector_db,
        "tax receipt scanner",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="full",
        as_json=True,
    )
    assert rc_query_full == 0
    payload_full = json.loads(capsys.readouterr().out)
    assert payload_full["count"] >= 1
    assert any("jane.doe@example.com" in result["preview"].lower() for result in payload_full["results"])
    assert all(result["source_filepath"] for result in payload_full["results"])
    assert payload_full["diagnostics"]["search_level_used"] == "redacted"
    assert payload_full["diagnostics"]["search_level_fallback"] == "full"

    rc_query_ocr = query_index(
        registry_db,
        vector_db,
        "receipt reimbursement finance",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="redacted",
        as_json=True,
    )
    assert rc_query_ocr == 0
    payload_ocr = json.loads(capsys.readouterr().out)
    assert payload_ocr["count"] >= 1

    rc_query_body = query_index(
        registry_db,
        vector_db,
        "INV-4312",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="redacted",
        as_json=True,
    )
    assert rc_query_body == 0
    payload_body = json.loads(capsys.readouterr().out)
    assert payload_body["count"] >= 1
    assert payload_body["results"][0]["metadata"]["index_text_kind"] == "body"

    rc_query_date = query_index(
        registry_db,
        vector_db,
        "reimbursement",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="redacted",
        to_date="2025-12-31",
        as_json=True,
    )
    assert rc_query_date == 0
    payload_date = json.loads(capsys.readouterr().out)
    assert payload_date["count"] >= 1

    rc_photo = query_index(
        registry_db,
        vector_db,
        "beach trip",
        top_k=5,
        embedding_client=emb,
        source_selection="photos",
        taxonomy="personal",
        clearance="redacted",
        as_json=True,
    )
    assert rc_photo == 0
    payload_photo = json.loads(capsys.readouterr().out)
    assert payload_photo["count"] >= 1
    assert payload_photo["results"][0]["source_kind"] == "photos"
    assert payload_photo["results"][0]["source_table"] == "photos_registry"
    assert payload_photo["results"][0]["source_filepath"] is None
    assert "caption" not in payload_photo["results"][0]["metadata"]
    assert "notes" not in payload_photo["results"][0]["metadata"]
    assert payload_photo["results"][0]["metadata"]["dates"][0]["value"] == "2025-07-01T11:00:00+00:00"

    conn = connect_vault_db(vector_db)
    try:
        shipping_channels = {
            str(row[0] or "")
            for row in conn.execute(
                """
                SELECT json_extract(metadata_json, '$.photo_channel')
                FROM vector_items_v2
                WHERE source_table='photos_registry' AND source_filepath=?
                """,
                ("/vault/photos/shipping-label.jpg",),
            ).fetchall()
        }
        beach_channels = {
            str(row[0] or "")
            for row in conn.execute(
                """
                SELECT json_extract(metadata_json, '$.photo_channel')
                FROM vector_items_v2
                WHERE source_table='photos_registry' AND source_filepath=?
                """,
                ("/vault/photos/beach-trip.jpg",),
            ).fetchall()
        }
        assert "ocr" in shipping_channels
        assert "ocr" not in beach_channels
    finally:
        conn.close()

    rc_photo_ocr = query_index(
        registry_db,
        vector_db,
        "waybill locator",
        top_k=5,
        embedding_client=emb,
        source_selection="photos",
        taxonomy="docs",
        clearance="redacted",
        as_json=True,
    )
    assert rc_photo_ocr == 0
    payload_photo_ocr = json.loads(capsys.readouterr().out)
    assert payload_photo_ocr["count"] >= 1
    assert payload_photo_ocr["results"][0]["source_kind"] == "photos"
    assert payload_photo_ocr["results"][0]["source_table"] == "photos_registry"
    assert len({result["source_id"] for result in payload_photo_ocr["results"]}) == payload_photo_ocr["count"]
    assert all("ocr_updated_at" not in result["metadata"] for result in payload_photo_ocr["results"])


def test_query_dedupes_results_by_source_id(capsys, tmp_path: Path) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_registry_db(registry_db)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    _ = capsys.readouterr()
    rc_query = query_index(
        registry_db,
        vector_db,
        "tax receipt reimbursement contact invoice finance scanner",
        top_k=10,
        embedding_client=emb,
        source_selection="docs",
        clearance="redacted",
        as_json=True,
    )
    assert rc_query == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert len({result["source_id"] for result in payload["results"]}) == 2
    assert [result["rank"] for result in payload["results"]] == [1, 2]


def test_query_hybrid_lexical_boost_improves_exact_match_ranking(capsys, tmp_path: Path) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_registry_db(registry_db)

    emb = FlatEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    _ = capsys.readouterr()
    rc_query = query_index(
        registry_db,
        vector_db,
        "t4x reciept",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="full",
        as_json=True,
    )
    assert rc_query == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["results"][0]["source_filepath"] == "/vault/docs/t4x-r3ceipt-ocr-noise.txt"


def test_photo_reindex_skips_timestamp_only_ocr_changes(tmp_path: Path) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_registry_db(registry_db)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="photos",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    vec = connect_vault_db(vector_db)
    try:
        initial_indexed_at = str(
            vec.execute(
                """
                SELECT indexed_at
                FROM source_state_v2
                WHERE source_table='photos_registry' AND source_filepath=? AND index_level='redacted'
                """,
                ("/vault/photos/shipping-label.jpg",),
            ).fetchone()[0]
        )
        metadata_json = str(
            vec.execute(
                """
                SELECT metadata_json
                FROM vector_items_v2
                WHERE source_table='photos_registry' AND source_filepath=? AND index_level='redacted'
                ORDER BY chunk_index
                LIMIT 1
                """,
                ("/vault/photos/shipping-label.jpg",),
            ).fetchone()[0]
        )
    finally:
        vec.close()

    metadata = json.loads(metadata_json)
    assert "ocr_updated_at" not in metadata

    reg = connect_vault_db(registry_db)
    try:
        reg.execute(
            """
            UPDATE photos_registry
            SET ocr_updated_at = ?
            WHERE filepath = ?
            """,
            (
                "2026-03-19T00:00:00+00:00",
                "/vault/photos/shipping-label.jpg",
            ),
        )
        reg.commit()
    finally:
        reg.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="photos",
        rebuild=False,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    vec = connect_vault_db(vector_db)
    try:
        resumed_indexed_at = str(
            vec.execute(
                """
                SELECT indexed_at
                FROM source_state_v2
                WHERE source_table='photos_registry' AND source_filepath=? AND index_level='redacted'
                """,
                ("/vault/photos/shipping-label.jpg",),
            ).fetchone()[0]
        )
        assert resumed_indexed_at == initial_indexed_at
    finally:
        vec.close()


def test_redacted_update_revisits_earlier_sources_when_map_grows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_consistency_registry_db(registry_db)

    def fake_model_detect_candidates(text: str, *, cfg, source: str):
        if "Passport renewal checklist for Jane Doe" in text:
            return [
                vault_redaction.RedactionCandidate(
                    key_name="PERSON",
                    value="Jane Doe",
                    source=source,
                )
            ]
        return []

    monkeypatch.setattr(vault_redaction, "_model_detect_candidates", fake_model_detect_candidates)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="hybrid", enabled=True),
    )
    assert rc == 0

    conn = connect_vault_db(vector_db)
    try:
        previews = [
            str(row[0] or "")
            for row in conn.execute(
                """
                SELECT text_preview_redacted
                FROM vector_items_v2
                WHERE source_filepath = ? AND index_level = 'redacted'
                ORDER BY chunk_index
                """,
                ("/vault/docs/private-letter.txt",),
            ).fetchall()
        ]
        assert previews
        assert all("Jane Doe" not in preview for preview in previews)
        assert any("<REDACTED_PERSON_" in preview for preview in previews)
    finally:
        conn.close()

    rc_query_full_level = query_index(
        registry_db,
        vector_db,
        "tax receipt scanner",
        top_k=5,
        embedding_client=emb,
        source_selection="docs",
        clearance="redacted",
        search_level="full",
        as_json=True,
    )
    assert rc_query_full_level == 2


def test_redacted_update_does_not_auto_rerun_consistency_pass(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_consistency_registry_db(registry_db)

    def fake_model_detect_candidates(text: str, *, cfg, source: str):
        if "Passport renewal checklist for Jane Doe" in text:
            return [
                vault_redaction.RedactionCandidate(
                    key_name="PERSON",
                    value="Jane Doe",
                    source=source,
                )
            ]
        return []

    monkeypatch.setattr(vault_redaction, "_model_detect_candidates", fake_model_detect_candidates)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        verbose=True,
        redaction_cfg=RedactionConfig(mode="hybrid", enabled=True),
    )
    assert rc == 0

    stdout = capsys.readouterr().out
    assert "stage=index-vectors.consistency" not in stdout
    assert "action=rerun-to-reconcile-redactions" not in stdout
    assert "stage=repair-redacted-registry.docs" not in stdout
    assert "stage=repair-redacted-registry.summary" not in stdout


def test_explicit_redacted_consistency_pass_logs_repair_sweep_progress(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _seed_consistency_registry_db(registry_db)

    def fake_model_detect_candidates(text: str, *, cfg, source: str):
        if "Passport renewal checklist for Jane Doe" in text:
            return [
                vault_redaction.RedactionCandidate(
                    key_name="PERSON",
                    value="Jane Doe",
                    source=source,
                )
            ]
        return []

    monkeypatch.setattr(vault_redaction, "_model_detect_candidates", fake_model_detect_candidates)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        verbose=True,
        redaction_cfg=RedactionConfig(mode="hybrid", enabled=True),
        consistency_pass=True,
    )
    assert rc == 0

    stdout = capsys.readouterr().out
    assert "stage=repair-redacted-registry.docs" in stdout
    assert "stage=repair-redacted-registry.summary" in stdout
    assert "[repaired=" in stdout


def test_redacted_resume_skips_unaffected_sources_when_map_grows_elsewhere(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _insert_generated_doc(
        registry_db,
        filepath="/vault/docs/packing-list.txt",
        checksum="doc-pack",
        updated_at="2026-03-17T00:00:00+00:00",
        text_content="General packing list for the mountain trip and camp stove setup.",
        summary_text="Packing list for the mountain trip.",
    )

    def fake_model_detect_candidates(text: str, *, cfg, source: str):
        if "Passport renewal checklist for Jane Doe" in text:
            return [
                vault_redaction.RedactionCandidate(
                    key_name="PERSON",
                    value="Jane Doe",
                    source=source,
                )
            ]
        return []

    monkeypatch.setattr(vault_redaction, "_model_detect_candidates", fake_model_detect_candidates)

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="hybrid", enabled=True),
    )
    assert rc == 0

    vec = connect_vault_db(vector_db)
    try:
        initial_indexed_at = str(
            vec.execute(
                """
                SELECT indexed_at
                FROM source_state_v2
                WHERE source_table='docs_registry' AND source_filepath=? AND index_level='redacted'
                """,
                ("/vault/docs/packing-list.txt",),
            ).fetchone()[0]
        )
    finally:
        vec.close()

    _insert_generated_doc(
        registry_db,
        filepath="/vault/docs/passport-note.txt",
        checksum="doc-passport",
        updated_at="2026-03-17T00:05:00+00:00",
        text_content="Passport renewal checklist for Jane Doe before the consulate appointment.",
        summary_text="Passport renewal checklist for Jane Doe.",
    )

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=False,
        redaction_cfg=RedactionConfig(mode="hybrid", enabled=True),
    )
    assert rc == 0

    vec = connect_vault_db(vector_db)
    try:
        resumed_indexed_at = str(
            vec.execute(
                """
                SELECT indexed_at
                FROM source_state_v2
                WHERE source_table='docs_registry' AND source_filepath=? AND index_level='redacted'
                """,
                ("/vault/docs/packing-list.txt",),
            ).fetchone()[0]
        )
        passport_preview = str(
            vec.execute(
                """
                SELECT text_preview_redacted
                FROM vector_items_v2
                WHERE source_filepath=? AND index_level='redacted'
                ORDER BY chunk_index LIMIT 1
                """,
                ("/vault/docs/passport-note.txt",),
            ).fetchone()[0]
        )
        assert resumed_indexed_at == initial_indexed_at
        assert "Jane Doe" not in passport_preview
        assert "<REDACTED_PERSON_" in passport_preview
    finally:
        vec.close()


def test_redacted_rerun_is_noop_when_regex_final_pass_catches_variant_format(
    tmp_path: Path,
) -> None:
    registry_db = tmp_path / "state" / "vault_registry.db"
    vector_db = tmp_path / "state" / "vault_vectors.db"
    _insert_generated_doc(
        registry_db,
        filepath="/vault/docs/phone-variants.txt",
        checksum="doc-phone-variants",
        updated_at="2026-03-17T00:00:00+00:00",
        text_content=(
            "Call logistics at 617-555-1212 before departure. "
            "The same number also appears as 6175551212 in the scanned footer."
        ),
        summary_text="Travel logistics note with repeated contact number.",
    )

    emb = StubEmbeddingClient(dim=8)
    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=True,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    vec = connect_vault_db(vector_db)
    try:
        initial_indexed_at = str(
            vec.execute(
                """
                SELECT indexed_at
                FROM source_state_v2
                WHERE source_table='docs_registry' AND source_filepath=? AND index_level='redacted'
                """,
                ("/vault/docs/phone-variants.txt",),
            ).fetchone()[0]
        )
        previews = [
            str(row[0] or "")
            for row in vec.execute(
                """
                SELECT text_preview_redacted
                FROM vector_items_v2
                WHERE source_filepath=? AND index_level='redacted'
                ORDER BY chunk_index
                """,
                ("/vault/docs/phone-variants.txt",),
            ).fetchall()
        ]
        assert any("<REDACTED_PHONE_A>" in preview for preview in previews)
        assert any("<REDACTED_PHONE>" in preview for preview in previews)
    finally:
        vec.close()

    rc = update_index(
        registry_db,
        vector_db,
        embedding_client=emb,
        source_selection="docs",
        rebuild=False,
        redaction_cfg=RedactionConfig(mode="regex", enabled=True),
    )
    assert rc == 0

    vec = connect_vault_db(vector_db)
    try:
        resumed_indexed_at = str(
            vec.execute(
                """
                SELECT indexed_at
                FROM source_state_v2
                WHERE source_table='docs_registry' AND source_filepath=? AND index_level='redacted'
                """,
                ("/vault/docs/phone-variants.txt",),
            ).fetchone()[0]
        )
        assert resumed_indexed_at == initial_indexed_at
    finally:
        vec.close()
