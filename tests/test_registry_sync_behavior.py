from __future__ import annotations

from pathlib import Path

from vault_db import connect_vault_db
from vault_redaction import PersistentRedactionMap
from vault_registry_sync import (
    Config,
    PdfParseConfig,
    PhotoAnalysisConfig,
    PhotoAnalysisResult,
    SummaryConfig,
    SummaryResult,
    backfill_missing_photo_analysis,
    count_pending_summary_backfill,
    default_docs_dest_root,
    default_docs_roots,
    default_inbox_scanner,
    default_photos_dest_root,
    default_photos_roots,
    ensure_db,
    index_photo_file,
    run,
)
from vault_service_defaults import (
    DEFAULT_LOCAL_MODEL_BASE_URL,
    DEFAULT_LOCAL_PDF_PARSE_URL,
    DEFAULT_LOCAL_PHOTO_ANALYSIS_URL,
)
from vault_vector_index import (
    ensure_redaction_table,
    fetch_redaction_entries,
    seed_redaction_map_key_counts,
)


class _DummyPhotoClient:
    def __init__(self, result: PhotoAnalysisResult | None = None) -> None:
        self.cfg = PhotoAnalysisConfig(
            enabled=True,
            analyze_url=DEFAULT_LOCAL_PHOTO_ANALYSIS_URL,
            timeout_seconds=30,
            force=False,
        )
        self._result = result or PhotoAnalysisResult(
            status="ok",
            route_kind="doc",
            taxonomy="docs",
            caption="shipping label on package",
            category_primary="document",
            category_secondary="label",
            analyzer_model="test-photo-model",
            analyzer_error="",
            analyzer_raw='{"caption":"shipping label on package","text_raw":"tracking 1234"}',
            ocr_text="tracking 1234",
        )

    def analyze(self, _path: Path) -> PhotoAnalysisResult:
        return self._result


def _summary_cfg(enabled: bool = True) -> SummaryConfig:
    return SummaryConfig(
        enabled=enabled,
        base_url=DEFAULT_LOCAL_MODEL_BASE_URL,
        model="qwen3-14b",
        api_key="local",
        timeout_seconds=30,
        max_input_chars=12000,
        max_output_chars=650,
    )


def _base_cfg(db_path: Path, *, summary_enabled: bool = True) -> Config:
    root = db_path.parent
    return Config(
        db_path=db_path,
        docs_roots=[],
        photos_roots=[],
        inbox_scanner=root / "scanner",
        docs_dest_root=root / "docs",
        photos_dest_root=root / "photos",
        text_cap=40000,
        max_seconds=0.0,
        max_items=0,
        skip_inbox=True,
        verbose=False,
        summary=_summary_cfg(enabled=summary_enabled),
        photo_analysis=PhotoAnalysisConfig(
            enabled=False,
            analyze_url=DEFAULT_LOCAL_PHOTO_ANALYSIS_URL,
            timeout_seconds=30,
            force=False,
        ),
        pdf_parse=PdfParseConfig(
            enabled=False,
            parse_url=DEFAULT_LOCAL_PDF_PARSE_URL,
            timeout_seconds=60,
            profile="auto",
        ),
        summary_reprocess_missing_limit=-1,
        photo_reprocess_missing_limit=0,
    )


def test_run_executes_summary_backfill_when_limit_is_negative(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        conn.execute(
            """
            INSERT INTO docs_registry (
              checksum, filepath, source, size, mtime, indexed_at, updated_at,
              text_content, text_chars_total, text_capped, parser, ocr_used, extraction_method,
              summary_text, summary_model, summary_hash, summary_status, summary_updated_at, summary_error,
              dates_json, primary_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "abc",
                str(tmp_path / "doc-a.txt"),
                "tests/docs",
                12,
                1000.0,
                "2026-03-22T00:00:00+00:00",
                "2026-03-22T00:00:00+00:00",
                "sample body",
                11,
                0,
                "plain",
                0,
                "plain",
                "",
                "",
                "",
                "",
                "",
                "",
                "[]",
                "",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    called: dict[str, int] = {"count": 0}

    monkeypatch.setattr("vault_registry_sync.LocalOpenAIChatClient", lambda cfg: object())

    def fake_backfill_missing_summaries(conn, *, summary_cfg, chat_client, limit, deadline, budget=None, verbose=False):
        called["count"] += 1
        assert limit == -1
        assert budget is None
        return (1, 0)

    monkeypatch.setattr("vault_registry_sync.backfill_missing_summaries", fake_backfill_missing_summaries)

    rc = run(_base_cfg(db_path), dry_run=False)
    assert rc == 0
    assert called["count"] == 1
    reopened = connect_vault_db(db_path)
    try:
        assert count_pending_summary_backfill(reopened, -1) == 1
    finally:
        reopened.close()


def test_run_marks_bounded_when_max_items_stops_summary_backfill(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        for name in ("doc-a.txt", "doc-b.txt"):
            conn.execute(
                """
                INSERT INTO docs_registry (
                  checksum, filepath, source, size, mtime, indexed_at, updated_at,
                  text_content, text_chars_total, text_capped, parser, ocr_used, extraction_method,
                  summary_text, summary_model, summary_hash, summary_status, summary_updated_at, summary_error,
                  dates_json, primary_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    str(tmp_path / name),
                    "tests/docs",
                    12,
                    1000.0,
                    "2026-03-22T00:00:00+00:00",
                    "2026-03-22T00:00:00+00:00",
                    f"body for {name}",
                    11,
                    0,
                    "plain",
                    0,
                    "plain",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "[]",
                    "",
                ),
            )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr("vault_registry_sync.LocalOpenAIChatClient", lambda cfg: object())

    def fake_summarize_doc_text(_chat_client, _summary_cfg, text_seed, _filepath):
        return SummaryResult(text=f"summary:{text_seed}", status="ok", error="")

    monkeypatch.setattr("vault_registry_sync.summarize_doc_text", fake_summarize_doc_text)

    cfg = _base_cfg(db_path)
    cfg.max_items = 1
    rc = run(cfg, dry_run=False)
    assert rc == 0

    reopened = connect_vault_db(db_path)
    try:
        statuses = reopened.execute(
            "SELECT summary_status FROM docs_registry ORDER BY filepath"
        ).fetchall()
        assert [row[0] for row in statuses].count("ok") == 1
        last_run = reopened.execute(
            "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert last_run is not None
        assert last_run[0] == "bounded"
    finally:
        reopened.close()


def test_index_photo_file_force_analyze_reports_nochange_when_snapshot_matches(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        photo_path = tmp_path / "photo.jpg"
        photo_path.write_bytes(b"fake-jpeg-bytes")
        client = _DummyPhotoClient()

        changed_first = index_photo_file(
            conn,
            photo_path,
            "tests/photos",
            dry_run=False,
            photo_client=client,
            force_analyze=True,
        )
        conn.commit()
        changed_second = index_photo_file(
            conn,
            photo_path,
            "tests/photos",
            dry_run=False,
            photo_client=client,
            force_analyze=True,
        )
        assert changed_first is True
        assert changed_second is False
        row = conn.execute(
            """
            SELECT ocr_text, ocr_status, ocr_source
            FROM photos_registry
            WHERE filepath = ?
            """,
            (str(photo_path),),
        ).fetchone()
        assert row is not None
        assert row[0] == "tracking 1234"
        assert row[1] == "ok"
        assert row[2] == "analyzer:text_raw"
    finally:
        conn.close()


def test_index_photo_file_persists_document_ocr_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        photo_path = tmp_path / "receipt.jpg"
        photo_path.write_bytes(b"fake-jpeg-bytes")
        client = _DummyPhotoClient()

        changed = index_photo_file(
            conn,
            photo_path,
            "tests/photos",
            dry_run=False,
            photo_client=client,
            force_analyze=True,
        )

        assert changed is True
        row = conn.execute(
            """
            SELECT taxonomy, category_primary, ocr_text, ocr_status, ocr_source, ocr_updated_at
            FROM photos_registry
            WHERE filepath = ?
            """,
            (str(photo_path),),
        ).fetchone()
        assert row is not None
        assert row[0] == "docs"
        assert row[1] == "document"
        assert row[2] == "tracking 1234"
        assert row[3] == "ok"
        assert row[4] == "analyzer:text_raw"
        assert row[5]
    finally:
        conn.close()


def test_portable_default_paths_use_repo_state_when_vault_root_env_is_unset(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("LLM_VAULT_CONTENT_ROOT", raising=False)
    monkeypatch.setattr("vault_registry_sync.ROOT", tmp_path)

    assert default_docs_roots() == []
    assert default_photos_roots() == []
    assert default_inbox_scanner() == str(tmp_path / "state" / "scanner_inbox")
    assert default_docs_dest_root() == str(tmp_path / "state" / "documents_scanner_inbox")
    assert default_photos_dest_root() == str(tmp_path / "state" / "photos_scanner_inbox")


def test_portable_default_paths_can_be_derived_from_vault_root_env(monkeypatch, tmp_path: Path) -> None:
    content_root = tmp_path / "vault"
    monkeypatch.setenv("LLM_VAULT_CONTENT_ROOT", str(content_root))

    assert default_docs_roots() == [
        str(content_root / "raw" / "documents"),
        str(content_root / "raw" / "testing" / "docs"),
    ]
    assert default_photos_roots() == [str(content_root / "raw" / "photos")]
    assert default_inbox_scanner() == str(content_root / "inbox" / "scanner_in")
    assert default_docs_dest_root() == str(content_root / "raw" / "documents" / "scanner_inbox")
    assert default_photos_dest_root() == str(content_root / "raw" / "photos" / "scanner_inbox")


def test_index_photo_file_marks_document_ocr_empty_when_text_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        photo_path = tmp_path / "document.jpg"
        photo_path.write_bytes(b"fake-jpeg-bytes")
        client = _DummyPhotoClient(
            PhotoAnalysisResult(
                status="ok",
                route_kind="doc",
                taxonomy="docs",
                caption="receipt on table",
                category_primary="receipt",
                category_secondary="paper",
                analyzer_model="test-photo-model",
                analyzer_error="",
                analyzer_raw='{"caption":"receipt on table","text_raw":null}',
                ocr_text="",
            )
        )

        changed = index_photo_file(
            conn,
            photo_path,
            "tests/photos",
            dry_run=False,
            photo_client=client,
            force_analyze=True,
        )

        assert changed is True
        row = conn.execute(
            """
            SELECT ocr_text, ocr_status, ocr_source, ocr_updated_at
            FROM photos_registry
            WHERE filepath = ?
            """,
            (str(photo_path),),
        ).fetchone()
        assert row is not None
        assert row[0] == ""
        assert row[1] == "empty"
        assert row[2] == ""
        assert row[3]
    finally:
        conn.close()


def test_index_photo_file_ignores_ocr_for_non_document_photos(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        photo_path = tmp_path / "portrait.jpg"
        photo_path.write_bytes(b"fake-jpeg-bytes")
        client = _DummyPhotoClient(
            PhotoAnalysisResult(
                status="ok",
                route_kind="photo",
                taxonomy="personal",
                caption="portrait at sunset",
                category_primary="portrait",
                category_secondary="selfie",
                analyzer_model="test-photo-model",
                analyzer_error="",
                analyzer_raw='{"caption":"portrait at sunset","text_raw":"secret badge 991"}',
                ocr_text="secret badge 991",
            )
        )

        changed = index_photo_file(
            conn,
            photo_path,
            "tests/photos",
            dry_run=False,
            photo_client=client,
            force_analyze=True,
        )

        assert changed is True
        row = conn.execute(
            """
            SELECT taxonomy, category_primary, ocr_text, ocr_status, ocr_source, ocr_updated_at
            FROM photos_registry
            WHERE filepath = ?
            """,
            (str(photo_path),),
        ).fetchone()
        assert row is not None
        assert row[0] == "personal"
        assert row[1] == "portrait"
        assert row[2] == ""
        assert row[3] == "not_applicable"
        assert row[4] == ""
        assert row[5]
    finally:
        conn.close()


def test_backfill_missing_photo_analysis_retries_document_like_empty_ocr(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_db(conn)
        photo_path = tmp_path / "label.jpg"
        photo_path.write_bytes(b"fake-jpeg-bytes")

        empty_client = _DummyPhotoClient(
            PhotoAnalysisResult(
                status="ok",
                route_kind="doc",
                taxonomy="docs",
                caption="shipping label on package",
                category_primary="document",
                category_secondary="label",
                analyzer_model="test-photo-model",
                analyzer_error="",
                analyzer_raw='{"caption":"shipping label on package","text_raw":null}',
                ocr_text="",
            )
        )
        good_client = _DummyPhotoClient()

        changed = index_photo_file(
            conn,
            photo_path,
            "tests/photos",
            dry_run=False,
            photo_client=empty_client,
            force_analyze=True,
        )
        assert changed is True
        conn.commit()

        updated, failed = backfill_missing_photo_analysis(
            conn,
            photo_client=good_client,
            limit=-1,
            deadline=float("inf"),
            verbose=False,
        )
        conn.commit()

        assert updated == 1
        assert failed == 0
        row = conn.execute(
            """
            SELECT ocr_text, ocr_status, ocr_source
            FROM photos_registry
            WHERE filepath = ?
            """,
            (str(photo_path),),
        ).fetchone()
        assert row is not None
        assert row[0] == "tracking 1234"
        assert row[1] == "ok"
        assert row[2] == "analyzer:text_raw"
    finally:
        conn.close()


def test_seed_redaction_map_key_counts_avoids_reusing_rejected_placeholders(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "vault_registry.db"
    conn = connect_vault_db(db_path, ensure_parent=True)
    try:
        ensure_redaction_table(conn)
        conn.execute(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm,
              original_value, source_mode, policy_version, status, validator_name,
              detector_sources, modality, source_field, first_seen_at, last_seen_at, hit_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "vault",
                "global",
                "PERSON",
                "<REDACTED_PERSON_A>",
                "bad placeholder seed",
                "LAST NAME",
                "model",
                "2026-03-22-precision-2",
                "rejected",
                "entity-validator-v1",
                "model",
                "text",
                "content",
                "2026-03-22T00:00:00+00:00",
                "2026-03-22T00:00:00+00:00",
                1,
            ),
        )
        conn.commit()

        rows = fetch_redaction_entries(conn, scope_type="vault", scope_id="global")
        table = PersistentRedactionMap.from_rows(rows)
        seed_redaction_map_key_counts(conn, scope_type="vault", scope_id="global", redaction_map=table)
        placeholder, _norm, is_new = table.register("PERSON", "Jane Doe")
        assert is_new is True
        assert placeholder == "<REDACTED_PERSON_B>"
    finally:
        conn.close()
