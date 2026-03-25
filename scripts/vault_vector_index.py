#!/usr/bin/env python3
"""Local-only vector index maintenance + query for vault-ops.

Reads docs_registry/photos_registry from vault_registry.db and maintains a
local vector store in vault_vectors.db.
"""

from __future__ import annotations

import argparse
import array
import hashlib
import heapq
import json
import math
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from vault_db import connect_vault_db
from vault_redaction import (
    DEFAULT_REDACTION_BASE_URL,
    DEFAULT_REDACTION_MODEL,
    DEFAULT_REDACTION_TIMEOUT_SECONDS,
    REDACTION_POLICY_VERSION,
    PersistentRedactionMap,
    RedactionConfig,
    is_redaction_value_allowed,
    redact_chunks_with_persistent_map,
    render_redacted_text,
)
from vault_service_defaults import DEFAULT_LOCAL_MODEL_BASE_URL
from vault_sources import (
    REGISTERED_SOURCES,
    SourceHandler,
    select_active_source_handlers,
    source_choices,
    source_handler_by_kind,
    source_kind_for_table,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_DB = str(ROOT / "state" / "vault_registry.db")
DEFAULT_VECTOR_DB = str(ROOT / "state" / "vault_vectors.db")
DEFAULT_EMBED_BASE_URL = DEFAULT_LOCAL_MODEL_BASE_URL
DEFAULT_EMBED_MODEL = "Qwen3-Embedding-8B"
DEFAULT_EMBED_TIMEOUT_SECONDS = 60
DEFAULT_EMBED_BATCH_SIZE = 16
DEFAULT_EMBED_BATCH_TOKENS = 3000
DEFAULT_EMBED_MAX_TEXT_CHARS = 3000
DEFAULT_MAIL_MAX_BODY_CHUNKS = 12
PROGRESS_HEARTBEAT_SECONDS = 5.0
INDEX_LEVEL_REDACTED = 'redacted'
INDEX_LEVEL_FULL = 'full'
INDEX_LEVEL_AUTO = 'auto'


@dataclass
class EmbeddingConfig:
    base_url: str
    model: str
    api_key: str
    timeout_seconds: int
    batch_size: int
    batch_tokens: int
    max_text_chars: int
    verbose: bool = False


@dataclass
class PreparedEmbeddingText:
    original_index: int
    text: str


class RetryableEmbeddingSizeError(RuntimeError):
    def __init__(self, *, status_code: int, approx_tokens: int, batch_items: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.approx_tokens = approx_tokens
        self.batch_items = batch_items
        self.message = message


@dataclass
class Item:
    source_table: str
    source_filepath: str
    source_checksum: str
    source_updated_at: str
    chunk_index: int
    chunk_count: int
    text: str
    text_redacted: str
    metadata: dict


@dataclass
class UpdateStats:
    processed_sources: int = 0
    skipped_sources: int = 0
    indexed_sources: int = 0
    deleted_sources: int = 0
    upserted_items: int = 0
    deleted_items: int = 0
    items_redacted: int = 0
    redaction_entries_added: int = 0
    redaction_entries_total: int = 0
    index_level: str = INDEX_LEVEL_REDACTED
    source_stats: dict[str, "SourceUpdateStats"] = field(default_factory=dict)


@dataclass
class SourceUpdateStats:
    processed: int = 0
    indexed: int = 0
    skipped: int = 0
    waiting: int = 0
    checksum_reused: int = 0


@dataclass
class SearchDiagnostics:
    requested_level: str
    used_level: str
    fallback_from_level: str | None = None
    full_level_available: bool = False


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _format_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "unknown"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _estimate_eta(elapsed_seconds: float, completed: int, total: int) -> float | None:
    if completed <= 0 or total <= 0 or completed >= total:
        return None
    return (elapsed_seconds / completed) * max(0, total - completed)


def _should_emit_progress(
    *,
    verbose: bool,
    now_mono: float,
    last_emit_mono: float,
    completed: int,
    total: int,
    force: bool = False,
) -> bool:
    if force or verbose:
        return True
    if completed <= 1:
        return True
    if total > 0 and completed >= total:
        return True
    if completed % 25 == 0:
        return True
    return (now_mono - last_emit_mono) >= PROGRESS_HEARTBEAT_SECONDS


def _emit_vector_progress(
    *,
    stage: str,
    stage_done: int,
    stage_total: int,
    overall_done: int,
    overall_total: int,
    action: str,
    started_mono: float,
    last_emit_mono: float,
    verbose: bool,
    stats: UpdateStats,
    force: bool = False,
) -> float:
    now_mono = time.monotonic()
    if not _should_emit_progress(
        verbose=verbose,
        now_mono=now_mono,
        last_emit_mono=last_emit_mono,
        completed=stage_done,
        total=stage_total,
        force=force,
    ):
        return last_emit_mono

    elapsed = max(0.0, now_mono - started_mono)
    eta = _estimate_eta(elapsed, overall_done, overall_total)
    stage_total_text = str(max(stage_total, 0))
    overall_total_text = str(max(overall_total, 0))
    print(
        "[progress] "
        f"[stage={stage}] "
        f"[item={stage_done}/{stage_total_text}] "
        f"[overall={overall_done}/{overall_total_text}] "
        f"[action={action}] "
        f"[elapsed={elapsed:.1f}s] "
        f"[eta={_format_eta(eta)}] "
        f"[indexed={stats.indexed_sources}] "
        f"[skipped={stats.skipped_sources}] "
        f"[reused={sum(source.checksum_reused for source in stats.source_stats.values())}] "
        f"[redacted={stats.items_redacted}]",
        flush=True,
    )
    return now_mono


def _source_update_stats(stats: UpdateStats, handler: SourceHandler) -> SourceUpdateStats:
    return stats.source_stats.setdefault(handler.kind, SourceUpdateStats())


def _source_stats_payload(
    stats: UpdateStats,
    handlers: Iterable[SourceHandler],
) -> dict[str, dict[str, int]]:
    payload: dict[str, dict[str, int]] = {}
    for handler in handlers:
        source_stats = stats.source_stats.get(handler.kind, SourceUpdateStats())
        payload[handler.kind] = {
            "processed": source_stats.processed,
            "indexed": source_stats.indexed,
            "skipped": source_stats.skipped,
            "waiting": source_stats.waiting,
            "checksum_reused": source_stats.checksum_reused,
        }
    return payload


def normalize_vector(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in values))
    if norm > 0:
        return [v / norm for v in values]
    return values


def blob_to_floats(blob: bytes) -> array.array:
    arr = array.array("f")
    arr.frombytes(blob)
    return arr


def floats_to_blob(values: list[float]) -> bytes:
    return array.array("f", values).tobytes()


def dot(a: array.array, b: array.array) -> float:
    return float(sum(x * y for x, y in zip(a, b)))


def chunk_text(
    text: str,
    *,
    max_words: int = 180,
    overlap_words: int = 45,
    max_chars: int = DEFAULT_EMBED_MAX_TEXT_CHARS,
) -> list[str]:
    clean = " ".join(str(text or "").split()).strip()
    if not clean:
        return []
    char_limit = max(256, int(max_chars))
    words = clean.split()
    if len(clean) <= char_limit and len(words) <= max_words:
        return [clean]
    if any(len(word) > char_limit for word in words):
        overlap_chars = max(64, char_limit // 4)
        chunks: list[str] = []
        step = max(1, char_limit - overlap_chars)
        start = 0
        while start < len(clean):
            piece = clean[start : start + char_limit].strip()
            if piece:
                chunks.append(piece)
            if start + char_limit >= len(clean):
                break
            start += step
        return chunks

    chunks: list[str] = []
    i = 0
    while i < len(words):
        part_words: list[str] = []
        part_chars = 0
        j = i
        while j < len(words) and len(part_words) < max_words:
            word = words[j]
            extra_chars = len(word) if not part_words else len(word) + 1
            if part_words and (part_chars + extra_chars) > char_limit:
                break
            part_words.append(word)
            part_chars += extra_chars
            j += 1
        if not part_words:
            break
        chunks.append(" ".join(part_words))
        if j >= len(words):
            break
        consumed = len(part_words)
        step = max(1, consumed - overlap_words)
        i += step
    return chunks


def item_id(source_table: str, source_filepath: str, chunk_index: int) -> str:
    fp_hash = hashlib.sha1(source_filepath.encode("utf-8", errors="ignore")).hexdigest()
    return f"{source_table}:{fp_hash}:{chunk_index}"


def state_hash_doc(
    row: sqlite3.Row,
    *,
    redaction_mode: str,
    redaction_output_signature: str = "",
) -> str:
    text = row["text_content"] or ""
    summary = row["summary_text"] or ""
    payload = {
        "checksum": row["checksum"] or "",
        "mtime": row["mtime"] or 0,
        "size": row["size"] or 0,
        "updated_at": row["updated_at"] or "",
        "summary_status": row["summary_status"] or "",
        "summary_hash": row["summary_hash"] or "",
        "dates_json": row["dates_json"] or "",
        "primary_date": row["primary_date"] or "",
        "redaction_mode": redaction_mode,
        "redaction_policy_version": REDACTION_POLICY_VERSION,
        "redaction_output_signature": redaction_output_signature,
        "text_hash": hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest(),
        "summary_text_hash": hashlib.sha256(summary.encode("utf-8", errors="ignore")).hexdigest(),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def state_hash_photo(
    row: sqlite3.Row,
    *,
    redaction_mode: str,
    redaction_output_signature: str = "",
) -> str:
    payload = {
        "checksum": row["checksum"] or "",
        "mtime": row["mtime"] or 0,
        "size": row["size"] or 0,
        "updated_at": row["updated_at"] or "",
        "notes": row["notes"] or "",
        "date_taken": row["date_taken"] or "",
        "caption": row["caption"] or "",
        "category_primary": row["category_primary"] or "",
        "category_secondary": row["category_secondary"] or "",
        "taxonomy": row["taxonomy"] or "",
        "analyzer_status": row["analyzer_status"] or "",
        "ocr_text": row["ocr_text"] or "",
        "ocr_status": row["ocr_status"] or "",
        "ocr_source": row["ocr_source"] or "",
        "dates_json": row["dates_json"] or "",
        "primary_date": row["primary_date"] or "",
        "redaction_mode": redaction_mode,
        "redaction_policy_version": REDACTION_POLICY_VERSION,
        "redaction_output_signature": redaction_output_signature,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def state_hash_mail(
    row: sqlite3.Row,
    *,
    redaction_mode: str,
    redaction_output_signature: str = "",
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
) -> str:
    payload = {
        "checksum": row["checksum"] or "",
        "updated_at": row["updated_at"] or "",
        "primary_date": row["primary_date"] or "",
        "dates_json": row["dates_json"] or "",
        "mail_max_body_chunks": max(0, int(mail_max_body_chunks)),
        "redaction_mode": redaction_mode,
        "redaction_policy_version": REDACTION_POLICY_VERSION,
        "redaction_output_signature": redaction_output_signature,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def redacted_output_signature_for_items(
    items: Iterable[Item],
    *,
    redaction_mode: str = "hybrid",
    table: PersistentRedactionMap | None = None,
) -> str:
    payload: list[tuple[int, int, str]] = []
    for item in items:
        text_value = (
            render_redacted_text(item.text, mode=redaction_mode, table=table)
            if table is not None
            else item.text_redacted
        )
        payload.append(
            (
                int(item.chunk_index),
                int(item.chunk_count),
                hashlib.sha256(str(text_value or "").encode("utf-8", errors="ignore")).hexdigest(),
            )
        )
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _parse_dates_json(raw: str | None) -> list[dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _stable_source_id(source_table: str, source_filepath: str) -> str:
    seed = f"{source_table}:{source_filepath}"
    return hashlib.sha1(seed.encode("utf-8", errors="ignore")).hexdigest()


def _sanitize_dates_for_output(dates: Any) -> list[dict[str, Any]]:
    if not isinstance(dates, list):
        return []
    sanitized: list[dict[str, Any]] = []
    for entry in dates:
        if not isinstance(entry, dict):
            continue
        cleaned: dict[str, Any] = {}
        for key in ("value", "kind", "source", "confidence"):
            if key in entry:
                cleaned[key] = entry[key]
        if cleaned:
            sanitized.append(cleaned)
    return sanitized


def _sanitize_metadata_for_output(
    *,
    metadata: dict[str, Any],
    clearance: str,
    source_table: str,
    source_filepath: str,
) -> dict[str, Any]:
    metadata_out = dict(metadata)
    metadata_out.pop("filepath", None)
    metadata_out["source_id"] = _stable_source_id(source_table, source_filepath)
    if "dates" in metadata_out:
        metadata_out["dates"] = _sanitize_dates_for_output(metadata_out.get("dates"))
    if clearance == "full":
        return metadata_out
    redacted_hidden_fields = {"notes", "caption", "ocr_text"}
    if source_table == "mail_registry":
        redacted_hidden_fields.update(
            {
                "msg_id",
                "thread_id",
                "account_email",
                "from_addr",
                "to_addr",
                "subject",
                "snippet",
                "summary_text",
                "labels",
            }
        )
    for hidden_field in redacted_hidden_fields:
        metadata_out.pop(hidden_field, None)
    return metadata_out


def _display_source_label(*, item: dict[str, Any], clearance: str) -> str:
    if clearance == "full":
        return f"file={item['source_filepath']}"
    return f"source_id={item['source_id']}"


def build_doc_items(row: sqlite3.Row) -> list[Item]:
    filepath = row["filepath"]
    summary_text = (row["summary_text"] or "").strip()
    summary_status = (row["summary_status"] or "").strip()
    body_text = (row["text_content"] or "").strip()
    primary_date = str(row["primary_date"] or "").strip()
    dates = _parse_dates_json(row["dates_json"])
    if not summary_text:
        return []

    source = row["source"] or ""
    parser = row["parser"] or ""
    items: list[Item] = []
    items.append(
        Item(
            source_table="docs_registry",
            source_filepath=filepath,
            source_checksum=row["checksum"] or "",
            source_updated_at=row["updated_at"] or "",
            chunk_index=0,
            chunk_count=0,
            text=summary_text,
            text_redacted=summary_text,
            metadata={
                "kind": "doc",
                "filepath": filepath,
                "source": source,
                "parser": parser,
                "chunk_index": 0,
                "chunk_count": 0,
                "index_text_kind": "summary",
                "summary_status": summary_status,
                "summary_model": row["summary_model"] or "",
                "primary_date": primary_date,
                "dates": dates,
            },
        )
    )
    body_chunks = chunk_text(body_text) if body_text else []
    for body_idx, chunk in enumerate(body_chunks, start=1):
        items.append(
            Item(
                source_table="docs_registry",
                source_filepath=filepath,
                source_checksum=row["checksum"] or "",
                source_updated_at=row["updated_at"] or "",
                chunk_index=body_idx,
                chunk_count=0,
                text=chunk,
                text_redacted=chunk,
                metadata={
                    "kind": "doc",
                    "filepath": filepath,
                    "source": source,
                    "parser": parser,
                    "chunk_index": body_idx,
                    "chunk_count": 0,
                    "index_text_kind": "body",
                    "summary_status": summary_status,
                    "summary_model": row["summary_model"] or "",
                    "primary_date": primary_date,
                    "dates": dates,
                },
            )
        )
    total = len(items)
    for idx, item in enumerate(items):
        item.chunk_index = idx
        item.chunk_count = total
        item.metadata["chunk_index"] = idx
        item.metadata["chunk_count"] = total
    return items


def build_photo_items(row: sqlite3.Row) -> list[Item]:
    filepath = row["filepath"]
    notes = (row["notes"] or "").strip()
    caption = (row["caption"] or "").strip()
    category_primary = (row["category_primary"] or "").strip()
    category_secondary = (row["category_secondary"] or "").strip()
    taxonomy = (row["taxonomy"] or "").strip()
    analyzer_status = (row["analyzer_status"] or "").strip()
    ocr_text = (row["ocr_text"] or "").strip()
    ocr_status = (row["ocr_status"] or "").strip()
    ocr_source = (row["ocr_source"] or "").strip()
    date_taken = row["date_taken"] or ""
    primary_date = str(row["primary_date"] or "").strip()
    dates = _parse_dates_json(row["dates_json"])
    source = row["source"] or ""

    base_metadata = {
        "kind": "photo",
        "filepath": filepath,
        "source": source,
        "date_taken": date_taken,
        "notes": notes,
        "caption": caption,
        "category_primary": category_primary,
        "category_secondary": category_secondary,
        "taxonomy": taxonomy,
        "analyzer_status": analyzer_status,
        "ocr_status": ocr_status,
        "ocr_source": ocr_source,
        "primary_date": primary_date,
        "dates": dates,
    }

    channel_specs: list[tuple[str, str]] = []
    metadata_parts: list[str] = []
    if category_primary:
        metadata_parts.append(f"category {category_primary}")
    if category_secondary:
        metadata_parts.append(f"subcategory {category_secondary}")
    if taxonomy:
        metadata_parts.append(f"taxonomy {taxonomy}")
    if date_taken:
        metadata_parts.append(f"taken {date_taken}")
    if metadata_parts:
        channel_specs.append(("metadata", "photo metadata " + ". ".join(metadata_parts)))
    if caption:
        channel_specs.append(("caption", f"photo caption {caption}"))
    if notes:
        channel_specs.append(("notes", f"photo notes {notes}"))
    if ocr_status == "ok" and ocr_text:
        channel_specs.append(("ocr", ocr_text))
    if not channel_specs:
        return []

    items: list[Item] = []
    total = len(channel_specs)
    for idx, (channel, text_value) in enumerate(channel_specs):
        metadata = dict(base_metadata)
        metadata["photo_channel"] = channel
        items.append(
            Item(
                source_table="photos_registry",
                source_filepath=filepath,
                source_checksum=row["checksum"] or "",
                source_updated_at=row["updated_at"] or "",
                chunk_index=idx,
                chunk_count=total,
                text=text_value,
                text_redacted=text_value,
                metadata=metadata,
            )
        )
    return items


def build_mail_items(
    row: sqlite3.Row,
    *,
    max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
) -> list[Item]:
    filepath = row["filepath"]
    body_chunk_cap = max(0, int(max_body_chunks))
    account_email = str(row["account_email"] or "").strip()
    thread_id = str(row["thread_id"] or "").strip()
    date_iso = str(row["date_iso"] or "").strip()
    from_addr = str(row["from_addr"] or "").strip()
    to_addr = str(row["to_addr"] or "").strip()
    subject = str(row["subject"] or "").strip()
    snippet = str(row["snippet"] or "").strip()
    body_text = str(row["body_text"] or "").strip()
    summary_text = str(row["summary_text"] or "").strip()
    primary_date = str(row["primary_date"] or "").strip()
    dates = _parse_dates_json(row["dates_json"])
    source = str(row["source"] or "").strip()
    labels: list[str] = []
    try:
        parsed_labels = json.loads(row["labels_json"] or "[]")
        if isinstance(parsed_labels, list):
            labels = [str(item).strip() for item in parsed_labels if str(item).strip()]
    except json.JSONDecodeError:
        labels = []

    body_chunks = chunk_text(body_text) if body_text else []
    total_body_chunks = len(body_chunks)
    indexed_body_chunks = body_chunks[:body_chunk_cap]
    body_truncated = total_body_chunks > len(indexed_body_chunks)

    base_metadata = {
        "kind": "mail",
        "filepath": filepath,
        "source": source,
        "msg_id": str(row["msg_id"] or "").strip(),
        "account_email": account_email,
        "thread_id": thread_id,
        "from_addr": from_addr,
        "to_addr": to_addr,
        "subject": subject,
        "snippet": snippet,
        "summary_text": summary_text,
        "labels": labels,
        "date_iso": date_iso,
        "primary_date": primary_date,
        "dates": dates,
        "mail_body_truncated": body_truncated,
        "mail_body_chunks_total": total_body_chunks,
        "mail_body_chunks_indexed": len(indexed_body_chunks),
    }

    channel_specs: list[tuple[str, str]] = []
    metadata_parts: list[str] = []
    if account_email:
        metadata_parts.append(f"account {account_email}")
    if date_iso:
        metadata_parts.append(f"date {date_iso}")
    if labels:
        metadata_parts.append("labels " + ", ".join(labels))
    if metadata_parts:
        channel_specs.append(("metadata", "mail metadata " + ". ".join(metadata_parts)))
    subject_snippet_parts = [part for part in (f"Subject: {subject}" if subject else "", f"Snippet: {snippet}" if snippet else "") if part]
    if subject_snippet_parts:
        channel_specs.append(("subject_snippet", "\n".join(subject_snippet_parts)))
    for chunk in indexed_body_chunks:
        channel_specs.append(("body", f"Body: {chunk}"))
    if summary_text:
        channel_specs.append(("summary", f"Summary: {summary_text}"))
    if not channel_specs:
        return []

    items: list[Item] = []
    total = len(channel_specs)
    for idx, (channel, text_value) in enumerate(channel_specs):
        metadata = dict(base_metadata)
        metadata["mail_channel"] = channel
        items.append(
            Item(
                source_table="mail_registry",
                source_filepath=filepath,
                source_checksum=row["checksum"] or "",
                source_updated_at=row["updated_at"] or "",
                chunk_index=idx,
                chunk_count=total,
                text=text_value,
                text_redacted=text_value,
                metadata=metadata,
            )
        )
    return items


def _doc_vector_ready(row: sqlite3.Row) -> bool:
    return bool(str(row["summary_text"] or "").strip())


def _doc_summary_backfill_pending(row: sqlite3.Row | dict[str, Any]) -> bool:
    summary_status = str(row["summary_status"] or "").strip().lower()
    summary_text = str(row["summary_text"] or "").strip()
    if summary_status in {"error", "disabled", "stale", "fallback-text"}:
        return True
    return not summary_text and not summary_status


def _photo_vector_ready(row: sqlite3.Row) -> bool:
    return any(
        str(row[key] or "").strip()
        for key in (
            "notes",
            "caption",
            "category_primary",
            "category_secondary",
            "taxonomy",
            "ocr_text",
        )
    )


def _mail_vector_ready(row: sqlite3.Row) -> bool:
    return any(
        str(row[key] or "").strip()
        for key in (
            "msg_id",
            "account_email",
            "subject",
            "snippet",
            "body_text",
            "summary_text",
            "date_iso",
        )
    )


def _flush_vector_skip_batch(
    *,
    batch_count: int,
    batch_reason: str | None,
    batch_stage_done: int,
    batch_overall_done: int,
    stage: str,
    stage_total: int,
    overall_total: int,
    started_mono: float,
    last_emit_mono: float,
    verbose: bool,
    stats: UpdateStats,
) -> tuple[float, int, str | None, int, int]:
    if batch_count <= 0 or not batch_reason:
        return last_emit_mono, 0, None, 0, 0
    next_emit = _emit_vector_progress(
        stage=stage,
        stage_done=batch_stage_done,
        stage_total=stage_total,
        overall_done=batch_overall_done,
        overall_total=overall_total,
        action=f"{batch_reason} count={batch_count}",
        started_mono=started_mono,
        last_emit_mono=last_emit_mono,
        verbose=verbose,
        stats=stats,
    )
    return next_emit, 0, None, 0, 0


class OpenAIEmbeddingClient:
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self.url = f"{config.base_url.rstrip('/')}" + "/embeddings"
        self._adaptive_batch_tokens = max(256, int(config.batch_tokens))
        self._adaptive_single_text_tokens = max(256, int(config.batch_tokens))
        self._adaptive_max_text_chars = max(256, int(config.max_text_chars))

    def _log(self, message: str) -> None:
        if self.config.verbose:
            print(f"[embedding] {message}", flush=True)

    def _normalize_text_for_embedding(self, text: str) -> str:
        clean = " ".join(str(text or "").split()).strip()
        char_limit = max(
            256,
            min(
                int(self._adaptive_max_text_chars),
                max(256, int(self._adaptive_single_text_tokens) * 4),
            ),
        )
        if len(clean) <= char_limit:
            return clean
        clipped = clean[:char_limit].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0].rstrip() or clipped
        return clipped

    @staticmethod
    def _estimate_text_tokens(text: str) -> int:
        clean = str(text or "").strip()
        if not clean:
            return 1
        token_like = re.findall(r"\w+|[^\w\s]", clean)
        estimate_chars = math.ceil(len(clean) / 4)
        estimate_tokens = len(token_like)
        if " " not in clean:
            estimate_tokens = max(estimate_tokens, math.ceil(len(clean) / 1.5))
        return max(1, estimate_chars, estimate_tokens)

    @staticmethod
    def _is_retryable_size_error(message: str) -> bool:
        lowered = str(message or "").lower()
        hints = (
            "context size has been exceeded",
            "too large to process",
            "free space in the kv cache",
            "kv cache",
            "physical batch size",
            "input is too large",
        )
        return any(hint in lowered for hint in hints)

    def _shrink_text_for_retry(self, prepared_text: str) -> str:
        clean = " ".join(str(prepared_text or "").split()).strip()
        if not clean:
            return clean
        if len(clean) <= 256:
            return clean
        next_limit = max(256, int(len(clean) * 0.6))
        clipped = clean[:next_limit].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0].rstrip() or clipped
        self._adaptive_max_text_chars = min(self._adaptive_max_text_chars, len(clipped))
        return clipped

    def _request_batch(self, batch: list[PreparedEmbeddingText]) -> tuple[list[bytes], int]:
        payload = {"model": self.config.model, "input": [entry.text for entry in batch]}
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        req = urllib.request.Request(self.url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=max(1, int(self.config.timeout_seconds))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            approx_tokens = sum(self._estimate_text_tokens(entry.text) for entry in batch)
            message = (
                f"embedding HTTP {exc.code}: batch_items={len(batch)} "
                f"approx_tokens={approx_tokens} {err_body[:600]}"
            )
            if self._is_retryable_size_error(err_body):
                raise RetryableEmbeddingSizeError(
                    status_code=int(exc.code),
                    approx_tokens=approx_tokens,
                    batch_items=len(batch),
                    message=message,
                ) from exc
            raise RuntimeError(message) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                "embedding connection failed "
                f"(url={self.url}, model={self.config.model}): {exc}. "
                "Set VAULT_EMBED_BASE_URL/VAULT_EMBED_MODEL or pass "
                "--embed-base-url/--embed-model."
            ) from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"embedding response is not JSON: {raw[:300]}") from exc

        data = parsed.get("data")
        if not isinstance(data, list):
            raise RuntimeError(f"embedding response missing data list: {str(parsed)[:300]}")
        if len(data) != len(batch):
            raise RuntimeError(
                f"embedding response item count mismatch: expected {len(batch)} got {len(data)}"
            )

        out: list[bytes] = []
        expected_dim: int | None = None
        for entry in data:
            emb = entry.get("embedding") if isinstance(entry, dict) else None
            if not isinstance(emb, list):
                raise RuntimeError(f"embedding entry missing list: {str(entry)[:200]}")
            vec = normalize_vector([float(x) for x in emb])
            if expected_dim is None:
                expected_dim = len(vec)
            elif len(vec) != expected_dim:
                raise RuntimeError(
                    f"embedding dimension mismatch in batch: expected {expected_dim} got {len(vec)}"
                )
            out.append(floats_to_blob(vec))
        return out, int(expected_dim or 0)

    def _build_batches(self, texts: list[PreparedEmbeddingText]) -> list[list[PreparedEmbeddingText]]:
        max_items = max(1, int(self.config.batch_size))
        token_budget = max(256, int(self._adaptive_batch_tokens))
        single_text_budget = max(256, int(self._adaptive_single_text_tokens))
        batches: list[list[PreparedEmbeddingText]] = []
        current: list[PreparedEmbeddingText] = []
        current_tokens = 0
        for entry in texts:
            adjusted = entry
            token_estimate = self._estimate_text_tokens(adjusted.text)
            while token_estimate > min(token_budget, single_text_budget) and len(adjusted.text) > 256:
                shrunk = self._shrink_text_for_retry(adjusted.text)
                if shrunk == adjusted.text:
                    break
                adjusted = PreparedEmbeddingText(original_index=entry.original_index, text=shrunk)
                token_estimate = self._estimate_text_tokens(adjusted.text)
            if current and (len(current) >= max_items or (current_tokens + token_estimate) > token_budget):
                batches.append(current)
                current = []
                current_tokens = 0
            current.append(adjusted)
            current_tokens += token_estimate
        if current:
            batches.append(current)
        return batches

    def embed_texts(self, texts: list[str]) -> tuple[list[bytes], int]:
        if not texts:
            return [], 0

        prepared = [
            PreparedEmbeddingText(original_index=idx, text=self._normalize_text_for_embedding(text))
            for idx, text in enumerate(texts)
        ]
        results: list[bytes | None] = [None] * len(prepared)
        expected_dim: int | None = None
        pending_batches = self._build_batches(prepared)

        while pending_batches:
            batch = pending_batches.pop(0)
            try:
                blobs, batch_dim = self._request_batch(batch)
            except RetryableEmbeddingSizeError as exc:
                self._adaptive_batch_tokens = min(
                    self._adaptive_batch_tokens,
                    max(256, int(exc.approx_tokens * 0.75)),
                )
                if len(batch) > 1:
                    midpoint = max(1, len(batch) // 2)
                    left = batch[:midpoint]
                    right = batch[midpoint:]
                    self._log(
                        "adaptive-retry "
                        f"reason=size-error batch_items={len(batch)} approx_tokens={exc.approx_tokens} "
                        f"next_batches={len(left)}+{len(right)} learned_batch_tokens={self._adaptive_batch_tokens}"
                    )
                    pending_batches = [left, right, *pending_batches]
                    continue

                entry = batch[0]
                shrunk_text = self._shrink_text_for_retry(entry.text)
                if shrunk_text == entry.text:
                    raise RuntimeError(exc.message) from exc
                next_tokens = self._estimate_text_tokens(shrunk_text)
                self._adaptive_single_text_tokens = min(
                    self._adaptive_single_text_tokens,
                    max(256, int(next_tokens * 1.25)),
                )
                self._log(
                    "adaptive-retry "
                    f"reason=shrink-single approx_tokens={exc.approx_tokens} "
                    f"old_chars={len(entry.text)} new_chars={len(shrunk_text)} "
                    f"learned_single_text_tokens={self._adaptive_single_text_tokens}"
                )
                pending_batches = [
                    [PreparedEmbeddingText(original_index=entry.original_index, text=shrunk_text)],
                    *pending_batches,
                ]
                continue

            if batch_dim <= 0:
                raise RuntimeError("embedding endpoint returned zero-dimension vectors")
            if expected_dim is None:
                expected_dim = batch_dim
            elif batch_dim != expected_dim:
                raise RuntimeError(
                    f"embedding dimension mismatch across batches: expected {expected_dim} got {batch_dim}"
                )
            for entry, blob in zip(batch, blobs):
                results[entry.original_index] = blob

        if any(blob is None for blob in results):
            raise RuntimeError("embedding pipeline returned incomplete results")
        return [blob for blob in results if blob is not None], int(expected_dim or 0)


def ensure_vector_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vector_items (
          item_id TEXT PRIMARY KEY,
          source_table TEXT NOT NULL,
          source_filepath TEXT NOT NULL,
          source_checksum TEXT,
          source_updated_at TEXT,
          chunk_index INTEGER NOT NULL DEFAULT 0,
          chunk_count INTEGER NOT NULL DEFAULT 1,
          content_hash TEXT NOT NULL,
          text_preview TEXT,
          text_preview_full TEXT,
          text_preview_redacted TEXT,
          metadata_json TEXT,
          embedding BLOB NOT NULL,
          embedding_dim INTEGER NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vector_source ON vector_items(source_table, source_filepath)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_state (
          source_table TEXT NOT NULL,
          source_filepath TEXT NOT NULL,
          state_hash TEXT NOT NULL,
          source_checksum TEXT,
          source_updated_at TEXT,
          item_count INTEGER NOT NULL DEFAULT 0,
          indexed_at TEXT NOT NULL,
          PRIMARY KEY (source_table, source_filepath)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vector_items_v2 (
          item_id TEXT NOT NULL,
          index_level TEXT NOT NULL,
          source_table TEXT NOT NULL,
          source_filepath TEXT NOT NULL,
          source_checksum TEXT,
          source_updated_at TEXT,
          chunk_index INTEGER NOT NULL DEFAULT 0,
          chunk_count INTEGER NOT NULL DEFAULT 1,
          content_hash TEXT NOT NULL,
          text_preview TEXT,
          text_preview_full TEXT,
          text_preview_redacted TEXT,
          metadata_json TEXT,
          embedding BLOB NOT NULL,
          embedding_dim INTEGER NOT NULL,
          redaction_policy_version TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (item_id, index_level)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vector_v2_source "
        "ON vector_items_v2(index_level, source_table, source_filepath)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_state_v2 (
          source_table TEXT NOT NULL,
          source_filepath TEXT NOT NULL,
          index_level TEXT NOT NULL,
          state_hash TEXT NOT NULL,
          redaction_policy_version TEXT NOT NULL,
          source_checksum TEXT,
          source_updated_at TEXT,
          item_count INTEGER NOT NULL DEFAULT 0,
          indexed_at TEXT NOT NULL,
          PRIMARY KEY (source_table, source_filepath, index_level)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_state_v2_level "
        "ON source_state_v2(index_level, source_table, source_filepath)"
    )
    conn.commit()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(vector_items)").fetchall()}
    if "text_preview_full" not in cols:
        conn.execute("ALTER TABLE vector_items ADD COLUMN text_preview_full TEXT")
    if "text_preview_redacted" not in cols:
        conn.execute("ALTER TABLE vector_items ADD COLUMN text_preview_redacted TEXT")
    conn.commit()


def _vector_content_hash(*, source_text: str, index_level: str) -> str:
    payload = {
        "index_level": index_level,
        "policy_version": REDACTION_POLICY_VERSION,
        "source_hash": hashlib.sha256(source_text.encode("utf-8", errors="ignore")).hexdigest(),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _vector_levels_available(conn: sqlite3.Connection) -> set[str]:
    levels: set[str] = set()
    if _table_exists(conn, "vector_items_v2"):
        levels.update(
            {
                str(row[0])
                for row in conn.execute(
                    "SELECT index_level FROM vector_items_v2 GROUP BY index_level HAVING COUNT(*) > 0"
                )
            }
        )
    if _table_exists(conn, "vector_items"):
        legacy_total = int(_safe_scalar(conn, "SELECT COUNT(*) FROM vector_items") or 0)
        if legacy_total > 0:
            levels.add(INDEX_LEVEL_REDACTED)
    return levels


def _resolve_effective_search_level(
    conn: sqlite3.Connection,
    *,
    clearance: str,
    search_level: str,
) -> SearchDiagnostics:
    requested = (search_level or INDEX_LEVEL_AUTO).strip().lower()
    if requested not in {INDEX_LEVEL_AUTO, INDEX_LEVEL_REDACTED, INDEX_LEVEL_FULL}:
        requested = INDEX_LEVEL_AUTO

    available = _vector_levels_available(conn)
    full_available = INDEX_LEVEL_FULL in available
    if requested == INDEX_LEVEL_FULL:
        if not full_available:
            raise ValueError("full search level is unavailable; run vault-ops upgrade --index-level full --yes to build it.")
        return SearchDiagnostics(
            requested_level=INDEX_LEVEL_FULL,
            used_level=INDEX_LEVEL_FULL,
            full_level_available=True,
        )
    if requested == INDEX_LEVEL_REDACTED:
        return SearchDiagnostics(
            requested_level=INDEX_LEVEL_REDACTED,
            used_level=INDEX_LEVEL_REDACTED,
            full_level_available=full_available,
        )

    desired = INDEX_LEVEL_FULL if clearance == "full" and full_available else INDEX_LEVEL_REDACTED
    fallback = INDEX_LEVEL_FULL if clearance == "full" and not full_available else None
    return SearchDiagnostics(
        requested_level=INDEX_LEVEL_AUTO,
        used_level=desired,
        fallback_from_level=fallback,
        full_level_available=full_available,
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _safe_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def _selection_tables(
    source_selection: str,
    *,
    mail_bridge_enabled: bool,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
) -> list[str]:
    return [
        handler.table
        for handler in _select_active_vector_source_handlers(
            source_selection,
            mail_bridge_enabled=mail_bridge_enabled,
            mail_max_body_chunks=mail_max_body_chunks,
        )
    ]


def _enabled_vector_source_kinds(*, mail_bridge_enabled: bool) -> set[str]:
    enabled = {"docs", "photos"}
    if mail_bridge_enabled:
        enabled.add("mail")
    return enabled


def _select_active_vector_source_handlers(
    source_selection: str | None,
    *,
    mail_bridge_enabled: bool,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
) -> tuple[SourceHandler, ...]:
    return select_active_source_handlers(
        source_selection,
        enabled_kinds=_enabled_vector_source_kinds(mail_bridge_enabled=mail_bridge_enabled),
        handlers=_vector_source_handlers(mail_max_body_chunks=mail_max_body_chunks),
    )


def rebuild_plan(
    registry_db: Path,
    vector_db: Path,
    *,
    source_selection: str,
    mail_bridge_enabled: bool,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
    index_level: str,
) -> dict[str, Any]:
    chosen_index_level = (index_level or INDEX_LEVEL_REDACTED).strip().lower()
    plan: dict[str, Any] = {
        "source_selection": source_selection,
        "tables": _selection_tables(
            source_selection,
            mail_bridge_enabled=mail_bridge_enabled,
            mail_max_body_chunks=mail_max_body_chunks,
        ),
        "index_level": chosen_index_level,
        "registry_sources_targeted": 0,
        "indexed_sources_targeted": 0,
        "vector_items_targeted": 0,
        "oldest_indexed_at": None,
        "newest_indexed_at": None,
        "count_source": "none",
    }
    reg_conn = connect_vault_db(registry_db, timeout=30.0)
    vec_conn = connect_vault_db(vector_db, timeout=30.0) if vector_db.exists() else None
    try:
        for table in plan["tables"]:
            plan["registry_sources_targeted"] += int(_safe_scalar(reg_conn, f"SELECT COUNT(*) FROM {table}") or 0)

        if vec_conn is None:
            return plan

        placeholders = ",".join("?" for _ in plan["tables"])
        params = tuple(plan["tables"])

        if _table_exists(vec_conn, "vector_items_v2"):
            plan["vector_items_targeted"] = int(
                _safe_scalar(
                    vec_conn,
                    (
                        f"SELECT COUNT(*) FROM vector_items_v2 "
                        f"WHERE source_table IN ({placeholders}) AND index_level = ?"
                    ),
                    params + (chosen_index_level,),
                )
                or 0
            )
            if _table_exists(vec_conn, "source_state_v2"):
                plan["indexed_sources_targeted"] = int(
                    _safe_scalar(
                        vec_conn,
                        (
                            f"SELECT COUNT(*) FROM source_state_v2 "
                            f"WHERE source_table IN ({placeholders}) AND index_level = ?"
                        ),
                        params + (chosen_index_level,),
                    )
                    or 0
                )
                plan["oldest_indexed_at"] = _safe_scalar(
                    vec_conn,
                    (
                        f"SELECT MIN(indexed_at) FROM source_state_v2 "
                        f"WHERE source_table IN ({placeholders}) AND index_level = ?"
                    ),
                    params + (chosen_index_level,),
                )
                plan["newest_indexed_at"] = _safe_scalar(
                    vec_conn,
                    (
                        f"SELECT MAX(indexed_at) FROM source_state_v2 "
                        f"WHERE source_table IN ({placeholders}) AND index_level = ?"
                    ),
                    params + (chosen_index_level,),
                )
            if plan["vector_items_targeted"] > 0 or plan["indexed_sources_targeted"] > 0:
                plan["count_source"] = "v2"

        if (
            plan["count_source"] == "none"
            and _table_exists(vec_conn, "vector_items")
            and chosen_index_level == INDEX_LEVEL_REDACTED
        ):
            plan["vector_items_targeted"] = int(
                _safe_scalar(
                    vec_conn,
                    f"SELECT COUNT(*) FROM vector_items WHERE source_table IN ({placeholders})",
                    params,
                )
                or 0
            )
            if _table_exists(vec_conn, "source_state"):
                plan["indexed_sources_targeted"] = int(
                    _safe_scalar(
                        vec_conn,
                        f"SELECT COUNT(*) FROM source_state WHERE source_table IN ({placeholders})",
                        params,
                    )
                    or 0
                )
                plan["oldest_indexed_at"] = _safe_scalar(
                    vec_conn,
                    f"SELECT MIN(indexed_at) FROM source_state WHERE source_table IN ({placeholders})",
                    params,
                )
                plan["newest_indexed_at"] = _safe_scalar(
                    vec_conn,
                    f"SELECT MAX(indexed_at) FROM source_state WHERE source_table IN ({placeholders})",
                    params,
                )
            if not plan["oldest_indexed_at"]:
                plan["oldest_indexed_at"] = _safe_scalar(
                    vec_conn,
                    f"SELECT MIN(updated_at) FROM vector_items WHERE source_table IN ({placeholders})",
                    params,
                )
            if not plan["newest_indexed_at"]:
                plan["newest_indexed_at"] = _safe_scalar(
                    vec_conn,
                    f"SELECT MAX(updated_at) FROM vector_items WHERE source_table IN ({placeholders})",
                    params,
                )
            if plan["vector_items_targeted"] > 0 or plan["indexed_sources_targeted"] > 0:
                plan["count_source"] = "legacy"
        return plan
    finally:
        reg_conn.close()
        if vec_conn is not None:
            vec_conn.close()


def _print_rebuild_plan(plan: dict[str, Any]) -> None:
    scope = str(plan.get("source_selection") or "all")
    tables = ",".join(plan.get("tables") or [])
    index_level = str(plan.get("index_level") or INDEX_LEVEL_REDACTED)
    count_source = str(plan.get("count_source") or "none")
    print("warning: rebuild will delete existing vector index data before re-embedding.")
    print(f"scope={scope} tables={tables} index_level={index_level} count_source={count_source}")
    print(
        " ".join(
            [
                f"registry_sources_targeted={plan.get('registry_sources_targeted', 0)}",
                f"indexed_sources_targeted={plan.get('indexed_sources_targeted', 0)}",
                f"vector_items_targeted={plan.get('vector_items_targeted', 0)}",
                f"oldest_indexed_at={plan.get('oldest_indexed_at') or 'unknown'}",
                f"newest_indexed_at={plan.get('newest_indexed_at') or 'unknown'}",
            ]
        )
    )
    print(
        "rebuild behavior: current vectors for the selected scope and index level are deleted first, then re-created from the registry. "
        "If interrupted, completed items stay rebuilt but deleted-yet-not-rebuilt items remain missing until the next run."
    )


def confirm_rebuild(
    registry_db: Path,
    vector_db: Path,
    *,
    source_selection: str,
    mail_bridge_enabled: bool,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
    index_level: str,
    assume_yes: bool,
) -> bool:
    plan = rebuild_plan(
        registry_db,
        vector_db,
        source_selection=source_selection,
        mail_bridge_enabled=mail_bridge_enabled,
        mail_max_body_chunks=mail_max_body_chunks,
        index_level=index_level,
    )
    _print_rebuild_plan(plan)
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print("error: rebuild confirmation required; rerun with --yes-rebuild to continue.", file=sys.stderr)
        return False
    try:
        answer = input("Type REBUILD to continue: ").strip()
    except EOFError:
        answer = ""
    if answer != "REBUILD":
        print("rebuild cancelled")
        return False
    return True


def iter_docs(reg_conn: sqlite3.Connection, *, updated_since: str | None = None) -> Iterable[sqlite3.Row]:
    sql = """
        SELECT filepath, checksum, source, text_content, parser, size, mtime, updated_at,
               summary_text, summary_model, summary_hash, summary_status, summary_updated_at,
               dates_json, primary_date
        FROM docs_registry
    """
    params: list[str] = []
    if updated_since:
        sql += " WHERE updated_at >= ?"
        params.append(updated_since)
    cur = reg_conn.execute(sql, params)
    yield from cur


def iter_photos(reg_conn: sqlite3.Connection, *, updated_since: str | None = None) -> Iterable[sqlite3.Row]:
    sql = """
        SELECT filepath, checksum, source, date_taken, size, mtime, updated_at, notes,
               category_primary, category_secondary, taxonomy, caption, analyzer_status,
               ocr_text, ocr_status, ocr_source, ocr_updated_at,
               dates_json, primary_date
        FROM photos_registry
    """
    params: list[str] = []
    if updated_since:
        sql += " WHERE updated_at >= ?"
        params.append(updated_since)
    cur = reg_conn.execute(sql, params)
    yield from cur


def iter_mail(reg_conn: sqlite3.Connection, *, updated_since: str | None = None) -> Iterable[sqlite3.Row]:
    sql = """
        SELECT filepath, checksum, source, msg_id, account_email, thread_id,
               date_iso, from_addr, to_addr, subject, snippet, body_text, labels_json,
               summary_text, dates_json, primary_date, updated_at
        FROM mail_registry
    """
    params: list[str] = []
    if updated_since:
        sql += " WHERE updated_at >= ?"
        params.append(updated_since)
    cur = reg_conn.execute(sql, params)
    yield from cur


def count_registry_rows(
    reg_conn: sqlite3.Connection,
    table: str,
    *,
    updated_since: str | None = None,
) -> int:
    sql = f"SELECT COUNT(*) FROM {table}"
    params: list[str] = []
    if updated_since:
        sql += " WHERE updated_at >= ?"
        params.append(updated_since)
    return int(reg_conn.execute(sql, params).fetchone()[0])


def _doc_wait_state(row: sqlite3.Row | dict[str, Any]) -> tuple[str, bool]:
    if _doc_summary_backfill_pending(row):
        return "waiting-summary", True
    return "summary-unavailable", False


def _photo_wait_state(_: sqlite3.Row | dict[str, Any]) -> tuple[str, bool]:
    return "waiting-photo-analysis", True


def _mail_wait_state(_: sqlite3.Row | dict[str, Any]) -> tuple[str, bool]:
    return "waiting-mail-sync", True


def _vector_source_handlers(
    *,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
) -> tuple[SourceHandler, ...]:
    normalized_mail_chunk_cap = max(0, int(mail_max_body_chunks))
    docs_handler = replace(
        source_handler_by_kind("docs", handlers=REGISTERED_SOURCES),
        row_iterator=lambda reg_conn, updated_since=None: iter_docs(reg_conn, updated_since=updated_since),
        row_count=lambda reg_conn, updated_since=None: count_registry_rows(
            reg_conn,
            "docs_registry",
            updated_since=updated_since,
        ),
        vector_ready=_doc_vector_ready,
        item_builder=build_doc_items,
        state_hash_builder=state_hash_doc,
        wait_state_resolver=_doc_wait_state,
        embedding_reuse_handler=reuse_doc_embeddings_by_checksum,
        checksum_reuse_supported=True,
    )
    photos_handler = replace(
        source_handler_by_kind("photos", handlers=REGISTERED_SOURCES),
        row_iterator=lambda reg_conn, updated_since=None: iter_photos(reg_conn, updated_since=updated_since),
        row_count=lambda reg_conn, updated_since=None: count_registry_rows(
            reg_conn,
            "photos_registry",
            updated_since=updated_since,
        ),
        vector_ready=_photo_vector_ready,
        item_builder=build_photo_items,
        state_hash_builder=state_hash_photo,
        wait_state_resolver=_photo_wait_state,
    )
    mail_handler = replace(
        source_handler_by_kind("mail", handlers=REGISTERED_SOURCES),
        row_iterator=lambda reg_conn, updated_since=None: iter_mail(reg_conn, updated_since=updated_since),
        row_count=lambda reg_conn, updated_since=None: count_registry_rows(
            reg_conn,
            "mail_registry",
            updated_since=updated_since,
        ),
        vector_ready=_mail_vector_ready,
        item_builder=lambda row: build_mail_items(row, max_body_chunks=normalized_mail_chunk_cap),
        state_hash_builder=lambda row, *, redaction_mode, redaction_output_signature="": state_hash_mail(
            row,
            redaction_mode=redaction_mode,
            redaction_output_signature=redaction_output_signature,
            mail_max_body_chunks=normalized_mail_chunk_cap,
        ),
        wait_state_resolver=_mail_wait_state,
    )
    return (docs_handler, photos_handler, mail_handler)


def ensure_redaction_table(reg_conn: sqlite3.Connection) -> None:
    reg_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS redaction_entries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          scope_type TEXT NOT NULL,
          scope_id TEXT NOT NULL,
          key_name TEXT NOT NULL,
          placeholder TEXT NOT NULL,
          value_norm TEXT NOT NULL,
          original_value TEXT NOT NULL,
          source_mode TEXT NOT NULL,
          policy_version TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'active',
          validator_name TEXT,
          detector_sources TEXT,
          modality TEXT,
          source_field TEXT,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          hit_count INTEGER NOT NULL DEFAULT 1,
          UNIQUE(scope_type, scope_id, key_name, value_norm),
          UNIQUE(scope_type, scope_id, placeholder)
        )
        """
    )
    reg_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_redaction_scope "
        "ON redaction_entries(scope_type, scope_id)"
    )
    reg_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_redaction_placeholder "
        "ON redaction_entries(scope_type, scope_id, placeholder)"
    )
    reg_conn.commit()
    cols = {row[1] for row in reg_conn.execute("PRAGMA table_info(redaction_entries)").fetchall()}
    for column, column_type in [
        ("policy_version", "TEXT NOT NULL DEFAULT ''"),
        ("status", "TEXT NOT NULL DEFAULT 'active'"),
        ("validator_name", "TEXT"),
        ("detector_sources", "TEXT"),
        ("modality", "TEXT"),
        ("source_field", "TEXT"),
    ]:
        if column not in cols:
            reg_conn.execute(f"ALTER TABLE redaction_entries ADD COLUMN {column} {column_type}")
    reg_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_redaction_status "
        "ON redaction_entries(scope_type, scope_id, status)"
    )
    reg_conn.execute(
        "UPDATE redaction_entries SET policy_version = ? WHERE COALESCE(policy_version, '') = ''",
        (REDACTION_POLICY_VERSION,),
    )
    reg_conn.execute(
        "UPDATE redaction_entries SET status = 'active' WHERE COALESCE(status, '') = ''"
    )
    reg_conn.commit()


def _ensure_registry_column(
    reg_conn: sqlite3.Connection,
    table: str,
    column: str,
    column_type: str,
) -> None:
    cols = {row[1] for row in reg_conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        reg_conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def ensure_registry_vector_columns(reg_conn: sqlite3.Connection) -> None:
    if _table_exists(reg_conn, "docs_registry"):
        for column, column_type in [
            ("summary_text", "TEXT"),
            ("summary_model", "TEXT"),
            ("summary_hash", "TEXT"),
            ("summary_status", "TEXT"),
            ("summary_updated_at", "TEXT"),
            ("dates_json", "TEXT"),
            ("primary_date", "TEXT"),
        ]:
            _ensure_registry_column(reg_conn, "docs_registry", column, column_type)
    if _table_exists(reg_conn, "photos_registry"):
        for column, column_type in [
            ("notes", "TEXT"),
            ("category_primary", "TEXT"),
            ("category_secondary", "TEXT"),
            ("taxonomy", "TEXT"),
            ("caption", "TEXT"),
            ("analyzer_status", "TEXT"),
            ("ocr_text", "TEXT"),
            ("ocr_status", "TEXT"),
            ("ocr_source", "TEXT"),
            ("ocr_updated_at", "TEXT"),
            ("dates_json", "TEXT"),
            ("primary_date", "TEXT"),
        ]:
            _ensure_registry_column(reg_conn, "photos_registry", column, column_type)
    if _table_exists(reg_conn, "mail_registry"):
        for column, column_type in [
            ("source", "TEXT"),
            ("msg_id", "TEXT"),
            ("account_email", "TEXT"),
            ("thread_id", "TEXT"),
            ("date_iso", "TEXT"),
            ("from_addr", "TEXT"),
            ("to_addr", "TEXT"),
            ("subject", "TEXT"),
            ("snippet", "TEXT"),
            ("body_text", "TEXT"),
            ("labels_json", "TEXT"),
            ("summary_text", "TEXT"),
            ("dates_json", "TEXT"),
            ("primary_date", "TEXT"),
            ("updated_at", "TEXT"),
        ]:
            _ensure_registry_column(reg_conn, "mail_registry", column, column_type)
    reg_conn.commit()


def fetch_redaction_entries(
    reg_conn: sqlite3.Connection,
    *,
    scope_type: str,
    scope_id: str,
) -> list[tuple[str, str, str, str]]:
    rows = reg_conn.execute(
        """
        SELECT key_name, placeholder, value_norm, original_value
        FROM redaction_entries
        WHERE scope_type = ? AND scope_id = ? AND COALESCE(status, 'active') = 'active'
        ORDER BY key_name, placeholder
        """,
        (scope_type, scope_id),
    ).fetchall()
    out: list[tuple[str, str, str, str]] = []
    for row in rows:
        key_name = str(row[0])
        placeholder = str(row[1])
        value_norm = str(row[2])
        original_value = str(row[3])
        if not is_redaction_value_allowed(key_name, original_value):
            continue
        out.append((key_name, placeholder, value_norm, original_value))
    return out


def seed_redaction_map_key_counts(
    reg_conn: sqlite3.Connection,
    *,
    scope_type: str,
    scope_id: str,
    redaction_map: PersistentRedactionMap,
) -> None:
    rows = reg_conn.execute(
        """
        SELECT key_name, placeholder
        FROM redaction_entries
        WHERE scope_type = ? AND scope_id = ?
        """,
        (scope_type, scope_id),
    ).fetchall()
    for row in rows:
        key_name = str(row[0] or "")
        placeholder = str(row[1] or "")
        match = re.search(r"_([A-Z]+)>$", placeholder)
        if not match:
            continue
        token = match.group(1)
        ordinal = 0
        for char in token:
            if not ("A" <= char <= "Z"):
                ordinal = 0
                break
            ordinal = (ordinal * 26) + (ord(char) - ord("A") + 1)
        if ordinal <= 0:
            continue
        normalized_key = re.sub(r"[^A-Za-z0-9]+", "_", key_name.strip().upper()).strip("_") or "CUSTOM"
        redaction_map.key_counts[normalized_key] = max(redaction_map.key_counts.get(normalized_key, 0), ordinal)


def prune_invalid_redaction_entries(
    reg_conn: sqlite3.Connection,
    *,
    scope_type: str,
    scope_id: str,
) -> int:
    rows = reg_conn.execute(
        """
        SELECT id, key_name, original_value, COALESCE(status, 'active')
        FROM redaction_entries
        WHERE scope_type = ? AND scope_id = ?
        """,
        (scope_type, scope_id),
    ).fetchall()
    invalid_ids = [
        int(row[0])
        for row in rows
        if str(row[3] or "active") != "rejected"
        and not is_redaction_value_allowed(str(row[1]), str(row[2]))
    ]
    if not invalid_ids:
        return 0
    now = now_iso()
    reg_conn.executemany(
        "UPDATE redaction_entries SET status='rejected', policy_version=?, last_seen_at=? WHERE id = ?",
        [(REDACTION_POLICY_VERSION, now, row_id) for row_id in invalid_ids],
    )
    reg_conn.commit()
    return len(invalid_ids)


def upsert_redaction_entries(
    reg_conn: sqlite3.Connection,
    *,
    scope_type: str,
    scope_id: str,
    entries: list[dict[str, str]],
) -> int:
    if not entries:
        return 0
    sanitized_entries = [
        entry
        for entry in entries
        if is_redaction_value_allowed(
            str(entry.get("key_name") or ""),
            str(entry.get("original_value") or ""),
        )
    ]
    if not sanitized_entries:
        return 0
    now = now_iso()
    for entry in sanitized_entries:
        source_mode = str(entry.get("source_mode") or "unknown")
        reg_conn.execute(
            """
            INSERT INTO redaction_entries (
              scope_type, scope_id, key_name, placeholder, value_norm,
              original_value, source_mode, policy_version, status, validator_name,
              detector_sources, modality, source_field, first_seen_at, last_seen_at, hit_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(scope_type, scope_id, key_name, value_norm) DO UPDATE SET
              placeholder=excluded.placeholder,
              original_value=excluded.original_value,
              source_mode=excluded.source_mode,
              policy_version=excluded.policy_version,
              status='active',
              validator_name=excluded.validator_name,
              detector_sources=excluded.detector_sources,
              modality=excluded.modality,
              source_field=excluded.source_field,
              last_seen_at=excluded.last_seen_at,
              hit_count=redaction_entries.hit_count + 1
            """,
            (
                scope_type,
                scope_id,
                entry["key_name"],
                entry["placeholder"],
                entry["value_norm"],
                entry["original_value"],
                source_mode,
                REDACTION_POLICY_VERSION,
                "active",
                str(entry.get("validator_name") or "entity-validator-v1"),
                str(entry.get("detector_sources") or source_mode),
                str(entry.get("modality") or "text"),
                str(entry.get("source_field") or "content"),
                now,
                now,
            ),
        )
    reg_conn.commit()
    return len(fetch_redaction_entries(reg_conn, scope_type=scope_type, scope_id=scope_id))


def unredact_with_scope(
    reg_conn: sqlite3.Connection,
    *,
    scope_type: str,
    scope_id: str,
    text: str,
) -> str:
    if not text:
        return ""
    rows = reg_conn.execute(
        """
        SELECT key_name, placeholder, original_value
        FROM redaction_entries
        WHERE scope_type = ? AND scope_id = ? AND COALESCE(status, 'active') = 'active'
        ORDER BY length(placeholder) DESC
        """,
        (scope_type, scope_id),
    ).fetchall()
    out = text
    for key_name, placeholder, original_value in rows:
        if not is_redaction_value_allowed(str(key_name), str(original_value)):
            continue
        out = out.replace(str(placeholder), str(original_value))
    return out


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    day_only = len(text) == 10 and text[4] == "-" and text[7] == "-"
    if day_only:
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _resolve_to_datetime(raw: str | None) -> datetime | None:
    dt = _parse_iso_datetime(raw)
    if dt is None:
        return None
    text = str(raw or "").strip()
    day_only = len(text) == 10 and text[4] == "-" and text[7] == "-"
    if day_only:
        return dt + timedelta(days=1)
    return dt


def _matches_time_filters(
    *,
    source_updated_at: str | None,
    date_taken: str | None,
    primary_date: str | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
) -> bool:
    candidate = (
        _parse_iso_datetime(primary_date)
        or _parse_iso_datetime(date_taken)
        or _parse_iso_datetime(source_updated_at)
    )
    if candidate is None:
        return True
    if from_dt is not None and candidate < from_dt:
        return False
    if to_dt is not None and candidate >= to_dt:
        return False
    return True


def _resolve_redaction_config(args: argparse.Namespace) -> RedactionConfig:
    mode = str(args.redaction_mode or "hybrid").strip().lower()
    if mode not in {"regex", "model", "hybrid"}:
        mode = "hybrid"
    base_url = str(
        args.redaction_base_url or os.getenv("VAULT_REDACTION_BASE_URL", DEFAULT_REDACTION_BASE_URL)
    ).strip()
    model = str(
        args.redaction_model or os.getenv("VAULT_REDACTION_MODEL", DEFAULT_REDACTION_MODEL)
    ).strip()
    api_key = str(
        args.redaction_api_key
        if args.redaction_api_key is not None
        else os.getenv("VAULT_REDACTION_API_KEY", "local")
    )
    timeout_seconds = int(
        args.redaction_timeout
        if args.redaction_timeout is not None
        else int(os.getenv("VAULT_REDACTION_TIMEOUT_SECONDS", str(DEFAULT_REDACTION_TIMEOUT_SECONDS)))
    )
    profile = str(args.redaction_profile or os.getenv("VAULT_REDACTION_PROFILE", "standard")).strip()
    instruction = str(
        args.redaction_instruction or os.getenv("VAULT_REDACTION_INSTRUCTION", "")
    ).strip()
    return RedactionConfig(
        mode=mode,
        profile=profile or "standard",
        instruction=instruction,
        enabled=not bool(getattr(args, "disable_redaction", False)),
        base_url=base_url,
        model=model or DEFAULT_REDACTION_MODEL,
        api_key=api_key,
        timeout_seconds=max(1, timeout_seconds),
    )


def load_source_state(
    vec_conn: sqlite3.Connection,
    source_table: str,
    source_filepath: str,
    *,
    index_level: str,
) -> tuple[str, str] | None:
    row = vec_conn.execute(
        """
        SELECT state_hash, redaction_policy_version
        FROM source_state_v2
        WHERE source_table = ? AND source_filepath = ? AND index_level = ?
        """,
        (source_table, source_filepath, index_level),
    ).fetchone()
    if not row:
        return None
    return str(row[0]), str(row[1] or "")


def prune_inactive_items(
    vec_conn: sqlite3.Connection,
    *,
    source_table: str,
    source_filepath: str,
    index_level: str,
    active_ids: list[str],
    stats: UpdateStats,
) -> None:
    placeholders = ",".join("?" for _ in active_ids)
    if placeholders:
        cur = vec_conn.execute(
            f"DELETE FROM vector_items_v2 WHERE source_table = ? AND source_filepath = ? AND index_level = ? AND item_id NOT IN ({placeholders})",
            [source_table, source_filepath, index_level, *active_ids],
        )
    else:
        cur = vec_conn.execute(
            "DELETE FROM vector_items_v2 WHERE source_table = ? AND source_filepath = ? AND index_level = ?",
            (source_table, source_filepath, index_level),
        )
    stats.deleted_items += int(cur.rowcount or 0)


def upsert_items_for_source(
    vec_conn: sqlite3.Connection,
    items: list[Item],
    *,
    embeddings: list[bytes],
    embedding_dim: int,
    index_level: str,
    stats: UpdateStats,
) -> None:
    if not items:
        return
    if len(items) != len(embeddings):
        raise ValueError(f"items/embeddings length mismatch: {len(items)} != {len(embeddings)}")

    source_table = items[0].source_table
    source_filepath = items[0].source_filepath
    active_ids: list[str] = []

    for it, emb in zip(items, embeddings):
        iid = item_id(it.source_table, it.source_filepath, it.chunk_index)
        active_ids.append(iid)
        vector_source_text = it.text_redacted if index_level == INDEX_LEVEL_REDACTED else it.text
        content_hash = _vector_content_hash(source_text=vector_source_text, index_level=index_level)
        preview_full = " ".join(it.text.split())[:240]
        preview_redacted = " ".join(it.text_redacted.split())[:240]

        vec_conn.execute(
            """
            INSERT INTO vector_items_v2 (
              item_id, index_level, source_table, source_filepath, source_checksum, source_updated_at,
              chunk_index, chunk_count, content_hash, text_preview, text_preview_full, text_preview_redacted, metadata_json,
              embedding, embedding_dim, redaction_policy_version, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, index_level) DO UPDATE SET
              source_checksum=excluded.source_checksum,
              source_updated_at=excluded.source_updated_at,
              chunk_count=excluded.chunk_count,
              content_hash=excluded.content_hash,
              text_preview=excluded.text_preview,
              text_preview_full=excluded.text_preview_full,
              text_preview_redacted=excluded.text_preview_redacted,
              metadata_json=excluded.metadata_json,
              embedding=excluded.embedding,
              embedding_dim=excluded.embedding_dim,
              redaction_policy_version=excluded.redaction_policy_version,
              updated_at=excluded.updated_at
            """,
            (
                iid,
                index_level,
                it.source_table,
                it.source_filepath,
                it.source_checksum,
                it.source_updated_at,
                it.chunk_index,
                it.chunk_count,
                content_hash,
                preview_redacted,
                preview_full,
                preview_redacted,
                json.dumps(it.metadata, ensure_ascii=False),
                emb,
                embedding_dim,
                REDACTION_POLICY_VERSION,
                now_iso(),
            ),
        )
        stats.upserted_items += 1

    prune_inactive_items(
        vec_conn,
        source_table=source_table,
        source_filepath=source_filepath,
        index_level=index_level,
        active_ids=active_ids,
        stats=stats,
    )


def reuse_doc_embeddings_by_checksum(
    vec_conn: sqlite3.Connection,
    row: sqlite3.Row,
    items: list[Item],
    *,
    index_level: str,
    stats: UpdateStats,
) -> bool:
    checksum = (row["checksum"] or "").strip()
    filepath = row["filepath"]
    if not checksum:
        return False

    source_row = vec_conn.execute(
        """
        SELECT source_filepath
        FROM source_state_v2
        WHERE source_table='docs_registry'
          AND index_level=?
          AND source_checksum=?
          AND source_filepath != ?
          AND item_count > 0
        ORDER BY indexed_at DESC
        LIMIT 1
        """,
        (index_level, checksum, filepath),
    ).fetchone()
    if not source_row:
        return False

    donor_filepath = source_row[0]
    donor_rows = vec_conn.execute(
        """
        SELECT chunk_index, chunk_count, content_hash, embedding, embedding_dim
        FROM vector_items_v2
        WHERE source_table='docs_registry' AND source_filepath=? AND index_level=?
        ORDER BY chunk_index ASC
        """,
        (donor_filepath, index_level),
    ).fetchall()

    if len(donor_rows) != len(items):
        return False

    for donor, it in zip(donor_rows, items):
        expected_hash = _vector_content_hash(
            source_text=(it.text_redacted if index_level == INDEX_LEVEL_REDACTED else it.text),
            index_level=index_level,
        )
        if int(donor["chunk_index"]) != int(it.chunk_index):
            return False
        if int(donor["chunk_count"]) != int(it.chunk_count):
            return False
        if donor["content_hash"] != expected_hash:
            return False

    active_ids: list[str] = []
    for donor, it in zip(donor_rows, items):
        iid = item_id(it.source_table, it.source_filepath, it.chunk_index)
        active_ids.append(iid)
        content_hash = _vector_content_hash(
            source_text=(it.text_redacted if index_level == INDEX_LEVEL_REDACTED else it.text),
            index_level=index_level,
        )
        preview_full = " ".join(it.text.split())[:240]
        preview_redacted = " ".join(it.text_redacted.split())[:240]
        vec_conn.execute(
            """
            INSERT INTO vector_items_v2 (
              item_id, index_level, source_table, source_filepath, source_checksum, source_updated_at,
              chunk_index, chunk_count, content_hash, text_preview, text_preview_full, text_preview_redacted, metadata_json,
              embedding, embedding_dim, redaction_policy_version, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, index_level) DO UPDATE SET
              source_checksum=excluded.source_checksum,
              source_updated_at=excluded.source_updated_at,
              chunk_count=excluded.chunk_count,
              content_hash=excluded.content_hash,
              text_preview=excluded.text_preview,
              text_preview_full=excluded.text_preview_full,
              text_preview_redacted=excluded.text_preview_redacted,
              metadata_json=excluded.metadata_json,
              embedding=excluded.embedding,
              embedding_dim=excluded.embedding_dim,
              redaction_policy_version=excluded.redaction_policy_version,
              updated_at=excluded.updated_at
            """,
            (
                iid,
                index_level,
                it.source_table,
                it.source_filepath,
                it.source_checksum,
                it.source_updated_at,
                it.chunk_index,
                it.chunk_count,
                content_hash,
                preview_redacted,
                preview_full,
                preview_redacted,
                json.dumps(it.metadata, ensure_ascii=False),
                donor["embedding"],
                int(donor["embedding_dim"]),
                REDACTION_POLICY_VERSION,
                now_iso(),
            ),
        )
        stats.upserted_items += 1

    prune_inactive_items(
        vec_conn,
        source_table='docs_registry',
        source_filepath=filepath,
        index_level=index_level,
        active_ids=active_ids,
        stats=stats,
    )
    _source_update_stats(stats, source_handler_by_kind("docs", handlers=REGISTERED_SOURCES)).checksum_reused += 1
    return True


def upsert_source_state(
    vec_conn: sqlite3.Connection,
    *,
    source_table: str,
    source_filepath: str,
    index_level: str,
    state_hash: str,
    checksum: str,
    updated_at: str,
    item_count: int,
) -> None:
    vec_conn.execute(
        """
        INSERT INTO source_state_v2 (
          source_table, source_filepath, index_level, state_hash, redaction_policy_version,
          source_checksum, source_updated_at, item_count, indexed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_table, source_filepath, index_level) DO UPDATE SET
          state_hash=excluded.state_hash,
          redaction_policy_version=excluded.redaction_policy_version,
          source_checksum=excluded.source_checksum,
          source_updated_at=excluded.source_updated_at,
          item_count=excluded.item_count,
          indexed_at=excluded.indexed_at
        """,
        (
            source_table,
            source_filepath,
            index_level,
            state_hash,
            REDACTION_POLICY_VERSION,
            checksum,
            updated_at,
            item_count,
            now_iso(),
        ),
    )


def cleanup_stale(vec_conn: sqlite3.Connection, reg_conn: sqlite3.Connection, stats: UpdateStats) -> None:
    live_sources: dict[str, set[str]] = {
        handler.table: {str(row[0]) for row in reg_conn.execute(f"SELECT filepath FROM {handler.table}")}
        for handler in _vector_source_handlers()
        if _table_exists(reg_conn, handler.table)
    }

    stale_sources: list[tuple[str, str, str]] = []
    for table, filepath, index_level in vec_conn.execute(
        "SELECT source_table, source_filepath, index_level FROM source_state_v2"
    ):
        live_filepaths = live_sources.get(str(table))
        if live_filepaths is not None and filepath not in live_filepaths:
            stale_sources.append((table, filepath, index_level))

    for table, filepath, index_level in stale_sources:
        cur = vec_conn.execute(
            "DELETE FROM source_state_v2 WHERE source_table = ? AND source_filepath = ? AND index_level = ?",
            (table, filepath, index_level),
        )
        stats.deleted_sources += int(cur.rowcount or 0)
        cur = vec_conn.execute(
            "DELETE FROM vector_items_v2 WHERE source_table = ? AND source_filepath = ? AND index_level = ?",
            (table, filepath, index_level),
        )
        stats.deleted_items += int(cur.rowcount or 0)


def update_index(
    registry_db: Path,
    vector_db: Path,
    *,
    embedding_client: OpenAIEmbeddingClient,
    source_selection: str = "all",
    mail_bridge_enabled: bool = False,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
    index_level: str = INDEX_LEVEL_REDACTED,
    rebuild: bool = False,
    verbose: bool = False,
    redaction_cfg: RedactionConfig | None = None,
    updated_since: str | None = None,
    consistency_pass: bool = False,
) -> int:
    if not registry_db.exists():
        print(f"error: registry db not found: {registry_db}", file=sys.stderr)
        return 2

    vector_db.parent.mkdir(parents=True, exist_ok=True)

    reg_conn = connect_vault_db(registry_db, timeout=30.0)
    vec_conn = connect_vault_db(vector_db, timeout=30.0, ensure_parent=True)
    closed = False

    all_handlers = _vector_source_handlers()
    try:
        selected_handlers = _select_active_vector_source_handlers(
            source_selection,
            mail_bridge_enabled=mail_bridge_enabled,
            mail_max_body_chunks=mail_max_body_chunks,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if redaction_cfg is None:
        redaction_cfg = RedactionConfig(mode="hybrid")

    ensure_vector_db(vec_conn)
    ensure_registry_vector_columns(reg_conn)
    ensure_redaction_table(reg_conn)
    chosen_index_level = (index_level or INDEX_LEVEL_REDACTED).strip().lower()
    if chosen_index_level not in {INDEX_LEVEL_REDACTED, INDEX_LEVEL_FULL}:
        raise ValueError(f"Unsupported index level: {index_level}")
    stats = UpdateStats(
        index_level=chosen_index_level,
        source_stats={handler.kind: SourceUpdateStats() for handler in selected_handlers},
    )
    started_mono = time.monotonic()
    last_progress_mono = started_mono
    source_totals = {
        handler.kind: (
            handler.row_count(reg_conn, updated_since)
            if handler.row_count is not None
            else count_registry_rows(reg_conn, handler.table, updated_since=updated_since)
        )
        for handler in selected_handlers
    }
    overall_total = sum(source_totals.values())

    try:
        print(
            "[progress] "
            f"[stage=index-vectors.setup] "
            "[item=0/1] "
            f"[overall=0/{max(overall_total, 0)}] "
            "[action=open-databases] "
            "[elapsed=0.0s] "
            "[eta=unknown]",
            flush=True,
        )
        redaction_scope_type = "vault"
        redaction_scope_id = "global"
        pruned_redactions = prune_invalid_redaction_entries(
            reg_conn,
            scope_type=redaction_scope_type,
            scope_id=redaction_scope_id,
        )
        if pruned_redactions > 0:
            print(
                "[progress] "
                f"[stage=index-vectors.setup] "
                "[item=1/1] "
                f"[overall=0/{max(overall_total, 0)}] "
                f"[action=pruned-invalid-redactions count={pruned_redactions}] "
                f"[elapsed={time.monotonic() - started_mono:.1f}s] "
                "[eta=unknown]",
                flush=True,
            )
        redaction_rows = fetch_redaction_entries(
            reg_conn,
            scope_type=redaction_scope_type,
            scope_id=redaction_scope_id,
        )
        redaction_map = PersistentRedactionMap.from_rows(redaction_rows)
        seed_redaction_map_key_counts(
            reg_conn,
            scope_type=redaction_scope_type,
            scope_id=redaction_scope_id,
            redaction_map=redaction_map,
        )
        stats.redaction_entries_total = len(redaction_map.value_to_placeholder)

        existing_sources = int(
            vec_conn.execute(
                "SELECT COUNT(*) FROM source_state_v2 WHERE index_level = ?",
                (chosen_index_level,),
            ).fetchone()[0]
            or 0
        )
        if existing_sources > 0:
            resume_action = (
                f"resume-consistency-pass existing_sources={existing_sources}"
                if consistency_pass
                else f"resume-existing-index existing_sources={existing_sources}"
            )
            print(
                "[progress] "
                f"[stage=index-vectors.setup] "
                "[item=1/1] "
                f"[overall=0/{max(overall_total, 0)}] "
                f"[action={resume_action}] "
                f"[elapsed={time.monotonic() - started_mono:.1f}s] "
                "[eta=unknown]",
                flush=True,
            )

        print(
            "[progress] "
            f"[stage=index-vectors.setup] "
            "[item=1/1] "
            f"[overall=0/{max(overall_total, 0)}] "
            "[action=embedding-probe] "
            f"[elapsed={time.monotonic() - started_mono:.1f}s] "
            "[eta=unknown]",
            flush=True,
        )
        target_embeddings, target_dim = embedding_client.embed_texts(["vault-index-dimension-probe"])
        if not target_embeddings or target_dim <= 0:
            raise RuntimeError("embedding endpoint did not return a valid probe embedding")

        if rebuild:
            if len(selected_handlers) == len(all_handlers):
                stats.deleted_items += int(vec_conn.execute("DELETE FROM vector_items_v2 WHERE index_level = ?", (chosen_index_level,)).rowcount or 0)
                stats.deleted_sources += int(vec_conn.execute("DELETE FROM source_state_v2 WHERE index_level = ?", (chosen_index_level,)).rowcount or 0)
            else:
                for handler in selected_handlers:
                    stats.deleted_items += int(
                        vec_conn.execute(
                            "DELETE FROM vector_items_v2 WHERE source_table = ? AND index_level = ?",
                            (handler.table, chosen_index_level),
                        ).rowcount
                        or 0
                    )
                    stats.deleted_sources += int(
                        vec_conn.execute(
                            "DELETE FROM source_state_v2 WHERE source_table = ? AND index_level = ?",
                            (handler.table, chosen_index_level),
                        ).rowcount
                        or 0
                    )
            vec_conn.commit()

        for handler in selected_handlers:
            stage = f"index-vectors.{handler.kind}"
            stage_total = int(source_totals.get(handler.kind, 0))
            last_progress_mono = _emit_vector_progress(
                stage=stage,
                stage_done=0,
                stage_total=stage_total,
                overall_done=stats.processed_sources,
                overall_total=overall_total,
                action="start",
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=verbose,
                stats=stats,
                force=True,
            )
            dim_row = vec_conn.execute(
                "SELECT embedding_dim FROM vector_items_v2 WHERE source_table = ? AND index_level = ? LIMIT 1",
                (handler.table, chosen_index_level),
            ).fetchone()
            if dim_row and int(dim_row[0]) != target_dim:
                print(
                    f"warning: {handler.kind} embedding dim changed {int(dim_row[0])} -> {target_dim}; forcing {handler.kind} reindex",
                    file=sys.stderr,
                )
                stats.deleted_items += int(
                    vec_conn.execute(
                        "DELETE FROM vector_items_v2 WHERE source_table = ? AND index_level = ?",
                        (handler.table, chosen_index_level),
                    ).rowcount
                    or 0
                )
                stats.deleted_sources += int(
                    vec_conn.execute(
                        "DELETE FROM source_state_v2 WHERE source_table = ? AND index_level = ?",
                        (handler.table, chosen_index_level),
                    ).rowcount
                    or 0
                )

            skip_batch = 0
            skip_reason: str | None = None
            skip_stage_done = 0
            skip_overall_done = 0
            source_stats = _source_update_stats(stats, handler)
            if handler.row_iterator is None or handler.item_builder is None or handler.state_hash_builder is None:
                raise RuntimeError(f"incomplete source handler: {handler.kind}")

            for row in handler.row_iterator(reg_conn, updated_since):
                stats.processed_sources += 1
                source_stats.processed += 1

                if not handler.vector_ready(row):
                    pending_reason = "waiting-source-data"
                    is_waiting = True
                    if handler.wait_state_resolver is not None:
                        pending_reason, is_waiting = handler.wait_state_resolver(row)
                    if is_waiting:
                        source_stats.waiting += 1
                    if skip_reason not in {None, pending_reason}:
                        (
                            last_progress_mono,
                            skip_batch,
                            skip_reason,
                            skip_stage_done,
                            skip_overall_done,
                        ) = _flush_vector_skip_batch(
                            batch_count=skip_batch,
                            batch_reason=skip_reason,
                            batch_stage_done=skip_stage_done,
                            batch_overall_done=skip_overall_done,
                            stage=stage,
                            stage_total=stage_total,
                            overall_total=overall_total,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=verbose,
                            stats=stats,
                        )
                    skip_reason = pending_reason
                    skip_batch += 1
                    skip_stage_done = source_stats.processed
                    skip_overall_done = stats.processed_sources
                    if time.monotonic() - last_progress_mono >= PROGRESS_HEARTBEAT_SECONDS:
                        (
                            last_progress_mono,
                            skip_batch,
                            skip_reason,
                            skip_stage_done,
                            skip_overall_done,
                        ) = _flush_vector_skip_batch(
                            batch_count=skip_batch,
                            batch_reason=skip_reason,
                            batch_stage_done=skip_stage_done,
                            batch_overall_done=skip_overall_done,
                            stage=stage,
                            stage_total=stage_total,
                            overall_total=overall_total,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=verbose,
                            stats=stats,
                        )
                    continue

                items = handler.item_builder(row)
                redaction_output_signature = (
                    redacted_output_signature_for_items(
                        items,
                        redaction_mode=redaction_cfg.mode,
                        table=redaction_map,
                    )
                    if chosen_index_level == INDEX_LEVEL_REDACTED
                    else ""
                )
                s_hash = handler.state_hash_builder(
                    row,
                    redaction_mode=redaction_cfg.mode,
                    redaction_output_signature=redaction_output_signature,
                )
                prev = load_source_state(vec_conn, handler.table, row["filepath"], index_level=chosen_index_level)
                if prev and prev[0] == s_hash and prev[1] == REDACTION_POLICY_VERSION:
                    stats.skipped_sources += 1
                    source_stats.skipped += 1
                    if skip_reason not in {None, "skipping-already-processed"}:
                        (
                            last_progress_mono,
                            skip_batch,
                            skip_reason,
                            skip_stage_done,
                            skip_overall_done,
                        ) = _flush_vector_skip_batch(
                            batch_count=skip_batch,
                            batch_reason=skip_reason,
                            batch_stage_done=skip_stage_done,
                            batch_overall_done=skip_overall_done,
                            stage=stage,
                            stage_total=stage_total,
                            overall_total=overall_total,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=verbose,
                            stats=stats,
                        )
                    skip_reason = "skipping-already-processed"
                    skip_batch += 1
                    skip_stage_done = source_stats.processed
                    skip_overall_done = stats.processed_sources
                    if time.monotonic() - last_progress_mono >= PROGRESS_HEARTBEAT_SECONDS:
                        (
                            last_progress_mono,
                            skip_batch,
                            skip_reason,
                            skip_stage_done,
                            skip_overall_done,
                        ) = _flush_vector_skip_batch(
                            batch_count=skip_batch,
                            batch_reason=skip_reason,
                            batch_stage_done=skip_stage_done,
                            batch_overall_done=skip_overall_done,
                            stage=stage,
                            stage_total=stage_total,
                            overall_total=overall_total,
                            started_mono=started_mono,
                            last_emit_mono=last_progress_mono,
                            verbose=verbose,
                            stats=stats,
                        )
                    continue

                (
                    last_progress_mono,
                    skip_batch,
                    skip_reason,
                    skip_stage_done,
                    skip_overall_done,
                ) = _flush_vector_skip_batch(
                    batch_count=skip_batch,
                    batch_reason=skip_reason,
                    batch_stage_done=skip_stage_done,
                    batch_overall_done=skip_overall_done,
                    stage=stage,
                    stage_total=stage_total,
                    overall_total=overall_total,
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=verbose,
                    stats=stats,
                )
                last_progress_mono = _emit_vector_progress(
                    stage=stage,
                    stage_done=source_stats.processed,
                    stage_total=stage_total,
                    overall_done=stats.processed_sources,
                    overall_total=overall_total,
                    action="processing",
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=verbose,
                    stats=stats,
                )
                redaction_run = redact_chunks_with_persistent_map(
                    [item.text for item in items],
                    mode=redaction_cfg.mode,
                    table=redaction_map,
                    cfg=redaction_cfg,
                )
                stats.items_redacted += redaction_run.items_redacted
                for idx, redacted_text in enumerate(redaction_run.chunk_text_redacted):
                    items[idx].text_redacted = redacted_text
                if redaction_run.inserted_entries:
                    stats.redaction_entries_added += len(redaction_run.inserted_entries)
                    stats.redaction_entries_total = upsert_redaction_entries(
                        reg_conn,
                        scope_type=redaction_scope_type,
                        scope_id=redaction_scope_id,
                        entries=redaction_run.inserted_entries,
                    )

                final_s_hash = handler.state_hash_builder(
                    row,
                    redaction_mode=redaction_cfg.mode,
                    redaction_output_signature=(
                        redacted_output_signature_for_items(items)
                        if chosen_index_level == INDEX_LEVEL_REDACTED
                        else ""
                    ),
                )

                reused = False
                if handler.embedding_reuse_handler is not None:
                    reused = bool(
                        handler.embedding_reuse_handler(
                            vec_conn,
                            row,
                            items,
                            index_level=chosen_index_level,
                            stats=stats,
                        )
                    )
                if reused:
                    upsert_source_state(
                        vec_conn,
                        source_table=handler.table,
                        source_filepath=row["filepath"],
                        index_level=chosen_index_level,
                        state_hash=final_s_hash,
                        checksum=row["checksum"] or "",
                        updated_at=row["updated_at"] or "",
                        item_count=len(items),
                    )
                    vec_conn.commit()
                    stats.indexed_sources += 1
                    source_stats.indexed += 1
                    last_progress_mono = _emit_vector_progress(
                        stage=stage,
                        stage_done=source_stats.processed,
                        stage_total=stage_total,
                        overall_done=stats.processed_sources,
                        overall_total=overall_total,
                        action=f"reused-checksum items={len(items)}",
                        started_mono=started_mono,
                        last_emit_mono=last_progress_mono,
                        verbose=verbose,
                        stats=stats,
                    )
                    continue

                texts = [
                    (it.text_redacted if chosen_index_level == INDEX_LEVEL_REDACTED else it.text)
                    for it in items
                ]
                last_progress_mono = _emit_vector_progress(
                    stage=stage,
                    stage_done=source_stats.processed,
                    stage_total=stage_total,
                    overall_done=stats.processed_sources,
                    overall_total=overall_total,
                    action=f"embedding items={len(items)}",
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=verbose,
                    stats=stats,
                )
                embeddings, embedding_dim = embedding_client.embed_texts(texts)
                if embedding_dim <= 0:
                    raise RuntimeError("embedding endpoint returned zero-dimension vectors")
                upsert_items_for_source(
                    vec_conn,
                    items,
                    embeddings=embeddings,
                    embedding_dim=embedding_dim,
                    index_level=chosen_index_level,
                    stats=stats,
                )
                upsert_source_state(
                    vec_conn,
                    source_table=handler.table,
                    source_filepath=row["filepath"],
                    index_level=chosen_index_level,
                    state_hash=final_s_hash,
                    checksum=row["checksum"] or "",
                    updated_at=row["updated_at"] or "",
                    item_count=len(items),
                )
                vec_conn.commit()
                stats.indexed_sources += 1
                source_stats.indexed += 1
                last_progress_mono = _emit_vector_progress(
                    stage=stage,
                    stage_done=source_stats.processed,
                    stage_total=stage_total,
                    overall_done=stats.processed_sources,
                    overall_total=overall_total,
                    action=f"indexed items={len(items)}",
                    started_mono=started_mono,
                    last_emit_mono=last_progress_mono,
                    verbose=verbose,
                    stats=stats,
                )
            (
                last_progress_mono,
                skip_batch,
                skip_reason,
                skip_stage_done,
                skip_overall_done,
            ) = _flush_vector_skip_batch(
                batch_count=skip_batch,
                batch_reason=skip_reason,
                batch_stage_done=skip_stage_done,
                batch_overall_done=skip_overall_done,
                stage=stage,
                stage_total=stage_total,
                overall_total=overall_total,
                started_mono=started_mono,
                last_emit_mono=last_progress_mono,
                verbose=verbose,
                stats=stats,
            )

        if (
            chosen_index_level == INDEX_LEVEL_REDACTED
            and stats.redaction_entries_added > 0
            and not consistency_pass
        ):
            print(
                "[progress] "
                "[stage=index-vectors.consistency] "
                "[item=1/1] "
                f"[overall={stats.processed_sources}/{max(overall_total, 0)}] "
                f"[action=rerun-to-reconcile-redactions new_redaction_entries={stats.redaction_entries_added}] "
                f"[elapsed={time.monotonic() - started_mono:.1f}s] "
                "[eta=unknown] "
                "[note=unchanged-sources-will-be-skipped]",
                flush=True,
            )
            reg_conn.commit()
            vec_conn.commit()
            reg_conn.close()
            vec_conn.close()
            closed = True
            return update_index(
                registry_db,
                vector_db,
                embedding_client=embedding_client,
                source_selection=source_selection,
                mail_bridge_enabled=mail_bridge_enabled,
                mail_max_body_chunks=mail_max_body_chunks,
                index_level=index_level,
                rebuild=False,
                verbose=verbose,
                redaction_cfg=redaction_cfg,
                updated_since=None,
                consistency_pass=True,
            )

        cleanup_stale(vec_conn, reg_conn, stats)
        vec_conn.commit()
        last_progress_mono = _emit_vector_progress(
            stage="index-vectors.cleanup",
            stage_done=1,
            stage_total=1,
            overall_done=stats.processed_sources,
            overall_total=overall_total,
            action="cleanup-stale",
            started_mono=started_mono,
            last_emit_mono=last_progress_mono,
            verbose=verbose,
            stats=stats,
            force=True,
        )

        item_total = int(
            vec_conn.execute(
                "SELECT COUNT(*) FROM vector_items_v2 WHERE index_level = ?",
                (chosen_index_level,),
            ).fetchone()[0]
        )
        source_total = int(
            vec_conn.execute(
                "SELECT COUNT(*) FROM source_state_v2 WHERE index_level = ?",
                (chosen_index_level,),
            ).fetchone()[0]
        )
        available_levels = ",".join(sorted(_vector_levels_available(vec_conn))) or "none"
        if stats.redaction_entries_total <= 0:
            stats.redaction_entries_total = len(
                fetch_redaction_entries(
                    reg_conn,
                    scope_type=redaction_scope_type,
                    scope_id=redaction_scope_id,
                )
            )
        print(
            "status: ok "
            f"index_level={chosen_index_level} "
            f"available_levels={available_levels} "
            f"selected_sources={','.join(handler.kind for handler in selected_handlers) or 'none'} "
            f"processed_sources={stats.processed_sources} "
            f"indexed_sources={stats.indexed_sources} "
            f"skipped_sources={stats.skipped_sources} "
            f"deleted_sources={stats.deleted_sources} "
            f"upserted_items={stats.upserted_items} "
            f"deleted_items={stats.deleted_items} "
            f"items_redacted={stats.items_redacted} "
            f"redaction_entries_added={stats.redaction_entries_added} "
            f"redaction_entries_total={stats.redaction_entries_total} "
            f"source_stats={json.dumps(_source_stats_payload(stats, selected_handlers), sort_keys=True, separators=(',', ':'))} "
            f"total_sources={source_total} "
            f"total_items={item_total}"
        )
        return 0
    finally:
        if not closed:
            reg_conn.close()
            vec_conn.close()


def query_index(
    registry_db: Path,
    vector_db: Path,
    query: str,
    *,
    top_k: int,
    embedding_client: OpenAIEmbeddingClient,
    source_selection: str | None = None,
    mail_bridge_enabled: bool = False,
    mail_max_body_chunks: int = DEFAULT_MAIL_MAX_BODY_CHUNKS,
    clearance: str = "redacted",
    search_level: str = INDEX_LEVEL_AUTO,
    from_date: str | None = None,
    to_date: str | None = None,
    taxonomy: str | None = None,
    category_primary: str | None = None,
    as_json: bool = False,
    verbose: bool = False,
) -> int:
    if not vector_db.exists():
        print(f"error: vector db not found: {vector_db}", file=sys.stderr)
        return 2
    if not registry_db.exists():
        print(f"error: registry db not found: {registry_db}", file=sys.stderr)
        return 2

    from_dt = _parse_iso_datetime(from_date)
    to_dt = _resolve_to_datetime(to_date)
    if from_dt is not None and to_dt is not None and from_dt >= to_dt:
        print("error: --from-date must be earlier than --to-date", file=sys.stderr)
        return 2

    vec_conn = connect_vault_db(vector_db, timeout=30.0)
    reg_conn = connect_vault_db(registry_db, timeout=30.0)
    ensure_vector_db(vec_conn)
    ensure_registry_vector_columns(reg_conn)
    ensure_redaction_table(reg_conn)
    try:
        try:
            selected_handlers = _select_active_vector_source_handlers(
                source_selection,
                mail_bridge_enabled=mail_bridge_enabled,
                mail_max_body_chunks=mail_max_body_chunks,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        selected_tables = [handler.table for handler in selected_handlers]
        try:
            diagnostics = _resolve_effective_search_level(
                vec_conn,
                clearance=clearance,
                search_level=search_level,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        chosen_level = diagnostics.used_level
        use_legacy = False

        total_candidates = 0
        if _table_exists(vec_conn, "vector_items_v2"):
            count_sql = "SELECT COUNT(*) FROM vector_items_v2 WHERE index_level = ?"
            count_params: list[Any] = [chosen_level]
            if selected_tables and len(selected_tables) < len(_vector_source_handlers()):
                if len(selected_tables) == 1:
                    count_sql += " AND source_table = ?"
                    count_params.append(selected_tables[0])
                else:
                    placeholders = ",".join("?" for _ in selected_tables)
                    count_sql += f" AND source_table IN ({placeholders})"
                    count_params.extend(selected_tables)
            total_candidates = int(vec_conn.execute(count_sql, count_params).fetchone()[0])

        if total_candidates == 0 and chosen_level == INDEX_LEVEL_REDACTED and _table_exists(vec_conn, "vector_items"):
            legacy_sql = "SELECT COUNT(*) FROM vector_items"
            legacy_params: list[str] = []
            if selected_tables and len(selected_tables) < len(_vector_source_handlers()):
                if len(selected_tables) == 1:
                    legacy_sql += " WHERE source_table = ?"
                    legacy_params.append(selected_tables[0])
                else:
                    placeholders = ",".join("?" for _ in selected_tables)
                    legacy_sql += f" WHERE source_table IN ({placeholders})"
                    legacy_params.extend(selected_tables)
            total_candidates = int(vec_conn.execute(legacy_sql, legacy_params).fetchone()[0])
            use_legacy = total_candidates > 0

        if total_candidates == 0:
            payload = {
                "query": query,
                "count": 0,
                "clearance": clearance,
                "diagnostics": {
                    "search_level_requested": diagnostics.requested_level,
                    "search_level_used": diagnostics.used_level,
                    "search_level_fallback": diagnostics.fallback_from_level,
                    "full_level_available": diagnostics.full_level_available,
                },
                "results": [],
            }
            if as_json:
                payload["note"] = "no vectors indexed for selected search level"
                print(json.dumps(payload, indent=2, ensure_ascii=False))
            else:
                print("no vectors indexed")
            return 0

        query_text = query
        if chosen_level == INDEX_LEVEL_REDACTED:
            redaction_rows = fetch_redaction_entries(
                reg_conn,
                scope_type="vault",
                scope_id="global",
            )
            if redaction_rows:
                query_text = PersistentRedactionMap.from_rows(redaction_rows).apply(query)

        query_embeddings, q_dim = embedding_client.embed_texts([query_text])
        if not query_embeddings:
            print("no query embedding returned", file=sys.stderr)
            return 2

        q = blob_to_floats(query_embeddings[0])

        if use_legacy:
            sql = """
                SELECT item_id, source_table, source_filepath, chunk_index, chunk_count,
                       text_preview, text_preview_full, text_preview_redacted, metadata_json, embedding, source_updated_at
                FROM vector_items
            """
            params: list[str] = []
            if selected_tables and len(selected_tables) < len(_vector_source_handlers()):
                if len(selected_tables) == 1:
                    sql += " WHERE source_table = ?"
                    params.append(selected_tables[0])
                else:
                    placeholders = ",".join("?" for _ in selected_tables)
                    sql += f" WHERE source_table IN ({placeholders})"
                    params.extend(selected_tables)
        else:
            sql = """
                SELECT item_id, source_table, source_filepath, chunk_index, chunk_count,
                       text_preview, text_preview_full, text_preview_redacted, metadata_json, embedding, source_updated_at
                FROM vector_items_v2
                WHERE index_level = ?
            """
            params = [chosen_level]
            if selected_tables and len(selected_tables) < len(_vector_source_handlers()):
                if len(selected_tables) == 1:
                    sql += " AND source_table = ?"
                    params.append(selected_tables[0])
                else:
                    placeholders = ",".join("?" for _ in selected_tables)
                    sql += f" AND source_table IN ({placeholders})"
                    params.extend(selected_tables)

        heap: list[tuple[float, int, dict]] = []
        seq = 0
        skipped_dim_mismatch = 0
        scanned = 0
        started_mono = time.monotonic()
        last_emit_mono = started_mono
        taxonomy_filter = (taxonomy or "").strip().lower()
        category_filter = (category_primary or "").strip().lower()
        for row in vec_conn.execute(sql, params):
            scanned += 1
            now_mono = time.monotonic()
            if (not as_json) and _should_emit_progress(
                verbose=verbose,
                now_mono=now_mono,
                last_emit_mono=last_emit_mono,
                completed=scanned,
                total=total_candidates,
                force=(scanned == 1),
            ):
                elapsed = max(0.0, now_mono - started_mono)
                eta = _estimate_eta(elapsed, scanned, total_candidates)
                print(
                    "[progress] "
                    f"[stage=query-vectors] "
                    f"[item={scanned}/{max(total_candidates, 0)}] "
                    "[action=scan] "
                    f"[elapsed={elapsed:.1f}s] "
                    f"[eta={_format_eta(eta)}] "
                    f"[candidates_kept={len(heap)}] "
                    f"[skipped_dim={skipped_dim_mismatch}]",
                    flush=True,
                )
                last_emit_mono = now_mono
            metadata = json.loads(row["metadata_json"] or "{}")
            if taxonomy_filter and str(metadata.get("taxonomy") or "").strip().lower() != taxonomy_filter:
                continue
            if category_filter and str(metadata.get("category_primary") or "").strip().lower() != category_filter:
                continue
            if not _matches_time_filters(
                source_updated_at=row["source_updated_at"],
                date_taken=str(metadata.get("date_taken") or ""),
                primary_date=str(metadata.get("primary_date") or ""),
                from_dt=from_dt,
                to_dt=to_dt,
            ):
                continue

            v = blob_to_floats(row["embedding"])
            if len(v) != q_dim:
                skipped_dim_mismatch += 1
                continue
            score = dot(q, v)
            payload = {
                "score": score,
                "item_id": row["item_id"],
                "source_table": row["source_table"],
                "source_filepath": row["source_filepath"],
                "source_updated_at": row["source_updated_at"],
                "chunk_index": row["chunk_index"],
                "chunk_count": row["chunk_count"],
                "text_preview_redacted": row["text_preview_redacted"] or row["text_preview"] or "",
                "text_preview_full": row["text_preview_full"] or row["text_preview"] or "",
                "metadata": metadata,
            }
            entry = (score, seq, payload)
            seq += 1
            if len(heap) < top_k:
                heapq.heappush(heap, entry)
            else:
                heapq.heappushpop(heap, entry)

        best = [item for _, _, item in sorted(heap, key=lambda x: x[0], reverse=True)]
        if not best:
            if skipped_dim_mismatch:
                msg = (
                    f"no matches (all candidates had embedding_dim != query_dim {q_dim}; "
                    f"skipped={skipped_dim_mismatch})"
                )
            else:
                msg = "no matches"
            if as_json:
                print(
                    json.dumps(
                        {
                            "query": query,
                            "count": 0,
                            "clearance": clearance,
                            "diagnostics": {
                                "search_level_requested": diagnostics.requested_level,
                                "search_level_used": diagnostics.used_level,
                                "search_level_fallback": diagnostics.fallback_from_level,
                                "full_level_available": diagnostics.full_level_available,
                            },
                            "results": [],
                            "note": msg,
                        },
                        indent=2,
                        ensure_ascii=False,
                    )
                )
            else:
                print(msg)
            return 0

        out_results: list[dict[str, Any]] = []
        for idx, hit in enumerate(best, start=1):
            full_preview = str(hit.get("text_preview_full") or "").replace("\n", " ").strip()
            redacted_preview = str(hit.get("text_preview_redacted") or "").replace("\n", " ").strip()
            if clearance == "full":
                preview = full_preview
            else:
                preview = redacted_preview
            source_table_name = str(hit["source_table"])
            source_kind = source_kind_for_table(
                source_table_name,
                handlers=_vector_source_handlers(mail_max_body_chunks=mail_max_body_chunks),
            ) or ""
            source_filepath = str(hit["source_filepath"])
            metadata = _sanitize_metadata_for_output(
                metadata=hit.get("metadata") or {},
                clearance=clearance,
                source_table=source_table_name,
                source_filepath=source_filepath,
            )
            source_id = _stable_source_id(source_table_name, source_filepath)
            out_results.append(
                {
                    "rank": idx,
                    "score": round(float(hit["score"]), 6),
                    "item_id": hit["item_id"],
                    "source_kind": source_kind,
                    "source_table": source_table_name,
                    "source_id": source_id,
                    "source_filepath": source_filepath if clearance == "full" else None,
                    "source_updated_at": hit.get("source_updated_at"),
                    "chunk_index": int(hit["chunk_index"]),
                    "chunk_count": int(hit["chunk_count"]),
                    "preview": preview,
                    "clearance": clearance,
                    "metadata": metadata,
                }
            )

        if as_json:
            print(
                json.dumps(
                    {
                        "query": query,
                        "count": len(out_results),
                        "clearance": clearance,
                        "diagnostics": {
                            "search_level_requested": diagnostics.requested_level,
                            "search_level_used": diagnostics.used_level,
                            "search_level_fallback": diagnostics.fallback_from_level,
                            "full_level_available": diagnostics.full_level_available,
                        },
                        "results": out_results,
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return 0

        for item in out_results:
            print(
                f"{item['rank']}. score={item['score']:.4f} table={item['source_table']} "
                f"chunk={item['chunk_index']+1}/{item['chunk_count']}"
            )
            print(f"   {_display_source_label(item=item, clearance=clearance)}")
            if item["preview"]:
                print(f"   preview={item['preview']}")
        return 0
    finally:
        vec_conn.close()
        reg_conn.close()


def print_stats(vector_db: Path) -> int:
    if not vector_db.exists():
        print(f"error: vector db not found: {vector_db}", file=sys.stderr)
        return 2
    conn = connect_vault_db(vector_db, timeout=30.0)
    try:
        ensure_vector_db(conn)
        levels = sorted(_vector_levels_available(conn))
        parts = [f"available_index_levels={','.join(levels) or 'none'}"]
        if _table_exists(conn, "vector_items"):
            parts.append(f"legacy_items={int(_safe_scalar(conn, 'SELECT COUNT(*) FROM vector_items') or 0)}")
        if _table_exists(conn, "source_state"):
            parts.append(f"legacy_sources={int(_safe_scalar(conn, 'SELECT COUNT(*) FROM source_state') or 0)}")
        if _table_exists(conn, "vector_items_v2"):
            source_handlers = _vector_source_handlers()
            for level in levels:
                level_items = int(
                    _safe_scalar(conn, "SELECT COUNT(*) FROM vector_items_v2 WHERE index_level = ?", (level,)) or 0
                )
                level_sources = int(
                    _safe_scalar(conn, "SELECT COUNT(*) FROM source_state_v2 WHERE index_level = ?", (level,)) or 0
                )
                level_parts = [
                    f"{level}_items={level_items}",
                    f"{level}_sources={level_sources}",
                ]
                for handler in source_handlers:
                    level_source_items = int(
                        _safe_scalar(
                            conn,
                            "SELECT COUNT(*) FROM vector_items_v2 WHERE index_level = ? AND source_table = ?",
                            (level, handler.table),
                        )
                        or 0
                    )
                    level_source_states = int(
                        _safe_scalar(
                            conn,
                            "SELECT COUNT(*) FROM source_state_v2 WHERE index_level = ? AND source_table = ?",
                            (level, handler.table),
                        )
                        or 0
                    )
                    level_parts.extend(
                        [
                            f"{level}_{handler.kind}_items={level_source_items}",
                            f"{level}_{handler.kind}_sources={level_source_states}",
                        ]
                    )
                parts.extend(level_parts)
        print(" ".join(parts))
        return 0
    finally:
        conn.close()


def resolve_embedding_config(args: argparse.Namespace) -> EmbeddingConfig:
    env_api = os.getenv("VAULT_EMBED_API_KEY")
    if env_api is None:
        env_api = os.getenv("OPENAI_API_KEY")
    api_key = args.embed_api_key if args.embed_api_key is not None else (env_api if env_api is not None else "local")

    return EmbeddingConfig(
        base_url=args.embed_base_url,
        model=args.embed_model,
        api_key=api_key,
        timeout_seconds=max(1, int(args.embed_timeout)),
        batch_size=max(1, int(args.embed_batch_size)),
        batch_tokens=max(256, int(args.embed_batch_tokens)),
        max_text_chars=max(256, int(args.embed_max_text_chars)),
        verbose=bool(args.verbose),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Maintain/query local vault vector index")
    p.add_argument("command", nargs="?", default="update", choices=["update", "rebuild", "query", "stats"])
    p.add_argument("query_text", nargs="?", default="")
    p.add_argument("--registry-db", default=DEFAULT_REGISTRY_DB)
    p.add_argument("--vectors-db", default=DEFAULT_VECTOR_DB)
    p.add_argument("--dim", type=int, default=256, help="deprecated: ignored, dimensions come from embedding model")
    p.add_argument("--top-k", type=int, default=5)
    p.add_argument("--source", choices=source_choices(_vector_source_handlers()), default="all")
    p.add_argument("--clearance", choices=["redacted", "full"], default="redacted")
    p.add_argument(
        "--search-level",
        choices=[INDEX_LEVEL_AUTO, INDEX_LEVEL_REDACTED, INDEX_LEVEL_FULL],
        default=INDEX_LEVEL_AUTO,
        help="choose which index level to rank against; auto prefers full only when available and requested by clearance",
    )
    p.add_argument(
        "--index-level",
        choices=[INDEX_LEVEL_REDACTED, INDEX_LEVEL_FULL],
        default=INDEX_LEVEL_REDACTED,
        help="choose which index level to build for update/rebuild",
    )
    p.add_argument("--json", action="store_true", help="emit JSON output for query results")
    p.add_argument(
        "--from-date",
        default=None,
        help="Optional inclusive UTC lower bound. YYYY-MM-DD or ISO-8601 datetime.",
    )
    p.add_argument(
        "--to-date",
        default=None,
        help="Optional exclusive UTC upper bound. YYYY-MM-DD resolves to next-day midnight UTC.",
    )
    p.add_argument(
        "--updated-since",
        default=None,
        help="Optional incremental-update lower bound for source updated_at timestamps.",
    )
    p.add_argument("--taxonomy", default=None, help="Optional taxonomy filter (photos)")
    p.add_argument(
        "--category-primary",
        default=None,
        help="Optional photo category filter (for example receipt, screenshot)",
    )
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--embed-base-url", default=os.getenv("VAULT_EMBED_BASE_URL", DEFAULT_EMBED_BASE_URL))
    p.add_argument("--embed-model", default=os.getenv("VAULT_EMBED_MODEL", DEFAULT_EMBED_MODEL))
    p.add_argument("--embed-api-key", default=None)
    p.add_argument(
        "--embed-timeout",
        type=int,
        default=int(os.getenv("VAULT_EMBED_TIMEOUT_SECONDS", str(DEFAULT_EMBED_TIMEOUT_SECONDS))),
    )
    p.add_argument(
        "--embed-batch-size",
        type=int,
        default=int(os.getenv("VAULT_EMBED_BATCH_SIZE", str(DEFAULT_EMBED_BATCH_SIZE))),
    )
    p.add_argument(
        "--embed-batch-tokens",
        type=int,
        default=int(os.getenv("VAULT_EMBED_BATCH_TOKENS", str(DEFAULT_EMBED_BATCH_TOKENS))),
        help="Approximate per-request token budget for embedding batches.",
    )
    p.add_argument(
        "--embed-max-text-chars",
        type=int,
        default=int(os.getenv("VAULT_EMBED_MAX_TEXT_CHARS", str(DEFAULT_EMBED_MAX_TEXT_CHARS))),
        help="Hard cap for a single text chunk before embedding.",
    )
    p.add_argument(
        "--mail-max-body-chunks",
        type=int,
        default=DEFAULT_MAIL_MAX_BODY_CHUNKS,
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--yes-rebuild",
        action="store_true",
        help="confirm destructive rebuild without interactive prompt",
    )
    p.add_argument("--disable-redaction", action="store_true")
    p.add_argument(
        "--redaction-mode",
        choices=["regex", "model", "hybrid"],
        default=os.getenv("VAULT_REDACTION_MODE", "hybrid"),
    )
    p.add_argument("--redaction-profile", default=os.getenv("VAULT_REDACTION_PROFILE", "standard"))
    p.add_argument("--redaction-instruction", default=os.getenv("VAULT_REDACTION_INSTRUCTION", ""))
    p.add_argument("--redaction-base-url", default=os.getenv("VAULT_REDACTION_BASE_URL", DEFAULT_REDACTION_BASE_URL))
    p.add_argument("--redaction-model", default=os.getenv("VAULT_REDACTION_MODEL", DEFAULT_REDACTION_MODEL))
    p.add_argument("--redaction-api-key", default=os.getenv("VAULT_REDACTION_API_KEY", "local"))
    p.add_argument(
        "--redaction-timeout",
        type=int,
        default=int(os.getenv("VAULT_REDACTION_TIMEOUT_SECONDS", str(DEFAULT_REDACTION_TIMEOUT_SECONDS))),
    )
    p.add_argument("--mail-bridge-enabled", action="store_true", help=argparse.SUPPRESS)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    registry_db = Path(args.registry_db)
    vectors_db = Path(args.vectors_db)

    if args.dim != 256:
        print("warning: --dim is deprecated and ignored (using embedding model dimensions)", file=sys.stderr)

    source_selection = str(args.source or "all")
    mail_bridge_enabled = bool(args.mail_bridge_enabled)
    mail_max_body_chunks = max(0, int(args.mail_max_body_chunks))

    embed_cfg = resolve_embedding_config(args)
    embedding_client = OpenAIEmbeddingClient(embed_cfg)
    redaction_cfg = _resolve_redaction_config(args)

    if args.command == "update":
        return update_index(
            registry_db,
            vectors_db,
            embedding_client=embedding_client,
            source_selection=source_selection,
            mail_bridge_enabled=mail_bridge_enabled,
            mail_max_body_chunks=mail_max_body_chunks,
            index_level=args.index_level,
            rebuild=False,
            verbose=args.verbose,
            redaction_cfg=redaction_cfg,
            updated_since=args.updated_since,
        )
    if args.command == "rebuild":
        if not confirm_rebuild(
            registry_db,
            vectors_db,
            source_selection=source_selection,
            mail_bridge_enabled=mail_bridge_enabled,
            mail_max_body_chunks=mail_max_body_chunks,
            index_level=args.index_level,
            assume_yes=bool(args.yes_rebuild),
        ):
            return 2
        return update_index(
            registry_db,
            vectors_db,
            embedding_client=embedding_client,
            source_selection=source_selection,
            mail_bridge_enabled=mail_bridge_enabled,
            mail_max_body_chunks=mail_max_body_chunks,
            index_level=args.index_level,
            rebuild=True,
            verbose=args.verbose,
            redaction_cfg=redaction_cfg,
            updated_since=args.updated_since,
        )
    if args.command == "stats":
        return print_stats(vectors_db)

    query_text = (args.query_text or "").strip()
    if not query_text:
        print("error: query text required, e.g. vault_vector_index.py query 'tax receipt'", file=sys.stderr)
        return 2
    return query_index(
        registry_db,
        vectors_db,
        query_text,
        top_k=max(1, args.top_k),
        embedding_client=embedding_client,
        source_selection=source_selection,
        mail_bridge_enabled=mail_bridge_enabled,
        mail_max_body_chunks=mail_max_body_chunks,
        clearance=args.clearance,
        search_level=args.search_level,
        from_date=args.from_date,
        to_date=args.to_date,
        taxonomy=args.taxonomy,
        category_primary=args.category_primary,
        as_json=bool(args.json),
        verbose=bool(args.verbose),
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
