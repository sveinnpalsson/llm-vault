#!/usr/bin/env python3
"""Shared source registry metadata for llm-vault handlers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

RowIterator = Callable[[sqlite3.Connection, str | None], Iterable[sqlite3.Row]]
RowCounter = Callable[[sqlite3.Connection, str | None], int]
VectorReady = Callable[[sqlite3.Row], bool]
ItemBuilder = Callable[[sqlite3.Row], list[Any]]
StateHashBuilder = Callable[..., str]
WaitStateResolver = Callable[[sqlite3.Row | dict[str, Any]], tuple[str, bool]]
InspectionPreparer = Callable[..., dict[str, Any]]
RegistryStatsCollector = Callable[[sqlite3.Connection], dict[str, Any]]
VectorMetadataExtractor = Callable[[sqlite3.Connection | None, str], dict[str, Any]]
EmbeddingReuseHandler = Callable[..., bool]


@dataclass(frozen=True)
class SourceHandler:
    kind: str
    table: str
    label: str
    row_iterator: RowIterator | None = None
    row_count: RowCounter | None = None
    vector_ready: VectorReady | None = None
    item_builder: ItemBuilder | None = None
    state_hash_builder: StateHashBuilder | None = None
    wait_state_resolver: WaitStateResolver | None = None
    inspection_preparer: InspectionPreparer | None = None
    registry_stats_collector: RegistryStatsCollector | None = None
    vector_metadata_extractor: VectorMetadataExtractor | None = None
    embedding_reuse_handler: EmbeddingReuseHandler | None = None
    checksum_reuse_supported: bool = False


REGISTERED_SOURCES: tuple[SourceHandler, ...] = (
    SourceHandler(kind="docs", table="docs_registry", label="Documents"),
    SourceHandler(kind="photos", table="photos_registry", label="Photos"),
    SourceHandler(kind="mail", table="mail_registry", label="Mail"),
)


def iter_registered_sources(handlers: Sequence[SourceHandler] = REGISTERED_SOURCES) -> tuple[SourceHandler, ...]:
    return tuple(handlers)


def source_choices(handlers: Sequence[SourceHandler] = REGISTERED_SOURCES) -> list[str]:
    return ["all", *(handler.kind for handler in handlers)]


def source_handler_by_kind(
    kind: str,
    *,
    handlers: Sequence[SourceHandler] = REGISTERED_SOURCES,
) -> SourceHandler:
    normalized = str(kind or "").strip().lower()
    for handler in handlers:
        if handler.kind == normalized:
            return handler
    raise KeyError(f"unknown source kind: {kind}")


def source_handler_by_table(
    table: str,
    *,
    handlers: Sequence[SourceHandler] = REGISTERED_SOURCES,
) -> SourceHandler:
    normalized = str(table or "").strip()
    for handler in handlers:
        if handler.table == normalized:
            return handler
    raise KeyError(f"unknown source table: {table}")


def source_kind_for_table(
    table: str,
    *,
    handlers: Sequence[SourceHandler] = REGISTERED_SOURCES,
) -> str | None:
    try:
        return source_handler_by_table(table, handlers=handlers).kind
    except KeyError:
        return None


def select_source_handlers(
    selection: str | None,
    *,
    handlers: Sequence[SourceHandler] = REGISTERED_SOURCES,
) -> tuple[SourceHandler, ...]:
    normalized = str(selection or "all").strip().lower() or "all"
    if normalized == "all":
        return tuple(handlers)
    try:
        return (source_handler_by_kind(normalized, handlers=handlers),)
    except KeyError as exc:
        choices = ", ".join(source_choices(handlers))
        raise ValueError(f"unknown source selection: {selection!r} (choices: {choices})") from exc


def source_tables(
    selection: str | None,
    *,
    handlers: Sequence[SourceHandler] = REGISTERED_SOURCES,
) -> list[str]:
    return [handler.table for handler in select_source_handlers(selection, handlers=handlers)]


def select_active_source_handlers(
    selection: str | None,
    *,
    enabled_kinds: set[str] | None = None,
    handlers: Sequence[SourceHandler] = REGISTERED_SOURCES,
) -> tuple[SourceHandler, ...]:
    normalized = str(selection or "all").strip().lower() or "all"
    enabled = set(enabled_kinds or {handler.kind for handler in handlers})
    if normalized == "all":
        return tuple(handler for handler in handlers if handler.kind in enabled)

    handler = source_handler_by_kind(normalized, handlers=handlers)
    if handler.kind not in enabled:
        raise ValueError(f"source kind is unavailable: {handler.kind}")
    return (handler,)
