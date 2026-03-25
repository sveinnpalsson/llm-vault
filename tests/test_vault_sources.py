from __future__ import annotations

from vault_sources import (
    REGISTERED_SOURCES,
    SourceHandler,
    select_source_handlers,
    source_choices,
    source_handler_by_kind,
    source_handler_by_table,
    source_tables,
)


def test_lookup_by_kind_and_table_uses_registered_sources() -> None:
    docs = source_handler_by_kind("docs")
    photos = source_handler_by_table("photos_registry")

    assert docs.table == "docs_registry"
    assert photos.kind == "photos"


def test_select_source_handlers_supports_all_docs_and_photos() -> None:
    assert [handler.kind for handler in select_source_handlers("all")] == ["docs", "photos", "mail"]
    assert [handler.kind for handler in select_source_handlers("docs")] == ["docs"]
    assert [handler.kind for handler in select_source_handlers("photos")] == ["photos"]
    assert [handler.kind for handler in select_source_handlers("mail")] == ["mail"]
    assert source_tables("photos") == ["photos_registry"]


def test_custom_future_source_can_be_registered_in_tests() -> None:
    handlers = REGISTERED_SOURCES + (
        SourceHandler(kind="calendar", table="calendar_registry", label="Calendar"),
    )

    assert source_choices(handlers) == ["all", "docs", "photos", "mail", "calendar"]
    assert [handler.kind for handler in select_source_handlers("calendar", handlers=handlers)] == ["calendar"]
