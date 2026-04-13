"""Tests for the context layer: discovery, schema inference, caching."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.context_layer import (
    ContextLayer,
    ContextStore,
    PaginationInfo,
    SourceContext,
    _infer_type,
    _merge_schema_field,
)


def test_infer_type_primitives():
    assert _infer_type(None) == "null"
    assert _infer_type(True) == "boolean"
    assert _infer_type(42) == "integer"
    assert _infer_type(3.14) == "number"
    assert _infer_type("hello") == "string"
    assert _infer_type([1, 2]) == "array"
    assert _infer_type({"a": 1}) == "object"


def test_merge_schema_field():
    columns: dict[str, dict] = {}
    _merge_schema_field(columns, "age", 25)
    assert "age" in columns
    assert "integer" in columns["age"]["types"]
    assert columns["age"]["nullable"] is False

    _merge_schema_field(columns, "age", None)
    assert columns["age"]["nullable"] is True
    assert "null" in columns["age"]["types"]

    _merge_schema_field(columns, "age", 3.5)
    assert "number" in columns["age"]["types"]


def test_source_context_schema_alias():
    ctx = SourceContext(
        source_id="test",
        inferred_schema={"columns": {"a": {"types": ["string"]}}},
        discovered_at=datetime.now(timezone.utc),
    )
    assert ctx.schema == ctx.inferred_schema
    assert "columns" in ctx.schema


def test_pagination_info_defaults():
    p = PaginationInfo()
    assert p.style is None
    assert p.total_count is None
    assert p.next_url is None


@pytest.mark.asyncio
async def test_context_store_ttl():
    store = ContextStore(ttl_seconds=0.1)
    ctx = SourceContext(
        source_id="test",
        inferred_schema={},
        discovered_at=datetime.now(timezone.utc),
    )
    await store.set(ctx)
    assert await store.get("test") is not None

    await asyncio.sleep(0.15)
    assert await store.get("test") is None


@pytest.mark.asyncio
async def test_context_store_miss():
    store = ContextStore(ttl_seconds=60)
    assert await store.get("nonexistent") is None


@pytest.mark.asyncio
async def test_context_layer_caches():
    config = {
        "context_layer": {
            "discovery": {"timeout_seconds": 5, "max_retries": 1, "cache_ttl_seconds": 3600},
            "reflection": {"sample_size": 10},
        }
    }
    layer = ContextLayer(config)

    mock_ctx = SourceContext(
        source_id="cached_source",
        inferred_schema={"columns": {"id": {"types": ["integer"]}}},
        estimated_rows=100,
        content_type="application/json",
        discovered_at=datetime.now(timezone.utc),
    )
    await layer._store.set(mock_ctx)

    result = await layer.get_or_discover("cached_source", {"connection": {"base_url": "http://example.com"}})
    assert result.source_id == "cached_source"
    assert result.estimated_rows == 100


def test_source_context_serialization():
    ctx = SourceContext(
        source_id="ser_test",
        inferred_schema={"columns": {"x": {"types": ["string"]}}},
        estimated_rows=50,
        content_type="application/json",
        etag='"abc123"',
        discovered_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        pagination_info=PaginationInfo(style="offset", total_count=500),
    )
    data = ctx.model_dump(mode="json", by_alias=True)
    assert data["source_id"] == "ser_test"
    assert data["estimated_rows"] == 50
    assert data["pagination_info"]["total_count"] == 500

    roundtrip = SourceContext.model_validate(data)
    assert roundtrip.source_id == ctx.source_id
    assert roundtrip.estimated_rows == ctx.estimated_rows
