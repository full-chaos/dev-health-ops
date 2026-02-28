from __future__ import annotations

import contextvars
from typing import Any

import pytest

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.services.auth import (
    _current_org_id,
    get_current_org_id,
    set_current_org_id,
)


class FakeSink:
    """Mock sink for testing query_dicts org_id injection."""

    def __init__(self):
        self.last_query: str | None = None
        self.last_params: dict[str, Any] | None = None

    def query_dicts(
        self, query: str, params: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        """Capture query and params, return empty result."""
        self.last_query = query
        self.last_params = params
        return []


def test_set_and_get_current_org_id_round_trip():
    """Test basic set/get round-trip for org_id contextvar."""
    try:
        # Initially unset
        assert get_current_org_id() is None

        # Set org_id
        token = set_current_org_id("org-123")
        assert get_current_org_id() == "org-123"

        # Reset using token
        _current_org_id.reset(token)
        assert get_current_org_id() is None
    finally:
        _current_org_id.set(None)


def test_get_current_org_id_returns_none_when_unset():
    """Test that get_current_org_id returns None when contextvar is unset."""
    try:
        _current_org_id.set(None)
        assert get_current_org_id() is None
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_query_dicts_injects_org_id_from_contextvar():
    """Test that query_dicts injects org_id from contextvar when set."""
    try:
        sink = FakeSink()
        set_current_org_id("org-456")

        await query_dicts(sink, "SELECT * FROM table", {"other_param": "value"})

        # Verify org_id was injected
        assert sink.last_params is not None
        assert sink.last_params["org_id"] == "org-456"
        assert sink.last_params["other_param"] == "value"
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_query_dicts_does_not_inject_org_id_when_unset():
    """Test that query_dicts does NOT inject org_id when contextvar is unset."""
    try:
        sink = FakeSink()
        _current_org_id.set(None)

        await query_dicts(sink, "SELECT * FROM table", {"other_param": "value"})

        # Verify org_id was NOT injected
        assert sink.last_params is not None
        assert "org_id" not in sink.last_params
        assert sink.last_params["other_param"] == "value"
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_query_dicts_overrides_explicit_org_id_with_contextvar():
    """Test that query_dicts overrides explicit org_id param with contextvar value."""
    try:
        sink = FakeSink()
        set_current_org_id("org-789")

        # Pass explicit org_id that should be overridden
        await query_dicts(sink, "SELECT * FROM table", {"org_id": "org-old"})

        # Verify contextvar value overrides explicit param
        assert sink.last_params is not None
        assert sink.last_params["org_id"] == "org-789"
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_query_dicts_with_none_params():
    """Test that query_dicts handles None params correctly."""
    try:
        sink = FakeSink()
        set_current_org_id("org-abc")

        await query_dicts(sink, "SELECT * FROM table", None)

        # Verify org_id was injected even with None params
        assert sink.last_params is not None
        assert sink.last_params["org_id"] == "org-abc"
    finally:
        _current_org_id.set(None)


def test_contextvar_isolation_between_contexts():
    """Test that contextvar values are isolated between different contexts."""
    try:
        # Set org_id in main context
        set_current_org_id("org-main")
        assert get_current_org_id() == "org-main"

        # Create a new context and set different org_id
        def check_in_new_context():
            set_current_org_id("org-child")
            return get_current_org_id()

        new_context = contextvars.copy_context()
        result = new_context.run(check_in_new_context)

        # New context should have its own value
        assert result == "org-child"

        # Main context should still have original value
        assert get_current_org_id() == "org-main"
    finally:
        _current_org_id.set(None)
