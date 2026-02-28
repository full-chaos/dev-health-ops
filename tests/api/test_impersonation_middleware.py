"""Tests for impersonation contextvar lifecycle and cache helpers.

Mirrors the pattern in tests/api/test_org_context.py: no DB required,
tests operate directly on contextvars and the in-memory cache module.
"""

from __future__ import annotations

import contextvars
import time
import types
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from dev_health_ops.api.middleware.impersonation import ImpersonationMiddleware
from dev_health_ops.api.services.auth import (
    _current_org_id,
    _impersonation_ctx,
    get_current_org_id,
    get_impersonation_context,
    is_impersonating,
    set_impersonation_context,
)
from dev_health_ops.api.services.impersonation_cache import _cache, invalidate

# ---------------------------------------------------------------------------
# Contextvar: is_impersonating
# ---------------------------------------------------------------------------


def test_is_impersonating_returns_false_when_no_context_set():
    """is_impersonating() returns False when the contextvar is unset."""
    try:
        _impersonation_ctx.set(None)
        assert is_impersonating() is False
    finally:
        _impersonation_ctx.set(None)


def test_is_impersonating_returns_true_when_context_active():
    """is_impersonating() returns True once set_impersonation_context() is called."""
    try:
        token = set_impersonation_context(
            target_user_id="user-1",
            target_org_id="org-1",
            target_role="member",
            real_user_id="admin-1",
        )
        assert is_impersonating() is True
        _impersonation_ctx.reset(token)
    finally:
        _impersonation_ctx.set(None)


# ---------------------------------------------------------------------------
# Contextvar: set / get round-trip
# ---------------------------------------------------------------------------


def test_set_and_get_impersonation_context_round_trip():
    """set_impersonation_context stores fields; get_impersonation_context retrieves them."""
    try:
        _impersonation_ctx.set(None)
        assert get_impersonation_context() is None

        token = set_impersonation_context(
            target_user_id="user-42",
            target_org_id="org-99",
            target_role="admin",
            real_user_id="superadmin-1",
        )
        ctx = get_impersonation_context()
        assert ctx is not None
        assert ctx.target_user_id == "user-42"
        assert ctx.target_org_id == "org-99"
        assert ctx.target_role == "admin"
        assert ctx.real_user_id == "superadmin-1"
        assert ctx.is_active is True

        _impersonation_ctx.reset(token)
        assert get_impersonation_context() is None
    finally:
        _impersonation_ctx.set(None)


def test_set_impersonation_context_returns_resettable_token():
    """set_impersonation_context returns a contextvars.Token usable for reset."""
    try:
        token = set_impersonation_context(
            target_user_id="user-a",
            target_org_id="org-a",
            target_role="viewer",
            real_user_id="admin-a",
        )
        assert isinstance(token, contextvars.Token)
        # Confirm active before reset
        assert get_impersonation_context() is not None
        # Reset restores None
        _impersonation_ctx.reset(token)
        assert get_impersonation_context() is None
    finally:
        _impersonation_ctx.set(None)


def test_get_impersonation_context_returns_none_when_unset():
    """get_impersonation_context() returns None before any value is set."""
    try:
        _impersonation_ctx.set(None)
        result = get_impersonation_context()
        assert result is None
    finally:
        _impersonation_ctx.set(None)


# ---------------------------------------------------------------------------
# Context isolation
# ---------------------------------------------------------------------------


def test_impersonation_contextvar_isolation_between_contexts():
    """ContextVar values are isolated between different execution contexts."""
    try:
        set_impersonation_context(
            target_user_id="user-main",
            target_org_id="org-main",
            target_role="member",
            real_user_id="admin-main",
        )
        assert get_impersonation_context().target_user_id == "user-main"

        def run_in_child():
            set_impersonation_context(
                target_user_id="user-child",
                target_org_id="org-child",
                target_role="viewer",
                real_user_id="admin-child",
            )
            return get_impersonation_context()

        new_context = contextvars.copy_context()
        child_result = new_context.run(run_in_child)

        # Child context has child value
        assert child_result.target_user_id == "user-child"
        # Main context unchanged
        assert get_impersonation_context().target_user_id == "user-main"
    finally:
        _impersonation_ctx.set(None)


# ---------------------------------------------------------------------------
# Cache: invalidate
# ---------------------------------------------------------------------------


def test_invalidate_removes_existing_cache_entry():
    """invalidate() removes a cached entry for the given admin_user_id."""
    admin_id = "test-admin-cache-entry"
    try:
        _cache[admin_id] = (None, time.monotonic() + 30.0)
        assert admin_id in _cache
        invalidate(admin_id)
        assert admin_id not in _cache
    finally:
        _cache.pop(admin_id, None)


def test_invalidate_nonexistent_key_is_silent_noop():
    """invalidate() does not raise when key is absent from cache."""
    admin_id = "ghost-admin-id"
    assert admin_id not in _cache
    # Must not raise
    invalidate(admin_id)
    assert admin_id not in _cache


def test_is_impersonating_false_after_context_reset():
    """After resetting the contextvar token, is_impersonating() returns False again."""
    try:
        token = set_impersonation_context(
            target_user_id="u",
            target_org_id="o",
            target_role="member",
            real_user_id="admin",
        )
        assert is_impersonating() is True
        _impersonation_ctx.reset(token)
        assert is_impersonating() is False
    finally:
        _impersonation_ctx.set(None)


# ---------------------------------------------------------------------------
# ASGI middleware: integration-style behaviour tests
#
# These tests exercise ImpersonationMiddleware as a full ASGI component by
# wrapping it in a minimal test-only stack and sending real HTTP requests via
# httpx.AsyncClient + ASGITransport.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test-stack helpers
# ---------------------------------------------------------------------------


def _fake_superuser(admin_user_id: str = "admin-asgi-1") -> Any:
    """Minimal superuser object: is_superuser=True, has user_id."""
    u = types.SimpleNamespace()
    u.user_id = admin_user_id
    u.is_superuser = True
    return u


def _fake_regular_user() -> Any:
    """Minimal non-superuser object."""
    u = types.SimpleNamespace()
    u.user_id = "regular-user-asgi"
    u.is_superuser = False
    return u


def _fake_session(
    admin_user_id: str = "admin-asgi-1",
    target_user_id: str = "target-asgi-1",
    target_org_id: str = "org-asgi-1",
    target_role: str = "member",
    expires_in: int = 300,
) -> Any:
    """Minimal ImpersonationSession-like object (no SQLAlchemy required)."""
    s = types.SimpleNamespace()
    s.admin_user_id = admin_user_id
    s.target_user_id = target_user_id
    s.target_org_id = target_org_id
    s.target_role = target_role
    s.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    s.ended_at = None
    return s


class _UserStateMiddleware:
    """Inject a fake user into scope['state'] so ImpersonationMiddleware can read it.

    Must wrap ImpersonationMiddleware on the outside so it runs first.
    """

    def __init__(self, app: Any, user: Any) -> None:
        self.app = app
        self.user = user

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] in ("http", "websocket"):
            state = types.SimpleNamespace()
            state.user = self.user
            scope["state"] = state
        await self.app(scope, receive, send)


def _build_test_app(user: Any, captured: dict) -> Any:
    """Build a minimal ASGI stack for impersonation middleware tests.

    Request flow::

        _UserStateMiddleware → ImpersonationMiddleware → _handler

    The handler records contextvar values in *captured* so tests can assert on them.
    """

    async def _handler(scope: Any, receive: Any, send: Any) -> None:
        captured["org_id"] = get_current_org_id()
        captured["imp_ctx"] = get_impersonation_context()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b"{}"})

    return _UserStateMiddleware(ImpersonationMiddleware(_handler), user)


_PATCH_TARGET = "dev_health_ops.api.middleware.impersonation.get_active_session"


# ---------------------------------------------------------------------------
# Test 1: org_id contextvar is overridden with target org when impersonating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asgi_middleware_sets_org_id_contextvar_when_impersonating():
    """_current_org_id is set to target_org_id when an active session exists."""
    admin_id = "admin-asgi-org"
    target_user_id = "target-asgi-org"
    target_org_id = "org-asgi-org"

    session = _fake_session(admin_id, target_user_id, target_org_id)
    captured: dict = {}
    app = _build_test_app(_fake_superuser(admin_id), captured)

    with patch(_PATCH_TARGET, AsyncMock(return_value=session)):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get("/")
        finally:
            _current_org_id.set(None)
            _impersonation_ctx.set(None)

    assert response.status_code == 200
    assert captured["org_id"] == target_org_id, (
        f"Expected org_id={target_org_id!r}, got {captured['org_id']!r}"
    )
    imp = captured["imp_ctx"]
    assert imp is not None, "ImpersonationContext should be set in handler"
    assert imp.target_user_id == target_user_id
    assert imp.target_org_id == target_org_id
    assert imp.real_user_id == admin_id


# ---------------------------------------------------------------------------
# Test 2: X-Impersonating and X-Impersonated-User-Id headers injected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asgi_middleware_injects_response_headers_when_impersonating():
    """Response has X-Impersonating: true and X-Impersonated-User-Id header.

    The header values must match the active session's target_user_id.
    """
    admin_id = "admin-asgi-hdr"
    target_user_id = "target-asgi-hdr"
    target_org_id = "org-asgi-hdr"

    session = _fake_session(admin_id, target_user_id, target_org_id)
    captured: dict = {}
    app = _build_test_app(_fake_superuser(admin_id), captured)

    with patch(_PATCH_TARGET, AsyncMock(return_value=session)):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get("/")
        finally:
            _current_org_id.set(None)
            _impersonation_ctx.set(None)

    assert response.status_code == 200
    assert response.headers.get("x-impersonating") == "true"
    assert response.headers.get("x-impersonated-user-id") == target_user_id


# ---------------------------------------------------------------------------
# Test 3: No impersonation headers when get_active_session returns None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asgi_middleware_no_headers_when_no_active_session():
    """When get_active_session returns None, no impersonation headers are injected.

    The request passes through transparently and contextvars are not set.
    """
    captured: dict = {}
    app = _build_test_app(_fake_superuser("admin-asgi-none"), captured)

    with patch(_PATCH_TARGET, AsyncMock(return_value=None)):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/")

    assert response.status_code == 200
    assert "x-impersonating" not in response.headers
    assert "x-impersonated-user-id" not in response.headers
    assert captured.get("imp_ctx") is None, "ImpersonationContext must not be set"


# ---------------------------------------------------------------------------
# Test 4: Non-superuser request passes through without activating impersonation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asgi_middleware_passthrough_for_non_superuser():
    """ImpersonationMiddleware is a no-op for non-superuser users.

    get_active_session must NOT be called and no impersonation headers are added.
    """
    captured: dict = {}
    app = _build_test_app(_fake_regular_user(), captured)

    with patch(_PATCH_TARGET, AsyncMock(return_value=_fake_session())) as mock_session:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/")

    assert response.status_code == 200
    assert "x-impersonating" not in response.headers
    mock_session.assert_not_called()
