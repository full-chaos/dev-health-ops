"""Tests for impersonation contextvar lifecycle and cache helpers.

Mirrors the pattern in tests/api/test_org_context.py: no DB required,
tests operate directly on contextvars and the in-memory cache module.
"""

from __future__ import annotations

import contextvars
import time

import pytest

from dev_health_ops.api.services.auth import (
    ImpersonationContext,
    _impersonation_ctx,
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
