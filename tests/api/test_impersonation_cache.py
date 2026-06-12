"""Tests for the Valkey-backed impersonation session cache (CHAOS-2328).

Uses fakeredis (FakeAsyncValkey) so the shared-store semantics — the whole
point of the migration away from the per-process dict — can be exercised:
an invalidate issued through ONE client connection must be observed by every
other client of the same store, which is how api replicas/workers share it.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

fakeredis = pytest.importorskip("fakeredis")

from dev_health_ops.api.services import impersonation_cache as cache  # noqa: E402
from dev_health_ops.api.services.impersonation_cache import (  # noqa: E402
    CachedImpersonationSession,
    get_active_session,
    invalidate,
)


def _make_cached(admin_id: str | None = None) -> CachedImpersonationSession:
    return CachedImpersonationSession(
        id=str(uuid.uuid4()),
        admin_user_id=admin_id or str(uuid.uuid4()),
        target_user_id=str(uuid.uuid4()),
        target_org_id=str(uuid.uuid4()),
        target_role="member",
        target_email="target@example.com",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )


@pytest.fixture
def shared_server():
    return fakeredis.FakeServer()


@pytest.fixture
def valkey_client(shared_server, monkeypatch):
    """Wire the module's client to a fakeredis store and reset the circuit."""
    client = fakeredis.FakeAsyncValkey(server=shared_server)
    monkeypatch.setattr(cache, "_client", client)
    monkeypatch.setattr(cache, "_circuit_open_until", 0.0)
    return client


@pytest.mark.asyncio
async def test_positive_result_is_cached(valkey_client, monkeypatch):
    """A DB-loaded session is served from Valkey on subsequent lookups."""
    session = _make_cached()
    loader = AsyncMock(return_value=session)
    monkeypatch.setattr(cache, "_load_from_db", loader)

    first = await get_active_session(session.admin_user_id)
    second = await get_active_session(session.admin_user_id)

    assert first == session
    assert second == session
    loader.assert_awaited_once()


@pytest.mark.asyncio
async def test_negative_result_is_cached(valkey_client, monkeypatch):
    """'No active session' is cached too — the common case for superusers."""
    loader = AsyncMock(return_value=None)
    monkeypatch.setattr(cache, "_load_from_db", loader)
    admin_id = str(uuid.uuid4())

    assert await get_active_session(admin_id) is None
    assert await get_active_session(admin_id) is None
    loader.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalidate_forces_db_reload(valkey_client, monkeypatch):
    """After invalidate(), the next lookup goes back to Postgres."""
    session = _make_cached()
    loader = AsyncMock(return_value=session)
    monkeypatch.setattr(cache, "_load_from_db", loader)

    await get_active_session(session.admin_user_id)
    await invalidate(session.admin_user_id)
    await get_active_session(session.admin_user_id)

    assert loader.await_count == 2


@pytest.mark.asyncio
async def test_invalidate_is_observed_across_clients(
    shared_server, valkey_client, monkeypatch
):
    """A DEL through another client of the same store is seen here.

    This is the CHAOS-2328 regression: with the old per-process dict, a stop
    handled by one replica left every other replica serving the stale session
    for up to 30s. With a shared store there is exactly one entry to delete.
    """
    session = _make_cached()
    loader = AsyncMock(return_value=session)
    monkeypatch.setattr(cache, "_load_from_db", loader)

    # Replica A fills the cache
    assert await get_active_session(session.admin_user_id) == session
    loader.assert_awaited_once()

    # "Replica B" (a second connection to the same store) handles the stop
    other_client = fakeredis.FakeAsyncValkey(server=shared_server)
    monkeypatch.setattr(cache, "_client", other_client)
    await invalidate(session.admin_user_id)

    # Replica A's next lookup misses and reloads from Postgres
    monkeypatch.setattr(cache, "_client", valkey_client)
    loader.return_value = None
    assert await get_active_session(session.admin_user_id) is None
    assert loader.await_count == 2


@pytest.mark.asyncio
async def test_valkey_outage_falls_back_to_db_and_trips_circuit(monkeypatch):
    """Valkey errors bypass the cache (fail-correct) and open the circuit."""

    class _BrokenClient:
        calls = 0

        async def get(self, *_args):
            _BrokenClient.calls += 1
            raise ConnectionError("valkey down")

        async def set(self, *_args, **_kwargs):
            raise ConnectionError("valkey down")

        async def delete(self, *_args):
            raise ConnectionError("valkey down")

    monkeypatch.setattr(cache, "_client", _BrokenClient())
    monkeypatch.setattr(cache, "_circuit_open_until", 0.0)
    session = _make_cached()
    loader = AsyncMock(return_value=session)
    monkeypatch.setattr(cache, "_load_from_db", loader)

    # Both lookups still resolve correctly from Postgres
    assert await get_active_session(session.admin_user_id) == session
    assert await get_active_session(session.admin_user_id) == session
    assert loader.await_count == 2

    # The circuit tripped on the first error: only one client.get attempt
    assert _BrokenClient.calls == 1

    # invalidate() during the open circuit must not raise
    await invalidate(session.admin_user_id)


@pytest.mark.asyncio
async def test_unconfigured_valkey_goes_straight_to_db(monkeypatch):
    """Without REDIS_URL there is no cache — every lookup hits Postgres."""
    monkeypatch.setattr(cache, "_client", None)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setattr(cache, "_circuit_open_until", 0.0)
    session = _make_cached()
    loader = AsyncMock(return_value=session)
    monkeypatch.setattr(cache, "_load_from_db", loader)

    assert await get_active_session(session.admin_user_id) == session
    assert await get_active_session(session.admin_user_id) == session
    assert loader.await_count == 2


def test_serialization_round_trip_preserves_fields():
    """JSON round-trip keeps every field, tz-aware datetime included."""
    session = _make_cached()
    assert cache._deserialize(cache._serialize(session)) == session

    no_email = CachedImpersonationSession(
        id=session.id,
        admin_user_id=session.admin_user_id,
        target_user_id=session.target_user_id,
        target_org_id=session.target_org_id,
        target_role=session.target_role,
        target_email=None,
        expires_at=session.expires_at,
    )
    assert cache._deserialize(cache._serialize(no_email)) == no_email

    assert cache._deserialize(cache._serialize(None)) is None
