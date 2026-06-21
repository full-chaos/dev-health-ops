"""Regression tests for the unit-worker runtime store lifecycle (CHAOS-2592).

The fan-out runtime cache (``ProviderRuntimeCache``) reuses one store across
many units. Before CHAOS-2592 the store was created but never entered, so
``ClickHouseStore.client`` stayed ``None`` and ``insert_repo`` asserted. These
tests lock the contract: ``_create_store`` enters the store's async context on
creation, and ``ProviderRuntime.close`` exits it on eviction.
"""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import Mock, patch

import pytest

from dev_health_ops.workers.sync_bootstrap import (
    ProviderRuntime,
    SyncTaskContext,
    _create_store,
)


class _FakeStore:
    """Async-context-manager store double mirroring ClickHouseStore's contract.

    ``client`` is only assigned inside ``__aenter__`` -- exactly the invariant
    the regression violated when the cache reused an un-entered store.
    """

    def __init__(self) -> None:
        self.client: Any | None = None
        self.org_id: str | None = None
        self.entered = 0
        self.exited = 0

    async def __aenter__(self) -> _FakeStore:
        self.client = object()
        self.entered += 1
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.client = None
        self.exited += 1


def _context(db_url: str = "clickhouse://localhost/default") -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="full-chaos/dev-health",
        provider="github",
        dataset_key="repo-metadata",
        cost_class="medium",
        mode="incremental",
        window_start=None,
        window_end=None,
        processor_flags={},
        credential_id="credential-1",
        decrypted_credentials={"token": "secret"},
        db_url=db_url,
    )


def test_create_store_enters_async_context_so_client_is_connected() -> None:
    store = _FakeStore()
    with patch("dev_health_ops.storage.create_store", return_value=store):
        created = _create_store(_context())

    assert created is store
    # Regression (CHAOS-2592): the store must be entered so ``client`` is set
    # before the cached runtime reuses it across units.
    assert store.entered == 1
    assert store.client is not None
    assert store.org_id == "org-1"


def test_create_store_returns_none_without_db_url() -> None:
    assert _create_store(_context(db_url="")) is None


def test_provider_runtime_close_exits_store_context() -> None:
    store = _FakeStore()
    with patch("dev_health_ops.storage.create_store", return_value=store):
        runtime = ProviderRuntime(store=_create_store(_context()))

    assert store.client is not None
    runtime.close()
    # Regression (CHAOS-2592): eviction must release the underlying client.
    assert store.exited == 1
    assert store.client is None


def test_create_store_skips_non_clickhouse_stores() -> None:
    # Hardening (CHAOS-2592, review finding 1): SQLAlchemy-backed analytics
    # stores hold loop-bound async sessions and MUST NOT be eager-entered and
    # cached for cross-loop reuse. _create_store returns None for them so the
    # per-unit run_with_store() path handles their lifecycle. create_store is
    # never even constructed for non-ClickHouse URLs.
    create_store = Mock(name="create_store")
    with patch("dev_health_ops.storage.create_store", create_store):
        result = _create_store(_context(db_url="postgresql://user@host/analytics"))

    assert result is None
    create_store.assert_not_called()


class _FailOnEnterStore:
    """Store whose __aenter__ opens the client and THEN fails, like a
    ClickHouseStore that connects then raises in _ensure_tables."""

    def __init__(self) -> None:
        self.client: object | None = None
        self.org_id: str | None = None
        self.exited = 0

    async def __aenter__(self) -> _FailOnEnterStore:
        self.client = object()
        raise RuntimeError("ensure_tables failed")

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.client = None
        self.exited += 1


def test_create_store_cleans_up_when_enter_fails_after_opening_client() -> None:
    # Hardening (CHAOS-2592, review finding 3): if __aenter__ opens the client
    # and then raises, the runtime is never cached and close() never runs, so
    # _create_store must best-effort __aexit__ to avoid leaking a live client.
    store = _FailOnEnterStore()
    with patch("dev_health_ops.storage.create_store", return_value=store):
        with pytest.raises(RuntimeError, match="ensure_tables failed"):
            _create_store(_context())

    assert store.exited == 1
    assert store.client is None


def test_concurrent_get_enters_exactly_one_store_per_key() -> None:
    # Hardening (CHAOS-2592, review finding 2): two worker threads missing the
    # same key must not both build + enter a store (leaking the loser's live
    # client). The lock + re-check guarantees exactly one creation per key.
    from dev_health_ops.workers.sync_bootstrap import ProviderRuntimeCache

    created: list[_FakeStore] = []
    created_lock = threading.Lock()

    def _factory(*args: Any, **kwargs: Any) -> _FakeStore:
        # Widen the race window so an unlocked get() would create two stores.
        time.sleep(0.02)
        store = _FakeStore()
        with created_lock:
            created.append(store)
        return store

    cache = ProviderRuntimeCache()
    ctx = _context()
    barrier = threading.Barrier(2)
    results: list[ProviderRuntime] = []
    results_lock = threading.Lock()

    def _worker() -> None:
        barrier.wait()
        runtime = cache.get(ctx)
        with results_lock:
            results.append(runtime)

    with patch("dev_health_ops.storage.create_store", _factory):
        threads = [threading.Thread(target=_worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert len(created) == 1
    assert created[0].entered == 1
    assert results[0] is results[1]
