"""CHAOS-3631: no caller waits on a hung graph store past its deadline.

``GraphArmStore`` used to construct its FalkorDB client with no connect,
socket or command deadline at all -- redis-py's documented "block forever"
defaults. A backend that accepts a connection but never answers (a firewall
that drops rather than refuses, a wedged server, an exhausted connection
slot) wedged the calling coroutine, and everything downstream of it,
indefinitely.

Two kinds of test, deliberately separated:

* fake-driver tests reproduce a HUNG operation deterministically and fast --
  a real network hang is not reliably scriptable in CI, so a coroutine that
  sleeps past a short configured deadline stands in for one. These prove the
  bound actually bounds: every store operation returns
  :class:`GraphOperationTimeoutError` within timeout + slack, never hangs,
  never returns an empty result instead of raising;
* live-store tests against the real container prove the deadline plumbing
  does not break the happy path the existing CHAOS-3617 live suite measures
  -- a normal operation completes well inside the configured deadline.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.backend import DeterministicEmbedder
from dev_health_ops.context_fabric.graph_arm.flags import (
    DEFAULT_GRAPH_CONNECT_TIMEOUT_S,
    DEFAULT_GRAPH_MAX_CONNECTIONS,
    DEFAULT_GRAPH_READ_TIMEOUT_S,
    DEFAULT_GRAPH_SOCKET_TIMEOUT_S,
    DEFAULT_GRAPH_WRITE_TIMEOUT_S,
    GRAPH_CONNECT_TIMEOUT_VAR,
    GRAPH_MAX_CONNECTIONS_VAR,
    GRAPH_READ_TIMEOUT_VAR,
    GRAPH_SOCKET_TIMEOUT_VAR,
    GRAPH_WRITE_TIMEOUT_VAR,
    GraphDeadlines,
    graph_deadlines,
)
from dev_health_ops.context_fabric.graph_arm.store import (
    GraphArmStore,
    GraphOperationTimeoutError,
    StoreUnavailableError,
    partition_exists_for,
)
from tests.context_fabric import live_gate

#: Short enough that every "hung" test completes in well under a second, and
#: comfortably longer than the small amount of real work (list/dict/asyncio
#: bookkeeping) the fake driver performs around its sleep. Both the read and
#: write bounds are set short: these tests exercise EVERY store operation,
#: including the write path, and each must trip its own bound fast.
_FAST_DEADLINES = GraphDeadlines(
    connect_timeout_s=0.05,
    socket_timeout_s=0.05,
    read_timeout_s=0.05,
    write_timeout_s=0.05,
)

#: How much past the configured deadline a bounded call may run before the
#: test itself concludes the bound did not bind. Generous relative to
#: ``_FAST_DEADLINES``'s 0.05s bounds so this is not a scheduler-jitter
#: flake, and still an order of magnitude tighter than "did not hang at all".
_SLACK_S = 2.0


class _HangingGraph:
    """Stands in for ``falkordb``'s ``select_graph(...)`` result."""

    def __init__(self, hang_seconds: float) -> None:
        self._hang_seconds = hang_seconds

    async def delete(self) -> None:
        await asyncio.sleep(self._hang_seconds)


class _HangingClient:
    """Stands in for ``FalkorDriver.client`` (the bare ``falkordb`` client)."""

    def __init__(self, hang_seconds: float) -> None:
        self._hang_seconds = hang_seconds
        self.closed = False

    async def list_graphs(self) -> list[str]:
        await asyncio.sleep(self._hang_seconds)
        return []

    def select_graph(self, _name: str) -> _HangingGraph:
        return _HangingGraph(self._hang_seconds)

    async def aclose(self) -> None:
        self.closed = True


class _HangingDriver:
    """Stands in for the Graphiti ``FalkorDriver``: every operation hangs."""

    def __init__(self, hang_seconds: float = 999.0) -> None:
        self._hang_seconds = hang_seconds
        self.client = _HangingClient(hang_seconds)
        self.closed = False

    async def health_check(self) -> None:
        await asyncio.sleep(self._hang_seconds)

    async def build_indices_and_constraints(self) -> None:
        await asyncio.sleep(self._hang_seconds)

    async def close(self) -> None:
        await asyncio.sleep(self._hang_seconds)
        self.closed = True

    async def execute_query(self, *_args: Any, **_kwargs: Any) -> Any:
        await asyncio.sleep(self._hang_seconds)
        return None


def _hanging_store(hang_seconds: float = 999.0) -> GraphArmStore:
    return GraphArmStore(
        org_id="orghang123",
        driver=_HangingDriver(hang_seconds),
        # A real, non-semantic embedder: cheap, makes no network call itself,
        # and write_projection's budget check touches `.semantic` before the
        # hanging operation is ever reached.
        embedder=DeterministicEmbedder(),
        deadlines=_FAST_DEADLINES,
    )


async def _times_out(coro: Any) -> float:
    """Run ``coro``, asserting it raises the timeout error, and time it.

    Returns wall-clock seconds. Callers assert this against the configured
    deadline plus :data:`_SLACK_S` -- proving the bound actually bounded the
    call, not merely that the right exception type was eventually raised
    (which a bound with no enforcement could also satisfy after a real hang).
    """

    start = time.monotonic()
    with pytest.raises(GraphOperationTimeoutError):
        await coro
    return time.monotonic() - start


class TestEveryStoreOperationIsBounded:
    """Each operation: hangs past the deadline -> raises, fast, and typed."""

    pytestmark = pytest.mark.asyncio

    async def test_health_check_times_out(self) -> None:
        store = _hanging_store()
        elapsed = await _times_out(store.health_check())
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S

    async def test_build_indices_times_out(self) -> None:
        store = _hanging_store()
        elapsed = await _times_out(store.build_indices())
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S

    async def test_close_times_out(self) -> None:
        store = _hanging_store()
        elapsed = await _times_out(store.close())
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S

    async def test_partition_exists_times_out(self) -> None:
        store = _hanging_store()
        elapsed = await _times_out(store.partition_exists())
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S

    async def test_count_nodes_times_out(self) -> None:
        """``count_nodes`` calls ``partition_exists`` first, which hangs."""

        store = _hanging_store()
        elapsed = await _times_out(store.count_nodes())
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S

    async def test_purge_org_times_out(self) -> None:
        """``purge_org`` calls ``partition_exists`` first, which hangs."""

        store = _hanging_store()
        elapsed = await _times_out(store.purge_org())
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S

    async def test_write_projection_times_out(self, monkeypatch) -> None:
        """The hang is monkeypatched at ``utils.bulk_utils`` only.

        ``write_projection`` builds real ``EntityNode``/``EntityEdge``
        objects via ``backend.to_graphiti_nodes``/``to_graphiti_edges``
        *before* the bounded call this test is timing, and those go through
        ``backend.graphiti_module`` -- a different reference than
        ``store.graphiti_module``, which is all this test patches. In an
        environment with no ``context-graph-trial`` extra installed at all
        (a real CI lane), that node/edge construction raises
        ``GraphitiUnavailableError`` before the hang is ever reached, which
        is a real "graphiti missing" condition, not the timeout this test
        measures -- so it must skip there, exactly like the sibling
        ``test_chaos_3619_observation_attachment.py`` tests that also build
        real Graphiti objects.
        """

        live_gate.require_graphiti_extra()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")

        class _HangingBulkUtils:
            @staticmethod
            async def add_nodes_and_edges_bulk(*_args: Any, **_kwargs: Any) -> None:
                await asyncio.sleep(999.0)

        from dev_health_ops.context_fabric.graph_arm import store as store_module

        monkeypatch.setattr(
            store_module,
            "graphiti_module",
            lambda dotted="": (
                _HangingBulkUtils
                if dotted == "utils.bulk_utils"
                else pytest.fail(f"unexpected graphiti_module({dotted!r})")
            ),
        )

        projection = build_projection(fixtures.alpha_batch())
        # Reorg the projection onto this test's org so write_projection's
        # ownership check passes.
        from dataclasses import replace

        from dev_health_ops.context_fabric.graph_arm.identity import (
            partition_for_org,
        )

        org_id = "orghang123"
        projection = replace(
            projection, org_id=org_id, partition=partition_for_org(org_id)
        )
        store = _hanging_store()
        elapsed = await _times_out(store.write_projection(projection))
        assert elapsed < _FAST_DEADLINES.write_timeout_s + _SLACK_S

    async def test_a_timeout_is_a_store_unavailable_error(self) -> None:
        """Every existing "graph unavailable -> fall back" caller is covered.

        ``GraphOperationTimeoutError`` is a ``StoreUnavailableError``
        subclass on purpose: a caller written against "catch
        StoreUnavailableError, fall back to the non-graph path" needs no
        separate except-clause to also survive a hang.
        """

        assert issubclass(GraphOperationTimeoutError, StoreUnavailableError)
        store = _hanging_store()
        with pytest.raises(StoreUnavailableError):
            await store.health_check()

    async def test_the_timeout_message_is_content_safe(self) -> None:
        """No entity label, title or body -- only operation, org id, partition."""

        store = _hanging_store()
        with pytest.raises(GraphOperationTimeoutError) as excinfo:
            await store.health_check()
        message = str(excinfo.value)
        assert "health_check" in message
        assert "orghang123" in message
        assert store.partition in message


class TestPartitionExistsForIsBounded:
    pytestmark = pytest.mark.asyncio

    async def test_a_hung_bare_client_times_out(self, monkeypatch) -> None:
        from dev_health_ops.context_fabric.graph_arm import store as store_module
        from dev_health_ops.context_fabric.graph_arm.flags import TrialStoreConfig

        hanging_client = _HangingClient(999.0)

        class _FakeFalkorDBModule:
            @staticmethod
            def FalkorDB(**_kwargs: Any) -> _HangingClient:
                return hanging_client

        monkeypatch.setattr(
            store_module,
            "graphiti_module",
            lambda dotted="": (
                _FakeFalkorDBModule
                if dotted == "driver.falkordb_driver"
                else pytest.fail(f"unexpected graphiti_module({dotted!r})")
            ),
        )

        elapsed = await _times_out(
            partition_exists_for(
                "orghang123",
                TrialStoreConfig(uri="falkor://127.0.0.1:6389"),
                deadlines=_FAST_DEADLINES,
            )
        )
        assert elapsed < _FAST_DEADLINES.read_timeout_s + _SLACK_S
        # The client is still closed on the way out, even though the
        # operation it was opened for timed out.
        assert hanging_client.closed is True


class TestForOrgConfiguresTheUnderlyingClient:
    """No live server needed: assert on the kwargs the client was built with."""

    def test_for_org_passes_deadlines_to_the_falkordb_client(self, monkeypatch) -> None:
        from dev_health_ops.context_fabric.graph_arm import store as store_module
        from dev_health_ops.context_fabric.graph_arm.flags import TrialStoreConfig

        captured_client_kwargs: dict[str, Any] = {}
        captured_driver_kwargs: dict[str, Any] = {}
        created_clients: list[Any] = []

        class _FakeClient:
            pass

        class _FakeFalkorDBModule:
            @staticmethod
            def FalkorDB(**kwargs: Any) -> _FakeClient:
                captured_client_kwargs.update(kwargs)
                client = _FakeClient()
                created_clients.append(client)
                return client

            @staticmethod
            def FalkorDriver(**kwargs: Any) -> object:
                captured_driver_kwargs.update(kwargs)
                return object()

        monkeypatch.setattr(
            store_module,
            "graphiti_module",
            lambda dotted="": (
                _FakeFalkorDBModule
                if dotted == "driver.falkordb_driver"
                else pytest.fail(f"unexpected graphiti_module({dotted!r})")
            ),
        )

        deadlines = GraphDeadlines(
            connect_timeout_s=1.5, socket_timeout_s=2.5, max_connections=7
        )
        store = GraphArmStore.for_org(
            "orgconfig123",
            config=TrialStoreConfig(uri="falkor://127.0.0.1:6389"),
            deadlines=deadlines,
        )

        assert captured_client_kwargs["socket_connect_timeout"] == 1.5
        assert captured_client_kwargs["socket_timeout"] == 2.5
        assert captured_client_kwargs["max_connections"] == 7
        # The driver must be constructed from the pre-built, timeout-bearing
        # client -- not from bare host/port, which is what let FalkorDriver
        # build its own unconfigured client in the first place.
        assert len(created_clients) == 1
        assert captured_driver_kwargs["falkor_db"] is created_clients[0]
        assert "host" not in captured_driver_kwargs
        assert "port" not in captured_driver_kwargs
        assert store.deadlines == deadlines


class TestGraphDeadlinesConfig:
    def test_defaults_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            GRAPH_CONNECT_TIMEOUT_VAR,
            GRAPH_SOCKET_TIMEOUT_VAR,
            GRAPH_READ_TIMEOUT_VAR,
            GRAPH_WRITE_TIMEOUT_VAR,
            GRAPH_MAX_CONNECTIONS_VAR,
        ):
            monkeypatch.delenv(var, raising=False)
        deadlines = graph_deadlines()
        assert deadlines.connect_timeout_s == DEFAULT_GRAPH_CONNECT_TIMEOUT_S
        assert deadlines.socket_timeout_s == DEFAULT_GRAPH_SOCKET_TIMEOUT_S
        assert deadlines.read_timeout_s == DEFAULT_GRAPH_READ_TIMEOUT_S
        assert deadlines.write_timeout_s == DEFAULT_GRAPH_WRITE_TIMEOUT_S
        assert deadlines.max_connections == DEFAULT_GRAPH_MAX_CONNECTIONS

    def test_env_overrides_are_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(GRAPH_CONNECT_TIMEOUT_VAR, "1.5")
        monkeypatch.setenv(GRAPH_SOCKET_TIMEOUT_VAR, "2.5")
        monkeypatch.setenv(GRAPH_READ_TIMEOUT_VAR, "3.5")
        monkeypatch.setenv(GRAPH_WRITE_TIMEOUT_VAR, "4.5")
        monkeypatch.setenv(GRAPH_MAX_CONNECTIONS_VAR, "42")
        deadlines = graph_deadlines()
        assert deadlines.connect_timeout_s == 1.5
        assert deadlines.socket_timeout_s == 2.5
        assert deadlines.read_timeout_s == 3.5
        assert deadlines.write_timeout_s == 4.5
        assert deadlines.max_connections == 42

    def test_a_non_numeric_override_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(GRAPH_CONNECT_TIMEOUT_VAR, "soon")
        with pytest.raises(ValueError, match="is not a number"):
            graph_deadlines()

    def test_a_zero_or_negative_timeout_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must be a positive number"):
            GraphDeadlines(connect_timeout_s=0)
        with pytest.raises(ValueError, match="must be a positive number"):
            GraphDeadlines(socket_timeout_s=-1.0)

    def test_a_non_positive_max_connections_is_refused(self) -> None:
        with pytest.raises(ValueError, match="max_connections must be"):
            GraphDeadlines(max_connections=0)


class TestLiveOperationsCompleteWellInsideTheDeadline:
    """Positive control: the deadline plumbing does not break the happy path."""

    pytestmark = pytest.mark.asyncio

    @pytest_asyncio.fixture
    async def live_store(self, monkeypatch) -> AsyncIterator[GraphArmStore]:
        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        org_id = "orgdeadline1"
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            yield store
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()

    @pytest.mark.graphiti
    async def test_health_check_completes_well_inside_the_deadline(
        self, live_store
    ) -> None:
        start = time.monotonic()
        await live_store.health_check()
        elapsed = time.monotonic() - start
        assert elapsed < live_store.deadlines.read_timeout_s / 2

    @pytest.mark.graphiti
    async def test_count_nodes_on_an_empty_partition_completes(
        self, live_store
    ) -> None:
        assert await live_store.count_nodes() == 0
