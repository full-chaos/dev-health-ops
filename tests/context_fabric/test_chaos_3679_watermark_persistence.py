"""CHAOS-3679: the projection watermark survives past the writing process.

``IndexWatermark`` used to exist only as ``GraphArmStore.write_projection``'s
in-memory return value -- nothing durable recorded it. A process other than
the one that performed the write (in particular, a future read-side query
service, which will typically be a different process from whatever last
projected the organization) had no way to learn a partition's
``indexed_through``/staleness at all.

``GraphArmStore.read_watermark`` closes that: it is a plain Redis key on the
FalkorDB client's own connection, not a Cypher graph node -- confirmed
deliberately, not assumed, by the negative-control tests below that pin
``count_nodes()`` unchanged. A graph node would have been counted by
``NODE_COUNT_QUERY``'s unconditional ``MATCH (n) RETURN count(n)`` and
silently broken the ``count_nodes() == len(projection.nodes)`` invariant the
CHAOS-3617 live suite already asserts.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark
from tests.context_fabric import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]


def _unique_org(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _reorg(batch, org_id: str):
    import dataclasses

    def rebind(record):
        return dataclasses.replace(record, org_id=org_id)

    return dataclasses.replace(
        batch,
        org_id=org_id,
        entities=tuple(rebind(item) for item in batch.entities),
        relationships=tuple(rebind(item) for item in batch.relationships),
        observations=tuple(rebind(item) for item in batch.observations),
        documents=tuple(rebind(item) for item in batch.documents),
    )


@pytest_asyncio.fixture
async def store(monkeypatch) -> AsyncIterator[GraphArmStore]:
    config = live_gate.require_live_store()
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
    org_id = _unique_org("orgwatermark")
    opened = GraphArmStore.for_org(org_id, config=config)
    try:
        yield opened
    finally:
        try:
            await opened.purge_org()
        finally:
            await opened.close()


class TestNeverProjected:
    async def test_a_fresh_partition_reads_back_as_never_projected(
        self, store: GraphArmStore
    ) -> None:
        watermark = await store.read_watermark()
        assert watermark.indexed_through is None
        assert watermark.never_projected is True


class TestRoundTrip:
    async def test_a_written_watermark_round_trips_exactly(
        self, store: GraphArmStore
    ) -> None:
        projection = build_projection(_reorg(fixtures.alpha_batch(), store.org_id))
        result = await store.write_projection(projection)

        read_back = await store.read_watermark()
        assert read_back.indexed_through == result.watermark.indexed_through
        assert read_back.projected_at == result.watermark.projected_at
        assert read_back.records_indexed == result.watermark.records_indexed
        assert read_back.partial == result.watermark.partial
        assert read_back.partial is False

    async def test_read_watermark_survives_a_fresh_store_handle(
        self, store: GraphArmStore
    ) -> None:
        """The point of the whole issue: a DIFFERENT store handle sees it.

        ``write_projection``'s return value only ever reached the process
        that called it. Opening a brand new ``GraphArmStore`` for the same
        organization and reading from THAT one is what proves the watermark
        is durable rather than merely returned.
        """

        projection = build_projection(_reorg(fixtures.alpha_batch(), store.org_id))
        written = await store.write_projection(projection)

        fresh_handle = GraphArmStore.for_org(
            store.org_id, config=live_gate.require_live_store()
        )
        try:
            read_back = await fresh_handle.read_watermark()
        finally:
            await fresh_handle.close()

        assert read_back.indexed_through == written.watermark.indexed_through
        assert read_back.records_indexed == written.watermark.records_indexed


class TestPurgeRemovesTheWatermark:
    async def test_purging_removes_the_watermark_key(
        self, store: GraphArmStore
    ) -> None:
        projection = build_projection(_reorg(fixtures.alpha_batch(), store.org_id))
        await store.write_projection(projection)
        assert (await store.read_watermark()).never_projected is False

        await store.purge_org()

        assert (await store.read_watermark()).never_projected is True

    async def test_a_dry_run_purge_leaves_the_watermark_untouched(
        self, store: GraphArmStore
    ) -> None:
        projection = build_projection(_reorg(fixtures.alpha_batch(), store.org_id))
        written = await store.write_projection(projection)

        await store.purge_org(dry_run=True)

        read_back = await store.read_watermark()
        assert read_back.never_projected is False
        assert read_back.indexed_through == written.watermark.indexed_through


class TestNoGraphNodeWasAdded:
    """The negative control that proves the storage mechanism choice."""

    async def test_count_nodes_is_unchanged_by_persisting_a_watermark(
        self, store: GraphArmStore
    ) -> None:
        projection = build_projection(_reorg(fixtures.alpha_batch(), store.org_id))
        result = await store.write_projection(projection)

        # write_projection already persists the watermark as part of the
        # call under test -- read it back explicitly too, so a version that
        # persisted nothing (and thus trivially left count_nodes alone)
        # cannot pass this test by doing nothing.
        assert (await store.read_watermark()).never_projected is False
        assert (
            await store.count_nodes() == len(projection.nodes) == (result.nodes_written)
        ), (
            "count_nodes() changed after persisting a watermark -- the "
            "watermark must never be stored as a Cypher graph node, because "
            "NODE_COUNT_QUERY counts every node unconditionally"
        )

    async def test_purge_removes_exactly_the_projected_node_count(
        self, store: GraphArmStore
    ) -> None:
        projection = build_projection(_reorg(fixtures.alpha_batch(), store.org_id))
        await store.write_projection(projection)

        deleted = await store.purge_org()

        assert deleted == len(projection.nodes), (
            "purge_org's node count included something other than the "
            "projected nodes -- a graph-stored watermark sentinel would do "
            "exactly this"
        )


class TestTransportFailureIsDistinctFromNeverProjected:
    async def test_a_hung_connection_raises_rather_than_reporting_absence(self) -> None:
        """ "Could not check" must never collapse into "checked and absent".

        Mirrors CHAOS-3631's ``GraphOperationTimeoutError`` pattern and
        ``org_deletion_visit``'s ``DeletionCompletenessUnknownError`` rule:
        a transport failure is a distinct outcome from a positively-confirmed
        never-projected partition.
        """

        import asyncio

        from dev_health_ops.context_fabric.graph_arm.flags import GraphDeadlines
        from dev_health_ops.context_fabric.graph_arm.store import (
            GraphOperationTimeoutError,
        )

        class _HangingConnection:
            async def get(self, _key: str) -> None:
                await asyncio.sleep(999.0)

        class _HangingClient:
            def __init__(self) -> None:
                self.connection = _HangingConnection()

        class _HangingDriver:
            def __init__(self) -> None:
                self.client = _HangingClient()

        from dev_health_ops.context_fabric.graph_arm.backend import (
            DeterministicEmbedder,
        )

        hanging_store = GraphArmStore(
            org_id="orghangwatermark",
            driver=_HangingDriver(),
            embedder=DeterministicEmbedder(),  # not touched by read_watermark
            deadlines=GraphDeadlines(
                connect_timeout_s=0.05,
                socket_timeout_s=0.05,
                read_timeout_s=0.05,
                write_timeout_s=0.05,
            ),
        )
        with pytest.raises(GraphOperationTimeoutError):
            await hanging_store.read_watermark()


class TestPartialWatermarkRoundTrips:
    async def test_partial_survives_the_round_trip(self, store: GraphArmStore) -> None:
        """Not exercised through ``write_projection`` (which never marks a
        watermark partial -- see its own docstring), so this writes one
        directly through the persistence primitive to prove ``partial``
        itself round-trips rather than being silently coerced to ``False``.
        """

        watermark = IndexWatermark(
            indexed_through=datetime(2026, 8, 8, tzinfo=UTC),
            projected_at=datetime(2026, 8, 8, 1, tzinfo=UTC),
            records_indexed=17,
            partial=True,
        )
        await store.persist_watermark(watermark)

        read_back = await store.read_watermark()
        assert read_back.partial is True
        assert read_back.records_indexed == 17
        assert read_back.indexed_through == watermark.indexed_through
        assert read_back.projected_at == watermark.projected_at
