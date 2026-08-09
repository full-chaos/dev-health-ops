"""CHAOS-3617: the arm against a real Graphiti/FalkorDB trial store.

Everything above this module runs on the in-memory reference. These tests
are the ones that can fail because Graphiti, Cypher or FalkorDB behaves
differently from the reference — which is the only reason the arm exists, so
they are the load-bearing half of the verification.

Four things are measured here and nowhere else:

* a **differential oracle** between the two readers. The traversal exists
  twice — an in-memory walk and a Cypher fetch — and no type checker,
  linter or code index can tell you whether they agree. Running both over
  the same world and comparing is the only thing that can;
* **cross-tenant isolation in the real store**, with both organizations
  written to the same server;
* **deterministic cleanup**, by dropping a partition and reading zero back;
* **indexing failure and fallback**, by pointing the arm at a dead endpoint.

Skips route through ``live_gate.require_live_store`` so that
``CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1`` turns "no store" into a failure.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    TrialContext,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
from dev_health_ops.context_fabric.graph_arm.readback import (
    InvestigationReadout,
    LiveGraphReader,
    ProjectionGraphReader,
)
from dev_health_ops.context_fabric.graph_arm.store import (
    GraphArmStore,
    ProjectionDisabledError,
)
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark
from tests.context_fabric import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_RUN_ID = "4f9a2c1e-1111-4222-8333-444455556666"


def _unique_org(prefix: str) -> str:
    """A fresh organization per test.

    Partition == keyspace, so a unique org id means a private keyspace: two
    tests cannot see each other's writes, and a failed test cannot poison
    the next run's store. The uuid is hex-only because
    ``partition_for_org`` refuses anything that is not a plain identifier.
    """

    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _reorg(batch, org_id: str):
    """Rebind a fixture batch to a fresh organization id."""

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
async def alpha_store(
    monkeypatch,
) -> AsyncIterator[tuple[GraphArmStore, GraphProjection]]:
    config = live_gate.require_live_store()
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
    live_gate.require_flag_state()

    org_id = _unique_org("orglive")
    projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
    store = GraphArmStore.for_org(org_id, config=config)
    try:
        await store.build_indices()
        await store.write_projection(projection)
        yield store, projection
    finally:
        # Deterministic cleanup: drop the keyspace whatever happened above.
        try:
            await store.purge_org()
        finally:
            await store.close()


class TestLiveWriteAndReadBack:
    async def test_the_projection_is_written_and_counted(self, alpha_store) -> None:
        store, projection = alpha_store
        assert await store.count_nodes() == len(projection.nodes)

    async def test_canonical_ids_survive_a_real_round_trip(self, alpha_store) -> None:
        """record -> Graphiti -> FalkorDB -> Cypher -> readout.

        The in-memory tests prove the mapping; only this proves the mapping
        survives serialization into and out of a real store.
        """

        store, projection = alpha_store
        readout = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
        reached = {entity.canonical_id for entity in readout.entities}
        assert "proj_nightfall_migration" in reached
        assert "team_platform" in reached
        assert "dep_authlib" in reached

    async def test_relationship_direction_survives_a_real_round_trip(
        self, alpha_store
    ) -> None:
        store, _ = alpha_store
        readout = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=1,
        )
        ownership = [
            step
            for path in readout.paths
            for step in path.steps
            if step.relationship.value == "owned_by_team"
        ]
        assert ownership
        for step in ownership:
            assert step.from_canonical_id == "proj_nightfall_migration"
            assert step.to_canonical_id == "team_platform"
            assert step.direction.value == "forward"

    async def test_the_stored_facts_are_triples_not_prose(self, alpha_store) -> None:
        """Read straight out of the store, not out of the projection.

        ``parse_triple_fact`` raises on anything that is not exactly three
        tokens, so a prose fact anywhere in the partition fails the read that
        this traversal performs.
        """

        store, projection = alpha_store
        readout = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
        assert readout.paths

    async def test_a_packet_built_from_the_live_readout_validates(
        self, alpha_store, signer
    ) -> None:
        store, _ = alpha_store
        readout = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
        packet = build_packet(
            readout=readout,
            job=JobContext(
                job_id="job_live",
                question_family=QuestionFamilyID("project_status_drivers"),
                job_statement="Status of the Nightfall Migration project.",
                comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
                window_start=fixtures.WINDOW_START,
                window_end=fixtures.WINDOW_END,
            ),
            watermark=IndexWatermark(
                indexed_through=fixtures.WINDOW_END,
                projected_at=fixtures.WINDOW_END,
                records_indexed=1,
            ),
            signer=signer,
            trial=TrialContext(run_id=_RUN_ID),
            produced_at=datetime(2026, 8, 8, 12, tzinfo=UTC),
        )
        assert packet.related_context.entities
        assert packet.organization_id == store.org_id


class TestReaderDifferential:
    """The differential oracle. Two implementations, one comparison."""

    @staticmethod
    def _comparable(readout: InvestigationReadout) -> dict[str, object]:
        """The fields both readers are expected to agree on.

        Scope stated plainly: observation *attachment* is excluded, because
        ``add_nodes_and_edges_bulk`` writes entity edges only, so the live
        reader cannot yet recover which entities an observation was about.
        That is a known gap in this revision, not a difference the readers
        are permitted to have -- the entity and path fields below are
        compared exactly.
        """

        return {
            "entities": [
                (entity.canonical_id, entity.kind.value, entity.display_label)
                for entity in readout.entities
            ],
            "paths": sorted(
                (
                    path.origin_canonical_id,
                    path.terminal_canonical_id,
                    tuple(
                        (
                            step.from_canonical_id,
                            step.relationship.value,
                            step.direction.value,
                            step.to_canonical_id,
                        )
                        for step in path.steps
                    ),
                )
                for path in readout.paths
            ),
            "authorization_filtered_count": readout.authorization_filtered_count,
            "entities_truncated": readout.entities_truncated,
            "paths_truncated": readout.paths_truncated,
        }

    @pytest.mark.parametrize(
        ("seeds", "max_hops"),
        [
            (["proj_nightfall_migration"], 1),
            (["proj_nightfall_migration"], 3),
            (["team_platform"], 2),
            (["pr_4412"], 3),
            (["proj_restricted_billing"], 3),
            (["does_not_exist"], 3),
        ],
    )
    async def test_the_live_reader_agrees_with_the_reference(
        self, alpha_store, seeds, max_hops
    ) -> None:
        store, projection = alpha_store
        reference = await ProjectionGraphReader(projection).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=seeds,
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=max_hops,
        )
        live = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=seeds,
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=max_hops,
        )
        assert self._comparable(live) == self._comparable(reference)

    async def test_the_differential_can_actually_fail(self, alpha_store) -> None:
        """The acceptance case for the comparator itself.

        A differential that cannot detect a planted difference is a green
        light with no bulb behind it. Two genuinely different traversals
        must compare unequal.
        """

        store, projection = alpha_store
        shallow = await ProjectionGraphReader(projection).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=1,
        )
        deep = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
        assert self._comparable(shallow) != self._comparable(deep)


class TestLiveTenantIsolation:
    async def test_two_organizations_on_one_server_never_see_each_other(
        self, monkeypatch
    ) -> None:
        """The near-duplicate case, in the real store.

        Both organizations are written to the same FalkorDB instance and both
        hold a ``team_platform``. Partition == keyspace, so alpha's read must
        return alpha's team and nothing of beta's.
        """

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")

        alpha_org = _unique_org("orgalpha")
        beta_org = _unique_org("orgbeta")
        alpha = build_projection(_reorg(fixtures.alpha_batch(), alpha_org))
        beta = build_projection(_reorg(fixtures.beta_batch(), beta_org))

        alpha_store = GraphArmStore.for_org(alpha_org, config=config)
        beta_store = GraphArmStore.for_org(beta_org, config=config)
        try:
            await alpha_store.write_projection(alpha)
            await beta_store.write_projection(beta)

            readout = await LiveGraphReader(alpha_store).neighbourhood(
                org_id=alpha_org,
                seed_canonical_ids=["proj_nightfall_migration"],
                authorized_entity_ids=(
                    *fixtures.alpha_authorized_ids(),
                    "proj_nightfall_migrations",
                ),
                max_hops=3,
            )
            reached = {entity.canonical_id for entity in readout.entities}
            assert "proj_nightfall_migrations" not in reached, (
                "beta's near-duplicate project crossed the tenant boundary"
            )
            assert "team_platform" in reached

            # The negative control: beta's own read *does* see it, so the
            # absence above is isolation rather than a broken write.
            beta_readout = await LiveGraphReader(beta_store).neighbourhood(
                org_id=beta_org,
                seed_canonical_ids=["proj_nightfall_migrations"],
                authorized_entity_ids=("proj_nightfall_migrations", "team_platform"),
                max_hops=2,
            )
            assert {entity.canonical_id for entity in beta_readout.entities} == {
                "proj_nightfall_migrations",
                "team_platform",
            }
        finally:
            for store in (alpha_store, beta_store):
                try:
                    await store.purge_org()
                finally:
                    await store.close()

    async def test_a_reader_refuses_another_organizations_partition(
        self, alpha_store
    ) -> None:
        store, _ = alpha_store
        with pytest.raises(PermissionError):
            await LiveGraphReader(store).neighbourhood(
                org_id=_unique_org("orgother"),
                seed_canonical_ids=["proj_nightfall_migration"],
                authorized_entity_ids=fixtures.alpha_authorized_ids(),
            )


class TestDeterministicCleanup:
    async def test_purging_an_organization_removes_every_node(
        self, monkeypatch
    ) -> None:
        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        org_id = _unique_org("orgpurge")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.write_projection(projection)
            assert await store.count_nodes() > 0

            deleted = await store.purge_org()
            assert deleted == len(projection.nodes)
            # Read back rather than trusting the return value: a purge that
            # reported a count without deleting would pass on the count alone.
            assert await store.count_nodes() == 0
        finally:
            await store.close()

    async def test_a_dry_run_counts_without_deleting(self, alpha_store) -> None:
        store, projection = alpha_store
        counted = await store.purge_org(dry_run=True)
        assert counted == len(projection.nodes)
        assert await store.count_nodes() == len(projection.nodes)

    async def test_purging_an_organization_that_never_projected_is_not_an_error(
        self,
    ) -> None:
        config = live_gate.require_live_store()
        store = GraphArmStore.for_org(_unique_org("orgempty"), config=config)
        try:
            assert await store.purge_org() == 0
        finally:
            await store.close()

    async def test_the_registered_deletion_visit_purges_the_real_partition(
        self, monkeypatch
    ) -> None:
        """End to end through the CHAOS-3566 registry, not the store directly."""

        from dev_health_ops.api.services.derived_store_registry import (
            EXTERNAL_DERIVED_STORES,
        )
        from dev_health_ops.context_fabric.graph_arm.store import (
            TRIAL_DERIVED_STORE_NAME,
        )

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_STORE_URI", config.uri)
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")

        org_id = _unique_org("orgregistry")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.write_projection(projection)
        finally:
            await store.close()

        entry = next(
            item
            for item in EXTERNAL_DERIVED_STORES
            if item.name == TRIAL_DERIVED_STORE_NAME
        )
        assert entry.visit is not None
        assert await entry.visit(org_id, True) == len(projection.nodes)
        assert await entry.visit(org_id, False) == len(projection.nodes)
        assert await entry.visit(org_id, False) == 0


class TestIndexingFailureAndFallback:
    async def test_a_write_with_the_projection_flag_off_is_refused(
        self, monkeypatch
    ) -> None:
        """Flag off means no write reaches the store, not a quiet no-op."""

        config = live_gate.require_live_store()
        monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", raising=False)
        org_id = _unique_org("orgflagoff")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            with pytest.raises(ProjectionDisabledError):
                await store.write_projection(projection)
            assert await store.count_nodes() == 0
        finally:
            await store.close()

    async def test_a_write_for_the_wrong_organization_is_refused(
        self, alpha_store, monkeypatch
    ) -> None:
        store, _ = alpha_store
        foreign = build_projection(
            _reorg(fixtures.beta_batch(), _unique_org("orgforeign"))
        )
        with pytest.raises(PermissionError):
            await store.write_projection(foreign)

    async def test_an_unreachable_store_fails_loudly_rather_than_silently(
        self, monkeypatch
    ) -> None:
        """Indexing failure must be an error the caller sees.

        A projection that swallowed a connection error would leave the
        watermark unchanged while reporting success, and every later answer
        would be built on a store nobody knew was empty.
        """

        from dev_health_ops.context_fabric.graph_arm.flags import TrialStoreConfig

        live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        org_id = _unique_org("orgdead")
        projection = build_projection(_reorg(fixtures.alpha_batch(), org_id))
        with pytest.raises(Exception) as excinfo:  # noqa: B017, PT011
            store = GraphArmStore.for_org(
                org_id, config=TrialStoreConfig(uri="falkor://127.0.0.1:1")
            )
            await store.write_projection(projection)
        assert not isinstance(excinfo.value, ProjectionDisabledError)

    async def test_canonical_records_do_not_wait_on_graph_indexing(self) -> None:
        """Projection is a separate step over already-canonical records.

        Structural, not timing-based: :func:`build_projection` is pure and
        takes an ``IngestionBatch`` that has already been read from the
        canonical stores, so there is no code path on which a canonical write
        can block on the graph. The write is the *only* thing that touches
        the store, and it happens strictly after.
        """

        import inspect

        from dev_health_ops.context_fabric.graph_arm import projection as module

        source = inspect.getsource(module)
        assert "await " not in source, (
            "the projection step became asynchronous, which means it can now "
            "wait on something; re-establish that canonical writes never wait "
            "on graph indexing before relaxing this assertion"
        )
