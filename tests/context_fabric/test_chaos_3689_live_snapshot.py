"""Issue 3689 (adapter PR): the differential oracle for ``live_snapshot``.

``cohort_discovery.discover_cohort`` was built and tested against
``build_projection``'s in-memory output; nothing before this PR ever fed it
a live partition. ``live_snapshot._live_graph_snapshot`` is the adapter that
does -- and the only thing that can prove it reconstructs
:class:`~.projection.GraphNode`/:class:`~.projection.GraphEdge` correctly is
running BOTH the real write path and the new read path over the SAME batch
and comparing what they produce. No type checker or code index can answer
that; only execution can.

Fixtures come from ``fixtures.alpha_batch()`` (the real producer, run
through the real ``build_projection`` -> ``store.write_projection`` path --
never a hand-authored ``GraphNode``/``GraphEdge``), plus one small,
dedicated batch that adds a node with a validity interval, because
``alpha_batch()``'s only ``valid_from``/``valid_to`` signal is on an EDGE
(``_EDGE_QUERY`` already read that back before this PR) -- the adapter's
own NEW node-level columns (``cf_valid_from``/``cf_valid_to`` on
``_ENTITY_QUERY``/``_OBSERVATION_QUERY``) need a node that actually carries
them to be exercised at all.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.live_snapshot import _live_graph_snapshot
from dev_health_ops.context_fabric.graph_arm.projection import (
    GraphEdge,
    GraphNode,
    GraphProjection,
)
from dev_health_ops.context_fabric.graph_arm.records import EntityRecord
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from tests.context_fabric import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]


def _unique_org(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _reorg(batch, org_id: str):
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


def _with_a_temporally_bounded_entity(batch):
    """Adds one entity with a real ``valid_from``/``valid_to`` interval.

    ``alpha_batch()`` never sets these on a node (only on one edge), so
    without this addition the oracle below would compare ``None`` against
    ``None`` on every node -- passing, but proving nothing about the two new
    columns this PR added to ``_ENTITY_QUERY``.
    """

    bounded = EntityRecord(
        org_id=batch.org_id,
        kind=GraphEntityKind.INITIATIVE,
        canonical_id="init_temporally_bounded",
        display_label="A time-boxed initiative",
        source_class=SourceClass.WORK_GRAPH,
        observed_at=fixtures.WINDOW_END,
        valid_from=datetime(2026, 2, 1, tzinfo=UTC),
        valid_to=datetime(2026, 5, 31, tzinfo=UTC),
    )
    return dataclasses.replace(batch, entities=(*batch.entities, bounded))


#: Two known, pre-existing gaps neither the write side nor any EXISTING live
#: reader can round-trip -- see live_snapshot.py's own module docstring for
#: the full explanation of each. Documented here, not silently tolerated:
#: excluding ``aliases``/``attributes`` wholesale would also hide a REAL
#: regression in the parts of each that ARE recoverable, so only the
#: specific known-lossy sub-parts are dropped before comparing.
_ALIAS_PROVIDER_GAP = (
    "backend.to_graphiti_nodes flattens AliasRecord into cf_alias_{kind} "
    "VALUE-only strings and never persists .provider -- no live reader can "
    "recover what the write side never wrote"
)
#: ``attributes`` is compared as an INCLUDE-list, not an exclude-list: only
#: keys READBACK_ATTRIBUTE_KEYS declares recoverable. Any OTHER key an
#: EntityRecord/ObservationRecord happens to carry (``archived``,
#: ``outcome``, ``supersedes``, ``prior_attempt_ids`` are all real examples
#: the alpha fixture already exercises) still reaches the store via
#: to_graphiti_nodes's generic ``cf_attr_{key}`` loop, but
#: _attributes_from_row -- the SAME helper every live reader already uses,
#: not something this adapter invented -- only ever reads back the declared
#: set. An exclude-list naming each such key one at a time would need a new
#: entry every time a fixture used a new one; this include-list names the
#: actual boundary once. Pre-existing gap in the declared read-back
#: vocabulary; not something this adapter PR widens.
_ATTRIBUTE_RECOVERY_GAP_REASON = (
    "attributes outside READBACK_ATTRIBUTE_KEYS reach the store but no "
    "live reader -- including this adapter -- can read them back; "
    "comparing only the declared set is comparing what live_snapshot "
    "actually claims to reconstruct, not silently tolerating a narrower "
    "claim than the one in its own docstring"
)


def _stable_node(node: GraphNode) -> dict[str, object]:
    """Every ``GraphNode`` field, derived from ``dataclasses.fields`` so a
    field added later is compared by default rather than silently skipped.
    """

    from dev_health_ops.context_fabric.graph_arm.projection import (
        READBACK_ATTRIBUTE_KEYS,
    )

    compared: dict[str, object] = {}
    for field in dataclasses.fields(GraphNode):
        value = getattr(node, field.name)
        if field.name == "aliases":
            # provider dropped -- see _ALIAS_PROVIDER_GAP.
            value = tuple(sorted((alias.kind.value, alias.value) for alias in value))
        elif field.name == "attributes":
            value = {
                key: val for key, val in value.items() if key in READBACK_ATTRIBUTE_KEYS
            }
        compared[field.name] = value
    return compared


def _stable_edge(edge: GraphEdge) -> dict[str, object]:
    """Every ``GraphEdge`` field, the same way. ``observation_ids`` is
    sorted before comparing: it round-trips through a comma-joined store
    property with no ordering guarantee either at write or at read, and
    order was never a claim either side makes -- only membership is.
    """

    compared: dict[str, object] = {}
    for field in dataclasses.fields(GraphEdge):
        value = getattr(edge, field.name)
        if field.name == "observation_ids":
            value = tuple(sorted(value))
        compared[field.name] = value
    return compared


class TestAttributeComparisonIsNotVacuous:
    """Anti-vacuity for the include-list in ``_stable_node``: if
    ``READBACK_ATTRIBUTE_KEYS`` were ever empty, or the fixture never
    populated any declared key, ``test_every_node_matches_field_for_field``
    would pass by comparing an empty ``attributes`` dict on both sides --
    mirrors test_chaos_3617_live_store.py's own anti-vacuity discipline for
    exactly this reason.
    """

    def test_the_declared_key_set_is_not_empty(self) -> None:
        from dev_health_ops.context_fabric.graph_arm.projection import (
            READBACK_ATTRIBUTE_KEYS,
        )

        assert READBACK_ATTRIBUTE_KEYS

    def test_the_fixture_actually_populates_a_declared_key(self) -> None:
        from dev_health_ops.context_fabric.graph_arm.projection import (
            READBACK_ATTRIBUTE_KEYS,
        )

        batch = fixtures.alpha_batch()
        used_keys = {key for entity in batch.entities for key in entity.attributes} | {
            key for observation in batch.observations for key in observation.attributes
        }
        assert used_keys & set(READBACK_ATTRIBUTE_KEYS), (
            "the fixture never sets a single declared-recoverable attribute "
            "key -- the node comparison's attributes field would agree on "
            "{} vs {} for every node, which is not a measurement"
        )


@pytest_asyncio.fixture
async def snapshot_store(
    monkeypatch,
) -> AsyncIterator[tuple[GraphArmStore, GraphProjection]]:
    config = live_gate.require_live_store()
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
    live_gate.require_flag_state()

    org_id = _unique_org("orglivesnapshot")
    batch = _with_a_temporally_bounded_entity(_reorg(fixtures.alpha_batch(), org_id))
    projection = build_projection(batch)
    store = GraphArmStore.for_org(org_id, config=config)
    try:
        await store.build_indices()
        await store.write_projection(projection)
        yield store, projection
    finally:
        try:
            await store.purge_org()
        finally:
            await store.close()


class TestLiveSnapshotMatchesTheInMemoryReference:
    async def test_every_node_matches_field_for_field(
        self, snapshot_store: tuple[GraphArmStore, GraphProjection]
    ) -> None:
        store, projection = snapshot_store
        live_nodes, _live_edges = await _live_graph_snapshot(
            store.driver, store.org_id, store.partition
        )

        assert live_nodes, "the live reconstruction returned no nodes; vacuous"
        assert len(live_nodes) == len(projection.nodes), (
            "node count differs -- something was dropped or duplicated on "
            "the way through the live store"
        )

        by_id_live = {node.canonical_id: node for node in live_nodes}
        by_id_reference = {node.canonical_id: node for node in projection.nodes}
        assert set(by_id_live) == set(by_id_reference)

        mismatches = {
            canonical_id: (
                _stable_node(by_id_reference[canonical_id]),
                _stable_node(by_id_live[canonical_id]),
            )
            for canonical_id in by_id_reference
            if _stable_node(by_id_reference[canonical_id])
            != _stable_node(by_id_live[canonical_id])
        }
        assert not mismatches, mismatches

    async def test_every_edge_matches_field_for_field(
        self, snapshot_store: tuple[GraphArmStore, GraphProjection]
    ) -> None:
        store, projection = snapshot_store
        _live_nodes, live_edges = await _live_graph_snapshot(
            store.driver, store.org_id, store.partition
        )

        assert live_edges, "the live reconstruction returned no edges; vacuous"
        assert len(live_edges) == len(projection.edges), (
            "edge count differs -- something was dropped or duplicated on "
            "the way through the live store"
        )

        def _key(edge: GraphEdge) -> tuple[str, str, str]:
            return (
                edge.relationship.value,
                edge.source_canonical_id,
                edge.target_canonical_id,
            )

        by_key_live = {_key(edge): edge for edge in live_edges}
        by_key_reference = {_key(edge): edge for edge in projection.edges}
        assert set(by_key_live) == set(by_key_reference)

        mismatches = {
            key: (_stable_edge(by_key_reference[key]), _stable_edge(by_key_live[key]))
            for key in by_key_reference
            if _stable_edge(by_key_reference[key]) != _stable_edge(by_key_live[key])
        }
        assert not mismatches, mismatches

    async def test_uuids_match_the_write_sides_own_identity_derivation(
        self, snapshot_store: tuple[GraphArmStore, GraphProjection]
    ) -> None:
        """The binding condition, checked directly rather than folded only
        into the field-for-field comparison above: a correct-but-different
        uuid scheme would still "match" a differential run only against
        itself if this weren't asserted against the WRITE side's own uuids.
        """

        store, projection = snapshot_store
        live_nodes, live_edges = await _live_graph_snapshot(
            store.driver, store.org_id, store.partition
        )

        live_uuid_by_id = {node.canonical_id: node.uuid for node in live_nodes}
        reference_uuid_by_id = {
            node.canonical_id: node.uuid for node in projection.nodes
        }
        assert live_uuid_by_id == reference_uuid_by_id

        def _key(edge: GraphEdge) -> tuple[str, str, str]:
            return (
                edge.relationship.value,
                edge.source_canonical_id,
                edge.target_canonical_id,
            )

        live_edge_uuid_by_key = {_key(edge): edge.uuid for edge in live_edges}
        reference_edge_uuid_by_key = {
            _key(edge): edge.uuid for edge in projection.edges
        }
        assert live_edge_uuid_by_key == reference_edge_uuid_by_key

    async def test_the_temporally_bounded_node_round_trips_its_validity_window(
        self, snapshot_store: tuple[GraphArmStore, GraphProjection]
    ) -> None:
        """The specific case the field-for-field comparison above would
        already catch, isolated and asserted directly so a future reader
        does not have to infer it from a big dict diff.
        """

        store, _projection = snapshot_store
        live_nodes, _live_edges = await _live_graph_snapshot(
            store.driver, store.org_id, store.partition
        )

        (bounded,) = (
            node
            for node in live_nodes
            if node.canonical_id == "init_temporally_bounded"
        )
        assert bounded.valid_from == datetime(2026, 2, 1, tzinfo=UTC)
        assert bounded.valid_to == datetime(2026, 5, 31, tzinfo=UTC)
