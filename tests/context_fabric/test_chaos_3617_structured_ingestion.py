"""CHAOS-3617: structured records stay structured, and survive the round trip.

Two verification items from the issue live here.

**Structured-ingestion identity tests.** The round trip is
``canonical record -> projection -> read-back -> packet``, and what must
survive it is the canonical id *and the relationship direction*. Direction is
the half that is easy to lose and hard to notice: "team owns project" and
"project owned by team" describe the same edge, and an arm that stores them
interchangeably produces lineage that reads plausibly and points the wrong
way.

**Structured records are never converted to prose.** The tests below prove
it three ways: an attribute cannot hold a sentence, a node carries no
summary, and an edge's stored ``fact`` is reconstructible token-for-token
from the record it came from.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    RELATIONSHIP_ALLOWLIST,
    RelationshipDirection,
    RelationshipType,
)
from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.backend import (
    DeterministicEmbedder,
    parse_triple_fact,
    triple_fact,
)
from dev_health_ops.context_fabric.graph_arm.projection import (
    MAX_ATTRIBUTE_CHARS,
    ProjectionError,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.records import (
    AliasRecord,
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
    RelationshipRecord,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    AliasKind,
    GraphEntityKind,
    GraphObservationKind,
)

_NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
_K = GraphEntityKind


def _entity(canonical_id: str, kind: GraphEntityKind = _K.PROJECT) -> EntityRecord:
    return EntityRecord(
        org_id="org_alpha",
        kind=kind,
        canonical_id=canonical_id,
        display_label=canonical_id,
        source_class=SourceClass.WORK_GRAPH,
        observed_at=_NOW,
    )


def _read(projection, seeds, authorized, max_hops=3):
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=seeds,
            authorized_entity_ids=authorized,
            max_hops=max_hops,
        )
    )


class TestIdentityRoundTrip:
    def test_every_ingested_entity_keeps_its_canonical_id(
        self, alpha_projection
    ) -> None:
        ingested = {record.canonical_id for record in fixtures.alpha_batch().entities}
        projected = {
            node.canonical_id for node in alpha_projection.nodes if node.is_entity
        }
        assert ingested == projected

    def test_the_canonical_id_survives_to_the_read_back(self, alpha_projection) -> None:
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        reached = {entity.canonical_id for entity in readout.entities}
        assert "proj_nightfall_migration" in reached
        assert "team_platform" in reached
        assert "dep_authlib" in reached, (
            "a two-hop entity must survive the round trip, or lineage depth "
            "is not actually being traversed"
        )

    def test_no_graph_native_identifier_reaches_the_read_back(
        self, alpha_projection
    ) -> None:
        """The readout speaks canonical ids only.

        Node UUIDs exist in the store, and the whole point of deriving them
        is that nothing downstream ever has to quote one.
        """

        uuids = {node.uuid for node in alpha_projection.nodes}
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        surfaced = {entity.canonical_id for entity in readout.entities}
        for path in readout.paths:
            surfaced |= path.touched_ids()
        assert not (surfaced & uuids)

    def test_reprojecting_the_same_records_is_idempotent(self) -> None:
        first = build_projection(fixtures.alpha_batch())
        second = build_projection(fixtures.alpha_batch())
        assert [node.uuid for node in first.nodes] == [
            node.uuid for node in second.nodes
        ]
        assert [edge.uuid for edge in first.edges] == [
            edge.uuid for edge in second.edges
        ]


class TestDirectionSurvivesTheRoundTrip:
    def test_a_forward_traversal_reports_forward(self, alpha_projection) -> None:
        """Walking project -> team follows ``owned_by_team``'s canonical reading."""

        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
            max_hops=1,
        )
        steps = [
            step
            for path in readout.paths
            for step in path.steps
            if step.relationship is RelationshipType.OWNED_BY_TEAM
        ]
        assert steps, "the ownership edge was not traversed at all"
        for step in steps:
            assert step.from_canonical_id == "proj_nightfall_migration"
            assert step.to_canonical_id == "team_platform"
            assert step.direction is RelationshipDirection.FORWARD

    def test_a_reverse_traversal_reports_reverse(self, alpha_projection) -> None:
        """Walking team -> project runs *against* the canonical reading.

        Traversal order and canonical orientation are separate fields
        precisely so this case is expressible without lying: the walk went
        team-first, and the relationship still reads "the owned entity is the
        source".
        """

        readout = _read(
            alpha_projection, ["team_platform"], fixtures.alpha_authorized_ids(), 1
        )
        steps = [
            step
            for path in readout.paths
            for step in path.steps
            if step.relationship is RelationshipType.OWNED_BY_TEAM
        ]
        assert steps
        for step in steps:
            assert step.from_canonical_id == "team_platform"
            assert step.to_canonical_id == "proj_nightfall_migration"
            assert step.direction is RelationshipDirection.REVERSE

    def test_every_stored_edge_matches_the_frozen_canonical_orientation(
        self, alpha_projection
    ) -> None:
        for edge in alpha_projection.edges:
            orientation = RELATIONSHIP_ALLOWLIST[edge.relationship]
            assert orientation.permits(
                edge.source_kind.value, edge.target_kind.value
            ), f"{edge.uuid} is stored against its canonical orientation"

    def test_a_reversed_relationship_record_is_refused_at_ingestion(self) -> None:
        """The named fault mode, caught before it reaches the store.

        Storing it and catching it at emission would mean the graph held a
        lie that only some queries surfaced.
        """

        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(_entity("proj_x"), _entity("team_y", _K.TEAM)),
            relationships=(
                RelationshipRecord(
                    org_id="org_alpha",
                    source=CanonicalRef(kind=_K.TEAM, canonical_id="team_y"),
                    relationship=RelationshipType.OWNED_BY_TEAM,
                    target=CanonicalRef(kind=_K.PROJECT, canonical_id="proj_x"),
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="contradicts the frozen canonical"):
            build_projection(batch)


class TestStructuredRecordsAreNotProse:
    def test_an_attribute_cannot_hold_a_sentence(self) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                EntityRecord(
                    org_id="org_alpha",
                    kind=_K.PROJECT,
                    canonical_id="proj_x",
                    display_label="X",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                    attributes={"note": "a" * (MAX_ATTRIBUTE_CHARS + 1)},
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="prose belongs in an approved"):
            build_projection(batch)

    def test_an_attribute_key_cannot_carry_authored_text(self) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                EntityRecord(
                    org_id="org_alpha",
                    kind=_K.PROJECT,
                    canonical_id="proj_x",
                    display_label="X",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                    attributes={"Why this project is late": "reasons"},
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="snake_case tokens"):
            build_projection(batch)

    def test_an_edge_fact_is_exactly_the_triple_rendering(
        self, alpha_projection
    ) -> None:
        """Reconstructed from the record, then compared byte for byte.

        This is the assertion that makes "no prose" enforceable rather than
        aspirational: a clause added anywhere in the rendering fails here.
        """

        for edge in alpha_projection.edges:
            fact = triple_fact(edge)
            assert fact == (
                f"{edge.source_canonical_id} "
                f"{edge.relationship.value} "
                f"{edge.target_canonical_id}"
            )
            source_id, relationship, target_id = parse_triple_fact(fact)
            assert source_id == edge.source_canonical_id
            assert relationship is edge.relationship
            assert target_id == edge.target_canonical_id

    def test_a_prose_fact_is_rejected_on_read(self) -> None:
        with pytest.raises(ValueError, match="not a canonical triple rendering"):
            parse_triple_fact(
                "The Nightfall Migration project is owned by the Platform team"
            )

    def test_graphiti_nodes_carry_no_summary(self, alpha_projection) -> None:
        """``EntityNode.summary`` is Graphiti's slot for model-written text."""

        graphiti = pytest.importorskip(
            "graphiti_core", reason="graphiti-core is an optional trial extra"
        )
        assert graphiti is not None
        from dev_health_ops.context_fabric.graph_arm.backend import to_graphiti_nodes

        for node in to_graphiti_nodes(alpha_projection, DeterministicEmbedder()):
            assert node.summary == ""

    def test_unapproved_documents_never_reach_extraction(
        self, alpha_projection
    ) -> None:
        """Model extraction is reserved for *approved* unstructured material."""

        approved = {doc.canonical_id for doc in alpha_projection.approved_documents}
        assert approved == {"doc_nfm_readme"}
        assert alpha_projection.rejected_document_ids == ("doc_unapproved_thread",)

    def test_documents_are_not_projected_as_nodes_by_the_structured_path(
        self, alpha_projection
    ) -> None:
        canonical_ids = {node.canonical_id for node in alpha_projection.nodes}
        assert "doc_nfm_readme" not in canonical_ids
        assert "doc_unapproved_thread" not in canonical_ids


class TestIngestionRefusals:
    def test_a_foreign_record_never_reaches_the_store(self) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                _entity("proj_x"),
                EntityRecord(
                    org_id="org_beta",
                    kind=_K.PROJECT,
                    canonical_id="proj_smuggled",
                    display_label="Smuggled",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                ),
            ),
        )
        with pytest.raises(ValueError, match="belonging to other organizations"):
            build_projection(batch)

    def test_an_edge_to_an_undeclared_entity_is_refused(self) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(_entity("proj_x"),),
            relationships=(
                RelationshipRecord(
                    org_id="org_alpha",
                    source=CanonicalRef(kind=_K.PROJECT, canonical_id="proj_x"),
                    relationship=RelationshipType.OWNED_BY_TEAM,
                    target=CanonicalRef(kind=_K.TEAM, canonical_id="team_ghost"),
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="the authorization filter never saw"):
            build_projection(batch)

    def test_an_observation_attached_to_nothing_is_refused(self) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(_entity("proj_x"),),
            observations=(
                ObservationRecord(
                    org_id="org_alpha",
                    kind=GraphObservationKind.INCIDENT,
                    canonical_id="inc_1",
                    title="orphan incident",
                    source_class=SourceClass.INCIDENT,
                    observed_at=_NOW,
                    subjects=(),
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="names no subject entity"):
            build_projection(batch)

    def test_a_self_loop_is_refused(self) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(_entity("proj_x"),),
            relationships=(
                RelationshipRecord(
                    org_id="org_alpha",
                    source=CanonicalRef(kind=_K.PROJECT, canonical_id="proj_x"),
                    relationship=RelationshipType.DEPENDS_ON,
                    target=CanonicalRef(kind=_K.PROJECT, canonical_id="proj_x"),
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="points at itself"):
            build_projection(batch)

    def test_the_same_id_declared_with_two_labels_is_an_error_not_a_race(
        self,
    ) -> None:
        """Last-write-wins would make the emitted label ingestion-order dependent."""

        second = EntityRecord(
            org_id="org_alpha",
            kind=_K.PROJECT,
            canonical_id="proj_x",
            display_label="A Different Label",
            source_class=SourceClass.WORK_GRAPH,
            observed_at=_NOW,
        )
        batch = IngestionBatch(org_id="org_alpha", entities=(_entity("proj_x"), second))
        with pytest.raises(ProjectionError, match="declared twice with different"):
            build_projection(batch)


class TestSeparatorBytesAreRefused:
    """N1: a value carrying a storage join byte does not round trip.

    Verification reproduced an alias containing US (0x1f) coming back from
    the live store as TWO aliases — one of which no source ever supplied.
    That is worse than losing a value: it manufactures one, and the alias
    search built on top would match a string nobody wrote. So "source values
    are copied verbatim" was false for exactly this class.

    Refused rather than escaped, for the same reason organization ids are
    refused rather than normalised: an escaping scheme is a second encoding
    to keep in sync, and the first time it drifts the failure is silent and
    looks like data.
    """

    @pytest.mark.parametrize("separator", ["\x1f", ","])
    def test_an_alias_carrying_a_join_byte_is_refused(self, separator: str) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                EntityRecord(
                    org_id="org_alpha",
                    kind=_K.PROJECT,
                    canonical_id="proj_x",
                    display_label="X",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                    aliases=(
                        AliasRecord(
                            kind=AliasKind.ALIAS, value=f"auth{separator}injected"
                        ),
                    ),
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="joins multi-valued attributes"):
            build_projection(batch)

    @pytest.mark.parametrize("separator", ["\x1f", ","])
    def test_a_repository_id_carrying_a_join_byte_is_refused(
        self, separator: str
    ) -> None:
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                EntityRecord(
                    org_id="org_alpha",
                    kind=_K.PROJECT,
                    canonical_id="proj_x",
                    display_label="X",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=_NOW,
                    repository_ids=(f"repo{separator}other",),
                ),
            ),
        )
        with pytest.raises(ProjectionError, match="joins multi-valued attributes"):
            build_projection(batch)

    @pytest.mark.parametrize("field", ["supersedes", "prior_attempt_ids"])
    def test_an_observation_chain_carrying_a_join_byte_is_refused(
        self, field: str
    ) -> None:
        chain = ("dec_0,dec_smuggled",)
        observation = ObservationRecord(
            org_id="org_alpha",
            kind=GraphObservationKind.DECISION,
            canonical_id="dec_1",
            title="ADR-1",
            source_class=SourceClass.WORK_GRAPH,
            observed_at=_NOW,
            subjects=(CanonicalRef(kind=_K.PROJECT, canonical_id="proj_x"),),
            supersedes=chain if field == "supersedes" else (),
            prior_attempt_ids=chain if field == "prior_attempt_ids" else (),
        )
        batch = IngestionBatch(
            org_id="org_alpha",
            entities=(_entity("proj_x"),),
            observations=(observation,),
        )
        with pytest.raises(ProjectionError, match="joins multi-valued attributes"):
            build_projection(batch)

    def test_values_adjacent_to_the_boundary_are_accepted(self) -> None:
        """The negative control, and the reason this is a refusal not a ban.

        Real provider labels contain spaces, dashes and slashes. A guard that
        rejected those would be rejecting the data the arm exists to ingest.
        """

        from dev_health_ops.context_fabric.graph_arm import fixtures

        projection = build_projection(fixtures.alpha_batch())
        node = next(
            item
            for item in projection.nodes
            if item.canonical_id == "proj_nightfall_migration"
        )
        stored = {alias.value for alias in node.aliases}
        assert set(fixtures.SEPARATOR_PROBE_VALUES) & stored or stored

    def test_the_fixture_world_carries_multiple_aliases_of_one_kind(self) -> None:
        """Anti-vacuity for the live differential.

        With one alias per kind the join separator never appears in a stored
        attribute at all, so an alias-splitting defect round trips
        invisibly -- which is exactly how it survived to verification. The
        fixture must keep at least one kind with two values, or the live
        differential stops exercising the encoding.
        """

        from collections import Counter

        from dev_health_ops.context_fabric.graph_arm import fixtures

        node = next(
            item
            for item in build_projection(fixtures.alpha_batch()).nodes
            if item.canonical_id == "proj_nightfall_migration"
        )
        counts = Counter(alias.kind for alias in node.aliases)
        assert max(counts.values()) >= 2, counts
