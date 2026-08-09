"""CHAOS-3617: tenant, repository and authorization negative tests.

The issue's requirement is absolute — "cross-tenant near-duplicates NEVER
cross" — so the fixture world is built to make a leak *visible*: ``org_beta``
holds a team whose canonical id is byte-identical to ``org_alpha``'s and a
project whose id differs by one trailing character.

The other half is that graph membership never grants access. A restricted
entity that is genuinely *connected* is the only interesting case: proving a
disconnected node stays unreached proves nothing, so
``proj_restricted_billing`` has a real ``depends_on`` edge into the
neighbourhood and is deliberately left out of the authorized set.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    TrialContext,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_PRODUCED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
_RUN_ID = "4f9a2c1e-1111-4222-8333-444455556666"


def _read(projection, seeds, authorized, *, org_id=None, max_hops=3):
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=org_id or projection.org_id,
            seed_canonical_ids=seeds,
            authorized_entity_ids=authorized,
            max_hops=max_hops,
        )
    )


def _packet(readout, signer):
    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_auth",
            question_family=QuestionFamilyID("project_status_drivers"),
            job_statement="Status of the Nightfall Migration project.",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=fixtures.WINDOW_START,
            window_end=fixtures.WINDOW_END,
        ),
        watermark=IndexWatermark(
            indexed_through=fixtures.WINDOW_END,
            projected_at=fixtures.WINDOW_END,
            records_indexed=42,
        ),
        signer=signer,
        trial=TrialContext(run_id=_RUN_ID),
        produced_at=_PRODUCED_AT,
    )


class TestTenantIsolation:
    def test_a_reader_refuses_a_projection_from_another_organization(
        self, alpha_projection
    ) -> None:
        with pytest.raises(PermissionError, match="belongs to organization"):
            _read(
                alpha_projection,
                ["proj_nightfall_migration"],
                fixtures.alpha_authorized_ids(),
                org_id=fixtures.BETA_ORG,
            )

    def test_the_two_tenants_partition_apart(
        self, alpha_projection, beta_projection
    ) -> None:
        assert alpha_projection.partition != beta_projection.partition

    def test_an_identical_canonical_id_in_two_tenants_is_two_distinct_nodes(
        self, alpha_projection, beta_projection
    ) -> None:
        """``team_platform`` exists in both. It must never be one node."""

        alpha_team = next(
            node
            for node in alpha_projection.nodes
            if node.canonical_id == "team_platform"
        )
        beta_team = next(
            node
            for node in beta_projection.nodes
            if node.canonical_id == "team_platform"
        )
        assert alpha_team.uuid != beta_team.uuid
        assert alpha_team.partition != beta_team.partition

    def test_betas_near_duplicate_project_is_unreachable_from_alpha(
        self, alpha_projection
    ) -> None:
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            # Even if the authorized set were wrong and *named* beta's
            # project, alpha's graph must not contain it.
            (*fixtures.alpha_authorized_ids(), "proj_nightfall_migrations"),
        )
        reached = {entity.canonical_id for entity in readout.entities}
        assert "proj_nightfall_migrations" not in reached

    def test_a_seed_from_the_other_tenant_resolves_to_nothing(
        self, alpha_projection
    ) -> None:
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migrations"],
            (*fixtures.alpha_authorized_ids(), "proj_nightfall_migrations"),
        )
        assert readout.entities == ()
        assert readout.paths == ()


class TestAuthorizationFiltering:
    def test_a_connected_but_unauthorized_entity_is_never_reached(
        self, alpha_projection
    ) -> None:
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        reached = {entity.canonical_id for entity in readout.entities}
        assert "proj_restricted_billing" not in reached

    def test_no_path_routes_through_an_unauthorized_entity(
        self, alpha_projection
    ) -> None:
        """Routing through leaks existence even when the record is withheld."""

        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        touched: set[str] = set()
        for path in readout.paths:
            touched |= path.touched_ids()
        assert "proj_restricted_billing" not in touched

    def test_the_filtering_is_counted_and_disclosed(
        self, alpha_projection, signer
    ) -> None:
        """A silent filter is indistinguishable from a complete answer."""

        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        assert readout.authorization_filtered_count >= 1
        packet = _packet(readout, signer)
        assert packet.subject_discovery.authorization_filtered_count == (
            readout.authorization_filtered_count
        )
        kinds = {item.kind.value for item in packet.evidence_coverage.limitations}
        assert "authorization_filtered" in kinds

    def test_authorizing_the_restricted_entity_makes_it_appear(
        self, alpha_projection
    ) -> None:
        """The negative control.

        Without this, "not reached" could mean the traversal was broken
        rather than that authorization refused it -- which would make every
        assertion above pass for the wrong reason.
        """

        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            (*fixtures.alpha_authorized_ids(), "proj_restricted_billing"),
        )
        reached = {entity.canonical_id for entity in readout.entities}
        assert "proj_restricted_billing" in reached
        assert readout.authorization_filtered_count == 0

    def test_an_unauthorized_seed_is_not_investigated(self, alpha_projection) -> None:
        readout = _read(
            alpha_projection,
            ["proj_restricted_billing"],
            fixtures.alpha_authorized_ids(),
        )
        assert readout.entities == ()

    def test_every_emitted_hop_endpoint_is_an_authorized_entity(
        self, alpha_projection, signer
    ) -> None:
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        packet = _packet(readout, signer)
        authorized_entities = set(fixtures.alpha_authorized_ids())
        for path in packet.related_context.paths:
            for hop in path.hops:
                assert hop.source_entity_id in authorized_entities
                assert hop.target_entity_id in authorized_entities

    def test_the_builder_refuses_a_readout_whose_paths_escape_the_set(
        self, alpha_projection, signer
    ) -> None:
        """Defence in depth: the builder re-checks rather than trusting.

        The readout is the builder's input, so a readback bug -- or a future
        second reader -- must not be able to smuggle an endpoint past the
        emission step.
        """

        import dataclasses

        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        tampered = dataclasses.replace(
            readout, authorized_entity_ids=("proj_nightfall_migration",)
        )
        with pytest.raises(PermissionError, match="not in the authorized entity set"):
            _packet(tampered, signer)


class TestRepositoryScoping:
    def test_repository_scoped_records_carry_their_repository_ids(
        self, alpha_projection
    ) -> None:
        """Repository scope has to reach the packet to be enforceable there."""

        pr_node = next(
            node for node in alpha_projection.nodes if node.canonical_id == "pr_4412"
        )
        assert pr_node.repository_ids == ("repo_auth_gateway",)

    def test_evidence_carries_repository_scope_through_to_the_packet(
        self, alpha_projection, signer
    ) -> None:
        readout = _read(
            alpha_projection,
            ["proj_nightfall_migration"],
            fixtures.alpha_authorized_ids(),
        )
        packet = _packet(readout, signer)
        review = next(
            entry
            for entry in packet.evidence_coverage.evidence_index
            if entry.evidence.entity_id == "rev_4412_1"
        )
        assert tuple(review.evidence.repository_ids) == ("repo_auth_gateway",)
