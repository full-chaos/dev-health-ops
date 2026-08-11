"""CHAOS-3617 PR2: comparison-cohort construction, and the claim it earns.

This module opens a refusal. Before it, ``build_packet`` raised
``UnsupportedComparisonShapeError`` for every non-singular shape; after it,
two cohort-bearing shapes are buildable. Three things therefore have to hold
together in the same commit, and each has tests below:

* **the capability** — peers derived from real edges, every member carrying a
  basis and a named anchor (:class:`TestMembershipIsDerivedNotAsserted`);
* **the refusal that remains** — ``PORTFOLIO_WIDE`` and
  ``ORGANIZATION_WIDE`` are still refused, and so is a cohort-bearing shape
  with no proposal behind it (:class:`TestTheRefusalThatRemains`);
* **the outcome** — a cohort-bearing packet does **not** become ``supported``
  just because it now has a cohort. Standing is the frozen contract's rule
  about *drivers*, and this revision synthesizes none
  (:class:`TestOutcomeIsDerivedFromDriversNotFromShape`).

The load-bearing negative is the third. A cohort is the most tempting thing
in the packet to mistake for an answer: it is populated, it is structured,
and it looks like a comparison was performed. It is not one until something
asserts why the members differ.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import FreshnessState
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.evidence_service import EvidenceRecord
from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    CohortCompleteness,
    CohortEvidenceClassification,
    CohortExclusionReason,
    CohortInclusionBasis,
    ComparisonDimension,
    ComparisonShape,
    DriverCandidate,
    InvestigationEvidenceEntry,
    InvestigationOutcome,
    PacketLimitationKind,
    QuestionFamilyID,
    RelationshipType,
    RelevanceState,
    TruncationReason,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import (
    ASSERTED_DRIVER_STANDINGS,
    AssertionBasis,
    ConfidenceQualifier,
    DriverCategory,
    DriverExclusionReason,
    DriverRole,
    DriverStanding,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.cohort import (
    ANCHOR_RELATIONSHIPS,
    COHORT_BEARING_RELATIONSHIPS,
    PEER_RELATIONSHIPS,
    build_cohort,
)
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    IncomparableCohortError,
    JobContext,
    TrialContext,
    UnsupportedComparisonShapeError,
    build_packet,
    derive_outcome,
)
from dev_health_ops.context_fabric.graph_arm.projection import GraphEdge
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.records import (
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    RelationshipRecord,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

#: The subject the corpus gives the richest peer structure: a portfolio, a
#: team and an explicit shared dependency, so every anchor family is live.
_SUBJECT = "proj_identity_rewrite"

_RUN_ID = "4f9a2c1e-1111-4222-8333-444455556666"
_PRODUCED_AT = datetime(2026, 8, 8, 12, tzinfo=UTC)

#: A separate org for the hand-shaped graphs, so a mini world can never
#: be mistaken for -- or accidentally merged with -- the corpus tenant.
_MINI_ORG = "org_cohort_probe"


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


@pytest.fixture(scope="module")
def analyst_grant():
    return adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)


@pytest.fixture(scope="module")
def compliance_grant():
    return adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE)


def _labels(projection) -> dict[str, tuple[GraphEntityKind, str]]:
    """The ``canonical_id -> (kind, label)`` map ``build_cohort`` takes.

    Excludes the organization partition root: it is an entity node with no
    emittable subject kind, and a cohort that could contain it would be a
    cohort containing the tenant.
    """

    return {
        node.canonical_id: (kind, node.display_label)
        for node in projection.nodes
        if node.is_entity
        and (kind := node.entity_kind) is not None
        and kind is not GraphEntityKind.ORGANIZATION
    }


@pytest.fixture(scope="module")
def labels(helio):
    return _labels(helio)


@pytest.fixture
def proposal(helio, labels, analyst_grant):
    return build_cohort(_SUBJECT, helio.edges, labels, analyst_grant)


# --------------------------------------------------------------------------
# The capability
# --------------------------------------------------------------------------


class TestMembershipIsDerivedNotAsserted:
    def test_the_cohort_supports_a_comparison_at_all(self, proposal) -> None:
        """Anti-vacuity for every negative in this module.

        Almost every other test here asserts something is *absent* from the
        cohort. If the cohort were empty they would all pass and prove
        nothing, so this runs first.
        """

        assert proposal.is_comparable
        assert len(proposal.members) >= 2
        assert proposal.dimensions

    def test_every_member_carries_at_least_one_basis(self, proposal) -> None:
        """A member with no stated basis is the unrelated-project fault."""

        for member in proposal.members:
            assert member.bases, member.canonical_id
            assert len(set(member.bases)) == len(member.bases)

    def test_every_anchored_basis_names_an_anchor_that_really_connects(
        self, proposal, helio
    ) -> None:
        """The rationale must be checkable against the graph, not decorative.

        For each (basis, anchors) pair this asserts the anchor is joined to
        BOTH the subject and the member by an edge of the exact relationship
        that basis came from. This is what separates "derived from a path"
        from "listed because it was nearby".
        """

        relationship_for = {
            basis: relationship for relationship, basis in ANCHOR_RELATIONSHIPS.items()
        }
        adjacency: set[tuple[str, RelationshipType, str]] = set()
        for edge in helio.edges:
            adjacency.add(
                (edge.source_canonical_id, edge.relationship, edge.target_canonical_id)
            )
            adjacency.add(
                (edge.target_canonical_id, edge.relationship, edge.source_canonical_id)
            )

        checked = 0
        for member in proposal.members:
            for basis, anchors in member.basis_anchors:
                if basis not in relationship_for:
                    continue
                relationship = relationship_for[basis]
                assert anchors, f"{member.canonical_id} claims {basis} with no anchor"
                for anchor in anchors:
                    assert (_SUBJECT, relationship, anchor) in adjacency
                    assert (member.canonical_id, relationship, anchor) in adjacency
                    checked += 1
        assert checked, "no anchored basis was checked; this assertion was vacuous"

    def test_a_directly_asserted_peer_is_a_member_not_an_anchor(self, proposal) -> None:
        """The defect the corpus caught before this module shipped.

        ``shares_dependency_with`` is a project-to-project edge: it *is* the
        peer relation. The first draft read it as an anchor edge, which made
        the peer an anchor, and anchors are excluded from their own cohort —
        so the most obviously comparable subject in the graph silently
        vanished. Naming it here so a future refactor that re-merges the two
        relationship families fails loudly.
        """

        beacon = next(
            member
            for member in proposal.members
            if member.canonical_id == "proj_beacon"
        )
        assert CohortInclusionBasis.SHARED_DEPENDENCY in beacon.bases
        assert dict(beacon.basis_anchors)[CohortInclusionBasis.SHARED_DEPENDENCY] == ()

    def test_dependency_edges_never_make_two_subjects_peers(self) -> None:
        """A project and the database it depends on are not comparable.

        Structural, then behavioural. The registry check alone would pass if
        the traversal ignored the registry, so a graph whose *only* edges are
        ``depends_on`` must also yield nothing.
        """

        assert RelationshipType.DEPENDS_ON not in COHORT_BEARING_RELATIONSHIPS

        kinds = {
            "proj_a": GraphEntityKind.PROJECT,
            "proj_b": GraphEntityKind.PROJECT,
            "dep_shared": GraphEntityKind.DEPENDENCY,
        }
        depends = [
            ("proj_a", RelationshipType.DEPENDS_ON, "dep_shared"),
            ("proj_b", RelationshipType.DEPENDS_ON, "dep_shared"),
        ]
        edges, labels, everyone = _mini(kinds, depends)
        assert build_cohort("proj_a", edges, labels, everyone).members == ()

        # The control: the same two projects, joined by an edge that IS
        # cohort-bearing, do become peers. Without this the assertion above
        # would also pass if ``build_cohort`` were broken outright.
        edges, labels, everyone = _mini(
            {**kinds, "pf_x": GraphEntityKind.PORTFOLIO},
            [
                *depends,
                ("proj_a", RelationshipType.BELONGS_TO_PORTFOLIO, "pf_x"),
                ("proj_b", RelationshipType.BELONGS_TO_PORTFOLIO, "pf_x"),
            ],
        )
        peered = build_cohort("proj_a", edges, labels, everyone)
        assert [member.canonical_id for member in peered.members] == ["proj_b"]

    def test_the_two_relationship_families_are_disjoint(self) -> None:
        """A relationship read both ways would make membership order-dependent."""

        assert not (set(ANCHOR_RELATIONSHIPS) & set(PEER_RELATIONSHIPS))
        assert set(COHORT_BEARING_RELATIONSHIPS) == set(ANCHOR_RELATIONSHIPS) | set(
            PEER_RELATIONSHIPS
        )

    def test_the_subject_is_never_its_own_peer(self, proposal) -> None:
        assert _SUBJECT not in {member.canonical_id for member in proposal.members}

    def test_construction_is_deterministic_under_edge_reordering(
        self, helio, labels, analyst_grant
    ) -> None:
        """A recorded trial run must be comparable with a re-run.

        The traversal already had one order-dependence defect caught by the
        differential oracle; this closes the same door on cohort building.
        """

        import random

        baseline = build_cohort(_SUBJECT, helio.edges, labels, analyst_grant)
        for seed in range(6):
            shuffled = list(helio.edges)
            random.Random(seed).shuffle(shuffled)
            other = build_cohort(_SUBJECT, shuffled, labels, analyst_grant)
            assert other.members == baseline.members
            assert other.dimensions == baseline.dimensions
            assert other.exclusions == baseline.exclusions


class TestDimensionsAreEarnedNotDeclared:
    def test_every_dimension_traces_to_a_basis_a_member_actually_holds(
        self, proposal
    ) -> None:
        from dev_health_ops.context_fabric.graph_arm.cohort import _BASIS_DIMENSIONS

        held = {basis for member in proposal.members for basis in member.bases}
        earned = {
            dimension
            for basis in held
            for dimension in _BASIS_DIMENSIONS.get(basis, ())
        }
        assert set(proposal.dimensions) == earned

    def test_no_dimension_the_graph_cannot_compute_is_ever_claimed(self) -> None:
        """Cycle time and throughput are canonical-service measurements.

        A cohort claiming to support them would be the unsupported-comparison
        fault: the packet asserts a comparison it has no numbers for.
        """

        from dev_health_ops.context_fabric.graph_arm.cohort import _BASIS_DIMENSIONS

        claimed = {
            dimension
            for dimensions in _BASIS_DIMENSIONS.values()
            for dimension in dimensions
        }
        forbidden = {
            ComparisonDimension.CYCLE_TIME,
            ComparisonDimension.DELIVERY_THROUGHPUT,
            ComparisonDimension.DEPLOYMENT_FREQUENCY,
            ComparisonDimension.INCIDENT_LOAD,
            ComparisonDimension.CAPACITY_LOAD_RATIO,
        }
        assert claimed, "no dimension is claimed at all; the assertion is vacuous"
        assert not (claimed & forbidden), sorted(str(d) for d in claimed & forbidden)

    def test_a_cohort_with_members_but_no_dimension_is_not_comparable(self) -> None:
        """Populated is not comparable. The frozen contract agrees."""

        from dataclasses import replace

        edges, labels, everyone = _mini(
            {
                "proj_a": GraphEntityKind.PROJECT,
                "proj_b": GraphEntityKind.PROJECT,
                "pf_x": GraphEntityKind.PORTFOLIO,
            },
            [
                ("proj_a", RelationshipType.BELONGS_TO_PORTFOLIO, "pf_x"),
                ("proj_b", RelationshipType.BELONGS_TO_PORTFOLIO, "pf_x"),
            ],
        )
        proposal = build_cohort("proj_a", edges, labels, everyone)
        assert proposal.is_comparable
        assert not replace(proposal, dimensions=()).is_comparable


# --------------------------------------------------------------------------
# Authorization
# --------------------------------------------------------------------------


class TestAuthorizationBoundsTheCohort:
    def test_a_restricted_peer_is_withheld_and_counted(
        self, helio, labels, analyst_grant
    ) -> None:
        """``proj_quarry`` shares a team with ``proj_pulse`` inside one tenant.

        A tenant-derived authorization set returns it. Only the true grant
        catches this, which is the whole reason the corpus plants it.
        """

        proposal = build_cohort("proj_pulse", helio.edges, labels, analyst_grant)
        assert world.PROJ_QUARRY not in {
            member.canonical_id for member in proposal.members
        }
        assert world.PROJ_QUARRY not in {
            exclusion.canonical_id for exclusion in proposal.exclusions
        }, "naming it as an exclusion discloses it just as surely as including it"
        assert proposal.authorization_filtered_count >= 1

    def test_the_compliance_principal_does_see_it(
        self, helio, labels, compliance_grant
    ) -> None:
        """The control. Otherwise 'withheld' could mean 'never a peer'."""

        proposal = build_cohort("proj_pulse", helio.edges, labels, compliance_grant)
        assert world.PROJ_QUARRY in {member.canonical_id for member in proposal.members}
        assert proposal.authorization_filtered_count == 0

    def test_a_peer_reachable_only_through_an_unseen_anchor_is_excluded(self) -> None:
        """Anchor-side authorization, which is easy to omit and invisible.

        The peer itself is authorized. Only the team joining it to the
        subject is not. Including the peer would tell the caller that some
        shared owner exists — the membership itself is the disclosure.
        """

        edges, labels, everyone = _mini(
            {
                "proj_a": GraphEntityKind.PROJECT,
                "proj_b": GraphEntityKind.PROJECT,
                "team_secret": GraphEntityKind.TEAM,
            },
            [
                ("proj_a", RelationshipType.OWNED_BY_TEAM, "team_secret"),
                ("proj_b", RelationshipType.OWNED_BY_TEAM, "team_secret"),
            ],
        )
        blind = build_cohort("proj_a", edges, labels, frozenset({"proj_a", "proj_b"}))
        assert blind.members == ()
        assert blind.authorization_filtered_count == 1

        # Control: with the anchor visible the same peer joins, so the
        # exclusion above is attributable to the anchor and nothing else.
        sighted = build_cohort("proj_a", edges, labels, everyone)
        assert [member.canonical_id for member in sighted.members] == ["proj_b"]

    def test_an_unauthorized_subject_yields_nothing_at_all(
        self, helio, labels, analyst_grant
    ) -> None:
        proposal = build_cohort(world.PROJ_QUARRY, helio.edges, labels, analyst_grant)
        assert proposal.members == ()
        assert proposal.exclusions == ()
        assert proposal.dimensions == ()


# --------------------------------------------------------------------------
# Exclusions and bounds
# --------------------------------------------------------------------------


class TestExclusionsAreResults:
    def test_a_kind_mismatched_peer_is_recorded_rather_than_dropped(
        self, proposal
    ) -> None:
        """ "Why is the repository not in this comparison" is a real question."""

        excluded = {
            exclusion.canonical_id: exclusion for exclusion in proposal.exclusions
        }
        assert excluded, "no exclusion was recorded; the assertion is vacuous"
        for exclusion in excluded.values():
            assert exclusion.reason is CohortExclusionReason.NOT_COMPARABLE_SLICE
            assert exclusion.rationale.strip()
        assert "repo_identity" in excluded

    def test_no_subject_is_both_included_and_excluded(self, proposal) -> None:
        included = {member.canonical_id for member in proposal.members}
        excluded = {exclusion.canonical_id for exclusion in proposal.exclusions}
        assert not (included & excluded)


class TestBoundsAreDisclosed:
    def test_the_size_bound_truncates_and_says_so(
        self, helio, labels, analyst_grant
    ) -> None:
        full = build_cohort(_SUBJECT, helio.edges, labels, analyst_grant)
        assert len(full.members) > 2, "the bound below would not bite"
        assert not full.truncated

        bounded = build_cohort(
            _SUBJECT, helio.edges, labels, analyst_grant, max_members=2
        )
        assert len(bounded.members) == 2
        assert bounded.truncated
        assert bounded.truncated_count == len(full.members) - 2

    def test_a_dimension_only_a_dropped_member_supported_is_dropped_too(self) -> None:
        """Otherwise the cohort claims a comparison it cannot make.

        ``proj_z`` is the only member carrying the shared-dependency basis,
        and dependency exposure is the only dimension that basis supports.
        Bound the cohort so ``proj_z`` falls off and the dimension must go
        with it.
        """

        edges, labels, everyone = _mini(
            {
                "proj_a": GraphEntityKind.PROJECT,
                "proj_b": GraphEntityKind.PROJECT,
                "proj_z": GraphEntityKind.PROJECT,
                "init_x": GraphEntityKind.INITIATIVE,
            },
            [
                ("proj_a", RelationshipType.CONTRIBUTES_TO, "init_x"),
                ("proj_b", RelationshipType.CONTRIBUTES_TO, "init_x"),
                ("proj_a", RelationshipType.SHARES_DEPENDENCY_WITH, "proj_z"),
            ],
        )

        full = build_cohort("proj_a", edges, labels, everyone)
        assert ComparisonDimension.DEPENDENCY_EXPOSURE in full.dimensions

        # ``proj_b`` sorts before ``proj_z``, so a bound of one keeps b.
        bounded = build_cohort("proj_a", edges, labels, everyone, max_members=1)
        assert [member.canonical_id for member in bounded.members] == ["proj_b"]
        assert ComparisonDimension.DEPENDENCY_EXPOSURE not in bounded.dimensions


# --------------------------------------------------------------------------
# The packet: what opened, and what did not
# --------------------------------------------------------------------------


class TestTheShapeThatOpened:
    def test_a_discovered_cohort_packet_passes_the_canonical_validator(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        packet = _packet(helio, analyst_grant, proposal, signer)
        payload = json.loads(packet.model_dump_json())
        model = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]
        model.model_validate(payload)

    def test_the_committed_subject_is_in_its_own_cohort(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        packet = _packet(helio, analyst_grant, proposal, signer)
        subject = next(
            member
            for member in packet.comparison_cohort.members
            if member.canonical_id == _SUBJECT
        )
        assert subject.inclusion_basis == (CohortInclusionBasis.EXPLICITLY_NAMED,)
        assert (
            subject.inclusion_evidence_classification
            is CohortEvidenceClassification.EXPLICITLY_NAMED_BY_QUESTION
        )

    def test_every_peer_states_a_graph_derived_basis_and_names_its_anchor(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        packet = _packet(helio, analyst_grant, proposal, signer)
        peers = [
            member
            for member in packet.comparison_cohort.members
            if member.canonical_id != _SUBJECT
        ]
        assert peers
        for peer in peers:
            assert CohortInclusionBasis.EXPLICITLY_NAMED not in peer.inclusion_basis
            assert (
                peer.inclusion_evidence_classification
                is CohortEvidenceClassification.CANONICAL_REGISTRY_MEMBERSHIP
            )
            source = next(
                item
                for item in proposal.members
                if item.canonical_id == peer.canonical_id
            )
            for anchor in source.via:
                assert anchor in peer.inclusion_rationale

    def test_the_declared_shape_matches_the_job(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        packet = _packet(helio, analyst_grant, proposal, signer)
        assert (
            packet.comparison_cohort.comparison_shape
            is ComparisonShape.DISCOVERED_COHORT
        )
        assert packet.comparison_cohort.completeness is CohortCompleteness.COMPLETE

    def test_a_truncated_cohort_discloses_a_reason_and_a_limitation(
        self, helio, labels, analyst_grant, signer
    ) -> None:
        """A truncation flag with no reason is the fault the contract names."""

        bounded = build_cohort(
            _SUBJECT, helio.edges, labels, analyst_grant, max_members=2
        )
        packet = _packet(helio, analyst_grant, bounded, signer)
        assert packet.comparison_cohort.completeness is CohortCompleteness.TRUNCATED
        assert packet.comparison_cohort.truncation_reason is (
            TruncationReason.COHORT_BUDGET
        )
        kinds = {item.kind for item in packet.evidence_coverage.limitations}
        assert PacketLimitationKind.TRUNCATED_TRAVERSAL in kinds

    def test_a_withheld_peer_surfaces_as_an_authorization_limitation(
        self, helio, labels, analyst_grant, signer
    ) -> None:
        """The count is on the cohort; the disclosure must reach the packet."""

        pulse = build_cohort("proj_pulse", helio.edges, labels, analyst_grant)
        assert pulse.authorization_filtered_count >= 1
        packet = _packet(helio, analyst_grant, pulse, signer, subject="proj_pulse")
        assert packet.comparison_cohort.authorization_filtered_count >= 1
        kinds = {item.kind for item in packet.evidence_coverage.limitations}
        assert PacketLimitationKind.AUTHORIZATION_FILTERED in kinds


class TestTheRefusalThatRemains:
    @pytest.mark.parametrize(
        "shape",
        [ComparisonShape.PORTFOLIO_WIDE, ComparisonShape.ORGANIZATION_WIDE],
    )
    def test_an_exhaustive_shape_is_still_refused(
        self, helio, analyst_grant, proposal, signer, shape
    ) -> None:
        """These assert completeness this arm cannot prove it achieved.

        A partial sweep presented as portfolio-wide is a stronger false claim
        than refusing, so opening the two peer shapes must not open these.
        """

        with pytest.raises(UnsupportedComparisonShapeError, match="exhaustive"):
            _packet(helio, analyst_grant, proposal, signer, shape=shape)

    def test_a_cohort_shape_with_no_proposal_is_refused(
        self, helio, analyst_grant, signer
    ) -> None:
        """The refusal PR1 had, unchanged where nothing earned its removal."""

        with pytest.raises(UnsupportedComparisonShapeError, match="fabricated"):
            _packet(helio, analyst_grant, None, signer)

    def test_a_proposal_under_a_singular_shape_is_refused(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        """Silently discarding it would hide a caller/job mismatch."""

        with pytest.raises(ValueError, match="singular-subject"):
            _packet(
                helio,
                analyst_grant,
                proposal,
                signer,
                shape=ComparisonShape.SINGULAR_SUBJECT,
                family="project_status_drivers",
            )

    def test_a_cohort_that_cannot_compare_is_refused_distinguishably(
        self, helio, labels, compliance_grant, signer
    ) -> None:
        """A capability gap and an empty result must not score the same.

        ``team_atlas`` has no peers at all: no team in the corpus shares an
        owner, a portfolio or an initiative with another team. That is not
        "the arm cannot build cohorts" — it is "there is no comparison
        here", and the two get different exceptions on purpose.

        A subject with exactly ONE peer is deliberately *not* this case: the
        subject is the second cohort member, so that is a legitimate
        one-to-one comparison and the contract accepts it.
        """

        empty = build_cohort("team_atlas", helio.edges, labels, compliance_grant)
        assert empty.members == ()
        with pytest.raises(IncomparableCohortError):
            _packet(helio, compliance_grant, empty, signer, subject="team_atlas")

        one_peer = build_cohort(
            "wu_ledger_backfill", helio.edges, labels, compliance_grant
        )
        assert len(one_peer.members) == 1
        assert one_peer.is_comparable

    def test_a_cohort_built_around_an_uncommitted_subject_is_refused(
        self, helio, labels, analyst_grant, signer
    ) -> None:
        """Peers of some other subject are a comparison of the wrong thing."""

        elsewhere = build_cohort("proj_beacon", helio.edges, labels, analyst_grant)
        with pytest.raises(ValueError, match="did not commit"):
            _packet(helio, analyst_grant, elsewhere, signer)

    def test_a_cohort_naming_an_unauthorized_entity_is_refused(
        self, helio, labels, compliance_grant, analyst_grant, signer
    ) -> None:
        """A cohort built against a wider grant than the traversal used.

        This is the realistic caller mistake: two authorization sets in one
        request. The builder must not take the cohort's word for it.
        """

        wide = build_cohort("proj_pulse", helio.edges, labels, compliance_grant)
        assert world.PROJ_QUARRY in {member.canonical_id for member in wide.members}
        with pytest.raises(PermissionError, match="outside the authorized set"):
            _packet(helio, analyst_grant, wide, signer, subject="proj_pulse")


# --------------------------------------------------------------------------
# The outcome
# --------------------------------------------------------------------------


class TestOutcomeIsDerivedFromDriversNotFromShape:
    def test_a_cohort_bearing_packet_is_still_unsupported(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        """The claim this whole commit rests on.

        A populated cohort looks like a comparison was performed. It is not
        one until something asserts *why* the members differ, and this
        revision synthesizes no drivers.
        """

        packet = _packet(helio, analyst_grant, proposal, signer)
        assert packet.comparison_cohort.members
        assert packet.driver_analysis.candidates == ()
        assert packet.outcome is InvestigationOutcome.UNSUPPORTED

    def test_the_rule_admits_a_supported_outcome_when_drivers_earn_it(
        self, signer
    ) -> None:
        """Both branches, or the negative above proves nothing.

        A constant returning ``UNSUPPORTED`` would satisfy every assertion in
        this class except this one. Exercising the promoting branch is what
        makes "the arm cannot over-claim" a checked property rather than an
        unfalsifiable statement.
        """

        evidence = [_evidence_entry(signer)]
        driver = _driver(DriverStanding.CONTRIBUTING_DRIVER)
        assert (
            derive_outcome([driver], evidence, gaps=False)
            is InvestigationOutcome.SUPPORTED
        )
        assert (
            derive_outcome([driver], evidence, gaps=True)
            is InvestigationOutcome.SUPPORTED_WITH_GAPS
        )

    @pytest.mark.parametrize(
        "weak", [DriverStanding.CANDIDATE_ONLY, DriverStanding.EXCLUDED]
    )
    @pytest.mark.parametrize("gaps", [False, True])
    def test_a_driver_without_standing_never_promotes_the_outcome(
        self, signer, weak, gaps
    ) -> None:
        """Standing is the contract's word, not the producer's confidence."""

        assert weak not in ASSERTED_DRIVER_STANDINGS
        assert (
            derive_outcome([_driver(weak)], [_evidence_entry(signer)], gaps=gaps)
            is InvestigationOutcome.UNSUPPORTED
        )

    def test_an_asserted_driver_with_no_evidence_never_promotes_the_outcome(
        self,
    ) -> None:
        """A judgment with nothing behind it is the redirect fault inverted."""

        driver = _driver(DriverStanding.CONTRIBUTING_DRIVER)
        assert derive_outcome([driver], [], gaps=False) is (
            InvestigationOutcome.UNSUPPORTED
        )

    def test_the_frozen_contract_enforces_the_same_rule_independently(
        self, helio, analyst_grant, proposal, signer
    ) -> None:
        """The rule is the contract's, not this module's invention.

        Overwriting the derived outcome on an otherwise valid packet must be
        rejected by the canonical validator. If it were not, ``derive_outcome``
        would be the only thing standing between the arm and a false claim,
        and a single edit could remove it silently.
        """

        packet = _packet(helio, analyst_grant, proposal, signer)
        payload = json.loads(packet.model_dump_json())
        model = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]

        # The control: as produced, the packet validates. So the refusal
        # below is attributable to the outcome and to nothing else.
        model.model_validate(payload)

        payload["outcome"] = InvestigationOutcome.SUPPORTED.value
        # Two independent contract rules fire on this edit -- the empty
        # driver section this family requires of a supported packet, and the
        # judgment a supported outcome must assert. Matching on "driver"
        # accepts either without accepting an unrelated failure.
        with pytest.raises(ValueError, match="driver"):
            model.model_validate(payload)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _mini(
    entities: Mapping[str, GraphEntityKind],
    relationships: Sequence[tuple[str, RelationshipType, str]],
) -> tuple[
    tuple[GraphEdge, ...], dict[str, tuple[GraphEntityKind, str]], frozenset[str]
]:
    """A small hand-shaped world, built through the REAL projection.

    Deliberately not hand-authored :class:`GraphEdge` objects. The projection
    is what enforces the relationship allowlist and the canonical
    orientation, so a fixture that skipped it could assert cohort behaviour
    over an edge the arm can never actually hold — the hand-authored-fixture
    trap, in miniature.

    Returns the edges, the label map ``build_cohort`` takes, and the set of
    every entity id, for callers that want an unrestricted grant.
    """

    batch = IngestionBatch(
        org_id=_MINI_ORG,
        entities=tuple(
            EntityRecord(
                org_id=_MINI_ORG,
                kind=kind,
                canonical_id=canonical_id,
                display_label=canonical_id.replace("_", " ").title(),
                source_class=SourceClass.WORK_GRAPH,
                observed_at=world.TRIAL_NOW,
            )
            for canonical_id, kind in sorted(entities.items())
        ),
        relationships=tuple(
            RelationshipRecord(
                org_id=_MINI_ORG,
                source=CanonicalRef(kind=entities[source], canonical_id=source),
                relationship=relationship,
                target=CanonicalRef(kind=entities[target], canonical_id=target),
                source_class=SourceClass.WORK_GRAPH,
                observed_at=world.TRIAL_NOW,
            )
            for source, relationship, target in relationships
        ),
    )
    projection = build_projection(batch)
    labels = _labels(projection)
    return projection.edges, labels, frozenset(labels)


def _driver(standing: DriverStanding) -> DriverCandidate:
    """A fully valid driver at the requested standing.

    Built through the real contract model rather than ``model_construct``,
    so a field the contract requires cannot be quietly omitted and
    ``derive_outcome`` cannot be exercised with a driver the contract would
    never accept.

    Deliberately independent of :func:`_packet`. An earlier version drew its
    subject and evidence handle from a freshly built packet, which coupled
    every assertion in the outcome class to the packet build — and the
    guard-injection harness caught it: disabling the derivation made the
    packet build raise first, so the class went red for the contract's
    reason rather than for the derivation's, and the mutation reported
    WRONG-REASON instead of KILLED.
    """

    return DriverCandidate(
        driver_id="drv001",
        category=DriverCategory.DEPENDENCY_PRESSURE,
        summary="a dependency is blocking the subject",
        affected_subject_ids=("proj_probe",),
        role=DriverRole.DRIVER,
        standing=standing,
        assertion_basis=AssertionBasis.SOURCE_ASSERTED,
        confidence_qualifier=ConfidenceQualifier.QUALIFIED,
        supporting_path_ids=("path001",),
        relevance=RelevanceState.CURRENT,
        exclusion_reason=(
            DriverExclusionReason.INSUFFICIENT_MEASUREMENT
            if standing is DriverStanding.EXCLUDED
            else None
        ),
    )


def _evidence_entry(signer) -> InvestigationEvidenceEntry:
    """One indexed evidence item, minted by the platform's own signer.

    The same ``EvidenceReferenceSigner.issue`` path ``packet_builder`` uses,
    so the handle would verify against the evidence service rather than
    against a scheme this test invented.
    """

    handle = signer.issue(
        world.ORG_HELIO,
        EvidenceRecord(
            source_system="context_fabric_graph_arm",
            source_version="graph_arm_source_read.v1",
            entity_type="decision",
            entity_id="obs_probe",
            display_label="a probe observation",
            observed_at=world.TRIAL_NOW,
            freshness=FreshnessState.FRESH,
            provenance="structured record projected into the trial graph",
            confidence=1.0,
        ),
    )
    return InvestigationEvidenceEntry(
        evidence={
            "schema_version": "dev_evidence_ref.v1",
            "evidence_ref_id": handle,
            "source_system": "context_fabric_graph_arm",
            "source_version": "graph_arm_source_read.v1",
            "entity_type": "decision",
            "entity_id": "obs_probe",
            "display_label": "a probe observation",
            "link": None,
            "observed_at": world.TRIAL_NOW,
            "freshness": FreshnessState.FRESH.value,
            "provenance": "structured record projected into the trial graph",
            "confidence": 1.0,
            "flags": {},
        },
        source_class=SourceClass.WORK_GRAPH,
        supports_subject_ids=("proj_probe",),
        relevance=RelevanceState.CURRENT,
    )


def _readout(projection, grant, subject):
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=2,
        )
    )


def _packet(
    projection,
    grant,
    cohort,
    signer,
    *,
    subject: str = _SUBJECT,
    shape: ComparisonShape = ComparisonShape.DISCOVERED_COHORT,
    family: str = "struggling_teams",
):
    return build_packet(
        readout=_readout(projection, grant, subject),
        job=JobContext(
            job_id="job_cohort",
            question_family=QuestionFamilyID(family),
            job_statement="How does this subject compare with its peers?",
            comparison_shape=shape,
            window_start=world.AS_OF_JUL_15,
            window_end=world.TRIAL_NOW,
        ),
        watermark=IndexWatermark(
            indexed_through=world.TRIAL_NOW,
            projected_at=world.TRIAL_NOW,
            records_indexed=1,
        ),
        signer=signer,
        trial=TrialContext(run_id=_RUN_ID, corpus_version=adapter.CORPUS_VERSION),
        produced_at=_PRODUCED_AT,
        cohort=cohort,
    )
