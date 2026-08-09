"""CHAOS-3620: does every material claim close to authorized evidence?

The issue's provenance bullets, taken one at a time against the composed
arm. Two of them do not hold, and both are asserted as defects rather than
described, so that closing either turns this module red:

* no emitted **relationship** closes to evidence — ``_lineage_path`` sets
  ``evidence_ref_ids=()`` as a literal (``packet_builder.py:632``) while the
  readout's ``PathStep`` carries the observation ids the corpus supplied;
* ``relevance`` is a literal ``CURRENT`` at eight emitter sites, so a
  relationship that ended two months before the trial instant is presented
  to a consumer as current.

The rest do hold, and hold for reasons this module makes visible rather than
assuming. In particular ``_assert_support_is_closed`` is shown *rejecting*
an arm-shaped bad response, not merely accepting a good one: a golden packet
that validates proves nothing about whether the validator can refuse.

**The conflict case is NOT_ACCEPTED.** Conflicting source assertions are
never retained — ``conflicts`` is an empty literal
(``packet_builder.py:1220``) — and the acceptance of that requirement is
blocked on CHAOS-3612, whose evidence-id vocabularies do not overlap. It is
recorded in ``chaos_3620_dispositions.py`` with that reason and is not
scored here as a pass.
"""

from __future__ import annotations

import dataclasses
from functools import cache

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    AssertionBasis,
    DriverStanding,
    RelationshipType,
)
from dev_health_ops.api.dev.investigation_contract.relationships import (
    RELATIONSHIP_ALLOWLIST,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import (
    ASSERTED_DRIVER_STANDINGS,
    RelationshipDirection,
)
from dev_health_ops.api.dev.investigation_corpus import world
from tests.context_fabric import chaos_3620_spine as spine

#: Subjects the corpus gives the arm enough structure to assert something
#: about. Chosen by what the arm actually produces rather than by hand: a
#: provenance sweep over subjects with no drivers is a sweep over nothing.
DRIVER_BEARING_SEEDS = ("proj_identity_rewrite", "proj_pulse", "proj_meridian")


@cache
def _every_finding():
    """Every driver finding the analyst can produce, over the WHOLE grant.

    Read from ``discover_drivers`` rather than from packets so no subject is
    lost to a downstream emission refusal, and swept over all 47 grant
    members rather than the three driver-bearing seeds — a "no finding
    anywhere does X" claim cannot rest on three subjects.
    """

    return tuple(
        (seed, finding)
        for seed in sorted(world.PRINCIPALS[world.PRINCIPAL_ANALYST].visible_entity_ids)
        for finding in spine.findings_for(seed)
    )


def _asserted(packet):
    return [
        candidate
        for candidate in packet.driver_analysis.candidates
        if candidate.standing in ASSERTED_DRIVER_STANDINGS
    ]


# --------------------------------------------------------------------------
# Driver closure
# --------------------------------------------------------------------------


class TestEveryAssertedDriverClosesToEvidenceInThisPacket:
    def test_at_least_one_subject_produces_an_asserted_driver(self) -> None:
        """Anti-vacuity for the whole class.

        Every closure claim below is universally quantified over asserted
        drivers. If the arm asserted none, all of them would hold and none
        would mean anything.
        """

        asserted = [
            candidate.driver_id
            for seed in DRIVER_BEARING_SEEDS
            for candidate in _asserted(
                spine.investigate(seed, with_drivers=True).packet
            )
        ]
        assert asserted, (
            "no seed produced an asserted driver, so every closure assertion "
            "in this class is vacuously true"
        )

    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_each_cites_lineage_the_packet_itself_declares(self, seed: str) -> None:
        packet = spine.investigate(seed, with_drivers=True).packet
        declared = {path.path_id for path in packet.related_context.paths}
        for candidate in _asserted(packet):
            assert candidate.supporting_path_ids, (
                f"asserted driver {candidate.driver_id} cites no lineage at all"
            )
            dangling = sorted(set(candidate.supporting_path_ids) - declared)
            assert not dangling, (
                f"asserted driver {candidate.driver_id} cites paths this "
                f"packet does not carry: {dangling}"
            )

    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_each_cites_evidence_the_packet_itself_indexes(self, seed: str) -> None:
        packet = spine.investigate(seed, with_drivers=True).packet
        indexed = {
            entry.evidence.evidence_ref_id
            for entry in packet.evidence_coverage.evidence_index
        }
        for candidate in _asserted(packet):
            assert candidate.supporting_evidence_ids, (
                f"asserted driver {candidate.driver_id} cites no evidence"
            )
            dangling = sorted(set(candidate.supporting_evidence_ids) - indexed)
            assert not dangling, (
                f"asserted driver {candidate.driver_id} cites handles this "
                f"packet never indexed: {dangling}"
            )

    def test_the_closure_check_REJECTS_a_driver_citing_unindexed_evidence(
        self,
    ) -> None:
        """The rejection, which is the only thing that proves the check.

        Arm-shaped rather than malformed: a real finding produced by the real
        discovery pass, with one evidence id swapped for another real
        observation id that this packet does not index. Nothing about the
        shape is wrong; only the closure is.
        """

        investigation = spine.investigate("proj_identity_rewrite", with_drivers=True)
        finding = next(
            item
            for item in investigation.findings
            if item.standing in ASSERTED_DRIVER_STANDINGS
        )
        forged = dataclasses.replace(
            finding,
            evidence_ids=(*finding.evidence_ids, "obs_this_packet_never_saw"),
        )
        with pytest.raises(ValueError, match="never indexed"):
            spine.packet_from(investigation.readout, drivers=(forged,))

    def test_the_same_finding_UNMODIFIED_still_emits(self) -> None:
        """The paired control. Without it the rejection above could be the
        emitter refusing everything."""

        investigation = spine.investigate("proj_identity_rewrite", with_drivers=True)
        packet = spine.packet_from(
            investigation.readout, drivers=investigation.findings
        )
        assert _asserted(packet), (
            "the unmodified findings produced no asserted driver, so the "
            "rejection above is not attributable to the forged citation"
        )

    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_every_indexed_item_says_what_it_supports(self, seed: str) -> None:
        """ "Driver paths identify why each item was included", from the
        evidence side: an indexed item supporting nothing is an item nobody
        can attribute."""

        packet = spine.investigate(seed, with_drivers=True).packet
        orphans = [
            entry.evidence.evidence_ref_id
            for entry in packet.evidence_coverage.evidence_index
            if not entry.supports_entity_ids
            and not entry.supports_subject_ids
            and not entry.supports_driver_ids
        ]
        assert not orphans, (
            f"packet for {seed} indexes evidence that supports nothing: {orphans[:5]}"
        )


class TestInclusionIsAlwaysExplained:
    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_every_related_entity_states_why_it_is_there(self, seed: str) -> None:
        packet = spine.investigate(seed, with_drivers=True).packet
        for entity in packet.related_context.entities:
            assert entity.inclusion_reason.strip(), (
                f"{entity.entity_id} is in the packet with no stated reason"
            )
            assert entity.supporting_path_ids, (
                f"{entity.entity_id} claims to be related with no lineage citing it"
            )

    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_every_path_states_why_it_is_there(self, seed: str) -> None:
        packet = spine.investigate(seed, with_drivers=True).packet
        for path in packet.related_context.paths:
            assert path.inclusion_reason.strip(), (
                f"path {path.path_id} is in the packet with no stated reason"
            )


# --------------------------------------------------------------------------
# Canonical identity and direction
# --------------------------------------------------------------------------


class TestCanonicalIdsAndDirectionSurviveEmission:
    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_every_hop_endpoint_is_a_canonical_world_id(self, seed: str) -> None:
        """Not a node uuid, not a partition-qualified name, not a label."""

        packet = spine.investigate(seed, with_drivers=True).packet
        unknown = sorted(
            {
                endpoint
                for path in packet.related_context.paths
                for hop in path.hops
                for endpoint in (hop.source_entity_id, hop.target_entity_id)
                if endpoint not in world.ENTITIES_BY_ID
            }
        )
        assert not unknown, (
            f"packet for {seed} emits hop endpoints that are not canonical "
            f"world ids: {unknown}"
        )

    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_every_hop_orientation_is_permitted_by_the_frozen_allowlist(
        self, seed: str
    ) -> None:
        packet = spine.investigate(seed, with_drivers=True).packet
        for path in packet.related_context.paths:
            for hop in path.hops:
                orientation = RELATIONSHIP_ALLOWLIST[hop.relationship]
                source_kind, target_kind = (
                    hop.source_entity_kind,
                    hop.target_entity_kind,
                )
                if hop.direction is RelationshipDirection.REVERSE:
                    source_kind, target_kind = target_kind, source_kind
                assert orientation.permits(source_kind, target_kind), (
                    f"hop {hop.source_entity_id} -{hop.relationship}-> "
                    f"{hop.target_entity_id} in {path.path_id} is oriented in "
                    "a direction the frozen allowlist forbids"
                )

    @pytest.mark.parametrize("seed", DRIVER_BEARING_SEEDS)
    def test_the_emitted_direction_agrees_with_the_world_that_produced_it(
        self, seed: str
    ) -> None:
        """The inversion this catches is invisible to the allowlist.

        A hop can be allowlist-legal and still point the wrong way if the
        emitter swapped the endpoints — the allowlist only constrains the
        *kinds*. Checked against the corpus edge that produced it, which is
        the only thing that knows the true orientation.
        """

        forward = {
            (edge.source_entity_id, edge.relationship, edge.target_entity_id)
            for edge in world.RELATIONSHIPS_BY_KEY.values()
        }
        packet = spine.investigate(seed, with_drivers=True).packet
        for path in packet.related_context.paths:
            for hop in path.hops:
                if hop.direction is RelationshipDirection.FORWARD:
                    triple = (
                        hop.source_entity_id,
                        hop.relationship,
                        hop.target_entity_id,
                    )
                else:
                    triple = (
                        hop.target_entity_id,
                        hop.relationship,
                        hop.source_entity_id,
                    )
                assert triple in forward, (
                    f"hop {triple} in {path.path_id} does not correspond to "
                    "any relationship in the world in that orientation: "
                    "causality is inverted"
                )

    def test_the_contract_REFUSES_a_reversed_hop(self) -> None:
        """The validator's own refusal, on a hop the allowlist forbids.

        Proves the direction checks above are backed by something that can
        say no, rather than by a producer that happens to be correct today.
        """

        from dev_health_ops.api.dev.investigation_contract.packet import LineageHop
        from dev_health_ops.api.dev.investigation_contract.vocabulary import (
            InvestigationSubjectKind,
            RelevanceState,
        )

        with pytest.raises(ValueError):
            LineageHop(
                source_entity_id="wu_authcore_release",
                source_entity_kind=InvestigationSubjectKind.WORK_UNIT,
                relationship=RelationshipType.OWNED_BY_TEAM,
                direction=RelationshipDirection.FORWARD,
                target_entity_id="proj_identity_rewrite",
                target_entity_kind=InvestigationSubjectKind.PROJECT,
                observed_at=world.TRIAL_NOW,
                relevance=RelevanceState.CURRENT,
            )


# --------------------------------------------------------------------------
# Measured vs source-asserted vs inferred
# --------------------------------------------------------------------------


class TestTheThreeKindsOfClaimStayDistinguishable:
    def test_no_structural_finding_claims_to_be_measured(self) -> None:
        """A structural driver is a claim about shape, never about a number.

        The first version of this test only checked that a MEASURED candidate
        carried *some* evidence — which a structural finding relabelled
        MEASURED would satisfy, since structural findings cite evidence too.
        Adversarial review pointed out it could not reject anything.

        The real invariant is about the MECHANISM: ``AssertionBasis.MEASURED``
        is reachable only on the cited-measurement path (``drivers.py:936``),
        so a finding whose mechanism is ``STRUCTURAL`` must never carry it.
        Swept over every authorized subject rather than three, because "no
        structural finding anywhere" is the claim being made.
        """

        from dev_health_ops.context_fabric.graph_arm.drivers import StandingMechanism

        structural = [
            (seed, finding)
            for seed, finding in _every_finding()
            if finding.mechanism is StandingMechanism.STRUCTURAL
        ]
        assert structural, (
            "no structural finding exists anywhere in the authorized world, "
            "so this ban is vacuous"
        )
        offenders = [
            (seed, finding.driver_id)
            for seed, finding in structural
            if finding.assertion_basis is AssertionBasis.MEASURED
        ]
        assert not offenders, (
            "a structural finding claims a MEASURED assertion basis, which "
            f"presents a graph traversal as a canonical measurement: {offenders}"
        )

    def test_and_a_MEASURED_basis_only_ever_comes_from_a_cited_measurement(
        self,
    ) -> None:
        """The same invariant from the other side, so neither direction is
        satisfied by the arm simply never using the basis."""

        from dev_health_ops.context_fabric.graph_arm.drivers import StandingMechanism

        measured = [
            (seed, finding)
            for seed, finding in _every_finding()
            if finding.assertion_basis is AssertionBasis.MEASURED
        ]
        for seed, finding in measured:
            assert finding.mechanism is StandingMechanism.CITED_MEASUREMENT, (
                f"{seed}/{finding.driver_id} carries a MEASURED basis from "
                f"mechanism {finding.mechanism}"
            )

    def test_a_measurement_finding_can_never_be_asserted_as_a_driver(self) -> None:
        """The ceiling that keeps a number from becoming a cause.

        Measurement-derived findings are capped at
        ``CONTEXTUAL_CORRELATE`` (``drivers.py:905``, ``:928``), and
        ``_classify`` refuses asserted standing to any non-DRIVER role
        (``drivers.py:609-616``). A cohort outlier is a thing worth showing
        and never a thing worth attributing.
        """

        seen = 0
        for seed in ("proj_meridian", "proj_lattice", "proj_beacon"):
            investigation = spine.investigate(seed, with_drivers=True)
            for finding in investigation.findings:
                if not finding.driver_id.startswith("drv_metric_"):
                    continue
                seen += 1
                assert finding.standing not in ASSERTED_DRIVER_STANDINGS, (
                    f"a measurement-derived finding {finding.driver_id} "
                    f"reached asserted standing {finding.standing}"
                )
        assert seen, (
            "no measurement-derived finding was produced by any seed, so the "
            "ceiling above was never tested"
        )

    def test_an_uncomparable_measurement_is_excluded_for_insufficient_measurement(
        self,
    ) -> None:
        """The refusal that IS exercised: a value with no cohort median.

        Distinct from the measurement-only *category* guard recorded below —
        guard injection established that the two are different code paths and
        that only this one fires on the corpus.
        """

        investigation = spine.investigate("proj_meridian", with_drivers=True)
        uncited = [
            finding
            for finding in investigation.findings
            if finding.standing is DriverStanding.EXCLUDED
            and finding.exclusion_reason is not None
            and "measurement" in str(finding.exclusion_reason)
        ]
        assert uncited, (
            "no finding was excluded for insufficient measurement, so the "
            "measurement-only refusal is not exercised by this corpus"
        )

    def test_the_measurement_only_CATEGORY_guard_is_currently_unreachable(
        self,
    ) -> None:
        """CHAOS-3620 RECORD — a defensive guard with no reachable input.

        ``_classify`` refuses a measurement-only category reached by anything
        other than a cited measurement (``drivers.py:544-560``). Guard
        injection disabled it and every test still passed: SURVIVED. The
        reason is structural rather than a missing test — no structural rule
        in this revision produces a measurement-only category at all, so the
        guard's condition can never be true.

        Recorded as an assertion rather than deleted, because the guard is
        the right guard for the rule someone adds next, and "unreachable
        today" is exactly the claim that stops being true silently. If a
        structural rule starts producing one of those categories this test
        goes red and the mutation becomes worth writing.
        """

        from dev_health_ops.context_fabric.graph_arm.drivers import (
            MEASUREMENT_ONLY_CATEGORIES,
            StandingMechanism,
        )

        reachable = [
            (seed, finding.driver_id, str(finding.category))
            for seed in (
                "proj_identity_rewrite",
                "proj_pulse",
                "proj_meridian",
                "proj_acr",
                "proj_ledger_migration",
                "proj_solstice",
                "proj_vertex",
            )
            for finding in spine.investigate(seed, with_drivers=True).findings
            if finding.category in MEASUREMENT_ONLY_CATEGORIES
            and finding.mechanism is not StandingMechanism.CITED_MEASUREMENT
        ]
        assert not reachable, (
            "a structural rule now produces a measurement-only category, so "
            "the guard at drivers.py:544-560 is reachable -- add the guard "
            f"injection mutation the CHAOS-3620 record defers: {reachable}"
        )


# --------------------------------------------------------------------------
# Multi-source provenance under authorization
# --------------------------------------------------------------------------


class TestMultiSourceFactsRetainOnlyAuthorizedProvenance:
    def test_evidence_indexed_for_the_analyst_is_only_about_entities_they_see(
        self,
    ) -> None:
        """The surviving-provenance claim, stated as a sweep.

        A fact supported by several sources must lose the sources the caller
        cannot see, and keep the rest — not be dropped wholesale and not be
        kept whole.
        """

        grant = world.PRINCIPALS[world.PRINCIPAL_ANALYST].visible_entity_ids
        for seed in DRIVER_BEARING_SEEDS:
            packet = spine.investigate(seed, with_drivers=True).packet
            for entry in packet.evidence_coverage.evidence_index:
                outside = sorted(set(entry.supports_entity_ids) - grant)
                assert not outside, (
                    f"an indexed item in the {seed} packet claims to support "
                    f"entities the caller cannot see: {outside}"
                )

    def test_a_subject_whose_evidence_is_partly_restricted_still_gets_an_answer(
        self,
    ) -> None:
        """Not refused wholesale.

        ``team_cinder`` owns the restricted project. A caller who cannot see
        one of a subject's neighbours must still receive the rest, with the
        narrowing disclosed — refusing the whole question would be the
        opposite failure and just as wrong.
        """

        investigation = spine.investigate("team_cinder")
        assert investigation.readout.authorization_filtered_count == 1, (
            "this subject no longer has restricted material near it, so the "
            "partial-answer claim is not being tested"
        )
        assert investigation.packet.related_context.entities, (
            "a subject with one restricted neighbour produced an empty "
            "related context: the answer was refused wholesale"
        )


# --------------------------------------------------------------------------
# What does not hold
# --------------------------------------------------------------------------


class TestRelationshipsDoNotCloseToEvidence:
    """CHAOS-3620 DEFECT RECORD — arm-side, merged code.

    "Every material graph-assisted driver or relationship closes to
    authorized canonical evidence." Drivers do. Relationships do not, and
    cannot: ``_lineage_path`` emits ``evidence_ref_ids=()`` as a literal
    (``packet_builder.py:632``).

    The data exists and is discarded. Corpus edges carry ``observation_ids``
    derived from their evidence slugs (``corpus_adapter.py:195``) and the
    readout's ``PathStep`` carries them to the emitter's door.
    """

    def test_the_readout_carries_evidence_for_its_relationships(self) -> None:
        readout = spine.readout_for(("proj_identity_rewrite",))
        with_evidence = [
            step
            for path in readout.paths
            for step in path.steps
            if step.observation_ids
        ]
        assert with_evidence, (
            "no traversal step carries observation ids, so nothing is being "
            "discarded and this defect record is stale"
        )

    def test_and_the_emitted_paths_carry_none(self) -> None:
        packet = spine.investigate("proj_identity_rewrite", with_drivers=True).packet
        cited = {
            handle
            for path in packet.related_context.paths
            for handle in path.evidence_ref_ids
        }
        assert not cited, (
            "lineage paths now cite evidence -- the CHAOS-3620 "
            "relationship-closure defect record must be updated and this "
            "test replaced by the proof that every path closes"
        )


class TestHistoricalRelationshipsAreEmittedAsCurrent:
    """CHAOS-3620 DEFECT RECORD — arm-side, merged code.

    "Current versus historical evidence remains explicit." The arm *knows*:
    ``PathStep.is_current_at`` returns False for the corpus's closed
    dependency, and ``discover_drivers`` correctly excludes the driver built
    on it with ``NOT_CURRENTLY_RELEVANT``. The emitted lineage says
    ``relevance = current`` regardless, because ``relevance`` is a literal at
    eight sites in ``packet_builder.py`` (542, 618, 630, 751, 799, 868, 935,
    982) and nothing computes it.
    """

    def _closed_edge(self):
        closed = [
            edge
            for edge in world.RELATIONSHIPS_BY_KEY.values()
            if edge.valid_to is not None and edge.valid_to < world.TRIAL_NOW
        ]
        assert closed, (
            "the corpus no longer plants a closed relationship, so this "
            "defect record is stale"
        )
        return closed[0]

    def test_the_traversal_knows_the_relationship_has_ended(self) -> None:
        edge = self._closed_edge()
        readout = spine.readout_for((edge.source_entity_id,))
        steps = [
            step
            for path in readout.paths
            for step in path.steps
            if {step.from_canonical_id, step.to_canonical_id}
            == {edge.source_entity_id, edge.target_entity_id}
        ]
        assert steps, "the closed relationship was not traversed at all"
        assert not any(step.is_current_at(world.TRIAL_NOW) for step in steps), (
            "the traversal considers the closed relationship current, so the "
            "emitter is not the only thing that lost the information"
        )

    def test_the_driver_layer_correctly_refuses_to_assert_on_it(self) -> None:
        """The half that works, recorded so a fix does not regress it."""

        investigation = spine.investigate("proj_pulse", with_drivers=True)
        stale = [
            finding
            for finding in investigation.findings
            if finding.cause_id == "dep_ratelimitd"
        ]
        assert stale, "the closed dependency produced no candidate"
        assert all(finding.standing is DriverStanding.EXCLUDED for finding in stale), (
            "a driver was asserted on a relationship that ended two months ago"
        )

    def test_but_the_emitted_lineage_calls_it_current(self) -> None:
        edge = self._closed_edge()
        packet = spine.investigate(edge.source_entity_id, with_drivers=True).packet
        hops = [
            hop
            for path in packet.related_context.paths
            for hop in path.hops
            if {hop.source_entity_id, hop.target_entity_id}
            == {edge.source_entity_id, edge.target_entity_id}
        ]
        assert hops, "the closed relationship was not emitted at all"
        assert all(str(hop.relevance) == "current" for hop in hops), (
            "emitted relevance is no longer an unconditional 'current' -- the "
            "CHAOS-3620 relevance-literal defect record must be updated"
        )


class TestConflictingAssertionsAreNotRetained:
    """CHAOS-3620 — NOT_ACCEPTED, blocked by CHAOS-3612.

    "Conflicts retain both source assertions rather than silently choosing
    one." The packet's ``conflicts`` tuple is an empty literal
    (``packet_builder.py:1220``), so nothing is retained and nothing is
    chosen either.

    This requirement is **not scored as a pass anywhere in this lane.** Its
    acceptance is blocked on CHAOS-3612, which is Backlog: the ground truth
    and the authored sources for the conflict case cite disjoint evidence-id
    vocabularies, so no expectation about conflict provenance is satisfiable
    by any arm. Recorded in ``chaos_3620_dispositions.py`` with that reason;
    the test below pins the current state so the gap cannot close silently.
    """

    def test_no_packet_retains_a_conflict(self) -> None:
        for seed in DRIVER_BEARING_SEEDS:
            packet = spine.investigate(seed, with_drivers=True).packet
            assert packet.evidence_coverage.conflicts == (), (
                f"the {seed} packet now retains conflicts -- CHAOS-3620's "
                "conflict/provenance requirement may be scoreable; check "
                "CHAOS-3612 and update the disposition"
            )

    def test_the_contract_can_express_a_conflict_even_though_the_arm_emits_none(
        self,
    ) -> None:
        """Separates "cannot" from "does not".

        The frozen contract has the field and the vocabulary; the arm simply
        never populates it. That distinction decides whether CHAOS-3612 is
        the only blocker or whether the contract needs work too.
        """

        from dev_health_ops.api.dev.investigation_contract.packet import (
            EvidenceCoverage,
        )

        assert "conflicts" in EvidenceCoverage.model_fields, (
            "the frozen contract has no place to retain a conflict, so "
            "CHAOS-3612 is not the only thing blocking this requirement"
        )
