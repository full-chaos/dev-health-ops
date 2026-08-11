"""CHAOS-3634/3643: capacity/staffing findings carry a real denominator
disclosure, and packet construction survives it.

Before this fix, ``drivers.discover_drivers`` produced a
``CAPACITY_OR_STAFFING`` finding (``drv_metric_atlas_load`` on
``team_atlas``, from the corpus's interruption-load measurement) with no
``staffing_qualification`` at all. The frozen contract's
``DriverCandidate.validate_staffing_claims_are_qualified`` refuses to
construct such a candidate — "a staffing claim that says nothing about its
denominator is an unsupported claim" — so ``team_atlas`` could never
produce a driver-bearing packet (CHAOS-3634's denial-of-packet), and the
same rule is what CHAOS-3643's "unsupported certainty" case is the other
half of: a weak denominator paired with ``MEASURED_CERTAIN`` is refused
independently.

Every test here plants the specific shape its guard exists to catch:
construct a finding that WOULD abort or WOULD overstate certainty unless
``drivers._qualify_staffing`` / ``packet_builder._staffing_qualification``
are what stop it.
"""

from __future__ import annotations

import asyncio

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ConfidenceQualifier,
    DriverCategory,
    DriverRole,
    DriverStanding,
    PacketLimitationKind,
    StaffingDenominatorState,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import (
    ASSERTED_DRIVER_STANDINGS,
    UNQUALIFIED_DENOMINATOR_STATES,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.drivers import (
    DriverFinding,
    _qualify_staffing,
    discover_drivers,
)
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    _staffing_qualification,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from tests.context_fabric import chaos_3620_spine as spine


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _findings(projection, subject: str) -> dict[str, DriverFinding]:
    grant = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
    readout = asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=2,
        )
    )
    return {
        item.driver_id: item
        for item in discover_drivers(readout, subject, as_of=world.TRIAL_NOW)[0]
    }


def _finding(**overrides: object) -> DriverFinding:
    base: dict[str, object] = {
        "driver_id": "drv_test",
        "subject_id": "proj_test",
        "cause_id": "proj_test",
        "category": DriverCategory.CAPACITY_OR_STAFFING,
        "role": DriverRole.CONTEXTUAL_CORRELATE,
        "standing": DriverStanding.CANDIDATE_ONLY,
        "mechanism": "cited_measurement",
        "summary_subject": "proj_test",
        "summary_detail": "test",
    }
    base.update(overrides)
    return DriverFinding(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# drivers._qualify_staffing: the unit under test, in isolation
# --------------------------------------------------------------------------


class TestQualifyStaffingUnit:
    def test_a_capacity_finding_gets_denominator_absent(self) -> None:
        qualified = _qualify_staffing(_finding())
        assert (
            qualified.staffing_denominator_state
            is StaffingDenominatorState.DENOMINATOR_ABSENT
        )
        assert qualified.staffing_qualification_note

    def test_the_note_names_what_evidence_actually_backs_the_claim(self) -> None:
        """A disclosure that says nothing is the CHAOS-3634 defect restated."""

        qualified = _qualify_staffing(_finding())
        note = qualified.staffing_qualification_note or ""
        assert "allocation" in note or "headcount" in note
        assert "interruption" in note or "workload" in note

    def test_a_non_capacity_finding_is_untouched(self) -> None:
        """The mutual-exclusion rule: a non-staffing driver must never
        carry a staffing qualification, so this must be a true no-op."""

        finding = _finding(category=DriverCategory.QUALITY_OR_DEFECT)
        qualified = _qualify_staffing(finding)
        assert qualified is finding
        assert qualified.staffing_denominator_state is None
        assert qualified.staffing_qualification_note is None

    def test_confidence_is_never_upgraded_by_qualification(self) -> None:
        """Qualifying the denominator must never itself grant certainty."""

        finding = _finding(confidence_qualifier=ConfidenceQualifier.QUALIFIED)
        qualified = _qualify_staffing(finding)
        assert qualified.confidence_qualifier is ConfidenceQualifier.QUALIFIED


# --------------------------------------------------------------------------
# packet_builder._staffing_qualification: the wire-shape translation
# --------------------------------------------------------------------------


class TestStaffingQualificationTranslation:
    def test_a_qualified_finding_becomes_a_contract_object(self) -> None:
        finding = _qualify_staffing(_finding())
        qualification = _staffing_qualification(finding)
        assert qualification is not None
        assert (
            qualification.denominator_state
            is StaffingDenominatorState.DENOMINATOR_ABSENT
        )
        assert qualification.qualification_note == finding.staffing_qualification_note

    def test_a_non_staffing_finding_translates_to_none(self) -> None:
        finding = _finding(category=DriverCategory.QUALITY_OR_DEFECT)
        assert _staffing_qualification(finding) is None

    def test_a_staffing_finding_with_no_note_raises_rather_than_guesses(self) -> None:
        """An internal-inconsistency guard: ``_qualify_staffing`` never
        produces this shape, but a future caller that sets the state
        without a note must not silently ship an empty disclosure."""

        broken = _finding(
            staffing_denominator_state=StaffingDenominatorState.DENOMINATOR_ABSENT,
            staffing_qualification_note=None,
        )
        with pytest.raises(ValueError, match="qualification note"):
            _staffing_qualification(broken)


# --------------------------------------------------------------------------
# End to end: the real corpus case that used to abort packet construction
# --------------------------------------------------------------------------


class TestTheTeamAtlasCase:
    def test_the_capacity_finding_is_produced_and_qualified(self, helio) -> None:
        found = _findings(helio, "team_atlas")
        load = found["drv_metric_atlas_load"]
        assert load.category is DriverCategory.CAPACITY_OR_STAFFING
        assert (
            load.staffing_denominator_state
            is StaffingDenominatorState.DENOMINATOR_ABSENT
        )
        assert load.staffing_qualification_note

    def test_the_finding_is_never_asserted(self, helio) -> None:
        """The standing cap (test_chaos_3620_semantic_safety.py's own
        rule) must hold regardless of this fix."""

        found = _findings(helio, "team_atlas")
        load = found["drv_metric_atlas_load"]
        assert load.standing not in ASSERTED_DRIVER_STANDINGS

    def test_the_confidence_never_exceeds_qualified(self, helio) -> None:
        """CHAOS-3643's own case: a weak denominator must never travel
        with certain confidence."""

        found = _findings(helio, "team_atlas")
        load = found["drv_metric_atlas_load"]
        assert load.confidence_qualifier is not ConfidenceQualifier.MEASURED_CERTAIN

    def test_the_full_packet_now_constructs_without_raising(self) -> None:
        """The denial-of-packet CHAOS-3634 exists to fix, observed closed."""

        investigation = spine.investigate("team_atlas", with_drivers=True)
        assert investigation.packet is not None

    def test_the_packet_discloses_the_absent_denominator(self) -> None:
        investigation = spine.investigate("team_atlas", with_drivers=True)
        limitations = investigation.packet.evidence_coverage.limitations
        assert any(
            item.kind is PacketLimitationKind.ABSENT_STAFFING_DENOMINATOR
            for item in limitations
        )

    def test_no_staffing_candidate_in_the_packet_is_asserted_or_certain(self) -> None:
        investigation = spine.investigate("team_atlas", with_drivers=True)
        capacity = [
            candidate
            for candidate in investigation.packet.driver_analysis.candidates
            if candidate.category is DriverCategory.CAPACITY_OR_STAFFING
        ]
        assert capacity, "no capacity candidate reached the packet; test is vacuous"
        for candidate in capacity:
            assert candidate.standing not in ASSERTED_DRIVER_STANDINGS
            assert (
                candidate.confidence_qualifier
                is not ConfidenceQualifier.MEASURED_CERTAIN
            )
            assert candidate.staffing_qualification is not None
            assert (
                candidate.staffing_qualification.denominator_state
                in UNQUALIFIED_DENOMINATOR_STATES
            )


# --------------------------------------------------------------------------
# The A05 boundary: what this fix does NOT claim
# --------------------------------------------------------------------------


class TestTheFixDoesNotClaimPersonLevelSafetyIsNew:
    """CHAOS-3634's acceptance mentions the A05 person-level-bait case.

    A05's own oracle (``oracles.py::A05_person_level_bait``) is about
    whether the QUESTION INTERPRETER ever commits ``team_atlas`` as a
    subject for a colloquial single-team question in the first place --
    that policy is out of this arm's reach entirely (it lives upstream of
    discovery, in Lane B's routing/interpretation territory) and is not
    touched by this fix. What this fix owns, and what this test pins, is
    the arm's OWN defence in depth: even when the arm IS asked to
    investigate ``team_atlas`` directly, nothing it produces is asserted,
    nothing is a per-person claim, and nothing crashes.
    """

    def test_the_capacity_finding_names_no_individual(self, helio) -> None:
        found = _findings(helio, "team_atlas")
        load = found["drv_metric_atlas_load"]
        # An aggregate team-level workload measurement, never a contributor
        # id or a person-shaped attribute.
        assert "contributor" not in load.driver_id
        assert load.subject_id == "team_atlas"
        assert load.cause_id == "team_atlas"

    def test_unrelated_evidence_for_team_atlas_survives_the_fix(self, helio) -> None:
        """The other half of "does not abort unrelated evidence": a fix
        that only qualified the capacity finding but broke something else
        about team_atlas's packet would be a regression this test catches."""

        investigation = spine.investigate("team_atlas", with_drivers=True)
        assert investigation.packet.related_context.entities, (
            "team_atlas produced an empty related context; unrelated "
            "evidence did not survive packet construction"
        )
