"""CHAOS-3619: what the runner decides an outcome MEANS.

The runner composes pieces that are individually tested; what it adds is
classification, and a mistake there produces a confident artifact from a
broken sweep. Each guard below plants the specific misreading it prevents.

The ordering assertions matter more than they look. Classification is a
cascade, and every pair that could be swapped changes what the trial
reports: a late arm's output must not be scored, a faulted arm must not be
credited with a capability statement, and an arm that was never invoked must
never look like an arm that declined.
"""

from __future__ import annotations

import pytest

from trials.chaos_3619.budget import BudgetOutcome
from trials.chaos_3619.dispositions import CaseDisposition
from trials.chaos_3619.legs import LEG_B_NATIVE_LABEL, LegId
from trials.chaos_3619.runner import ArmAttempt, arm_result, classify, run_id_for


def _ok(elapsed: float = 0.5) -> BudgetOutcome:
    return BudgetOutcome(elapsed_seconds=elapsed, limit_seconds=120.0, exceeded=False)


class TestClassificationOrdering:
    def test_a_timeout_beats_anything_the_arm_reported(self) -> None:
        """A late arm may have produced something; using it would score a
        case already recorded as NOT RUN."""

        attempt = ArmAttempt(invoked=True, payload={"schema_version": "x"})
        budget = BudgetOutcome(
            elapsed_seconds=200.0, limit_seconds=120.0, exceeded=True
        )
        disposition, _ = classify(attempt, budget)
        assert disposition is CaseDisposition.NOT_RUN_TIMEOUT

    def test_an_abandoned_thread_is_a_timeout_and_discloses_the_leak(self) -> None:
        budget = BudgetOutcome(
            elapsed_seconds=120.0,
            limit_seconds=120.0,
            exceeded=True,
            abandoned_thread=True,
        )
        disposition, detail = classify(None, budget)
        assert disposition is CaseDisposition.NOT_RUN_TIMEOUT
        assert "Python cannot kill" in detail

    def test_a_fault_beats_a_refusal(self) -> None:
        """An arm that faulted has made no capability statement.

        Recording one would publish a defect as an honest boundary of the
        technique -- which the ADR would then read as evidence about graph
        assistance itself.
        """

        attempt = ArmAttempt(
            invoked=True, payload=None, refusal="looks like a boundary", fault="boom"
        )
        disposition, detail = classify(attempt, _ok())
        assert disposition is CaseDisposition.ARM_FAULT
        assert detail == "boom"

    def test_a_never_invoked_arm_is_a_precondition_failure_not_a_gap(self) -> None:
        """The distinction the whole trial's gap column rests on."""

        attempt = ArmAttempt(invoked=False, payload=None)
        disposition, detail = classify(attempt, _ok())
        assert disposition is CaseDisposition.NOT_RUN_PRECONDITION
        assert "harness artefact" in detail

    def test_an_invoked_arm_returning_nothing_is_a_declared_gap(self) -> None:
        """A RESULT, not a failure: how often the baseline must decline is
        one of the numbers the comparison turns on."""

        attempt = ArmAttempt(invoked=True, payload=None)
        disposition, _ = classify(attempt, _ok())
        assert disposition is CaseDisposition.ARM_DECLARED_GAP

    def test_a_named_refusal_is_distinct_from_a_bare_gap(self) -> None:
        attempt = ArmAttempt(
            invoked=True, payload=None, refusal="UnsupportedComparisonShapeError: ..."
        )
        disposition, detail = classify(attempt, _ok())
        assert disposition is CaseDisposition.ARM_REFUSED
        assert "UnsupportedComparisonShape" in detail

    def test_a_payload_is_scored(self) -> None:
        attempt = ArmAttempt(invoked=True, payload={"schema_version": "x"})
        disposition, _ = classify(attempt, _ok())
        assert disposition is CaseDisposition.SCORED

    def test_a_missing_attempt_is_never_silently_a_gap(self) -> None:
        disposition, detail = classify(None, _ok())
        assert disposition is CaseDisposition.NOT_RUN_PRECONDITION
        assert "nothing about this arm was observed" in detail


class TestUnscoredRowsCannotAcquireVerdicts:
    """The scoring call sits inside the SCORED branch.

    If it ran before classification, an unscored case would acquire verdicts
    by an ordering accident -- the shape in which a timed-out case becomes a
    column of failures in the report.
    """

    @pytest.mark.parametrize(
        ("attempt", "budget"),
        [
            (ArmAttempt(invoked=True, payload=None), _ok()),
            (ArmAttempt(invoked=False, payload=None), _ok()),
            (
                ArmAttempt(invoked=True, payload={"schema_version": "x"}),
                BudgetOutcome(
                    elapsed_seconds=200.0, limit_seconds=120.0, exceeded=True
                ),
            ),
        ],
        ids=["declared_gap", "never_invoked", "timed_out_with_a_payload"],
    )
    def test_no_verdicts_are_recorded(
        self, attempt: ArmAttempt, budget: BudgetOutcome
    ) -> None:
        result = arm_result(
            arm_id="native",
            leg=LegId.AS_DEPLOYED,
            case_id="T01_clearly_struggling_team",
            attempt=attempt,
            budget=budget,
        )
        assert result.dimension_outcomes == ()
        assert result.is_clean is None

    def test_the_timed_out_row_does_not_claim_a_packet_was_scored(self) -> None:
        """``packet_emitted`` stays truthful -- the arm DID emit one -- while
        the disposition says it was not scored. Both facts, neither implying
        the other."""

        result = arm_result(
            arm_id="graph_assisted_shadow_arm",
            leg=LegId.AS_DEPLOYED,
            case_id="T01_clearly_struggling_team",
            attempt=ArmAttempt(invoked=True, payload={"schema_version": "x"}),
            budget=BudgetOutcome(
                elapsed_seconds=200.0, limit_seconds=120.0, exceeded=True
            ),
        )
        assert result.packet_emitted is True
        assert result.disposition == CaseDisposition.NOT_RUN_TIMEOUT.value
        assert result.dimension_outcomes == ()


class TestTheLegBLabelIsAppliedOnlyWhereItBelongs:
    def test_leg_b_native_rows_are_labelled(self) -> None:
        result = arm_result(
            arm_id="native",
            leg=LegId.JOB_HELD_CONSTANT,
            case_id="T01_clearly_struggling_team",
            attempt=ArmAttempt(invoked=True, payload=None),
            budget=_ok(),
        )
        assert result.figure_label == LEG_B_NATIVE_LABEL

    def test_leg_a_native_rows_are_not_labelled(self) -> None:
        """Leg A IS the deployed baseline; labelling it would say the
        opposite of what is true."""

        result = arm_result(
            arm_id="native",
            leg=LegId.AS_DEPLOYED,
            case_id="T01_clearly_struggling_team",
            attempt=ArmAttempt(invoked=True, payload=None),
            budget=_ok(),
        )
        assert result.figure_label == ""

    def test_leg_b_graph_rows_are_not_labelled(self) -> None:
        """The label is about a classification the NATIVE arm was handed and
        could not derive. The graph arm derives no family either way, so
        labelling its rows would attach the caveat to the wrong column."""

        result = arm_result(
            arm_id="graph_assisted_shadow_arm",
            leg=LegId.JOB_HELD_CONSTANT,
            case_id="T01_clearly_struggling_team",
            attempt=ArmAttempt(invoked=True, payload=None),
            budget=_ok(),
        )
        assert result.figure_label == ""


class TestRunIdsAreDeterministicAndLegDistinct:
    def test_the_same_leg_and_case_always_mint_the_same_id(self) -> None:
        """Two sweeps of one tree must diff cleanly."""

        first = run_id_for(LegId.AS_DEPLOYED, "T01")
        second = run_id_for(LegId.AS_DEPLOYED, "T01")
        assert first == second

    def test_the_two_legs_mint_different_ids_for_one_case(self) -> None:
        """The seam rejects a packet whose trial run id names a different
        run, so a shared id would make Leg B's packets look like stale Leg A
        ones."""

        assert run_id_for(LegId.AS_DEPLOYED, "T01") != run_id_for(
            LegId.JOB_HELD_CONSTANT, "T01"
        )

    def test_the_id_is_a_uuid_as_the_contract_requires(self) -> None:
        """``TrialMetadata.run_id`` is a ServerHandle; a non-UUID makes the
        frozen contract reject every packet, which would report both arms as
        unprojectable for a reason neither arm has."""

        import uuid

        uuid.UUID(run_id_for(LegId.AS_DEPLOYED, "T01"))


class TestTheSweepClockIsPinned:
    def test_produced_at_is_the_corpus_clock_not_wall_time(self) -> None:
        """Wall-clock time makes two runs of one tree differ in every row, so
        a diff between artifacts shows the clock rather than the
        measurement."""

        from dev_health_ops.api.dev.investigation_corpus import world
        from trials.chaos_3619.runner import deterministic_produced_at

        assert deterministic_produced_at([]) == world.TRIAL_NOW


class TestDeferredDefectsAreAttributedButSurprisesAreNot:
    """CHAOS-3634 is descoped, so its fault PERSISTS into the results.

    A fault row carrying no owner is indistinguishable from an unexplained
    crash. The important half is the other direction: an UNEXPECTED fault
    must stay unattributed, or a genuine surprise gets quietly absorbed into
    a known defect and stops looking like news.
    """

    def test_a_known_deferred_fault_carries_its_ticket(self) -> None:
        result = arm_result(
            arm_id="graph_assisted_shadow_arm",
            leg=LegId.AS_DEPLOYED,
            case_id="A05_person_level_bait",
            attempt=ArmAttempt(
                invoked=True,
                payload=None,
                fault=(
                    "ValidationError: driver drv_metric_atlas_load is a "
                    "capacity/staffing driver with no staffing_qualification"
                ),
            ),
            budget=_ok(),
        )
        assert result.disposition == CaseDisposition.ARM_FAULT.value
        assert result.limitation_owner == "CHAOS-3634"

    def test_an_unexpected_fault_stays_unattributed(self) -> None:
        """The half that matters more.

        Attributing an unrecognised fault to a known ticket would hide a new
        defect inside an accepted one -- the trial would report a surprise as
        business as usual.
        """

        result = arm_result(
            arm_id="graph_assisted_shadow_arm",
            leg=LegId.AS_DEPLOYED,
            case_id="S01",
            attempt=ArmAttempt(
                invoked=True, payload=None, fault="ZeroDivisionError: nobody expected"
            ),
            budget=_ok(),
        )
        assert result.disposition == CaseDisposition.ARM_FAULT.value
        assert result.limitation_owner == "", (
            "an unrecognised fault was attributed to a known ticket; a new "
            "defect would be published as an accepted one"
        )

    def test_a_declared_gap_is_not_given_a_defect_owner(self) -> None:
        """Attribution is scoped to faults. A gap is a capability result and
        giving it a defect ticket would recast a boundary as a bug."""

        result = arm_result(
            arm_id="native",
            leg=LegId.AS_DEPLOYED,
            case_id="T01",
            attempt=ArmAttempt(invoked=True, payload=None),
            budget=_ok(),
        )
        assert result.disposition == CaseDisposition.ARM_DECLARED_GAP.value
        assert result.limitation_owner == ""

    def test_every_deferred_entry_names_a_ticket_and_a_transfer(self) -> None:
        from trials.chaos_3619.unsound import DEFERRED_DEFECTS

        for entry in DEFERRED_DEFECTS:
            assert entry.owner.startswith("CHAOS-"), entry
            assert entry.signature, "an empty signature matches every fault"
            assert len(entry.note) > 80, "note too thin to act on"
