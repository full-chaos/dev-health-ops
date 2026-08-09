"""CHAOS-3619: the trial runner's own guards, run by the standard gate.

The measurement lives in ``trials/chaos_3619`` and is deliberately outside
``testpaths`` so an unrun trial reads as NOT MEASURED. The *logic* that
decides what a result means is not exempt from that -- a runner that
mislabels a timeout as a failure, or renders an unscored case as a column of
zeros, would produce a confident artifact from a broken sweep. Those rules
are asserted here, where the gate runs them.

Each guard below is paired with the defect it exists to catch, planted in
the test itself, so "this passes" means "the specific bad thing was
constructed and rejected" rather than "nothing objected".
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from trials.chaos_3619.budget import (
    DEFAULT_PER_CASE_TIMEOUT_SECONDS,
    BudgetOutcome,
    enforce,
)
from trials.chaos_3619.dispositions import (
    MEASURED_DISPOSITIONS,
    NOT_RUN_DISPOSITIONS,
    CaseDisposition,
    is_measured,
)
from trials.chaos_3619.records import (
    ArmResult,
    CaseRecord,
    DimensionOutcome,
    TrialRecordSet,
    load_records,
    write_records,
)

# ---------------------------------------------------------------------------
# The per-case budget (CHAOS-3625)
# ---------------------------------------------------------------------------


class _FakeClock:
    """A clock the test drives, so no guard here has to sleep.

    A timeout test that sleeps is slow and flaky in exactly the constrained
    environments where a timeout matters, and it tempts the next person to
    shorten the bound until the guard stops discriminating.
    """

    def __init__(self, *readings: float) -> None:
        self._readings = list(readings)

    def __call__(self) -> float:
        return self._readings.pop(0)


class TestThePerCaseBudget:
    def test_a_call_inside_its_budget_returns_its_value(self) -> None:
        """The control. Without it every assertion below is satisfiable by a
        budget that rejects everything."""

        outcome = enforce(
            lambda: "packet", limit_seconds=10.0, clock=_FakeClock(0.0, 1.5)
        )
        assert outcome.exceeded is False
        assert outcome.value == "packet"
        assert outcome.elapsed_seconds == pytest.approx(1.5)

    def test_a_call_past_its_budget_is_exceeded_and_yields_nothing(self) -> None:
        """The whole point: a late value must not be scorable.

        Asserting only ``exceeded`` would pass against an implementation
        that flagged the overrun and still handed the value back -- and the
        runner would score a case it had just recorded as NOT RUN.
        """

        outcome = enforce(
            lambda: "packet", limit_seconds=10.0, clock=_FakeClock(0.0, 10.5)
        )
        assert outcome.exceeded is True
        assert outcome.value is None, (
            "a value produced past the deadline was returned; the runner "
            "would score a case it recorded as NOT RUN"
        )
        assert "not retried" in outcome.detail

    def test_a_fault_is_returned_rather_than_raised(self) -> None:
        """One arm faulting on one case must not end the sweep.

        The predecessor trial lost every case after the first raise, and the
        loss was silent. This is that regression, pinned.
        """

        def boom() -> object:
            raise RuntimeError("arm exploded")

        outcome = enforce(boom, limit_seconds=10.0, clock=_FakeClock(0.0, 0.2))
        assert outcome.exceeded is False
        assert isinstance(outcome.fault, RuntimeError)
        assert "arm exploded" in outcome.detail

    def test_a_fault_past_the_deadline_is_a_timeout_not_a_fault(self) -> None:
        """Two abnormal outcomes at once must resolve to one disposition.

        If both survived, the runner would have to pick, and picking the
        fault would report a hung arm as a defective one.
        """

        def slow_boom() -> object:
            raise RuntimeError("arm exploded slowly")

        outcome = enforce(slow_boom, limit_seconds=1.0, clock=_FakeClock(0.0, 9.0))
        assert outcome.exceeded is True
        assert outcome.fault is None

    @pytest.mark.parametrize("limit", [0.0, -1.0])
    def test_a_non_positive_budget_is_refused(self, limit: float) -> None:
        """A zero budget would mark every case NOT RUN.

        The sweep would then complete, write an artifact, and report a clean
        absence of measurement -- the exact shape of a harness that reads as
        coverage while measuring nothing.
        """

        with pytest.raises(ValueError, match="positive"):
            enforce(lambda: None, limit_seconds=limit)

    def test_the_default_budget_is_a_hang_detector_not_a_latency_gate(self) -> None:
        """Pinned so nobody tightens it into a performance assertion.

        The trial REPORTS latency; it does not judge it. A bound tight
        enough to fail a slow case would silently convert one of the
        reported dimensions into a pass/fail gate.
        """

        assert DEFAULT_PER_CASE_TIMEOUT_SECONDS >= 60.0


# ---------------------------------------------------------------------------
# Dispositions
# ---------------------------------------------------------------------------


class TestDispositions:
    def test_only_scored_is_a_measured_disposition(self) -> None:
        assert MEASURED_DISPOSITIONS == {CaseDisposition.SCORED}
        assert is_measured(CaseDisposition.SCORED) is True
        for disposition in CaseDisposition:
            if disposition is not CaseDisposition.SCORED:
                assert is_measured(disposition) is False, disposition

    def test_the_four_required_distinctions_are_all_representable(self) -> None:
        """CHAOS-3619 names FAIL, NOT_RUN, NOT_COMPARABLE and expected
        limitation as things the records must tell apart.

        FAIL is not a disposition: a scored case that failed dimensions is
        ``SCORED`` with failing verdicts, which is a stronger distinction
        than a flat FAIL because it says *which* dimension failed.
        """

        assert CaseDisposition.NOT_COMPARABLE in NOT_RUN_DISPOSITIONS
        assert CaseDisposition.EXPECTED_LIMITATION in NOT_RUN_DISPOSITIONS
        assert CaseDisposition.NOT_RUN_TIMEOUT in NOT_RUN_DISPOSITIONS
        assert CaseDisposition.NOT_RUN_PRECONDITION in NOT_RUN_DISPOSITIONS
        # An arm that ran and declared a gap is NOT a non-run: it is a
        # result, and how often the baseline must declare one is one of the
        # numbers the comparison turns on.
        assert CaseDisposition.ARM_DECLARED_GAP not in NOT_RUN_DISPOSITIONS
        assert CaseDisposition.ARM_REFUSED not in NOT_RUN_DISPOSITIONS

    def test_an_arm_fault_is_not_a_capability_limit(self) -> None:
        """Distinct members, because conflating them would let a defect be
        published as an honest boundary of the technique."""

        assert CaseDisposition.ARM_FAULT is not CaseDisposition.ARM_REFUSED
        assert CaseDisposition.ARM_FAULT not in NOT_RUN_DISPOSITIONS


# ---------------------------------------------------------------------------
# The raw record's own invariants -- each with the defect planted
# ---------------------------------------------------------------------------


def _scored_arm(**overrides: object) -> ArmResult:
    defaults: dict[str, object] = {
        "arm_id": "native",
        "disposition": CaseDisposition.SCORED.value,
        "detail": "scored",
        "latency_ms": 12,
        "packet_emitted": True,
        "dimension_outcomes": (DimensionOutcome("subject_top_1", "pass", "matched"),),
    }
    defaults.update(overrides)
    return ArmResult(**defaults)  # type: ignore[arg-type]


class TestAnUnscoredRowMayNotCarryVerdicts:
    """The defect: a timed-out case rendered as a column of failures.

    This is the single most consequential mislabel the runner could make.
    A NOT RUN row carrying dimension verdicts would appear in the per-family
    table as measured results, and the report would state coverage the sweep
    never had.
    """

    @pytest.mark.parametrize(
        "disposition",
        [d for d in CaseDisposition if d is not CaseDisposition.SCORED],
    )
    def test_planting_verdicts_on_an_unscored_row_is_refused(
        self, disposition: CaseDisposition
    ) -> None:
        extra: dict[str, object] = {"disposition": disposition.value}
        if disposition in {
            CaseDisposition.EXPECTED_LIMITATION,
            CaseDisposition.NOT_COMPARABLE,
        }:
            extra["limitation_owner"] = "H3"
        with pytest.raises(ValueError, match="not a measured disposition"):
            _scored_arm(**extra)

    def test_the_same_row_scored_is_accepted(self) -> None:
        """The control the parametrised refusals need.

        Without it, every refusal above would also pass against a
        constructor that rejected all rows.
        """

        assert _scored_arm().dimension_outcomes


class TestALimitationMustNameItsOwner:
    """The defect: an unattributed expected-limitation.

    A limitation with no named debt is indistinguishable from an untested
    cell, and the issue requires expected limitations to be attributable.
    """

    @pytest.mark.parametrize(
        "disposition",
        [CaseDisposition.EXPECTED_LIMITATION, CaseDisposition.NOT_COMPARABLE],
    )
    def test_an_unowned_limitation_is_refused(
        self, disposition: CaseDisposition
    ) -> None:
        with pytest.raises(ValueError, match="no named owner"):
            ArmResult(
                arm_id="graph_assisted_shadow_arm",
                disposition=disposition.value,
                detail="cannot measure",
                latency_ms=0,
                packet_emitted=False,
            )

    @pytest.mark.parametrize(
        "disposition",
        [CaseDisposition.EXPECTED_LIMITATION, CaseDisposition.NOT_COMPARABLE],
    )
    def test_the_same_row_with_an_owner_is_accepted(
        self, disposition: CaseDisposition
    ) -> None:
        result = ArmResult(
            arm_id="graph_assisted_shadow_arm",
            disposition=disposition.value,
            detail="cannot measure",
            latency_ms=0,
            packet_emitted=False,
            limitation_owner="CHAOS-3569",
        )
        assert result.limitation_owner == "CHAOS-3569"

    def test_other_dispositions_do_not_require_an_owner(self) -> None:
        """Scoped deliberately: requiring an owner everywhere would push
        callers to write a placeholder, which is worse than no field."""

        assert (
            ArmResult(
                arm_id="native",
                disposition=CaseDisposition.ARM_DECLARED_GAP.value,
                detail="unprojectable",
                latency_ms=3,
                packet_emitted=False,
            ).limitation_owner
            == ""
        )


class TestTheRecordSetCarriesNoAggregate:
    """The plan's hardest rule, asserted structurally rather than by review.

    "No single aggregate score may hide that one arm improves ambiguity
    while harming driver precision." A field named like a total is the shape
    that rule forbids, and a reviewer scanning a large dataclass is exactly
    who misses one.
    """

    def test_no_field_on_any_record_type_looks_like_a_headline_number(self) -> None:
        import dataclasses

        banned = ("score", "total", "percent", "aggregate", "overall", "grade")
        offenders: list[str] = []
        for cls in (TrialRecordSet, CaseRecord, ArmResult, DimensionOutcome):
            for entry in dataclasses.fields(cls):
                lowered = entry.name.lower()
                if any(word in lowered for word in banned):
                    offenders.append(f"{cls.__name__}.{entry.name}")
        assert not offenders, offenders


# ---------------------------------------------------------------------------
# The artifact is the source of truth, and refuses to be read wrongly
# ---------------------------------------------------------------------------


def _record_set(binding: object) -> TrialRecordSet:
    return TrialRecordSet(
        schema_version="chaos_3619_trial_results.v1",
        binding=binding,  # type: ignore[arg-type]
        cases=(
            CaseRecord(
                case_id="T01",
                question="Which teams are struggling?",
                question_family="struggling_teams",
                corpus_family="team_intelligence",
                comparison_shape="discovered_cohort",
                variant_kind="exact",
                expected_answer="direct",
                principal_id="principal_helio_analyst",
                organization_id="org_helio",
                declared_dimension_ids=("subject_top_1",),
                arms=(_scored_arm(),),
            ),
        ),
    )


class TestTheArtifactRefusesToBeMisread:
    def test_a_missing_artifact_is_an_error_not_an_empty_report(
        self, tmp_path: Path
    ) -> None:
        """An empty report reads as a trial that measured nothing and passed.

        This is the "a measurement that did not happen must FAIL, loudly"
        rule at the file boundary.
        """

        with pytest.raises(FileNotFoundError, match="run the sweep first"):
            load_records(tmp_path / "absent.records.json")

    def test_a_foreign_schema_version_is_refused_rather_than_parsed(
        self, tmp_path: Path
    ) -> None:
        """The defect: best-effort parsing of a shape we do not understand.

        A report rendered from an unrecognised layout quietly omits whatever
        moved, and looks complete.
        """

        path = tmp_path / "old.records.json"
        path.write_text(json.dumps({"schema_version": "chaos_3619_trial.v0"}))
        with pytest.raises(ValueError, match="Refused rather"):
            load_records(path)

    def test_a_written_artifact_round_trips(self, tmp_path: Path) -> None:
        """The control for both refusals above."""

        from trials.chaos_3619.binding import collect_binding

        binding = collect_binding(
            per_case_timeout_seconds=DEFAULT_PER_CASE_TIMEOUT_SECONDS,
            trial_store_backend="falkordb (test)",
            graph_embedder_model_id="deterministic_blake2b.v1.d1024",
        )
        path = tmp_path / "trial.records.json"
        write_records(_record_set(binding), path)
        loaded = load_records(path)
        assert loaded["cases"][0]["case_id"] == "T01"
        assert loaded["binding"]["corpus_version"] == "ask_dev_investigation_corpus.v1"

    def test_the_artifact_is_byte_reproducible_from_the_same_record_set(
        self, tmp_path: Path
    ) -> None:
        """Two writes of one sweep must not differ.

        Otherwise a diff between two runs shows dict iteration order rather
        than what the runs did, and nobody can tell drift from noise.
        """

        from trials.chaos_3619.binding import collect_binding

        binding = collect_binding(
            per_case_timeout_seconds=DEFAULT_PER_CASE_TIMEOUT_SECONDS,
            trial_store_backend="falkordb (test)",
            graph_embedder_model_id="deterministic_blake2b.v1.d1024",
        )
        records = _record_set(binding)
        first, second = tmp_path / "a.json", tmp_path / "b.json"
        write_records(records, first)
        write_records(records, second)
        assert first.read_bytes() == second.read_bytes()


class TestTheBindingIsReadFromTheSystem:
    """A hand-maintained version block drifts silently. These assert the
    values came from the thing itself."""

    def test_the_binding_names_the_real_arm_versions(self) -> None:
        from dev_health_ops.context_fabric.graph_arm.packet_builder import ARM_ID
        from dev_health_ops.context_fabric.native_arm.projection import NATIVE_ARM_ID
        from trials.chaos_3619.binding import collect_binding

        binding = collect_binding(
            per_case_timeout_seconds=1.0,
            trial_store_backend="falkordb",
            graph_embedder_model_id="deterministic_blake2b.v1.d1024",
        )
        assert binding.native_arm_id == NATIVE_ARM_ID
        assert binding.graph_arm_id == ARM_ID

    def test_the_corpus_hash_is_a_real_digest_of_the_committed_manifest(
        self,
    ) -> None:
        """Anti-vacuity: a missing manifest yields a marker, not a digest,
        and a marker must never be mistaken for a binding."""

        from trials.chaos_3619.binding import collect_binding

        binding = collect_binding(
            per_case_timeout_seconds=1.0,
            trial_store_backend="falkordb",
            graph_embedder_model_id="deterministic_blake2b.v1.d1024",
        )
        assert len(binding.corpus_manifest_sha256) == 64, binding.corpus_manifest_sha256
        assert not binding.corpus_manifest_sha256.startswith("<")

    def test_a_git_failure_marker_is_never_read_as_a_clean_tree(self) -> None:
        """The defect: ``tree_clean = not status`` with status an error string.

        A git failure returns a non-empty marker, which is falsy under
        ``not`` only if it is empty -- so the naive spelling would report a
        FAILED git query as a dirty tree, and the inverted spelling would
        report it as clean. Pinned to equality against the empty string.
        """

        import trials.chaos_3619.binding as binding_module

        source = Path(binding_module.__file__).read_text()
        assert 'tree_clean=status == ""' in source, (
            "tree_clean is no longer an equality against the empty string; a "
            "truthiness test would misreport a git failure as a tree state"
        )


class TestBudgetOutcomeDetailIsDiscriminating:
    """Detail strings reach the artifact, so they must say different things
    for different outcomes -- a shared message would make the raw record
    unable to distinguish what the enum already distinguishes."""

    def test_the_three_outcomes_produce_three_different_details(self) -> None:
        timed_out = BudgetOutcome(elapsed_seconds=9.0, limit_seconds=1.0, exceeded=True)
        faulted = BudgetOutcome(
            elapsed_seconds=0.2,
            limit_seconds=1.0,
            exceeded=False,
            fault=RuntimeError("x"),
        )
        fine = BudgetOutcome(elapsed_seconds=0.2, limit_seconds=1.0, exceeded=False)
        details = {timed_out.detail, faulted.detail, fine.detail}
        assert len(details) == 3, details
