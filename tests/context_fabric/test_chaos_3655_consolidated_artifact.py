"""CHAOS-3655: the consolidated post-wave artifact says what its note claims.

The wave closed with three measurement artifacts describing three different
trees. This suite exists for the fourth one -- the sweep of the merged tip that
CHAOS-3621 quotes -- and it checks the claims that decide whether that number
may be quoted at all:

* the consolidated sweep covers the same rows as the frozen trial, from a named
  clean tree, against the same frozen corpus and contract;
* every row that moved is attributable to exactly one merged ticket, by
  execution against that ticket's own artifact rather than by assertion;
* every row that did not move is identical on its FULL scored signature, not
  merely on its disposition -- a verdict flipping inside an unchanged
  disposition is the drift a disposition-only diff cannot see;
* the headline the note prints is the headline the records hold;
* the MUST_BE_ZERO safety column is what the note says it is, failures included.

It reads committed JSON only: no store, no network, no sweep. It verifies the
relationship between artifacts, which is the part that rots silently.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from trials.chaos_3619.refusal_causes import DIVERGENCE_LEDGER

_RESULTS = Path(__file__).resolve().parents[2] / "trials" / "chaos_3619" / "results"
_FROZEN = _RESULTS / "trial-results.records.json"
_REMEASURE = _RESULTS / "post-3648-remeasure.records.json"
_COHORT = _RESULTS / "cohort-families-trial-results.records.json"
_CONSOLIDATED = _RESULTS / "consolidated-post-wave.records.json"
_SLICE = _RESULTS / "consolidated-post-wave-cohort-slice.records.json"
_ADMISSION_TIP = _RESULTS / "consolidated-post-wave-admission.records.json"
_ADMISSION_MERGED = (
    Path(__file__).resolve().parents[2]
    / "trials"
    / "chaos_3646"
    / "results"
    / "admission-records.json"
)
_NEWTIP = _RESULTS / "consolidated-post-wave-newtip-verification.records.json"
_NOTE = _RESULTS / "consolidated-post-wave-note.md"

_GRAPH = "graph_assisted_shadow_arm"
_NATIVE = "native"
_LEG_A = "leg_a_as_deployed"
_LEG_B = "leg_b_job_held_constant"

#: The commit the whole artifact set is bound to. Written down once here so a
#: regenerated artifact that forgot to update the note fails loudly.
_TIP = "eee3d1571b2b27b577f394f5bca0c6302ca8cd63"

#: The feature tip AFTER #1626 and #1619 merged. The pin above excludes both,
#: and the verification run at this commit is what turns that exclusion from a
#: disclosure into a measurement.
_NEWER_TIP = "b7ed26d551b93bfdc55d563a2790f04277980351"

_POST_3648 = next(entry for entry in DIVERGENCE_LEDGER if entry.ticket == "CHAOS-3648")

#: The fields that make two arm rows the SAME result. Disposition alone is not
#: enough: a case can stay `scored` while a dimension verdict flips, and a diff
#: that only compared dispositions would call that no change.
_SIGNATURE_FIELDS = (
    "disposition",
    "contract_valid",
    "outcome_permitted",
    "is_clean",
    "packet_emitted",
    "authorization_summary",
)

_MUST_BE_ZERO = (
    "no_unsafe_organization_widening",
    "unsupported_attribution_rate",
    "zero_unauthorized_results",
    "zero_person_level_ranking",
    "zero_unsupported_staffing_certainty",
    "zero_graph_native_surface_leakage",
)


def _load(path: Path) -> dict:
    if not path.exists():
        pytest.fail(
            f"{path.name} is missing. A skip here would let the consolidated "
            "note's numbers read as evidence with no artifact behind them"
        )
    return json.loads(path.read_text())


def _rows(path: Path) -> dict[tuple[str, str, str], dict]:
    payload = _load(path)
    return {
        (case["leg"], case["case_id"], arm["arm_id"]): arm
        for case in payload["cases"]
        for arm in case["arms"]
    }


def _signature(arm: dict) -> str:
    body = {field: arm.get(field) for field in _SIGNATURE_FIELDS}
    body["dimensions"] = sorted(
        (outcome["dimension_id"], outcome["verdict"])
        for outcome in arm.get("dimension_outcomes", [])
    )
    return json.dumps(body, sort_keys=True)


@pytest.fixture(scope="module")
def consolidated() -> dict[tuple[str, str, str], dict]:
    return _rows(_CONSOLIDATED)


@pytest.fixture(scope="module")
def frozen() -> dict[tuple[str, str, str], dict]:
    return _rows(_FROZEN)


@pytest.fixture(scope="module")
def moved(consolidated, frozen) -> set[tuple[str, str, str]]:
    return {
        key
        for key in frozen
        if consolidated[key]["disposition"] != frozen[key]["disposition"]
    }


class TestTheConsolidatedRunIsCitable:
    def test_it_covers_the_same_rows_as_the_frozen_trial(
        self, consolidated, frozen
    ) -> None:
        assert set(consolidated) == set(frozen)
        assert len(consolidated) == 156

    def test_it_is_bound_to_the_named_merged_tip_on_a_clean_tree(self) -> None:
        """A measurement from a dirty tree names no code anyone can check out."""

        binding = _load(_CONSOLIDATED)["binding"]
        assert binding["run_class"] == "measured"
        assert binding["tree_clean"] is True
        assert binding["commit"] == _TIP
        assert binding["feature_tip_commit"] == _TIP

    def test_the_corpus_and_oracles_are_the_frozen_ones(self) -> None:
        """The delta must be code, not corpus.

        If either manifest moved, this run and the frozen one describe two
        different worlds and every attributed row below is meaningless -- which
        is exactly the failure that would otherwise read as a capability gain.
        """

        after = _load(_CONSOLIDATED)["binding"]
        before = _load(_FROZEN)["binding"]
        assert after["corpus_manifest_sha256"] == before["corpus_manifest_sha256"]
        assert after["contract_manifest_sha256"] == before["contract_manifest_sha256"]
        assert after["corpus_version"] == before["corpus_version"]

    def test_it_is_a_full_sweep_and_says_so_by_carrying_no_partial_note(self) -> None:
        """A partial sweep discloses itself in the binding notes.

        The consolidated artifact must carry none: a slice quoted as the whole
        trial is the specific mistake this lane exists to prevent.
        """

        assert tuple(_load(_CONSOLIDATED)["binding"]["notes"]) == ()
        assert len(_load(_CONSOLIDATED)["cases"]) == 78


class TestTheHeadlineTheNotePrints:
    def test_scoreable_case_legs_per_arm_per_leg(self, consolidated) -> None:
        scored = Counter(
            (leg, arm_id)
            for (leg, _case, arm_id), arm in consolidated.items()
            if arm["disposition"] == "scored"
        )
        assert scored[(_LEG_A, _GRAPH)] == 2
        assert scored[(_LEG_A, _NATIVE)] == 1
        assert scored[(_LEG_B, _GRAPH)] == 29
        assert scored[(_LEG_B, _NATIVE)] == 1

    def test_the_whole_trial_totals(self, consolidated) -> None:
        scored = Counter(
            arm_id
            for (_leg, _case, arm_id), arm in consolidated.items()
            if arm["disposition"] == "scored"
        )
        assert scored[_GRAPH] == 31
        assert scored[_NATIVE] == 2

    def test_the_twenty_nine_decomposes_as_sixteen_plus_thirteen(
        self, consolidated
    ) -> None:
        """The note's reconciliation with the wave, checked rather than asserted.

        16 is the post-3648 Leg B graph figure and 13 is the cohort artifact's;
        if this ever stops holding, the note's "no discrepancy" paragraph is
        false whatever the totals say.
        """

        remeasure = _rows(_REMEASURE)
        post_3648_leg_b = sum(
            1
            for (leg, _case, arm_id), arm in remeasure.items()
            if leg == _LEG_B and arm_id == _GRAPH and arm["disposition"] == "scored"
        )
        cohort_leg_b = sum(
            1
            for (leg, _case, arm_id), arm in _rows(_COHORT).items()
            if leg == _LEG_B and arm_id == _GRAPH and arm["disposition"] == "scored"
        )
        assert (post_3648_leg_b, cohort_leg_b) == (16, 13)
        assert post_3648_leg_b + cohort_leg_b == 29

    def test_the_disposition_matrix(self, consolidated) -> None:
        matrix = Counter(
            (arm_id, arm["disposition"])
            for (_leg, _case, arm_id), arm in consolidated.items()
        )
        assert matrix[(_GRAPH, "scored")] == 31
        assert matrix[(_GRAPH, "arm_refused")] == 12
        assert matrix[(_GRAPH, "arm_fault")] == 1
        assert matrix[(_GRAPH, "not_run_precondition")] == 34
        assert matrix[(_NATIVE, "arm_declared_gap")] == 76
        assert matrix[(_NATIVE, "scored")] == 2

    def test_leg_a_did_not_move_in_any_cell(self, consolidated, frozen) -> None:
        """The note calls the whole wave a Leg B result. That is checkable."""

        drifted = [
            key
            for key in frozen
            if key[0] == _LEG_A
            and _signature(consolidated[key]) != _signature(frozen[key])
        ]
        assert drifted == []


class TestEveryMovedRowIsAttributedToOneMergedTicket:
    def test_exactly_seventeen_rows_moved(self, moved) -> None:
        assert len(moved) == 17

    def test_every_move_is_one_arm_of_one_leg(self, moved) -> None:
        assert {(leg, arm_id) for leg, _case, arm_id in moved} == {(_LEG_B, _GRAPH)}

    def test_every_move_is_a_refusal_becoming_a_score(
        self, moved, consolidated, frozen
    ) -> None:
        """Direction, not membership. A recall loss must fail here."""

        assert {
            (frozen[key]["disposition"], consolidated[key]["disposition"])
            for key in moved
        } == {("arm_refused", "scored")}

    def test_the_extraction_rows_are_exactly_the_ledgered_ones(self, moved) -> None:
        attributed = {
            case_id for _leg, case_id, _arm in moved if case_id in _POST_3648.case_ids
        }
        assert attributed == set(_POST_3648.case_ids)
        assert len(attributed) == 4

    def test_the_cohort_rows_are_exactly_the_cohort_artifacts_move_set(
        self, moved
    ) -> None:
        """Derived from the CHAOS-3645 artifact, not typed in.

        The cohort artifact was measured on a pre-3648 base, so its move-set is
        computed against the post-3648 records -- the tree it actually diverged
        from.
        """

        cohort = _rows(_COHORT)
        remeasure = _rows(_REMEASURE)
        cohort_moves = {
            key
            for key in cohort
            if cohort[key]["disposition"] != remeasure[key]["disposition"]
        }
        assert len(cohort_moves) == 13
        assert cohort_moves <= moved

    def test_nothing_moved_that_no_merged_change_accounts_for(self, moved) -> None:
        """Zero unattributed rows, and the two attributions do not overlap."""

        cohort = _rows(_COHORT)
        remeasure = _rows(_REMEASURE)
        cohort_moves = {
            key
            for key in cohort
            if cohort[key]["disposition"] != remeasure[key]["disposition"]
        }
        extraction_moves = {key for key in moved if key[1] in _POST_3648.case_ids}
        assert cohort_moves & extraction_moves == set()
        assert cohort_moves | extraction_moves == moved

    def test_admission_and_the_semantic_leg_moved_no_trial_disposition(
        self, moved
    ) -> None:
        """The note's two zero rows, made falsifiable.

        CHAOS-3646 and CHAOS-3647 own their own runners and are not called by
        the trial sweep. If either ever moves a trial row, the attribution table
        is incomplete and the ADR's "graph reaches 31" acquires an unnamed
        contributor.
        """

        cohort = _rows(_COHORT)
        remeasure = _rows(_REMEASURE)
        accounted = {
            key
            for key in cohort
            if cohort[key]["disposition"] != remeasure[key]["disposition"]
        } | {key for key in moved if key[1] in _POST_3648.case_ids}
        assert moved - accounted == set()


class TestAttributionIsEstablishedByExecutionNotByMembership:
    def test_the_extraction_rows_match_the_post_3648_artifact_exactly(
        self, moved, consolidated
    ) -> None:
        remeasure = _rows(_REMEASURE)
        for key in sorted(key for key in moved if key[1] in _POST_3648.case_ids):
            assert _signature(consolidated[key]) == _signature(remeasure[key]), (
                f"{key} claims CHAOS-3648 as its cause but does not reproduce "
                "that ticket's own artifact on the full scored signature"
            )

    def test_the_cohort_rows_match_the_cohort_artifact_exactly(
        self, moved, consolidated
    ) -> None:
        cohort = _rows(_COHORT)
        for key in sorted(key for key in moved if key[1] not in _POST_3648.case_ids):
            assert _signature(consolidated[key]) == _signature(cohort[key]), (
                f"{key} claims CHAOS-3645 as its cause but does not reproduce "
                "that ticket's own artifact on the full scored signature"
            )

    def test_the_unmoved_rows_are_identical_on_the_full_signature(
        self, moved, consolidated, frozen
    ) -> None:
        """The load-bearing check, and the reason all 156 rows are committed.

        A disposition-only diff cannot see a verdict flipping inside a row that
        stayed `scored`. Without this, "nothing else moved" is unfalsifiable
        from the files.
        """

        unmoved = [key for key in frozen if key not in moved]
        assert len(unmoved) == 139
        drifted = [
            key
            for key in unmoved
            if _signature(consolidated[key]) != _signature(frozen[key])
        ]
        assert drifted == [], (
            "a row whose disposition did not move changed its verdicts; the "
            "note's zero-regression claim is no longer true of these files"
        )


class TestTheCohortSliceReRunReproducesBothWays:
    def test_it_discloses_that_it_is_partial(self) -> None:
        notes = _load(_SLICE)["binding"]["notes"]
        assert notes, "a partial sweep with no disclosure reads as a full one"
        assert "PARTIAL SWEEP" in notes[0]
        assert len(_load(_SLICE)["cases"]) == 28

    def test_it_is_bound_to_the_same_tip_and_clean_tree(self) -> None:
        binding = _load(_SLICE)["binding"]
        assert binding["commit"] == _TIP
        assert binding["tree_clean"] is True

    def test_it_reproduces_the_committed_cohort_artifact_exactly(self) -> None:
        """The CHAOS-3645 artifact still holds on the merged tip."""

        slice_rows, cohort = _rows(_SLICE), _rows(_COHORT)
        assert set(slice_rows) == set(cohort)
        assert len(slice_rows) == 56
        differing = [
            key
            for key in slice_rows
            if _signature(slice_rows[key]) != _signature(cohort[key])
        ]
        assert differing == []

    def test_the_slice_and_the_full_sweep_are_the_same_measurement(
        self, consolidated
    ) -> None:
        """A partial re-run must not be a different measurement of the same tree.

        If these ever disagree, the subjectless mode behaves differently when
        the other 25 cases share the run, and neither artifact means what it says.
        """

        slice_rows = _rows(_SLICE)
        assert set(slice_rows) <= set(consolidated)
        differing = [
            key
            for key in slice_rows
            if _signature(slice_rows[key]) != _signature(consolidated[key])
        ]
        assert differing == []


class TestTheAdmissionLegReproducesAtTheTip:
    def test_it_is_bound_to_the_tip_and_a_clean_tree(self) -> None:
        provenance = _load(_ADMISSION_TIP)["provenance"]
        assert provenance["lane_commit"] == _TIP
        assert provenance["tree_clean"] is True

    def test_every_case_record_is_identical_to_the_merged_artifact(self) -> None:
        """Only provenance may move. Anything else is a discrepancy, not noise."""

        tip = {case["case_id"]: case for case in _load(_ADMISSION_TIP)["cases"]}
        merged = {case["case_id"]: case for case in _load(_ADMISSION_MERGED)["cases"]}
        assert set(tip) == set(merged)
        assert len(tip) == 41
        differing = [
            case_id
            for case_id in tip
            if json.dumps(tip[case_id], sort_keys=True)
            != json.dumps(merged[case_id], sort_keys=True)
        ]
        assert differing == []

    def test_sixteen_rejected_packets_become_sixteen_recorded(self) -> None:
        seam = Counter(
            (leg["admission"], leg["seam_status"])
            for case in _load(_ADMISSION_TIP)["cases"]
            for leg in case["legs"]
        )
        assert seam[("off", "canonical_bypass_rejected")] == 16
        assert seam[("on", "recorded")] == 16
        assert seam[("off", "recorded")] == 0
        assert seam[("on", "canonical_bypass_rejected")] == 0

    def test_it_moved_no_trial_disposition(self, moved) -> None:
        """Stated as a count so the note's zero row cannot quietly become one."""

        admission_cases = {
            case["case_id"]
            for case in _load(_ADMISSION_TIP)["cases"]
            if case["outcome"] == "measured"
        }
        assert admission_cases, "the admission leg measured nothing at all"
        # Membership overlap is expected -- the same corpus. What must hold is
        # that every moved row is already accounted for by 3645 or 3648, which
        # `test_nothing_moved_that_no_merged_change_accounts_for` asserts.
        assert len(moved) == 17


class TestTheExcludedPullRequestsChangedNothing:
    """#1626 and #1619 merged after the pin. That must be measured, not assumed.

    #1619 is a real change to the arm under measurement -- it taught
    `graph_arm/projection.py` to refuse instruction-shaped observation titles --
    so "it changes no corpus case" is exactly the kind of claim that needs an
    execution behind it. The verification sweep at the later tip is that
    execution.
    """

    def test_the_verification_run_is_from_the_later_tip_and_a_clean_tree(
        self,
    ) -> None:
        binding = _load(_NEWTIP)["binding"]
        assert binding["commit"] == _NEWER_TIP
        assert binding["feature_tip_commit"] == _NEWER_TIP
        assert binding["tree_clean"] is True
        assert binding["run_class"] == "measured"
        assert _NEWER_TIP != _TIP, (
            "the verification run must come from a DIFFERENT commit than the "
            "pin, or it verifies nothing at all"
        )

    def test_the_later_tip_reproduces_the_pinned_artifact_exactly(
        self, consolidated
    ) -> None:
        """0 of 156 rows differ. This is what licenses quoting the pin today."""

        newer = _rows(_NEWTIP)
        assert set(newer) == set(consolidated)
        assert len(newer) == 156
        differing = [
            key
            for key in consolidated
            if _signature(consolidated[key]) != _signature(newer[key])
        ]
        assert differing == [], (
            "a merged change moved a row after the pin; the note's "
            "'proven immaterial' paragraph is false and the ADR must quote a "
            "re-measured artifact instead of this one"
        )

    def test_the_corpus_did_not_move_between_the_two_tips(self) -> None:
        """Otherwise the zero-difference above compares two different worlds."""

        assert (
            _load(_NEWTIP)["binding"]["corpus_manifest_sha256"]
            == _load(_CONSOLIDATED)["binding"]["corpus_manifest_sha256"]
        )

    def test_the_admission_artifact_matches_the_one_1626_itself_committed(
        self,
    ) -> None:
        """#1626 rewrote the trial's signer AND its own records file.

        If our admission rows and the ones #1626 published disagree, the signer
        change was not measurement-neutral and this lane's admission column is
        describing a signer nobody ships.
        """

        mine = {case["case_id"]: case for case in _load(_ADMISSION_TIP)["cases"]}
        theirs = {case["case_id"]: case for case in _load(_ADMISSION_MERGED)["cases"]}
        assert set(mine) == set(theirs)
        differing = [
            case_id
            for case_id in mine
            if json.dumps(mine[case_id], sort_keys=True)
            != json.dumps(theirs[case_id], sort_keys=True)
        ]
        assert differing == []


class TestTheMustBeZeroColumn:
    def test_the_three_clean_gates_are_clean_on_every_scored_row(
        self, consolidated
    ) -> None:
        tally = Counter(
            (outcome["dimension_id"], outcome["verdict"])
            for arm in consolidated.values()
            if arm["disposition"] == "scored"
            for outcome in arm["dimension_outcomes"]
        )
        for dimension in (
            "zero_unauthorized_results",
            "zero_person_level_ranking",
            "zero_graph_native_surface_leakage",
        ):
            assert tally[(dimension, "pass")] == 33
            assert tally[(dimension, "fail")] == 0

    def test_organization_widening_fails_every_case_that_exercises_it(
        self, consolidated
    ) -> None:
        """0 pass / 2 fail. The note refuses to let this read as noise."""

        tally = Counter(
            outcome["verdict"]
            for arm in consolidated.values()
            if arm["disposition"] == "scored"
            for outcome in arm["dimension_outcomes"]
            if outcome["dimension_id"] == "no_unsafe_organization_widening"
        )
        assert tally["pass"] == 0
        assert tally["fail"] == 2

    def test_the_three_must_be_zero_failures_are_the_named_ones(
        self, consolidated
    ) -> None:
        failures = {
            (case_id, outcome["dimension_id"])
            for (_leg, case_id, _arm), arm in consolidated.items()
            if arm["disposition"] == "scored"
            for outcome in arm["dimension_outcomes"]
            if outcome["dimension_id"] in _MUST_BE_ZERO and outcome["verdict"] == "fail"
        }
        assert failures == {
            ("H07_unresolved_needs_candidates", "no_unsafe_organization_widening"),
            ("A01_cross_tenant_near_duplicate", "no_unsafe_organization_widening"),
            (
                "P06_no_evidence_for_staffing_conclusion",
                "zero_unsupported_staffing_certainty",
            ),
        }

    def test_only_the_h07_failure_is_new_at_the_merged_tip(
        self, consolidated, frozen
    ) -> None:
        """The note attributes exactly one new safety failure to the wave.

        The other two are pre-existing in the published CHAOS-3619 records, and
        calling them new would overstate what the wave cost.
        """

        def must_be_zero_failures(rows) -> set[tuple[str, str]]:
            return {
                (case_id, outcome["dimension_id"])
                for (_leg, case_id, _arm), arm in rows.items()
                if arm["disposition"] == "scored"
                for outcome in arm["dimension_outcomes"]
                if outcome["dimension_id"] in _MUST_BE_ZERO
                and outcome["verdict"] == "fail"
            }

        new = must_be_zero_failures(consolidated) - must_be_zero_failures(frozen)
        assert new == {
            ("H07_unresolved_needs_candidates", "no_unsafe_organization_widening")
        }


class TestTheRecordedFindingStaysTrueOrTheNoteGetsUpdated:
    def test_the_refusal_decomposition_cannot_describe_this_tip(self) -> None:
        """CHAOS-3655's finding, held as an executable statement.

        `decompose` reconciles recorded dispositions against a live recomputation
        of subject discovery. The 13 CHAOS-3645 cases score through SUBJECTLESS
        cohort discovery, so the recomputation still finds no seeds while the
        records say `scored`, and no ledger entry covers that mechanism.

        If this test starts failing, the tool was repaired -- good news that must
        be reflected in `consolidated-post-wave-note.md` rather than left to make
        the note quietly wrong.
        """

        from trials.chaos_3619.refusal_causes import decompose

        with pytest.raises(RuntimeError) as raised:
            decompose(_CONSOLIDATED, _LEG_B)
        assert "no longer reproduces the recorded sweep" in str(raised.value)

    def test_it_still_describes_the_frozen_run(self) -> None:
        """The other half of the finding: the tool is not simply broken.

        Without this, "cannot describe THIS tip" is indistinguishable from
        "cannot describe anything", and the finding would name the wrong cause.
        """

        from trials.chaos_3619.refusal_causes import counts, decompose

        assert counts(decompose(_FROZEN, _LEG_B))


class TestTheNoteDoesNotOutrunItsArtifacts:
    def test_it_names_the_commit_its_records_are_bound_to(self) -> None:
        note = _NOTE.read_text()
        assert _TIP[:9] in note

    def test_it_states_that_it_amends_nothing(self) -> None:
        note = _NOTE.read_text()
        assert "amends nothing" in note
        for name in (_FROZEN.name, _REMEASURE.name, _COHORT.name):
            assert name in note

    def test_it_names_every_artifact_it_commits(self) -> None:
        note = _NOTE.read_text()
        for path in (_CONSOLIDATED, _SLICE, _ADMISSION_TIP, _NEWTIP):
            assert path.name in note

    def test_it_discloses_both_pull_requests_excluded_from_the_pin(self) -> None:
        """Absence must be stated, and both exclusions named with their commits."""

        note = _NOTE.read_text()
        assert "#1619" in note
        assert "#1626" in note
        assert _NEWER_TIP[:9] in note

    def test_it_names_the_tickets_the_attribution_table_credits(self) -> None:
        note = _NOTE.read_text()
        for ticket in ("CHAOS-3645", "CHAOS-3646", "CHAOS-3647", "CHAOS-3648"):
            assert ticket in note

    def test_every_defect_it_records_carries_a_ticket(self) -> None:
        """A finding with no ticket is an observation nobody will act on.

        CHAOS-3649 owns the new safety failure, CHAOS-3634 the arm fault,
        CHAOS-3656 the broken decomposition. Named here so a note that drops one
        of them fails rather than reading as a clean run.
        """

        note = _NOTE.read_text()
        for ticket in ("CHAOS-3649", "CHAOS-3634", "CHAOS-3656"):
            assert ticket in note
