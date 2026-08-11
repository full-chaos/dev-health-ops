"""CHAOS-3648: the post-3648 re-measurement says what its note claims it says.

A committed measurement artifact with nothing checking it is the worst kind of
evidence: it reads as covered while nobody would notice it going stale. The
artifact's load-bearing claim is narrow and checkable -- *the frozen trial and
the post-3648 re-measurement differ in exactly the cases the divergence ledger
names, in exactly one direction, in one arm of one leg* -- so it is checked
here rather than asserted in prose.

This suite reads two committed JSON files and the ledger. It needs no store, no
network and no sweep: it verifies the relationship between artifacts, not the
measurement itself, which is the part that can rot silently.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from trials.chaos_3619.refusal_causes import DIVERGENCE_LEDGER

_RESULTS = Path(__file__).resolve().parents[2] / "trials" / "chaos_3619" / "results"
_FROZEN = _RESULTS / "trial-results.records.json"
_REMEASURE = _RESULTS / "post-3648-remeasure.records.json"
_NOTE = _RESULTS / "post-3648-remeasure-note.md"

_GRAPH_ARM = "graph_assisted_shadow_arm"
_LEG_B = "leg_b_job_held_constant"

_POST_3648 = next(entry for entry in DIVERGENCE_LEDGER if entry.ticket == "CHAOS-3648")


def _dispositions(path: Path) -> dict[tuple[str, str, str], str]:
    payload = json.loads(path.read_text())
    return {
        (case["leg"], case["case_id"], arm["arm_id"]): arm["disposition"]
        for case in payload["cases"]
        for arm in case["arms"]
    }


@pytest.fixture(scope="module")
def frozen() -> dict[tuple[str, str, str], str]:
    if not _FROZEN.exists():
        pytest.fail(f"the frozen trial records are missing at {_FROZEN}")
    return _dispositions(_FROZEN)


@pytest.fixture(scope="module")
def remeasure() -> dict[tuple[str, str, str], str]:
    if not _REMEASURE.exists():
        pytest.fail(
            f"the post-3648 re-measurement is missing at {_REMEASURE}. A skip "
            "here would let the note's numbers read as evidence with no "
            "artifact behind them"
        )
    return _dispositions(_REMEASURE)


class TestTheRemeasurementIsComparableToTheFrozenRun:
    def test_it_covers_the_same_rows(self, frozen, remeasure) -> None:
        """Same shape, or the two are not comparable and no delta is real."""

        assert set(remeasure) == set(frozen)
        assert len(remeasure) == 156

    def test_it_was_produced_from_a_named_clean_tree(self) -> None:
        """A measurement from a dirty tree names no code anyone can check out.

        The sweep records this itself; asserting it is what stops a rerun from
        a work-in-progress tree being committed as evidence.
        """

        binding = json.loads(_REMEASURE.read_text())["binding"]
        assert binding["tree_clean"] is True
        assert binding["commit"]
        assert binding["feature_tip_commit"]

    def test_the_corpus_and_contract_are_the_frozen_ones(self) -> None:
        """The delta must be code, not corpus.

        If either manifest hash moved, the comparison is measuring two
        different worlds and every number in the note is meaningless -- which
        is precisely the failure that would otherwise read as a capability
        gain.
        """

        after = json.loads(_REMEASURE.read_text())["binding"]
        before = json.loads(_FROZEN.read_text())["binding"]
        assert after["corpus_manifest_sha256"] == before["corpus_manifest_sha256"]
        assert after["contract_manifest_sha256"] == before["contract_manifest_sha256"]
        assert after["corpus_version"] == before["corpus_version"]


class TestTheDeltaIsExactlyWhatTheLedgerNames:
    def test_only_the_ledgered_cases_moved(self, frozen, remeasure) -> None:
        moved = {key for key, value in remeasure.items() if value != frozen[key]}
        assert {case_id for _leg, case_id, _arm in moved} == set(_POST_3648.case_ids), (
            "the re-measurement moved a case the ledger does not name, or "
            "failed to move one it does; either way the note's attribution to "
            "CHAOS-3648 is no longer true of these files"
        )

    def test_every_move_is_one_arm_of_one_leg(self, frozen, remeasure) -> None:
        moved = {key for key, value in remeasure.items() if value != frozen[key]}
        assert {(leg, arm) for leg, _case_id, arm in moved} == {(_LEG_B, _GRAPH_ARM)}, (
            "a native-arm or Leg A row moved. The note states plainly that "
            "neither did, and that claim is the reason the +4 is not read as "
            "a product-level gain"
        )

    def test_every_move_is_a_refusal_becoming_a_score(self, frozen, remeasure) -> None:
        """Direction, not just membership -- a regression must fail here.

        The ledger licenses `arm_refused` -> `scored`. The mirror image in the
        same cases is a recall loss, and a test that only counted moved rows
        would call it success.
        """

        moved = {key for key, value in remeasure.items() if value != frozen[key]}
        assert {(frozen[key], remeasure[key]) for key in moved} == {
            ("arm_refused", "scored")
        }

    def test_the_untouched_rows_are_why_the_full_set_is_committed(
        self, frozen, remeasure
    ) -> None:
        """The zero-regression claim, made checkable rather than asserted.

        Committing only the singular-subject subset would have left this
        unverifiable from the artifact -- which is the reason the whole
        156-row set is in the file.
        """

        unchanged = [key for key in frozen if remeasure[key] == frozen[key]]
        assert len(unchanged) == 152

    def test_a_ledgered_case_moved_in_one_row_and_no_other(
        self, frozen, remeasure
    ) -> None:
        """The four cases have four rows each; only one of each may move.

        A ledgered case whose Leg A row or native row also moved would mean
        the extractor reached somewhere the note says it did not, and the
        "Leg A gains nothing" and "the native arm gains nothing" statements
        would be false while every count above still added up.
        """

        for case_id in sorted(_POST_3648.case_ids):
            rows = {key: value for key, value in frozen.items() if key[1] == case_id}
            assert len(rows) == 4, f"{case_id} does not have both legs x both arms"
            moved = {key for key in rows if remeasure[key] != frozen[key]}
            assert moved == {(_LEG_B, case_id, _GRAPH_ARM)}, (
                f"{case_id} moved in {sorted(moved)}; only its Leg B graph row "
                "may move, and the note's Leg A / native claims depend on it"
            )


class TestTheNoteDoesNotOutrunItsArtifact:
    def test_the_note_names_the_commit_the_records_were_bound_to(self) -> None:
        """A note citing a different commit than the artifact is a fiction.

        Cheap to check, and the exact thing that goes stale first when an
        artifact is regenerated and its prose is not.
        """

        binding = json.loads(_REMEASURE.read_text())["binding"]
        note = _NOTE.read_text()
        assert binding["commit"][:9] in note
        assert binding["feature_tip_commit"][:9] in note
        assert _POST_3648.ticket in note

    def test_the_note_states_the_frozen_run_is_not_amended(self) -> None:
        note = _NOTE.read_text()
        assert "does not amend" in note
        assert _FROZEN.name in note
