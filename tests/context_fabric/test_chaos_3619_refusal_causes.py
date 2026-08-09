"""CHAOS-3619: the graph arm's refusals, separated by cause.

One recorded string covers all 26 refusals, and it cannot distinguish an arm
that was not ALLOWED to see a match from one that found nothing to match.
Those are opposite facts and a reader deciding whether refusals are a
capability gap needs them apart.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from trials.chaos_3619.refusal_causes import (
    AUTHORIZATION,
    NO_MATCH,
    NO_MENTION,
    counts,
    decompose,
)

_RECORDS = (
    Path(__file__).resolve().parents[2]
    / "trials"
    / "chaos_3619"
    / "results"
    / "trial-results.records.json"
)
_LEG_B = "leg_b_job_held_constant"


@pytest.fixture(scope="module")
def causes():
    if not _RECORDS.exists():
        pytest.fail(
            f"no measured records at {_RECORDS}. This suite reads the "
            "committed sweep; a skip here would let the decomposition's "
            "numbers go unchecked while the file reads as covered"
        )
    return decompose(_RECORDS, _LEG_B)


class TestTheRefusalsDecomposeByRecordedCause:
    def test_every_refusal_is_accounted_for(self, causes) -> None:
        assert len(causes) == 26, (
            f"decomposed {len(causes)} refusals; the measured sweep records "
            "26 in Leg B. A decomposition that covers a different number is "
            "describing a different run"
        )

    def test_the_dominant_cause_is_absent_subject_extraction(self, causes) -> None:
        """21 of 26, and the reason this is the headline.

        `mention_texts` runs production `extract_mentions` plus the untyped
        backstop -- the SAME extraction the native interpreter runs. So this
        category is a common-mode limit both arms sit behind, not a graph-arm
        capability gap, and reading it as the latter would attribute a shared
        upstream ceiling to the arm under test.
        """

        assert counts(causes)[NO_MENTION] == 21

    def test_exactly_one_refusal_is_authorization_doing_its_job(self, causes) -> None:
        """A09 is the planted restricted same-tenant entity.

        Counted apart from the misses because it is CORRECT behaviour: the
        arm matched and the grant withheld. Folding it in with the
        no-match rows would report the arm as weaker for being safe.
        """

        authorization = [c for c in causes if c.category == AUTHORIZATION]
        assert [c.case_id for c in authorization] == [
            "A09_unauthorized_same_tenant_entity"
        ]
        assert authorization[0].authorization_filtered_count > 0

    def test_the_remaining_four_extracted_phrases_that_matched_nothing(
        self, causes
    ) -> None:
        assert counts(causes)[NO_MATCH] == 4
        assert all(c.mentions_extracted > 0 for c in causes if c.category == NO_MATCH)

    def test_no_refusal_is_attributed_to_an_unsupported_shape(self, causes) -> None:
        """The claim the decomposition must NOT support.

        Every `discovered_cohort` case refused, which invites the reading
        that the arm has no cohort handling. The records do not say that:
        all 14 refused with no subject phrase extracted, so the arm never
        reached shape handling at all. Asserted so the convenient story
        cannot be told from these numbers.
        """

        cohort = [c for c in causes if c.comparison_shape == "discovered_cohort"]
        assert len(cohort) == 14
        assert {c.category for c in cohort} == {NO_MENTION}, (
            "a cohort refusal was attributed to something other than absent "
            "mention extraction; if that is real the 'no cohort support' "
            "reading becomes available and must be argued, not assumed"
        )


class TestTheDecompositionRefusesToDescribeADifferentRun:
    def test_a_records_file_that_disagrees_is_a_failure_not_a_report(
        self, tmp_path: Path
    ) -> None:
        """The self-check, observed firing.

        Without this the drift guard is a branch nobody has seen taken, and
        a decomposition silently describing a superseded sweep is exactly the
        failure it exists to prevent.
        """

        import json

        payload = json.loads(_RECORDS.read_text())
        flipped = False
        for case in payload["cases"]:
            if case["leg"] != _LEG_B:
                continue
            for arm in case["arms"]:
                if arm["arm_id"] == "graph_assisted_shadow_arm" and (
                    arm["disposition"] == "arm_refused"
                ):
                    # Claim the arm scored a case it actually refused.
                    arm["disposition"] = "scored"
                    arm["dimension_outcomes"] = []
                    flipped = True
                    break
            if flipped:
                break
        assert flipped, "no refused row to flip; the fixture is vacuous"
        target = tmp_path / "drifted.records.json"
        target.write_text(json.dumps(payload))
        with pytest.raises(RuntimeError, match="no longer reproduces"):
            decompose(target, _LEG_B)
