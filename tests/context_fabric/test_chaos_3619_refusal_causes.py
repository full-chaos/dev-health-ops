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
    DIVERGENCE_LEDGER,
    NO_MATCH,
    NO_MENTION,
    PHRASE_EXTRACTED_POST_3648,
    counts,
    decompose,
)

#: The CHAOS-3648 entry, read from the ledger rather than restated here: a
#: test that hardcodes the four case ids would keep passing after someone
#: edited the ledger, which is the one thing the ledger exists to prevent.
_POST_3648 = next(entry for entry in DIVERGENCE_LEDGER if entry.ticket == "CHAOS-3648")

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
        """21 of 26 as recorded -- 17 still absent, 4 ledgered to CHAOS-3648.

        `mention_texts` runs production `extract_mentions` plus the untyped
        backstop -- the SAME extraction the native interpreter runs. So this
        category is a common-mode limit both arms sit behind, not a graph-arm
        capability gap, and reading it as the latter would attribute a shared
        upstream ceiling to the arm under test.

        Both facts are asserted, with the citation between them, because both
        are true and each alone misleads. "21" alone describes a ceiling the
        current code no longer has; "17" alone silently restates the pinned
        sweep's headline as if it had always been 17.
        """

        by_cause = counts(causes)
        assert by_cause[NO_MENTION] == 17
        assert by_cause[PHRASE_EXTRACTED_POST_3648] == 4
        assert by_cause[NO_MENTION] + by_cause[PHRASE_EXTRACTED_POST_3648] == 21, (
            "the recorded sweep's 21 absent-extraction refusals must still "
            "add up; if they do not, either the ledger or the recomputation "
            "has stopped describing the pinned run"
        )

    def test_the_ledgered_four_are_exactly_the_cases_chaos_3648_moved(
        self, causes
    ) -> None:
        """The divergence is attributed to a ticket, not absorbed silently.

        Asserted against the ledger's own case ids so that widening the
        ledger without widening the extractor -- or the reverse -- fails
        here rather than quietly enlarging what "accounted for" means.
        """

        ledgered = {
            c.case_id for c in causes if c.category == PHRASE_EXTRACTED_POST_3648
        }
        assert ledgered == set(_POST_3648.case_ids)
        assert _POST_3648.pull_request and _POST_3648.landed_on
        # The entry's stated cause, checked rather than trusted: each of these
        # is ledgered *because* extraction now yields a phrase.
        assert all(
            c.mentions_extracted > 0
            for c in causes
            if c.category == PHRASE_EXTRACTED_POST_3648
        )

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
        # CHAOS-3648 moved four singular-subject cases and no cohort case, so
        # the claim above is unchanged by the ledger rather than merely
        # surviving it. Stated here because "the shape argument still holds"
        # is exactly the kind of thing a reader would otherwise assume.
        assert not (set(_POST_3648.case_ids) & {c.case_id for c in cohort})


class TestTheDecompositionRefusesToDescribeADifferentRun:
    def test_a_records_file_that_disagrees_is_a_failure_not_a_report(
        self, tmp_path: Path
    ) -> None:
        """The self-check, observed firing.

        Without this the drift guard is a branch nobody has seen taken, and
        a decomposition silently describing a superseded sweep is exactly the
        failure it exists to prevent.
        """

        target = _records_with_disposition_flipped(
            tmp_path, "drifted.records.json", ledgered=False
        )
        with pytest.raises(RuntimeError, match="no longer reproduces"):
            decompose(target, _LEG_B)

    def test_a_ledgered_case_diverging_the_other_way_still_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ledger licenses a DIRECTION, not a case id.

        CHAOS-3648 ledgers "the pin refused it, the live code resolves it".
        The mirror image -- the pin scored it, the live code refuses it -- is a
        recall regression in the very case the ledger names, and a
        membership-only check would wave it through.

        Both halves are staged: the pinned row is flipped to `scored`, and the
        live extraction for that case is emptied, so the recomputation and the
        pin disagree in the direction the entry does NOT cover.
        """

        import json

        from trials.chaos_3619 import graph_leg

        payload = json.loads(_RECORDS.read_text())
        victim = sorted(_POST_3648.case_ids)[0]
        question = None
        for case in payload["cases"]:
            if case["leg"] != _LEG_B or case["case_id"] != victim:
                continue
            question = case["question"]
            for arm in case["arms"]:
                if arm["arm_id"] == "graph_assisted_shadow_arm":
                    arm["disposition"] = "scored"
                    arm["dimension_outcomes"] = []
        assert question is not None, "the ledgered case is absent from the pin"
        target = tmp_path / "reversed.records.json"
        target.write_text(json.dumps(payload))

        real_mention_texts = graph_leg.mention_texts

        def no_phrase_for_the_victim(text: str) -> tuple[str, ...]:
            return () if text == question else real_mention_texts(text)

        monkeypatch.setattr(graph_leg, "mention_texts", no_phrase_for_the_victim)

        with pytest.raises(RuntimeError, match="no longer reproduces") as raised:
            decompose(target, _LEG_B)
        assert victim in str(raised.value)
        assert _POST_3648.ticket in str(raised.value), (
            "the drift message must name the entry it declined to apply, or a "
            "reader cannot tell a ledgered case from an unledgered one"
        )


def _records_with_disposition_flipped(
    tmp_path: Path, name: str, *, ledgered: bool
) -> Path:
    """A copy of the pinned records with one refused row claimed as scored."""

    import json

    payload = json.loads(_RECORDS.read_text())
    flipped = False
    for case in payload["cases"]:
        if case["leg"] != _LEG_B:
            continue
        if (case["case_id"] in _POST_3648.case_ids) is not ledgered:
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
    assert flipped, "no matching refused row to flip; the fixture is vacuous"
    target = tmp_path / name
    target.write_text(json.dumps(payload))
    return target
