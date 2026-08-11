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
    COHORT_RESOLVED_POST_3645,
    DIVERGENCE_LEDGER,
    NO_COHORT_FAMILY_SUPPORT,
    NO_MATCH,
    NO_MENTION,
    PHRASE_EXTRACTED_POST_3648,
    counts,
    decompose,
)

#: The CHAOS-3648 and CHAOS-3645 entries, read from the ledger rather than
#: restated here: a test that hardcodes case ids would keep passing after
#: someone edited the ledger, which is the one thing the ledger exists to
#: prevent.
_POST_3648 = next(entry for entry in DIVERGENCE_LEDGER if entry.ticket == "CHAOS-3648")
_POST_3645 = next(entry for entry in DIVERGENCE_LEDGER if entry.ticket == "CHAOS-3645")

_RECORDS = (
    Path(__file__).resolve().parents[2]
    / "trials"
    / "chaos_3619"
    / "results"
    / "trial-results.records.json"
)
_CONSOLIDATED = (
    Path(__file__).resolve().parents[2]
    / "trials"
    / "chaos_3619"
    / "results"
    / "consolidated-post-wave.records.json"
)
_LEG_B = "leg_b_job_held_constant"
_GRAPH_ARM = "graph_assisted_shadow_arm"


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

    def test_the_seeded_modes_absent_extraction_cases_still_add_up(
        self, causes
    ) -> None:
        """7 of 26 as recorded through the SEEDED mode -- 3 still absent, 4
        ledgered to CHAOS-3648.

        Before CHAOS-3656 this category also swallowed 14 ``discovered_cohort``
        refusals that were never an extraction story at all -- a cohort
        question carries no subject phrase by design, so counting them here
        conflated "the seeded path found no phrase" with "no mechanism was
        even tried yet". Mechanism-aware recomputation moves every one of
        those 14 into cohort-mode categories (see
        ``TestTheCohortModeIsDecomposedSeparately``), and what remains here is
        exactly the non-cohort seeded-mode story: 3 still absent, 4 ledgered.

        `mention_texts` runs production `extract_mentions` plus the untyped
        backstop -- the SAME extraction the native interpreter runs. So this
        category is a common-mode limit both arms sit behind, not a graph-arm
        capability gap, and reading it as the latter would attribute a shared
        upstream ceiling to the arm under test.
        """

        by_cause = counts(causes)
        assert by_cause[NO_MENTION] == 3
        assert by_cause[PHRASE_EXTRACTED_POST_3648] == 4
        assert by_cause[NO_MENTION] + by_cause[PHRASE_EXTRACTED_POST_3648] == 7, (
            "the seeded mode's 7 absent-extraction refusals must still add "
            "up; if they do not, either the ledger or the recomputation has "
            "stopped describing the pinned run"
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

    def test_no_seeded_mode_refusal_is_cohort_shaped(self, causes) -> None:
        """The claim the decomposition must NOT support, restated for CHAOS-3656.

        A `no_mention_extracted` / `no_authorized_match` category describes a
        check the SEEDED mode ran. Cohort discovery never runs it -- it reads
        no question text at all -- so no cohort-shaped refusal may carry
        either category, or the decomposition would be crediting the seeded
        mode with having looked at a question it never saw.
        """

        seeded_categories = {NO_MENTION, NO_MATCH}
        offenders = [
            c
            for c in causes
            if c.comparison_shape == "discovered_cohort"
            and c.category in seeded_categories
        ]
        assert offenders == []


class TestTheCohortModeIsDecomposedSeparately:
    """CHAOS-3656: the arm's second entry mode gets its own recomputation.

    Every `discovered_cohort` case in the CHAOS-3619 frozen pin refused,
    because the pin predates CHAOS-3645 -- the seeded path is the only one
    that existed, and a cohort question carries no subject phrase for it to
    find. Reading that as "the arm cannot do cohorts" was already wrong (the
    original CHAOS-3619 report says so), and now the decomposition can prove
    it rather than assert it: recomputing through
    `discover_cohort_for` -- the SAME mechanism `trials.chaos_3619.sweep`
    selects for this shape -- shows 13 of the 14 resolving, and the frozen
    pin's continued refusal for those 13 is exactly what
    `DIVERGENCE_LEDGER`'s CHAOS-3645 entry now cites.
    """

    def test_thirteen_of_fourteen_cohort_refusals_are_the_chaos_3645_divergence(
        self, causes
    ) -> None:
        ledgered = {
            c.case_id for c in causes if c.category == COHORT_RESOLVED_POST_3645
        }
        assert ledgered == set(_POST_3645.case_ids)
        assert len(ledgered) == 13
        assert _POST_3645.pull_request and _POST_3645.landed_on
        # The entry's stated cause, checked rather than trusted: the ledger
        # licenses this category only for cases the live cohort mechanism
        # actually resolves, so every carrier must be a real cohort case.
        assert all(
            c.comparison_shape == "discovered_cohort"
            for c in causes
            if c.category == COHORT_RESOLVED_POST_3645
        )

    def test_the_fourteenth_cohort_case_is_a_named_boundary_not_a_mention_gap(
        self, causes
    ) -> None:
        """T07's family has no subjectless entry -- a decision, not a gap.

        Distinct from the other 13 for a reason that survives CHAOS-3645: its
        family (`clarification_and_no_match`) is deliberately absent from
        `FAMILY_CANDIDATE_KINDS`, so it refuses under BOTH mechanisms and is
        never ledgered -- there is no divergence to admit, because nothing
        about this case's outcome ever changed.
        """

        boundary = [c for c in causes if c.category == NO_COHORT_FAMILY_SUPPORT]
        assert [c.case_id for c in boundary] == ["T07_going_sideways_open_question"]
        assert boundary[0].comparison_shape == "discovered_cohort"
        assert boundary[0].mentions_extracted == 0

    def test_the_fourteen_cohort_cases_partition_into_exactly_these_two_categories(
        self, causes
    ) -> None:
        cohort = [c for c in causes if c.comparison_shape == "discovered_cohort"]
        assert len(cohort) == 14
        assert {c.category for c in cohort} == {
            COHORT_RESOLVED_POST_3645,
            NO_COHORT_FAMILY_SUPPORT,
        }


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

    def test_a_cohort_ledgered_case_diverging_the_other_way_still_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """CHAOS-3656's own version of the CHAOS-3648 mirror-image test.

        CHAOS-3645 ledgers "the pin refused it, live cohort discovery now
        resolves it". The mirror image -- the pin scored it, live cohort
        discovery now refuses it -- is a recall regression in the very case
        the ledger names, and must stop the run even though the case id is
        ledgered, because the ledger licenses a DIRECTION, not a case id.

        The victim, `S06_declared_complete_without_delivery_evidence`, is the
        one CHAOS-3645 case whose question family
        (`declared_versus_actual`) no sibling case shares, so patching
        `discover_cohort_for` for that family alone cannot also mutate a
        different case's outcome and mask what this test is checking.
        """

        import json

        from dev_health_ops.api.dev.investigation_contract import QuestionFamilyID
        from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
            UnsupportedCohortFamilyError,
        )
        from trials.chaos_3619 import graph_leg

        victim = "S06_declared_complete_without_delivery_evidence"
        assert victim in _POST_3645.case_ids

        payload = json.loads(_RECORDS.read_text())
        found = False
        for case in payload["cases"]:
            if case["leg"] != _LEG_B or case["case_id"] != victim:
                continue
            for arm in case["arms"]:
                if arm["arm_id"] == _GRAPH_ARM:
                    arm["disposition"] = "scored"
                    arm["dimension_outcomes"] = []
                    found = True
        assert found, "the ledgered cohort case is absent from the pin"
        target = tmp_path / "reversed_cohort.records.json"
        target.write_text(json.dumps(payload))

        real_discover_cohort_for = graph_leg.discover_cohort_for

        def refuse_only_the_victims_family(*, question_family, **kwargs):
            if question_family is QuestionFamilyID.DECLARED_VERSUS_ACTUAL:
                raise UnsupportedCohortFamilyError("forced for the regression test")
            return real_discover_cohort_for(question_family=question_family, **kwargs)

        monkeypatch.setattr(
            graph_leg, "discover_cohort_for", refuse_only_the_victims_family
        )

        with pytest.raises(RuntimeError, match="no longer reproduces") as raised:
            decompose(target, _LEG_B)
        assert victim in str(raised.value)
        assert _POST_3645.ticket in str(raised.value), (
            "the drift message must name the entry it declined to apply, or a "
            "reader cannot tell a ledgered cohort case from an unledgered one"
        )

    def test_a_current_cohort_case_regressing_to_refusal_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The scenario CHAOS-3656 exists to keep catching, not just admit.

        Making the decomposition mechanism-aware must not turn into "cohort
        cases never raise": a case the CURRENT tip actually scores (no
        ledger involved -- `consolidated-post-wave.records.json` already
        records it as `scored`) whose live cohort recomputation regresses to
        empty must still be reported as drift.
        """

        from dev_health_ops.api.dev.investigation_contract import QuestionFamilyID
        from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
            UnsupportedCohortFamilyError,
        )
        from trials.chaos_3619 import graph_leg

        victim = "S06_declared_complete_without_delivery_evidence"

        real_discover_cohort_for = graph_leg.discover_cohort_for

        def refuse_only_the_victims_family(*, question_family, **kwargs):
            if question_family is QuestionFamilyID.DECLARED_VERSUS_ACTUAL:
                raise UnsupportedCohortFamilyError("forced for the regression test")
            return real_discover_cohort_for(question_family=question_family, **kwargs)

        monkeypatch.setattr(
            graph_leg, "discover_cohort_for", refuse_only_the_victims_family
        )

        with pytest.raises(RuntimeError, match="no longer reproduces") as raised:
            decompose(_CONSOLIDATED, _LEG_B)
        assert victim in str(raised.value)


#: Every case id any ledger entry names, regardless of mechanism. Used so
#: "pick an UNLEDGERED refused row" stays true as the ledger grows -- picking
#: from `_POST_3648.case_ids` alone would, after CHAOS-3656, sometimes land on
#: a CHAOS-3645 cohort case whose live recomputation already resolves it,
#: which would make the flip a no-op match instead of the drift this fixture
#: exists to manufacture.
_ALL_LEDGERED_IDS = frozenset(
    case_id for entry in DIVERGENCE_LEDGER for case_id in entry.case_ids
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
        if (case["case_id"] in _ALL_LEDGERED_IDS) is not ledgered:
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
