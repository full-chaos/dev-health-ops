"""CHAOS-3531: a disclosure must survive the warnings bound, whichever one it is.

``DevAnswer.warnings`` is capped at twenty. Both server-owned disclosures
ride that list -- it is what ``streaming`` publishes as ``warning`` frames
and what ``terminal_frames.wrap_legacy_answer_as_frame`` copies into the
frame's ``limitations`` -- so one append reaches the wire and the rendered
answer together.

CHAOS-3525's adversarial review found that yielding at the bound lets a
model-chosen subject reach a reader with no disclosure at all, and fixed
``disclose_subject_match`` to take the last slot instead. This module is the
same proof for its sibling: ``disclose_scope_widening`` (CHAOS-3497) had the
identical hole, and CHAOS-3497's own write-up claimed the widening "is said
out loud" without qualification -- a claim that was false at the bound.

The rule is now one rule with one implementation. Two disclosure helpers
behaving oppositely at the same bound is a trap for the next reader, and the
third disclosure someone adds should inherit the behaviour rather than
re-decide it.

Why displacing a producer warning is the right trade, stated once here
because it is the judgement the whole module rests on: an undisclosed scope
decision is a claim the reader cannot check, and a dropped twentieth warning
is not. The answer is already degenerate at twenty warnings; the disclosure
is the one entry whose absence changes what the answer MEANS.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevAnswer
from dev_health_ops.api.dev.no_match_terminal import (
    SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE,
    disclose_scope_widening,
    disclose_subject_match,
    subject_matched_disclosure,
)

_MATCHED_SPAN = "ACR"
_MATCHED_LABEL = "Dev Health Agent Context Runtime (Context Fabric)"


def _answer(warnings: list[str]) -> DevAnswer:
    base = DevAnswer.model_validate(positive_fixtures()["dev_answer.v1"])
    return base.model_copy(update={"warnings": warnings})


def _saturated() -> DevAnswer:
    """An answer whose warning list is exactly at the contract bound."""

    return _answer([f"producer warning {index}" for index in range(20)])


def test_the_widening_disclosure_survives_a_full_warning_list() -> None:
    """The CHAOS-3497 hole, stated as the state the answer must reach.

    Before this fix the helper returned the answer unchanged at the bound, so
    a run that widened to organization scope could answer organization-wide
    with no prose disclosure of the widening -- exactly the human-reader gap
    CHAOS-3497 part 2 existed to close.
    """

    disclosed = disclose_scope_widening(_saturated())

    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE in disclosed.warnings
    assert len(disclosed.warnings) == 20, "the contract bound still holds"


def test_the_subject_match_disclosure_survives_a_full_warning_list() -> None:
    """The sibling, kept green so the two cannot drift apart again.

    This one was fixed in CHAOS-3525; the assertion lives here beside its
    twin so a future change to the shared helper is caught for BOTH callers
    rather than only whichever module the author happened to open.
    """

    disclosed = disclose_subject_match(
        _saturated(), span=_MATCHED_SPAN, label=_MATCHED_LABEL
    )

    assert (
        subject_matched_disclosure(span=_MATCHED_SPAN, label=_MATCHED_LABEL)
        in disclosed.warnings
    )
    assert len(disclosed.warnings) == 20


@pytest.mark.parametrize(
    ("disclose", "sentence"),
    [
        (
            lambda answer: disclose_scope_widening(answer),
            SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE,
        ),
        (
            lambda answer: disclose_subject_match(
                answer, span=_MATCHED_SPAN, label=_MATCHED_LABEL
            ),
            subject_matched_disclosure(span=_MATCHED_SPAN, label=_MATCHED_LABEL),
        ),
    ],
    ids=["widening", "subject-match"],
)
def test_a_disclosure_displaces_exactly_one_producer_warning(
    disclose, sentence: str
) -> None:
    """The cost of the trade is bounded and identical for both.

    Exactly one producer warning is lost, it is the LAST one, and every other
    producer warning survives in its original order. A helper that dropped
    more than it needed -- or reordered what it kept -- would be a quieter
    kind of data loss than the one being fixed.
    """

    saturated = _saturated()
    disclosed = disclose(saturated)

    assert disclosed.warnings[0] == sentence, (
        "the disclosure goes first so a truncating renderer keeps it"
    )
    assert disclosed.warnings[1:] == list(saturated.warnings)[:-1]
    assert saturated.warnings[-1] not in disclosed.warnings


@pytest.mark.parametrize(
    ("disclose", "sentence"),
    [
        (
            lambda answer: disclose_scope_widening(answer),
            SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE,
        ),
        (
            lambda answer: disclose_subject_match(
                answer, span=_MATCHED_SPAN, label=_MATCHED_LABEL
            ),
            subject_matched_disclosure(span=_MATCHED_SPAN, label=_MATCHED_LABEL),
        ),
    ],
    ids=["widening", "subject-match"],
)
def test_disclosing_twice_costs_only_one_slot(disclose, sentence: str) -> None:
    """Idempotent at the bound, where a non-idempotent helper would be worst.

    ``finish()`` is the one funnel every terminal passes through, and nothing
    structurally forbids a future path from disclosing twice. If each call
    displaced another producer warning, repeated disclosure would quietly eat
    the list.
    """

    once = disclose(_saturated())
    twice = disclose(once)

    assert list(twice.warnings) == list(once.warnings)
    assert once.warnings.count(sentence) == 1


@pytest.mark.parametrize(
    ("disclose", "sentence"),
    [
        (
            lambda answer: disclose_scope_widening(answer),
            SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE,
        ),
        (
            lambda answer: disclose_subject_match(
                answer, span=_MATCHED_SPAN, label=_MATCHED_LABEL
            ),
            subject_matched_disclosure(span=_MATCHED_SPAN, label=_MATCHED_LABEL),
        ),
    ],
    ids=["widening", "subject-match"],
)
def test_an_unsaturated_answer_loses_nothing(disclose, sentence: str) -> None:
    """The ordinary case: below the bound nothing is displaced at all.

    The control for the tests above -- if displacement happened on every
    answer rather than only at the bound, they would still pass while the
    helper quietly dropped a warning from every disclosed run.
    """

    answer = _answer(["one real warning"])
    disclosed = disclose(answer)

    assert sentence in disclosed.warnings
    assert "one real warning" in disclosed.warnings
    assert len(disclosed.warnings) == 2


@pytest.mark.asyncio
async def test_the_two_disclosures_can_never_both_fire_on_one_run() -> None:
    """Mutual exclusion, pinned rather than left to a code reading.

    Adversarial review traced that these two can never both apply -- the QUA
    promotion sets ``legacy_guard_required=False`` and commits an EXACT
    resolution, and the widening disclosure requires that flag true AND an
    ``organization_fallback`` outcome -- but nothing tested it. Two
    independent guards holding today is exactly the kind of property that
    quietly stops holding when someone touches one of them.

    It matters because the two sentences contradict each other: one says the
    named subject could not be matched and the answer went organization-wide,
    the other says it WAS matched to a specific entity. A reader shown both
    learns nothing except that the system is unsure.

    Asserted end to end through the real promotion path rather than by
    re-checking the guards in isolation.

    What this test does and does NOT catch, measured rather than assumed:
    removing EITHER guard alone leaves it green, because the other still
    holds -- verified by mutating each in turn. It fails when BOTH go
    (verified). That is the honest scope of a test over a double-guarded
    property: it is a regression guard on the observable outcome, not a
    proof that each guard individually matters. Stated here so nobody reads
    a green run as evidence that both guards are still load-bearing.
    """

    from dev_health_ops.api.dev.contracts import ScopeResolutionOutcome
    from tests._chaos_3292_preflight import ORG_ID, run_preflight_orchestrator
    from tests.api.dev.test_chaos_3525_qua_commit import (
        ACR_PROJECT,
        ACR_QUESTION,
        _selecting_shadow,
    )

    output = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="3531-mutual-exclusion",
        qua_shadow=_selecting_shadow(text_span="ACR", script_id="3531-mx"),
    )

    answer = output.result.answer
    assert answer is not None, "the promotion must have produced an answer"
    assert answer.resolved_scope.outcome is ScopeResolutionOutcome.EXACT

    assert (
        subject_matched_disclosure(span="ACR", label=ACR_PROJECT.label)
        in answer.warnings
    )
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE not in answer.warnings, (
        "a run that committed a specific subject must not also claim it "
        "could not match one -- the two disclosures contradict each other"
    )
