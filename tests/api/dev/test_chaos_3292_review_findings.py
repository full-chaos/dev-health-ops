"""Regressions for the defects adversarial review reproduced (CHAOS-3292).

Each test names the behaviour that was wrong before the fix, so a revert shows
up as a failing assertion about the *outcome*, not about an implementation
detail.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import DevTranscriptRunState, DirectScope
from dev_health_ops.api.dev.contracts_v2 import ResolutionOutcome
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.prompts import LEGACY_PROMPT_VERSION, PROMPT_VERSION
from dev_health_ops.api.dev.question_interpreter import (
    ClassifierProposal,
    QuestionInterpreter,
    untyped_name_candidates,
)
from dev_health_ops.api.dev.scope_service import (
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    NIGHTFALL_PROJECT,
    ORG_ID,
    SeededCatalog,
    fixed_now,
    request_for,
    run_preflight_orchestrator,
    sequential_ids,
)

# ---------------------------------------------------------------------------
# A partial name must not commit a different entity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_partial_name_does_not_commit_the_entity_it_is_a_substring_of() -> None:
    """The catalog's text search is a ``%query%`` LIKE.

    "the Dev project" returns only "Ask Dev", and treating a sole result as an
    exact match committed it — answering about an entity the user never named.
    """

    service = ScopeResolutionService(
        SeededCatalog([(ORG_ID, ASK_DEV_PROJECT)]), cache=ScopeRequestCache()
    )
    partial = await service.resolve_mention(
        ORG_ID, "permissions_01", lookup_text="Dev", kinds=(EntityKind.PROJECT,)
    )
    assert partial.outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES
    assert partial.entity is None
    # The real match is still offered back, so a partial name is useful rather
    # than merely rejected.
    assert [candidate.canonical_id for candidate in partial.candidates] == [
        ASK_DEV_PROJECT.canonical_id
    ]

    exact = await service.resolve_mention(
        ORG_ID, "permissions_01", lookup_text="Ask Dev", kinds=(EntityKind.PROJECT,)
    )
    assert exact.outcome is ResolutionOutcome.EXACT_MATCH
    assert exact.entity == ASK_DEV_PROJECT


@pytest.mark.asyncio
async def test_a_partial_name_never_reaches_a_subject_bearing_tool() -> None:
    output = await run_preflight_orchestrator(
        question="What is the status of the Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="partial",
    )

    assert output.result.error is not None
    assert output.result.error.code == "scope_ambiguous"
    assert output.calls == []


# ---------------------------------------------------------------------------
# Page context must not stand in for a name we merely failed to type
# ---------------------------------------------------------------------------


def test_page_context_is_not_substituted_for_an_untyped_name() -> None:
    from dev_health_ops.api.dev.contracts import DevEntityRef, EntityType
    from dev_health_ops.api.dev.question_interpreter import extract_mentions

    page_ref = DevEntityRef(
        entity_type=EntityType.PROJECT,
        entity_id=ASK_DEV_PROJECT.canonical_id,
        display_label=ASK_DEV_PROJECT.label,
    )
    mentions = extract_mentions(
        "How is Nightfall doing?", context_refs=[page_ref], mint_id=sequential_ids()
    )

    # Before the fix the kind-noun grammar found nothing, the question looked
    # subject-free, and the page's own project was committed — so the run
    # answered about Ask Dev under the name Nightfall.
    assert [mention.normalized_lookup_text for mention in mentions] != [
        ASK_DEV_PROJECT.canonical_id
    ]
    assert mentions == ()


@pytest.mark.asyncio
async def test_a_page_scoped_run_does_not_answer_under_an_unresolved_name() -> None:
    project_ref = {
        "entity_type": "project",
        "entity_id": ASK_DEV_PROJECT.canonical_id,
        "display_label": ASK_DEV_PROJECT.label,
        "repository_id": None,
    }
    output = await run_preflight_orchestrator(
        question="How is Nightfall doing?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        scope_overrides={"direct_scope": "project", "entity_refs": [project_ref]},
        script_id="page-ctx",
    )

    # The bare name is unresolved, so nothing is committed for it and the
    # legacy backstop is re-armed to judge the answer's own text.
    assert output.preflight_outcomes() == ("proceeded_unresolved_bare_name",)


# ---------------------------------------------------------------------------
# Bare names without a kind noun
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("How is Nightfall doing?", ("Nightfall",)),
        ("Anything about Nightfall worth knowing?", ("Nightfall",)),
        # Sentence-initial single words are capitalized by grammar, not naming.
        ("What is the current state?", ()),
        ("How are we doing on delivery this month?", ()),
        ("Show me the status of everything", ()),
    ],
)
def test_bare_names_are_recognized_without_sentence_initial_false_positives(
    question: str, expected: tuple[str, ...]
) -> None:
    assert untyped_name_candidates(question) == expected


@pytest.mark.asyncio
async def test_a_resolvable_bare_name_commits_a_subject() -> None:
    output = await run_preflight_orchestrator(
        question="How is Nightfall doing?",
        entities=[(ORG_ID, NIGHTFALL_PROJECT)],
        script_id="bare-ok",
    )

    assert output.result.state is RunState.COMPLETED
    assert output.calls[0].scope.direct_scope is DirectScope.PROJECT
    assert [ref.entity_id for ref in output.calls[0].scope.entity_refs] == [
        NIGHTFALL_PROJECT.canonical_id
    ]


@pytest.mark.asyncio
async def test_an_unresolvable_bare_name_does_not_block_the_run() -> None:
    """R4, concretely: "what is our DORA score?" must still answer.

    A bare name we cannot resolve is not proof that it was a subject, so it
    re-arms the backstop instead of terminating — otherwise an acronym in an
    ordinary organization-wide question would break a working question.
    """

    with_acronym = await run_preflight_orchestrator(
        question="What is our DORA score this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="dora",
    )
    without = await run_preflight_orchestrator(
        question="What is our delivery score this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="dora",
    )

    # The acronym is recognized as a bare name and resolves to nothing, but the
    # run reaches exactly the same terminal state, under exactly the same
    # organization scope, as the same question without it.
    assert with_acronym.preflight_outcomes() == ("proceeded_unresolved_bare_name",)
    assert with_acronym.result.state is without.result.state
    assert [request.tool_id.value for request in with_acronym.calls] == [
        request.tool_id.value for request in without.calls
    ]
    for request in with_acronym.calls:
        assert request.scope.direct_scope is DirectScope.ORGANIZATION


@pytest.mark.asyncio
async def test_an_unresolved_bare_name_rearms_only_the_name_specific_reasons() -> None:
    """Re-arming the backstop must be narrow.

    Only the reasons that are *evidence about the unresolved name* may
    terminate: the answer narrating it, or a resolution attempt that came back
    ambiguous or not-found. ``no_evidence_backed_claims`` describes the shape
    of the answer, not the name, and terminating on it would fail an ordinary
    organization-wide question containing a capitalized acronym.
    """

    from dev_health_ops.api.dev.orchestrator import DevOrchestrator
    from dev_health_ops.api.dev.subject_preflight import (
        PreflightDecision,
        SubjectPreflightResult,
    )

    def preflight(*, bare_name_unresolved: bool) -> SubjectPreflightResult:
        return SubjectPreflightResult(
            decision=PreflightDecision.PROCEED,
            interpretation=None,  # type: ignore[arg-type]
            ledger=None,
            committed_resolution=None,
            answer=None,
            outcome=None,
            allowed_tools=frozenset(),
            diagnostic="test",
            legacy_guard_required=bare_name_unresolved,
        )

    armed = preflight(bare_name_unresolved=True)
    quiet = preflight(bare_name_unresolved=False)

    assert (
        DevOrchestrator._legacy_guard_is_terminal(armed, "narrated_unresolved_entity")
        is True
    )
    assert (
        DevOrchestrator._legacy_guard_is_terminal(armed, "resolve_scope_not_found")
        is True
    )
    assert (
        DevOrchestrator._legacy_guard_is_terminal(armed, "no_evidence_backed_claims")
        is False
    )
    # A committed-subject run never lets the backstop decide anything...
    assert (
        DevOrchestrator._legacy_guard_is_terminal(quiet, "narrated_unresolved_entity")
        is False
    )
    # ...and the flag-off path keeps every reason terminal, as today.
    for reason in (
        "narrated_unresolved_entity",
        "no_evidence_backed_claims",
        "resolve_scope_ambiguous",
    ):
        assert DevOrchestrator._legacy_guard_is_terminal(None, reason) is True


# ---------------------------------------------------------------------------
# The prompt must describe the run it is actually composing for
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_committed_subject_prompt_is_only_used_when_one_is_committed() -> (
    None
):
    committed = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="prompt-committed",
    )
    assert committed.provider is not None
    assert '"prompt_version":"' + PROMPT_VERSION in committed.provider.system_texts[0]
    assert "committed_subject" in committed.provider.system_texts[0]

    organization_wide = await run_preflight_orchestrator(
        question="How are we doing on delivery this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="prompt-orgwide",
    )
    assert organization_wide.provider is not None
    system_text = organization_wide.provider.system_texts[0]
    # Telling a run with no committed subject that "resolution is already
    # complete" would be false, and on the flag-off path it deletes the only
    # instruction that makes named-entity resolution happen at all.
    assert '"prompt_version":"' + LEGACY_PROMPT_VERSION in system_text
    assert "named_entity_resolution" in system_text
    assert "resolve_scope.v1" in system_text


@pytest.mark.asyncio
async def test_a_flag_off_run_keeps_the_v1_prompt() -> None:
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="prompt-off",
        preflight_enabled=False,
    )
    assert output.provider is not None
    assert (
        '"prompt_version":"' + LEGACY_PROMPT_VERSION in output.provider.system_texts[0]
    )


# ---------------------------------------------------------------------------
# The transcript contract must admit the new in-flight states
# ---------------------------------------------------------------------------


def test_the_transcript_contract_admits_every_run_state() -> None:
    from typing import get_args

    admitted = set(get_args(DevTranscriptRunState))
    # A transcript can be fetched while a run is mid-preflight; omitting these
    # turned an in-progress run into a server error at transcript validation.
    assert {state.value for state in RunState} <= admitted


# ---------------------------------------------------------------------------
# The constrained classifier cannot point at an arbitrary entity
# ---------------------------------------------------------------------------


class _SpanClassifier:
    def __init__(self, span: str) -> None:
        self.span = span

    async def classify(self, *, question: str) -> ClassifierProposal:
        del question
        return ClassifierProposal(
            intent_id="entity_status",
            cardinality="singular",
            candidates=((self.span, "project"),),
            confidence=0.95,
        )


@pytest.mark.parametrize(
    "span",
    ["a", "A", "ightfal", "the", " Nightfall"],
    ids=["one_char", "one_capital", "mid_word_fragment", "stop_word", "unstripped"],
)
@pytest.mark.asyncio
async def test_a_substring_that_is_not_a_whole_name_is_rejected(span: str) -> None:
    """Literal-substring alone was too weak.

    A single character or a mid-word fragment appears in almost any question,
    so it satisfied "quoted from the user" while letting the classifier point
    at an arbitrary catalog entry.
    """

    interpreted = await QuestionInterpreter(
        classifier=_SpanClassifier(span), mint_id=sequential_ids(), now=fixed_now
    ).interpret(request_for("Anything worth a look at Nightfall today?"))

    assert interpreted.intent.interpretation_reasons[-1] == "fallback.rejected"


@pytest.mark.asyncio
async def test_a_whole_word_span_from_the_question_is_still_accepted() -> None:
    interpreted = await QuestionInterpreter(
        classifier=_SpanClassifier("Nightfall"), mint_id=sequential_ids(), now=fixed_now
    ).interpret(request_for("Anything worth a look at Nightfall today?"))

    assert "fallback.model_assisted" in interpreted.intent.interpretation_reasons
    assert any(
        mention.original_text_span == "Nightfall" for mention in interpreted.mentions
    )


def _unused(value: Any) -> None:  # pragma: no cover - keeps Any import honest
    del value
