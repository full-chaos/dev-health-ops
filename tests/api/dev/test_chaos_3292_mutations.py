"""CHAOS-3292 mutation suite M1-M9.

Every test here is a **pair**: it first observes the invariant holding on
unmutated code, then defeats exactly one named seam and observes the same
invariant fail. A mutation test that only asserted the mutated behaviour would
not distinguish a guard that closes a gap from one that merely sits beside it.

Mutations are clause-level, not wholesale: the two enforcement points of the
deny-by-default gate are defeated separately (M1a/M1b) and then together
(M1c), because a compound guard mutated as a whole can report KILLED while one
clause inside it is wrong and unasserted.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import QuestionClass, ToolID
from dev_health_ops.api.dev.contracts_v2 import ResolutionOutcome, no_answer_policy
from dev_health_ops.api.dev.contracts_v2.validators import scan_public_text
from dev_health_ops.api.dev.orchestrator import DevOrchestrator
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.question_interpreter import (
    ClassifierProposal,
    QuestionInterpreter,
)
from dev_health_ops.api.dev.scope_service import (
    MentionResolution,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeSearchRequest,
)
from dev_health_ops.api.dev.subject_preflight import (
    PreflightDecision,
    SubjectPreflight,
    SubjectPreflightResult,
)
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    NIGHTFALL_PROJECT,
    ORG_ID,
    OTHER_ORG_ID,
    RunOutput,
    SeededCatalog,
    case_a1,
    case_a2,
    case_a4,
    case_a8,
    case_a9,
    fixed_now,
    request_for,
    sequential_ids,
    versions,
)


def _subject_tool_calls(output: RunOutput) -> list[str]:
    from dev_health_ops.api.dev.subject_preflight import SUBJECT_BEARING_TOOLS

    return [
        request.tool_id.value
        for request in output.calls
        if request.tool_id in SUBJECT_BEARING_TOOLS
    ]


def _preflight_for(entities: Any, **kwargs: Any) -> SubjectPreflight:
    mint = sequential_ids()
    return SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog(entities, **kwargs), cache=ScopeRequestCache()
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )


# ---------------------------------------------------------------------------
# M1 — the two enforcement points of the deny-by-default gate
# ---------------------------------------------------------------------------


def _proceed_anyway(keep_ledger: bool) -> Any:
    """Defeat the pre-loop gate: every termination becomes "proceed"."""

    original = SubjectPreflight.run

    async def mutated(self: SubjectPreflight, **kwargs: Any) -> SubjectPreflightResult:
        result = await original(self, **kwargs)
        if result.decision is PreflightDecision.PROCEED:
            return result
        return SubjectPreflightResult(
            decision=PreflightDecision.PROCEED,
            interpretation=result.interpretation,
            ledger=result.ledger if keep_ledger else None,
            committed_resolution=None,
            answer=None,
            outcome=None,
            allowed_tools=frozenset(ToolID),
            diagnostic="mutated_proceed",
        )

    return mutated


@pytest.mark.asyncio
async def test_m1a_pre_loop_gate_defeated_is_still_caught_at_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _subject_tool_calls(await case_a2()) == []

    monkeypatch.setattr(SubjectPreflight, "run", _proceed_anyway(keep_ledger=True))
    mutated = await case_a2()

    # The run no longer terminates early, but the dispatch gate independently
    # refuses to execute a subject-bearing tool for an unresolved mention.
    assert mutated.result.state is not RunState.INSUFFICIENT_EVIDENCE
    assert _subject_tool_calls(mutated) == []


@pytest.mark.asyncio
async def test_m1b_dispatch_gate_defeated_is_still_caught_before_the_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _subject_tool_calls(await case_a2()) == []

    monkeypatch.setattr(
        DevOrchestrator,
        "_subject_gate_rejection",
        staticmethod(lambda **_kwargs: None),
    )
    mutated = await case_a2()

    assert mutated.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert _subject_tool_calls(mutated) == []


@pytest.mark.asyncio
async def test_m1c_defeating_both_gates_lets_the_subject_tool_execute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fail-before observation that makes M1a and M1b non-vacuous.

    If a subject-bearing tool could not execute for this question even with
    *both* gates removed, neither gate would be proven to be doing anything.
    """

    assert _subject_tool_calls(await case_a2()) == []

    monkeypatch.setattr(SubjectPreflight, "run", _proceed_anyway(keep_ledger=False))
    monkeypatch.setattr(
        DevOrchestrator,
        "_subject_gate_rejection",
        staticmethod(lambda **_kwargs: None),
    )
    mutated = await case_a2()

    assert _subject_tool_calls(mutated) == ["status_snapshot.v1"]


# ---------------------------------------------------------------------------
# M2 — an unresolved subject falling through to organization scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m2_no_authorized_match_must_not_fall_through_to_organization_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = await case_a2()
    assert baseline.result.error is not None
    assert baseline.result.error.code == "scope_not_found"
    assert _subject_tool_calls(baseline) == []

    # The CHAOS-3289 defect, reintroduced: the named subject is unresolved and
    # the run answers about the organization under that name anyway.
    monkeypatch.setattr(SubjectPreflight, "run", _proceed_anyway(keep_ledger=False))
    monkeypatch.setattr(
        DevOrchestrator,
        "_subject_gate_rejection",
        staticmethod(lambda **_kwargs: None),
    )
    mutated = await case_a2()

    assert _subject_tool_calls(mutated) == ["status_snapshot.v1"]
    assert mutated.calls[0].scope.direct_scope.value == "organization"


@pytest.mark.asyncio
async def test_m2_multi_mention_fallthrough_is_caught_in_both_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests._chaos_3292_preflight import (
        case_a6_fake_then_real,
        case_a6_real_then_fake,
    )

    for case in (case_a6_real_then_fake, case_a6_fake_then_real):
        assert _subject_tool_calls(await case()) == []

    monkeypatch.setattr(SubjectPreflight, "run", _proceed_anyway(keep_ledger=False))
    monkeypatch.setattr(
        DevOrchestrator,
        "_subject_gate_rejection",
        staticmethod(lambda **_kwargs: None),
    )
    for case in (case_a6_real_then_fake, case_a6_fake_then_real):
        assert _subject_tool_calls(await case()) == ["status_snapshot.v1"]


# ---------------------------------------------------------------------------
# M3 — an internal token reaching public copy
# ---------------------------------------------------------------------------


_LEAKY_COPY = "No matching subject was found (forbidden_or_not_found)."


@pytest.mark.asyncio
async def test_m3a_leaky_canonical_copy_cannot_be_constructed_at_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer one: the frame refuses to exist.

    Mutating the canonical table also defeats the no-answer *allowlist*
    projection, because the builder and the allowlist read the same object.
    Guardrail (a) is independent of it and scans the frame's public copy
    fields, so the leaked token fails the whole termination closed instead of
    reaching a client.
    """

    baseline = await case_a8()
    assert baseline.result.error is not None
    assert baseline.result.error.code == "scope_not_found"

    monkeypatch.setitem(
        no_answer_policy.CANONICAL_NO_ANSWER_COPY,
        "not_found",
        _LEAKY_COPY,
    )
    mutated = await case_a8()

    assert mutated.result.error is not None
    assert mutated.result.error.code == "internal_error"
    assert scan_public_text(mutated.result.error.safe_message) == []


@pytest.mark.asyncio
async def test_m3b_with_the_leakage_guard_also_disabled_a8_is_the_net(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer two: defeat guardrail (a) as well, and watch A8's scan catch it."""

    from dev_health_ops.api.dev.contracts_v2 import validators as validators_module

    monkeypatch.setitem(
        no_answer_policy.CANONICAL_NO_ANSWER_COPY,
        "not_found",
        _LEAKY_COPY,
    )
    monkeypatch.setattr(
        validators_module, "validate_no_internal_leakage", lambda _frame: None
    )
    mutated = await case_a8()

    assert mutated.result.error is not None
    assert scan_public_text(mutated.result.error.safe_message) == [
        "forbidden_or_not_found"
    ]


# ---------------------------------------------------------------------------
# M4 — the ledger overwriting instead of appending
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m4_ledger_overwrite_instead_of_append_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    atlas_ref = {
        "entity_type": "project",
        "entity_id": ATLAS_PROJECT_TWO.canonical_id,
        "display_label": "Atlas",
        "repository_id": None,
    }
    request = request_for(
        "What's the status of the Atlas project?",
        scope_overrides={
            "direct_scope": "project",
            "entity_refs": [atlas_ref],
            "surface_context": {
                "route_id": "project_detail",
                "entity_refs": [atlas_ref],
                "filter_fingerprint": "filters_01",
            },
        },
    )

    async def run_once() -> Any:
        preflight = _preflight_for(
            [(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)]
        )
        return await preflight.run(
            request=request,
            org_id=ORG_ID,
            permission_fingerprint="permissions_01",
            authorized_scope=request.scope,
            run_id="run_01",
            answer_id="answer_01",
            conversation_id="conversation_01",
        )

    baseline = await run_once()
    assert baseline.ledger is not None
    assert len(baseline.ledger.entries) == 2
    assert baseline.ledger.entries[0].outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES

    from dev_health_ops.api.dev import subject_preflight as preflight_module

    def overwrite_last(
        self: SubjectPreflight, ledger: Any, entries: Any, *, resolved_at: Any
    ) -> Any:
        del self, resolved_at
        if not entries:
            return ledger
        # "Erase the earlier unresolved entry and put the successful one in its
        # place" — the exact history rewrite the append-only ledger forbids.
        # ``model_copy`` does not re-validate, so the model's own contiguity
        # rule cannot see this; the cross-snapshot check is the only guard, and
        # it is still called here so the mutation isolates that one clause.
        candidate = ledger.model_copy(update={"entries": tuple(entries)})
        preflight_module.validate_ledger_extends(ledger, candidate)
        return candidate

    monkeypatch.setattr(SubjectPreflight, "_append", overwrite_last)
    with pytest.raises(ValueError, match="cannot shrink|cannot rewrite or erase"):
        await run_once()

    # ...and with that one guard disabled the erasure goes through unnoticed,
    # which is what makes the assertion above non-vacuous.
    monkeypatch.setattr(
        preflight_module, "validate_ledger_extends", lambda _previous, _candidate: None
    )
    erased = await run_once()
    assert erased.ledger is not None
    assert len(erased.ledger.entries) == 1
    assert erased.ledger.entries[0].outcome is ResolutionOutcome.EXACT_MATCH


# ---------------------------------------------------------------------------
# M5 — the legacy guard terminating on the preflight path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m5_legacy_guard_must_not_terminate_a_preflight_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = await case_a4()
    assert baseline.result.state is RunState.COMPLETED
    # The guard did fire — it just did not decide anything.
    assert baseline.guard_reasons() == ("no_evidence_backed_claims",)

    monkeypatch.setattr(
        DevOrchestrator,
        "_legacy_guard_is_terminal",
        staticmethod(lambda _preflight: True),
    )
    mutated = await case_a4()

    assert mutated.result.state is RunState.INSUFFICIENT_EVIDENCE


# ---------------------------------------------------------------------------
# M6 — the constrained classifier authoring an entity name
# ---------------------------------------------------------------------------


class _FabricatingClassifier:
    """Proposes a span that is not in the question at all."""

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


@pytest.mark.asyncio
async def test_m6_a_span_absent_from_the_question_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    question = "How are things looking?"
    interpreter = QuestionInterpreter(
        classifier=_FabricatingClassifier("Nightfall"),
        mint_id=sequential_ids(),
        now=fixed_now,
    )
    baseline = await interpreter.interpret(request_for(question))

    # Rejected: the model cannot introduce a subject the user never named.
    assert baseline.mentions == ()
    assert "fallback.rejected" in baseline.intent.interpretation_reasons

    original = QuestionInterpreter._validate_proposal

    def without_substring_check(proposal: ClassifierProposal, *, question: str) -> Any:
        return original(proposal, question=question + proposal.candidates[0][0])

    monkeypatch.setattr(
        QuestionInterpreter,
        "_validate_proposal",
        staticmethod(without_substring_check),
    )
    mutated = await QuestionInterpreter(
        classifier=_FabricatingClassifier("Nightfall"),
        mint_id=sequential_ids(),
        now=fixed_now,
    ).interpret(request_for(question))

    assert [mention.original_text_span for mention in mutated.mentions] == ["Nightfall"]


@pytest.mark.asyncio
async def test_m6_an_in_question_span_is_accepted() -> None:
    """Anti-vacuity: the post-validation must not reject everything."""

    question = "How is Nightfall looking?"
    interpreted = await QuestionInterpreter(
        classifier=_FabricatingClassifier("Nightfall"),
        mint_id=sequential_ids(),
        now=fixed_now,
    ).interpret(request_for(question))

    assert [mention.original_text_span for mention in interpreted.mentions] == [
        "Nightfall"
    ]


# ---------------------------------------------------------------------------
# M7 — the interpreter planning from the client's question_class
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m7_intent_is_independent_of_the_client_question_class_hint() -> None:
    question = "What's the status of the Ask Dev project?"
    intents = set()
    for question_class in QuestionClass:
        interpreted = await QuestionInterpreter(
            mint_id=sequential_ids(), now=fixed_now
        ).interpret(request_for(question, question_class=question_class))
        intents.add(interpreted.intent.intent_id)
        # Recorded for audit, with its mandatory deprecation warning.
        assert interpreted.intent.client_question_class_hint is question_class
        assert interpreted.intent.client_hint_deprecation_warning is not None

    assert len(intents) == 1


@pytest.mark.asyncio
async def test_m7_a_hint_reading_interpreter_is_caught_by_that_invariant() -> None:
    """The fail-before half: the invariant above can actually fail.

    A real interpreter that plans from the client's ``question_class`` — the
    thing the Wave 3.1 request amendment forbids — is run through the exact
    same loop, and produces a different intent per hint.
    """

    from dev_health_ops.api.dev.contracts_v2 import QuestionIntentID
    from dev_health_ops.api.dev.question_interpreter import InterpretedQuestion

    hint_to_intent = {
        QuestionClass.STATUS: QuestionIntentID.ENTITY_STATUS,
        QuestionClass.REMAINING_WORK: QuestionIntentID.REMAINING_WORK,
        QuestionClass.OBSERVED_CHANGE: QuestionIntentID.OBSERVED_CHANGE,
        QuestionClass.REGISTERED_STATISTICS: QuestionIntentID.REGISTERED_STATISTICS,
        QuestionClass.DATA_TRUST: QuestionIntentID.DATA_TRUST,
        QuestionClass.INVESTIGATION: QuestionIntentID.BOUNDED_INVESTIGATION,
    }

    class HintPlanningInterpreter(QuestionInterpreter):
        async def interpret(self, request: Any) -> InterpretedQuestion:
            interpreted = await super().interpret(request)
            return InterpretedQuestion(
                intent=interpreted.intent.model_copy(
                    update={"intent_id": hint_to_intent[request.question_class]}
                ),
                mentions=interpreted.mentions,
            )

    question = "What's the status of the Ask Dev project?"
    intents = set()
    for question_class in QuestionClass:
        interpreted = await HintPlanningInterpreter(
            mint_id=sequential_ids(), now=fixed_now
        ).interpret(request_for(question, question_class=question_class))
        intents.add(interpreted.intent.intent_id)

    assert len(intents) > 1


# ---------------------------------------------------------------------------
# M8 — the tool-availability seam restoring a withheld tool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m8_manifest_ignoring_the_allowlist_readvertises_resolve_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = await case_a1()
    assert baseline.provider is not None
    for advertised in baseline.provider.tool_ids:
        assert ToolID.RESOLVE_SCOPE.value not in advertised

    original = AskDevToolRegistry.manifest

    def unfiltered(
        self: AskDevToolRegistry, *, allowed_tool_ids: Any = None
    ) -> dict[str, object]:
        del allowed_tool_ids
        return original(self)

    monkeypatch.setattr(AskDevToolRegistry, "manifest", unfiltered)
    mutated = await case_a1()

    assert mutated.provider is not None
    assert any(
        ToolID.RESOLVE_SCOPE.value in advertised
        for advertised in mutated.provider.tool_ids
    )
    # Defense in depth: advertising it does not make it executable.
    assert "resolve_scope.v1" not in [
        request.tool_id.value for request in mutated.calls
    ]


# ---------------------------------------------------------------------------
# M9 — resolve_mention's catalog wrapper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m9_removing_the_catalog_wrapper_turns_an_outage_into_internal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = await case_a9()
    assert baseline.result.error is not None
    assert baseline.result.error.code == "source_unavailable"
    assert baseline.result.error.retryable is True

    async def unwrapped(
        self: ScopeResolutionService,
        org_id: str,
        permission_fingerprint: str,
        *,
        lookup_text: str,
        kinds: Any,
        exact: bool = False,
    ) -> MentionResolution:
        del exact
        # The pre-CHAOS-3292 shape: search() has no exception handling of its
        # own, so a catalog failure escapes to the orchestrator's generic
        # handler and is reported as an opaque server fault.
        result = await self.search(
            org_id,
            permission_fingerprint,
            ScopeSearchRequest(query=lookup_text[:256], kinds=tuple(kinds)),
        )
        return MentionResolution(
            outcome=(
                ResolutionOutcome.EXACT_MATCH
                if len(result.candidates) == 1
                else ResolutionOutcome.NO_AUTHORIZED_MATCH
            ),
            entity=result.candidates[0] if len(result.candidates) == 1 else None,
            candidates=(),
            catalog_watermark=result.catalog_watermark,
            query_version=result.query_version,
        )

    monkeypatch.setattr(ScopeResolutionService, "resolve_mention", unwrapped)
    mutated = await case_a9()

    assert mutated.result.error is not None
    assert mutated.result.error.code == "internal_error"
    assert mutated.result.error.retryable is False


@pytest.mark.asyncio
async def test_m9_catalog_unavailable_is_reachable_at_the_service_level() -> None:
    """Directly, without the orchestrator, so the typing is not inferred."""

    service = ScopeResolutionService(
        SeededCatalog([(ORG_ID, ASK_DEV_PROJECT)], fail_search=True),
        cache=ScopeRequestCache(),
    )
    from dev_health_ops.api.dev.scope_service import EntityKind

    resolution = await service.resolve_mention(
        ORG_ID,
        "permissions_01",
        lookup_text="Ask Dev",
        kinds=(EntityKind.PROJECT,),
    )
    assert resolution.outcome is ResolutionOutcome.CATALOG_UNAVAILABLE

    healthy = ScopeResolutionService(
        SeededCatalog([(ORG_ID, ASK_DEV_PROJECT)]), cache=ScopeRequestCache()
    )
    assert (
        await healthy.resolve_mention(
            ORG_ID,
            "permissions_01",
            lookup_text="Ask Dev",
            kinds=(EntityKind.PROJECT,),
        )
    ).outcome is ResolutionOutcome.EXACT_MATCH


@pytest.mark.asyncio
async def test_cross_tenant_never_reports_forbidden() -> None:
    """A8's other half, at the service level: cross-tenant is not-found."""

    from dev_health_ops.api.dev.scope_service import EntityKind

    service = ScopeResolutionService(
        SeededCatalog([(OTHER_ORG_ID, NIGHTFALL_PROJECT)]), cache=ScopeRequestCache()
    )
    resolution = await service.resolve_mention(
        ORG_ID,
        "permissions_01",
        lookup_text="Nightfall",
        kinds=(EntityKind.PROJECT,),
    )
    assert resolution.outcome is ResolutionOutcome.NO_AUTHORIZED_MATCH
    assert resolution.candidates == ()
