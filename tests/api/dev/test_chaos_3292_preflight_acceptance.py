"""CHAOS-3292 acceptance harness A1-A13.

Every case asserts the *state the system exists to reach* — which tool actually
received which scope, and which tools were never called at all — rather than
that a code path ran. A2/A3/A9 in particular assert against a recorded call
list, never against the absence of an exception: a run that crashed before
dispatch would satisfy "no exception" while proving nothing.

Anti-vacuity guards are asserted explicitly (``test_harness_is_not_vacuous``):
the registry really does register every subject-bearing tool, and every
determinism loop really did run twenty iterations.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import DirectScope, QuestionClass, ToolID
from dev_health_ops.api.dev.contracts_v2 import ResolutionOutcome
from dev_health_ops.api.dev.contracts_v2.validators import scan_public_text
from dev_health_ops.api.dev.orchestrator import DevOrchestrator
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.subject_preflight import SUBJECT_BEARING_TOOLS
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    NIGHTFALL_PROJECT,
    ORG_ID,
    OTHER_ORG_ID,
    PAGE_PROJECT,
    PLATFORM_TEAM,
    RunOutput,
    answer_payload,
    case_a1,
    case_a2,
    case_a3,
    case_a4,
    case_a6_fake_then_real,
    case_a6_real_then_fake,
    case_a8,
    case_a9,
    case_a10,
    case_a11,
    run_preflight_orchestrator,
)

DETERMINISM_ITERATIONS = 20


def _subject_bearing_calls(output: RunOutput) -> list[str]:
    return [
        request.tool_id.value
        for request in output.calls
        if request.tool_id in SUBJECT_BEARING_TOOLS
    ]


def _public_strings(output: RunOutput) -> list[str]:
    values: list[str] = []
    if output.result.error is not None:
        values.append(output.result.error.safe_message)
        values.append(output.result.error.code)
        values.extend(output.result.error.remediation)
    if output.result.answer is not None:
        values.append(output.result.answer.direct_summary)
        values.extend(claim.text for claim in output.result.answer.claims)
        values.extend(output.result.answer.warnings)
    return values


async def _twenty(coro_factory: Any) -> list[tuple[Any, ...]]:
    results = [(await coro_factory()).outcome_tuple() for _ in range(20)]
    # Rule 4: a measurement that did not happen must fail loudly. An empty or
    # short list would otherwise make `len(set(results)) == 1` vacuously true.
    assert len(results) == DETERMINISM_ITERATIONS
    return results


# ---------------------------------------------------------------------------
# A1 — a known real project commits before any subject-bearing tool runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a1_known_project_commits_scope_before_the_status_tool() -> None:
    output = await case_a1()

    assert output.result.state is RunState.COMPLETED
    assert [request.tool_id.value for request in output.calls] == ["status_snapshot.v1"]
    status_call = output.calls[0]
    assert status_call.scope.direct_scope is DirectScope.PROJECT
    assert [ref.entity_id for ref in status_call.scope.entity_refs] == [
        ASK_DEV_PROJECT.canonical_id
    ]
    # The commit is recorded before the first tool dispatch, not after it.
    assert output.preflight_outcomes() == ("proceeded_committed_subject",)
    assert output.recorder is not None
    assert output.recorder.transitions.index(
        RunState.RESOLVING_SUBJECTS
    ) < output.recorder.transitions.index(RunState.TOOL_EXECUTION)


@pytest.mark.asyncio
async def test_a1_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_a1)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# A2 — a nonexistent target executes nothing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a2_unknown_target_is_not_found_and_runs_no_subject_tool() -> None:
    output = await case_a2()

    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert output.result.error.retryable is False
    assert _subject_bearing_calls(output) == []
    assert output.calls == []
    assert output.provider is not None
    # Not one provider round either: the preflight spends zero model tokens.
    assert output.provider.tool_ids == []


@pytest.mark.asyncio
async def test_a2_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_a2)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# A3 — an ambiguous target asks for clarification and carries both candidates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a3_ambiguous_target_needs_clarification_with_candidates() -> None:
    output = await case_a3()

    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.error is not None
    assert output.result.error.code == "scope_ambiguous"
    assert _subject_bearing_calls(output) == []


@pytest.mark.asyncio
async def test_a3_ledger_records_both_authorized_candidates() -> None:
    from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
    from dev_health_ops.api.dev.scope_service import (
        ScopeRequestCache,
        ScopeResolutionService,
    )
    from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
    from tests._chaos_3292_preflight import (
        SeededCatalog,
        fixed_now,
        request_for,
        sequential_ids,
        versions,
    )

    mint = sequential_ids()
    catalog = SeededCatalog([(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)])
    preflight = SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(catalog, cache=ScopeRequestCache()),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )
    request = request_for("What's the status of the Atlas project?")
    result = await preflight.run(
        request=request,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        authorized_scope=request.scope,
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
    )

    assert result.ledger is not None
    entry = next(iter(result.ledger.latest_by_mention().values()))
    assert entry.outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES
    assert sorted(candidate.entity_ref.entity_id for candidate in entry.candidates) == [
        ATLAS_PROJECT_ONE.canonical_id,
        ATLAS_PROJECT_TWO.canonical_id,
    ]


# ---------------------------------------------------------------------------
# A4 — an organization-wide question is not blocked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a4_organization_wide_question_executes_normally() -> None:
    output = await case_a4()

    assert output.result.state is RunState.COMPLETED
    assert _subject_bearing_calls(output) == ["status_snapshot.v1"]
    assert output.calls[0].scope.direct_scope is DirectScope.ORGANIZATION
    assert output.preflight_outcomes()[0] == "proceeded_organization_wide"
    # With nothing committed, resolve_scope.v1 stays available: a name the
    # extractor missed must still be resolvable by the model.
    assert output.provider is not None
    assert ToolID.RESOLVE_SCOPE.value in output.provider.tool_ids[0]


@pytest.mark.asyncio
async def test_a4_legacy_guard_fires_as_telemetry_without_changing_the_outcome() -> (
    None
):
    """The demotion, observed rather than asserted in the abstract.

    This claim-free organization-wide answer is exactly the shape the
    CHAOS-3289 backstop terminates on today. On the preflight path it still
    *fires* — TRD §10 keeps it as defense in depth — but the run completes and
    the reason is recorded as a content-free diagnostic instead of becoming
    public copy.
    """

    with_preflight = await case_a4()
    assert with_preflight.guard_reasons() == ("no_evidence_backed_claims",)
    assert with_preflight.result.state is RunState.COMPLETED

    without_preflight = await run_preflight_orchestrator(
        question="How are we doing on delivery this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="a4-off",
        preflight_enabled=False,
    )
    # Same guard, same input, opposite consequence: on the flag-off path it is
    # still the terminating check it is in production today.
    assert without_preflight.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert without_preflight.result.error is not None
    assert without_preflight.result.error.code == "insufficient_evidence"


@pytest.mark.asyncio
async def test_a4_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_a4)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# A5 — the guarantee holds for non-status named-target intents
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("question", "question_class", "metric_ids"),
    [
        ("What's left on the Nightfall project?", QuestionClass.REMAINING_WORK, ()),
        (
            "What changed in the Nightfall project since last week?",
            QuestionClass.OBSERVED_CHANGE,
            (),
        ),
        (
            "Compare items completed for the Nightfall project against last month",
            QuestionClass.INVESTIGATION,
            ("items_completed",),
        ),
        ("Is the data for the Nightfall project stale?", QuestionClass.DATA_TRUST, ()),
    ],
)
@pytest.mark.asyncio
async def test_a5_non_status_named_targets_are_gated_identically(
    question: str, question_class: QuestionClass, metric_ids: tuple[str, ...]
) -> None:
    output = await run_preflight_orchestrator(
        question=question,
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        question_class=question_class,
        requested_metric_ids=metric_ids,
        script_id="a5",
    )

    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert _subject_bearing_calls(output) == []


@pytest.mark.asyncio
async def test_a5_non_status_named_target_commits_when_it_resolves() -> None:
    output = await run_preflight_orchestrator(
        question="What's left on the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        question_class=QuestionClass.REMAINING_WORK,
        script_id="a5-ok",
    )

    assert output.result.state is RunState.COMPLETED
    assert output.calls[0].scope.direct_scope is DirectScope.PROJECT


# ---------------------------------------------------------------------------
# A6 — multi-mention, both orders
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case",
    [case_a6_real_then_fake, case_a6_fake_then_real],
    ids=["real_first", "fake_first"],
)
@pytest.mark.asyncio
async def test_a6_one_unresolved_mention_blocks_regardless_of_order(
    case: Any,
) -> None:
    output = await case()

    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert _subject_bearing_calls(output) == []


@pytest.mark.asyncio
async def test_a6_ledger_records_every_mention_including_the_resolved_one() -> None:
    from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
    from dev_health_ops.api.dev.scope_service import (
        ScopeRequestCache,
        ScopeResolutionService,
    )
    from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
    from tests._chaos_3292_preflight import (
        SeededCatalog,
        fixed_now,
        request_for,
        sequential_ids,
        versions,
    )

    mint = sequential_ids()
    preflight = SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog([(ORG_ID, ASK_DEV_PROJECT)]), cache=ScopeRequestCache()
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )
    request = request_for("Compare project Ask Dev and project Nightfall")
    result = await preflight.run(
        request=request,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        authorized_scope=request.scope,
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
    )

    assert result.ledger is not None
    assert len(result.ledger.entries) >= 2
    outcomes = {entry.outcome for entry in result.ledger.latest_by_mention().values()}
    assert outcomes == {
        ResolutionOutcome.EXACT_MATCH,
        ResolutionOutcome.NO_AUTHORIZED_MATCH,
    }


# ---------------------------------------------------------------------------
# A7 — the ledger appends across a retry, never overwrites
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a7_ledger_append_across_a_context_tiebreak_extends_the_snapshot() -> (
    None
):
    from dev_health_ops.api.dev.contracts_v2 import (
        DevResolutionLedger,
        validate_ledger_extends,
    )
    from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
    from dev_health_ops.api.dev.scope_service import (
        ScopeRequestCache,
        ScopeResolutionService,
    )
    from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
    from tests._chaos_3292_preflight import (
        SeededCatalog,
        fixed_now,
        request_for,
        sequential_ids,
        versions,
    )

    mint = sequential_ids()
    preflight = SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog([(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)]),
            cache=ScopeRequestCache(),
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )
    # The page context names one of the two Atlas projects, so the ambiguity is
    # broken by a *second appended entry*, never by rewriting the first.
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
    result = await preflight.run(
        request=request,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        authorized_scope=request.scope,
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
    )

    assert result.ledger is not None
    assert len(result.ledger.entries) == 2
    assert result.ledger.entries[0].outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES
    assert result.ledger.entries[1].outcome is ResolutionOutcome.EXACT_MATCH
    # The earlier unresolved entry survives verbatim.
    first_snapshot = DevResolutionLedger(
        schema_version="dev_resolution_ledger.v1",
        ledger_id=result.ledger.ledger_id,
        mention_ids=result.ledger.mention_ids,
        entries=result.ledger.entries[:1],
        updated_at=result.ledger.updated_at,
    )
    validate_ledger_extends(first_snapshot, result.ledger)


@pytest.mark.asyncio
async def test_a7_a_ledger_that_erases_an_entry_is_rejected() -> None:
    """The fail-before half: prove the append-only check can actually fail."""

    from dev_health_ops.api.dev.contracts_v2 import (
        DevResolutionEntry,
        DevResolutionLedger,
        validate_ledger_extends,
    )
    from tests._chaos_3292_preflight import fixed_now

    def entry(ordinal: int, outcome: ResolutionOutcome) -> DevResolutionEntry:
        return DevResolutionEntry(
            entry_ordinal=ordinal,
            mention_id="00000000-0000-0000-0000-000000000001",
            outcome=outcome,
            committed_entity_ref=(
                {
                    "entity_kind": "project",
                    "entity_id": ASK_DEV_PROJECT.canonical_id,
                    "display_label": "Ask Dev",
                }
                if outcome is ResolutionOutcome.EXACT_MATCH
                else None
            ),
            resolver_version="resolve-scope.v1",
            query_version="watermark-1",
            resolved_at=fixed_now(),
        )

    def ledger(*entries: DevResolutionEntry) -> DevResolutionLedger:
        return DevResolutionLedger(
            schema_version="dev_resolution_ledger.v1",
            ledger_id="00000000-0000-0000-0000-0000000000ff",
            mention_ids=("00000000-0000-0000-0000-000000000001",),
            entries=entries,
            updated_at=fixed_now(),
        )

    previous = ledger(entry(0, ResolutionOutcome.NO_AUTHORIZED_MATCH))
    erased = ledger(entry(0, ResolutionOutcome.EXACT_MATCH))
    with pytest.raises(ValueError, match="cannot rewrite or erase"):
        validate_ledger_extends(previous, erased)


# ---------------------------------------------------------------------------
# A8 — cross-tenant reads as not-found and discloses nothing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a8_cross_tenant_subject_is_not_found_and_leaks_nothing() -> None:
    output = await case_a8()

    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert _subject_bearing_calls(output) == []
    for value in _public_strings(output):
        assert scan_public_text(value) == []
        assert NIGHTFALL_PROJECT.label.casefold() not in value.casefold()
        assert NIGHTFALL_PROJECT.canonical_id.casefold() not in value.casefold()


@pytest.mark.asyncio
async def test_a8_the_same_subject_resolves_for_its_own_tenant() -> None:
    """Anti-vacuity: A8 would pass on a broken catalog that finds nothing."""

    output = await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(OTHER_ORG_ID, NIGHTFALL_PROJECT)],
        org_id=OTHER_ORG_ID,
        script_id="a8-owner",
    )

    assert output.result.state is RunState.COMPLETED
    assert output.calls[0].scope.direct_scope is DirectScope.PROJECT


# ---------------------------------------------------------------------------
# A9 — a catalog outage is typed, not an internal error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a9_catalog_outage_is_temporarily_unavailable_and_retryable() -> None:
    output = await case_a9()

    assert output.result.state is RunState.FAILED
    assert output.result.error is not None
    # Not internal_error: search() has no try/except of its own, so before
    # resolve_mention wrapped it a ClickHouse failure escaped as an exception
    # and this run reported an opaque server fault.
    assert output.result.error.code == "source_unavailable"
    assert output.result.error.retryable is True
    assert _subject_bearing_calls(output) == []


@pytest.mark.asyncio
async def test_a9_is_deterministic_across_twenty_runs() -> None:
    results = await _twenty(case_a9)
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# A10 — a stale context ref is not-found, not a silent page-scope fall-through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a10_stale_context_ref_is_not_found() -> None:
    output = await case_a10()

    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert _subject_bearing_calls(output) == []


@pytest.mark.asyncio
async def test_a10_a_live_context_ref_still_commits() -> None:
    """Anti-vacuity for A10: the context-ref path must be able to succeed."""

    output = await run_preflight_orchestrator(
        question="What is the current state?",
        entities=[(ORG_ID, PAGE_PROJECT)],
        scope_overrides={
            "entity_refs": [
                {
                    "entity_type": "project",
                    "entity_id": PAGE_PROJECT.canonical_id,
                    "display_label": PAGE_PROJECT.label,
                    "repository_id": None,
                }
            ],
            "direct_scope": "project",
        },
        script_id="a10-ok",
    )

    assert output.result.state is RunState.COMPLETED
    assert [ref.entity_id for ref in output.calls[0].scope.entity_refs] == [
        PAGE_PROJECT.canonical_id
    ]


# ---------------------------------------------------------------------------
# A11 — a TEAM subject resolves and commits (CHAOS-3301 superseded this
# section: team is now a real v1 direct scope, not an interim "resolved but
# unsupported" kind — see tests/api/dev/test_chaos_3301_controls.py's P1/N1
# for the full CHAOS-3301 control set this A11 pair now defers to).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a11_resolved_team_is_a_committed_subject_end_to_end() -> None:
    from dev_health_ops.api.dev.contracts import DirectScope

    output = await case_a11()

    assert output.result.state is RunState.COMPLETED
    assert [request.tool_id.value for request in output.calls] == ["status_snapshot.v1"]
    status_call = output.calls[0]
    assert status_call.scope.direct_scope is DirectScope.TEAM
    assert [ref.entity_id for ref in status_call.scope.entity_refs] == [
        PLATFORM_TEAM.canonical_id
    ]


@pytest.mark.asyncio
async def test_a11_team_resolution_records_an_exact_match_and_commits() -> None:
    from dev_health_ops.api.dev.contracts import DirectScope
    from dev_health_ops.api.dev.contracts_v2 import EntityKind as ContractEntityKind
    from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
    from dev_health_ops.api.dev.scope_service import (
        ScopeRequestCache,
        ScopeResolutionService,
    )
    from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
    from tests._chaos_3292_preflight import (
        SeededCatalog,
        fixed_now,
        request_for,
        sequential_ids,
        versions,
    )

    mint = sequential_ids()
    preflight = SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog([(ORG_ID, PLATFORM_TEAM)]), cache=ScopeRequestCache()
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )
    request = request_for("What is the status of the Platform team?")
    # No ValueError from DirectScope(...)/EntityType(...): CHAOS-3301 gave
    # both a real TEAM member, so the preflight's committed_resolution_for
    # now builds a genuine v1 DevScope(direct_scope=TEAM, ...) rather than
    # only recording the exact_match ledger entry.
    result = await preflight.run(
        request=request,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        authorized_scope=request.scope,
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
    )

    assert result.ledger is not None
    entry = next(iter(result.ledger.latest_by_mention().values()))
    assert entry.outcome is ResolutionOutcome.EXACT_MATCH
    assert entry.committed_entity_ref is not None
    assert entry.committed_entity_ref.entity_kind is ContractEntityKind.TEAM
    assert result.diagnostic == "proceeded_committed_subject"
    assert result.committed_resolution is not None
    committed_scope = result.committed_resolution.resolved_scope
    assert committed_scope is not None
    assert committed_scope.direct_scope is DirectScope.TEAM
    assert committed_scope.team_ids == [PLATFORM_TEAM.canonical_id]


@pytest.mark.asyncio
async def test_a11_the_graphql_searchable_set_still_rejects_team() -> None:
    """R1: widening the ceiling must not expose team search on a v1 surface."""

    from dev_health_ops.api.dev.scope_service import (
        SEARCHABLE_ENTITY_KINDS,
        EntityKind,
        ScopeSearchRequest,
    )

    with pytest.raises(ValueError, match="approved searchable V1 direct scopes"):
        ScopeSearchRequest(query="Platform", kinds=(EntityKind.TEAM,))
    # ...and the ceiling itself does admit it, so the rejection above is the
    # caller allowlist doing the work rather than the kind being unsearchable.
    assert EntityKind.TEAM in SEARCHABLE_ENTITY_KINDS
    ScopeSearchRequest(
        query="Platform",
        kinds=(EntityKind.TEAM,),
        allowed_kinds=SEARCHABLE_ENTITY_KINDS,
    )


@pytest.mark.asyncio
async def test_a11_resolve_query_contract_rejects_an_unrepresentable_kind() -> None:
    from dev_health_ops.api.dev.scope_service import (
        SEARCHABLE_ENTITY_KINDS,
        EntityKind,
        ScopeRequestCache,
        ScopeResolutionService,
        ScopeSearchRequest,
    )
    from tests._chaos_3292_preflight import SeededCatalog, request_for

    service = ScopeResolutionService(
        SeededCatalog([(ORG_ID, PLATFORM_TEAM)]), cache=ScopeRequestCache()
    )
    with pytest.raises(ValueError, match="cannot represent entity kinds"):
        await service.resolve_query_contract(
            ORG_ID,
            "permissions_01",
            ScopeSearchRequest(
                query="Platform",
                kinds=(EntityKind.TEAM,),
                allowed_kinds=SEARCHABLE_ENTITY_KINDS,
            ),
            base_scope=request_for("x").scope,
        )


# ---------------------------------------------------------------------------
# A12 — resolve_scope.v1 is withheld once a subject is committed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a12_resolve_scope_is_withheld_and_its_token_never_reaches_the_model() -> (
    None
):
    output = await case_a1()

    import json

    assert output.provider is not None
    assert output.provider.tool_ids
    for advertised in output.provider.tool_ids:
        assert ToolID.RESOLVE_SCOPE.value not in advertised
    # The system prompt's own advertised registry must agree with the provider
    # tool definitions. (Its embedded dev_tool_request.v1 JSON Schema still
    # enumerates the full closed ToolID enum — that is the wire contract, not
    # an offer, and narrowing it would be a contract change.)
    for system_text in output.provider.system_texts:
        manifest = json.loads(system_text)["tool_registry"]
        assert ToolID.RESOLVE_SCOPE.value not in {
            item["tool_id"] for item in manifest["tools"]
        }
    # And no tool-result JSON carrying the internal combined outcome reaches
    # the composed user text.
    for user_text in output.provider.user_texts:
        assert "forbidden_or_not_found" not in user_text


@pytest.mark.asyncio
async def test_a12_a_dispatched_withheld_tool_never_reaches_the_executor() -> None:
    """The second enforcement point, exercised directly.

    A model that ignores the advertised registry and asks for the withheld tool
    anyway is degraded to one failed tool result, not executed.
    """

    def script(script_id: str) -> list[ScriptedStep]:
        payload = answer_payload(script_id=script_id)
        # The withheld call becomes one failed tool result, which legitimately
        # makes the run's coverage incomplete — so the answer must not claim
        # completeness. That is the degrade working, not a workaround.
        payload["status"] = "degraded"
        return [
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id="resolve_scope.v1",
                    arguments={"query": "Ask Dev", "limit": 25},
                    call_id="tool_call_01",
                ),
                usage=AgentUsage(input_tokens=100, output_tokens=10),
            ),
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id="status_snapshot.v1",
                    arguments={"limit": 25, "include_comparison": False},
                    call_id="tool_call_02",
                ),
                usage=AgentUsage(input_tokens=100, output_tokens=10),
            ),
            ScriptedStep(decision=AgentFinalAnswer(payload)),
        ]

    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script=script,
        script_id="a12-dispatch",
    )

    assert [request.tool_id.value for request in output.calls] == ["status_snapshot.v1"]
    # Degraded per call, not fatal to the run.
    assert output.result.state is RunState.COMPLETED
    assert output.provider is not None
    assert "tool_unavailable" in output.provider.user_texts[-1]


# ---------------------------------------------------------------------------
# A13 — disabling the legacy guard does not reduce the new path's correctness
# ---------------------------------------------------------------------------

_GUARD_INDEPENDENT_CASES = (
    ("a1", case_a1),
    ("a2", case_a2),
    ("a3", case_a3),
    ("a4", case_a4),
    ("a8", case_a8),
    ("a9", case_a9),
    ("a10", case_a10),
    ("a11", case_a11),
)


@pytest.mark.parametrize(
    ("name", "case"),
    _GUARD_INDEPENDENT_CASES,
    ids=[n for n, _ in _GUARD_INDEPENDENT_CASES],
)
@pytest.mark.asyncio
async def test_a13_outcomes_are_unchanged_with_the_legacy_guard_disabled(
    name: str, case: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """What "the 3289 guard can be disabled" means operationally.

    Removal itself is blocked on the cutover issue (TRD §15 Phase D); this is
    the proof that the preflight path does not depend on it.
    """

    del name
    expected = (await case()).outcome_tuple()
    monkeypatch.setattr(
        DevOrchestrator,
        "_legacy_named_entity_guard_reason",
        staticmethod(lambda **_kwargs: None),
    )
    assert (await case()).outcome_tuple() == expected


# ---------------------------------------------------------------------------
# Anti-vacuity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_harness_is_not_vacuous() -> None:
    """A2/A3/A9 would pass trivially against a registry with no gated tool."""

    from tests._chaos_3292_preflight import recording_registry

    registry = recording_registry([])
    assert SUBJECT_BEARING_TOOLS <= set(registry._executors)
    assert SUBJECT_BEARING_TOOLS
    # And the happy path really does exercise one of them, so "no
    # subject-bearing call" is a statement about this run, not about the suite.
    output = await case_a1()
    assert _subject_bearing_calls(output) == ["status_snapshot.v1"]
