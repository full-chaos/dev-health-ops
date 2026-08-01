"""Unit coverage for the subject preflight and its outcome mapping (CHAOS-3292)."""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import ToolID
from dev_health_ops.api.dev.contracts_v2 import (
    PublicOutcome,
    QuestionIntentID,
    ResolutionOutcome,
)
from dev_health_ops.api.dev.contracts_v2.plan import PLAN_REGISTRY
from dev_health_ops.api.dev.contracts_v2.subject import UNRESOLVED_OUTCOMES
from dev_health_ops.api.dev.orchestrator_states import TERMINAL_STATES
from dev_health_ops.api.dev.preflight_outcomes import (
    PLAN_ID_BY_INTENT,
    PREFLIGHT_OUTCOME_BY_RESOLUTION,
    TERMINAL_STATE_BY_OUTCOME,
    build_preflight_answer,
    project_preflight_error,
)
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_service import (
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import (
    SUBJECT_BEARING_TOOLS,
    PreflightDecision,
    SubjectPreflight,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    NIGHTFALL_PROJECT,
    ORG_ID,
    PLATFORM_TEAM,
    SeededCatalog,
    fixed_now,
    request_for,
    sequential_ids,
    versions,
)


def _preflight(entities: Any, **catalog_kwargs: Any) -> SubjectPreflight:
    mint = sequential_ids()
    return SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog(entities, **catalog_kwargs), cache=ScopeRequestCache()
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )


async def _run(preflight: SubjectPreflight, request: Any, **kwargs: Any) -> Any:
    return await preflight.run(
        request=request,
        org_id=kwargs.pop("org_id", ORG_ID),
        permission_fingerprint="permissions_01",
        authorized_scope=kwargs.pop("authorized_scope", request.scope),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_single_exact_subject_commits_and_withholds_resolve_scope() -> None:
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("What's the status of the Ask Dev project?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.committed_resolution is not None
    assert result.allowed_tools == frozenset(ToolID) - {ToolID.RESOLVE_SCOPE}
    assert SUBJECT_BEARING_TOOLS <= result.allowed_tools
    assert result.all_subjects_committed is True


@pytest.mark.asyncio
async def test_an_organization_wide_question_keeps_every_tool() -> None:
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("How are we doing overall?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.ledger is None
    assert result.allowed_tools == frozenset(ToolID)


@pytest.mark.asyncio
async def test_a_resolved_cohort_is_unsupported_not_partially_committed() -> None:
    """Two real subjects: v1 ``DevScope`` names one, so neither is guessed.

    Committing only the first of several named subjects is the fabricated
    premise this issue exists to close; the landed v2-to-v1 projector reaches
    the same conclusion independently for a ``subject_set_ref`` cohort frame.
    CHAOS-3301 owns subject sets and batch execution.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)]),
        request_for("Compare project Ask Dev and project Nightfall"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.UNSUPPORTED
    assert result.diagnostic == "cohort_unsupported_in_v1"
    assert result.committed_resolution is None
    # Both resolved honestly; nothing was discarded to make one fit.
    assert result.ledger is not None
    assert all(
        entry.outcome is ResolutionOutcome.EXACT_MATCH
        for entry in result.ledger.latest_by_mention().values()
    )


@pytest.mark.asyncio
async def test_the_lowest_ordinal_unresolved_mention_decides_the_outcome() -> None:
    """Stable and explainable — and independent of catalog latency.

    A severity ordering would let a slow catalog change the reported outcome
    between runs, which breaks the determinism criterion directly.
    """

    # Ordinal 0 is ambiguous (two Atlas), ordinal 1 does not exist at all.
    from tests._chaos_3292_preflight import ATLAS_PROJECT_ONE, ATLAS_PROJECT_TWO

    result = await _run(
        _preflight([(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)]),
        request_for("Compare project Atlas and project Nightfall"),
    )
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION

    reversed_result = await _run(
        _preflight([(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)]),
        request_for("Compare project Nightfall and project Atlas"),
    )
    assert reversed_result.outcome is PublicOutcome.NOT_FOUND


# ---------------------------------------------------------------------------
# The catalog-outage reuse rule
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_catalog_outage_reuses_only_an_exact_prior_commit() -> None:
    project_ref = {
        "entity_type": "project",
        "entity_id": ASK_DEV_PROJECT.canonical_id,
        "display_label": ASK_DEV_PROJECT.label,
        "repository_id": None,
    }
    request = request_for(
        "What's the status of the Ask Dev project?",
        scope_overrides={"direct_scope": "project", "entity_refs": [project_ref]},
    )
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)], fail_search=True), request
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.committed_resolution is not None


@pytest.mark.asyncio
async def test_a_catalog_outage_without_a_matching_commit_is_unavailable() -> None:
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)], fail_search=True),
        request_for("What's the status of the Ask Dev project?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.TEMPORARILY_UNAVAILABLE


@pytest.mark.asyncio
async def test_an_outage_never_reuses_a_scope_for_a_different_subject() -> None:
    """A stale guess about a *different* named subject is the same defect."""

    other_ref = {
        "entity_type": "project",
        "entity_id": NIGHTFALL_PROJECT.canonical_id,
        "display_label": NIGHTFALL_PROJECT.label,
        "repository_id": None,
    }
    request = request_for(
        "What's the status of the Ask Dev project?",
        scope_overrides={"direct_scope": "project", "entity_refs": [other_ref]},
    )
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)], fail_search=True), request
    )

    assert result.outcome is PublicOutcome.TEMPORARILY_UNAVAILABLE


# ---------------------------------------------------------------------------
# Outcome mapping and the emitted v2 objects
# ---------------------------------------------------------------------------


def test_every_unresolved_outcome_has_a_public_mapping() -> None:
    assert set(PREFLIGHT_OUTCOME_BY_RESOLUTION) == set(UNRESOLVED_OUTCOMES)
    # EXACT_MATCH is deliberately absent: it is not a termination.
    assert ResolutionOutcome.EXACT_MATCH not in PREFLIGHT_OUTCOME_BY_RESOLUTION


def test_every_public_outcome_maps_to_a_terminal_run_state() -> None:
    assert set(TERMINAL_STATE_BY_OUTCOME) >= set(
        PREFLIGHT_OUTCOME_BY_RESOLUTION.values()
    )
    for state in TERMINAL_STATE_BY_OUTCOME.values():
        assert state in TERMINAL_STATES


def test_every_launch_intent_maps_to_a_registered_plan() -> None:
    assert set(PLAN_ID_BY_INTENT) == set(QuestionIntentID)
    assert set(PLAN_ID_BY_INTENT.values()) <= PLAN_REGISTRY


@pytest.mark.parametrize(
    ("outcome", "expected_code", "expected_retryable"),
    [
        (PublicOutcome.NOT_FOUND, "scope_not_found", False),
        (PublicOutcome.TEMPORARILY_UNAVAILABLE, "source_unavailable", True),
        (PublicOutcome.UNSUPPORTED, "feature_not_enabled", False),
        (PublicOutcome.NEEDS_CLARIFICATION, "scope_ambiguous", False),
    ],
)
def test_each_outcome_projects_to_its_v1_error(
    outcome: PublicOutcome, expected_code: str, expected_retryable: bool
) -> None:
    answer = build_preflight_answer(
        outcome=outcome,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
    )
    error = project_preflight_error(answer, request_id="request_01")

    assert error.code == expected_code
    assert error.retryable is expected_retryable
    assert error.request_id == "request_01"


def test_a_no_answer_frame_carries_no_provenance_block_or_subject() -> None:
    answer = build_preflight_answer(
        outcome=PublicOutcome.NOT_FOUND,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
    )

    assert answer.frame.versions is None
    assert answer.frame.subject_ref is None
    assert answer.frame.subject_set_ref is None
    assert answer.frame.facts == ()
    assert answer.narrative is None


def test_a_needs_clarification_frame_does_carry_provenance() -> None:
    """It is not one of ``NO_ANSWER_OUTCOMES``, so the contract requires one."""

    answer = build_preflight_answer(
        outcome=PublicOutcome.NEEDS_CLARIFICATION,
        intent_id=QuestionIntentID.PROJECT_HEALTH,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
    )

    assert answer.frame.versions is not None
    assert answer.frame.versions.plan_id == "health.project.v1"
    assert answer.frame.versions.interpreter_version == "intent_interpreter.v1"


def test_correlation_handles_are_canonical_server_handles() -> None:
    """Non-UUID run ids are folded, never rejected and never rendered as text."""

    import re

    answer = build_preflight_answer(
        outcome=PublicOutcome.NOT_FOUND,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
    )
    grammar = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    for handle in (
        answer.run_id,
        answer.answer_id,
        answer.conversation_id,
        answer.frame.frame_id,
    ):
        assert grammar.match(handle)
    # Deterministic: the same id folds to the same handle every time.
    again = build_preflight_answer(
        outcome=PublicOutcome.NOT_FOUND,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
    )
    assert again.run_id == answer.run_id


def test_a_real_uuid_run_id_is_preserved_verbatim() -> None:
    run_id = "3f2a1c88-9d4e-4b71-8a2f-0c5e7d6b1a90"
    answer = build_preflight_answer(
        outcome=PublicOutcome.NOT_FOUND,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id=run_id,
        answer_id=run_id,
        conversation_id=run_id,
        generated_at=fixed_now(),
    )
    assert answer.run_id == run_id


@pytest.mark.asyncio
async def test_a_team_subject_never_reaches_the_v1_scope_constructors() -> None:
    result = await _run(
        _preflight([(ORG_ID, PLATFORM_TEAM)]),
        request_for("How is the Platform team doing?"),
    )

    assert result.outcome is PublicOutcome.UNSUPPORTED
    assert result.committed_resolution is None
    assert result.ledger is not None
    entry = next(iter(result.ledger.latest_by_mention().values()))
    assert entry.outcome is ResolutionOutcome.EXACT_MATCH
    assert entry.team_attribution == PLATFORM_TEAM.canonical_id
