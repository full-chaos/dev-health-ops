"""Unit coverage for the subject preflight and its outcome mapping (CHAOS-3292)."""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import ToolID
from dev_health_ops.api.dev.contracts_v2 import (
    DevEntityRefV2,
    PublicOutcome,
    QuestionIntentID,
    ResolutionOutcome,
)
from dev_health_ops.api.dev.contracts_v2 import (
    EntityKind as ContractEntityKind,
)
from dev_health_ops.api.dev.contracts_v2.plan import PLAN_REGISTRY
from dev_health_ops.api.dev.contracts_v2.subject import (
    UNRESOLVED_OUTCOMES,
    DevResolutionCandidate,
)
from dev_health_ops.api.dev.orchestrator_states import TERMINAL_STATES, RunState
from dev_health_ops.api.dev.preflight_outcomes import (
    PLAN_ID_BY_INTENT,
    PREFLIGHT_OUTCOME_BY_RESOLUTION,
    TERMINAL_STATE_BY_OUTCOME,
    build_preflight_answer,
    project_preflight_error,
)
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
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
    run_preflight_orchestrator,
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
async def test_an_unresolved_bare_name_withholds_resolve_scope() -> None:
    """CHAOS-3421: unlike the names-nothing case above, this branch DID
    name something -- a bare span the catalog could not confirm under any
    kind. Model-called ``resolve_scope.v1`` on that exact name can only
    ever repeat the preflight's own failed lookup, returning the raw
    ``forbidden_or_not_found`` outcome for the model to (as the live
    incident did) echo verbatim into its own answer text. Withholding the
    tool here closes that leak channel at its source, mirroring the
    committed-subject branch's own ``ALL_TOOLS - {RESOLVE_SCOPE}`` (a
    subject already failed to resolve here just as surely as it succeeded
    there -- in neither case is there anything left for the model to
    productively resolve).
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("How is Nightfall doing?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "proceeded_unresolved_bare_name"
    assert result.legacy_guard_required is True
    assert result.allowed_tools == frozenset(ToolID) - {ToolID.RESOLVE_SCOPE}
    # Every OTHER subject-bearing tool stays available -- this withholds
    # exactly the one tool that could re-derive and leak the same failed
    # resolution, never the whole subject-bearing surface.
    assert SUBJECT_BEARING_TOOLS <= result.allowed_tools


@pytest.mark.asyncio
async def test_an_unresolved_bare_name_without_organization_noun_still_widens() -> None:
    """Regression guard for CHAOS-3574's new termination path below: an
    ordinary ambiguous bare word with no "organization"/"org" noun nearby
    must still widen exactly as before -- this is the shape the corpus case
    ``scope.outcome.organization-fallback`` pins as ``answered_with_gaps``/
    ``organization_fallback``, and the fix must not touch it.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("What's the status of Zephyr?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "proceeded_unresolved_bare_name"
    assert result.legacy_guard_required is True


@pytest.mark.asyncio
async def test_a_cross_tenant_organization_reference_terminates_not_found() -> None:
    """CHAOS-3574: ``adv.cross-tenant.organization-id``'s exact question.

    "Orbit" is the sibling tenant's real display name -- a well-formed,
    unambiguous organization-shaped mention the primary org's own catalog
    can never contain (an organization is never a searchable catalog entity
    at all). Before this fix the mention stayed untyped (no ``organization``
    kind noun exists in the closed ``_KIND_NOUNS`` table) and hit the
    generic ``unresolved_untyped`` branch, which widened to organization
    scope and went on to ANSWER — observed twice on the Phase 3 armed corpus
    run as ``answered_with_gaps`` where the corpus case allows only
    ``not_found``.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("What's the status of the Orbit organization?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NOT_FOUND
    assert result.diagnostic == "unresolved_no_authorized_match"
    # Never the widen-and-disclose path: no answer text is ever computed for
    # this run at all, so there is nothing that could narrate "Orbit".
    assert result.legacy_guard_required is False
    assert result.committed_resolution is None
    assert result.allowed_tools == frozenset()


@pytest.mark.asyncio
async def test_cross_tenant_and_nonexistent_organizations_are_indistinguishable() -> (
    None
):
    """CHAOS-3574: the security property ``no_unauthorized_candidate_surfaces``
    already protects (and both armed runs PASSED) is that a genuine sibling
    org and a name that names nothing at all must read identically to the
    requester. "Orbit" is the sibling tenant's real name; "Zzyzx" is authored
    to not exist anywhere in this fixture world. Both must terminate on the
    exact same public outcome and the exact same user-visible frame shape —
    never distinguishable by content, only by the fact that neither is ever
    found.
    """

    sibling = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("What's the status of the Orbit organization?"),
    )
    nonexistent = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("What's the status of the Zzyzx organization?"),
    )

    for result in (sibling, nonexistent):
        assert result.decision is PreflightDecision.TERMINATE
        assert result.outcome is PublicOutcome.NOT_FOUND
        assert result.answer is not None

    assert sibling.answer.public_outcome == nonexistent.answer.public_outcome
    assert (
        sibling.answer.outcome_display_label == nonexistent.answer.outcome_display_label
    )
    assert (
        sibling.answer.frame.public_outcome == nonexistent.answer.frame.public_outcome
    )
    assert sibling.answer.frame.direct_answer == nonexistent.answer.frame.direct_answer
    assert sibling.answer.frame.coverage == nonexistent.answer.frame.coverage
    assert sibling.answer.frame.versions == nonexistent.answer.frame.versions


#: CHAOS-3574 review round 2 fixtures: the reviewer's own live-repro
#: repositories, both real and both individually resolvable -- same ids
#: CHAOS-3551's own corpus case uses, for a fully-resolved, homogeneous,
#: v1-renderable repository cohort.
_REVIEW2_WEB_APP = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/web-app", "meridian/web-app"
)
_REVIEW2_API_GATEWAY = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/api-gateway", "meridian/api-gateway"
)
_REVIEW2_COHORT_BYSTANDER_QUESTION = (
    'What\'s the status of repo "meridian/web-app" and repo '
    '"meridian/api-gateway" compared to the Orbit organization?'
)


@pytest.mark.asyncio
async def test_a_bystander_organization_mention_does_not_discard_a_resolved_cohort() -> (
    None
):
    """CHAOS-3574 review round 2 (CONFIRMED, blocking): before this fix, the
    org-probe (and, underneath it, the pre-existing plain widen-to-org
    fallback) ran BEFORE the CHAOS-3551 cohort-commit/render logic and
    unconditionally decided the run's outcome the moment ANY unresolved
    untyped mention existed -- discarding a fully-resolved, two-repository
    cohort because an UNRELATED mention ("the Orbit organization") also
    happened to be in the question and could not resolve. Reproduced live by
    the reviewer with the CHAOS-3551 fixtures; both repositories are real and
    individually resolvable here too.

    The org-probe (and the general widen fallback) must only decide the
    outcome when it is LOAD-BEARING -- nothing else in the question resolved.
    A bystander must never preempt an already-resolved cohort: the cohort
    still commits and renders, and the unresolved bystander is disclosed via
    a warning instead of silently vanishing or terminating the run.
    """

    result = await _run(
        _preflight([(ORG_ID, _REVIEW2_WEB_APP), (ORG_ID, _REVIEW2_API_GATEWAY)]),
        request_for(_REVIEW2_COHORT_BYSTANDER_QUESTION),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "committed_cohort_v1_render"
    assert result.outcome is None
    assert result.committed_resolution is not None
    assert result.allowed_tools == frozenset(ToolID) - {ToolID.RESOLVE_SCOPE}
    assert SUBJECT_BEARING_TOOLS <= result.allowed_tools
    # `all_subjects_committed` must read True for the two repositories that
    # DID resolve -- the bystander must never gate subject-bearing tools shut
    # for a run that has a real, committed subject.
    assert result.all_subjects_committed is True
    assert result.subject_set is not None
    committed_ids = {ref.entity_id for ref in result.subject_set.committed_entity_refs}
    assert committed_ids == {"meridian/web-app", "meridian/api-gateway"}
    # The cohort itself is complete (both its own members resolved) --
    # the bystander is not one of ITS members, so it must not make an
    # otherwise-complete repository cohort read as partial.
    assert result.subject_set.cohort_complete is True
    assert result.subject_set.unresolved_mention_ids == ()
    # Disclosed, not vanished.
    assert any(w for w in result.subject_set.warnings), (
        "the unresolved bystander mention must be disclosed via a warning, "
        "not silently dropped"
    )


@pytest.mark.asyncio
async def test_a_bystander_organization_mention_does_not_discard_a_cohort_end_to_end() -> (
    None
):
    """CHAOS-3574 review round 2, driven through the REAL
    ``DevOrchestrator.run()`` (not just ``SubjectPreflight`` in isolation),
    mirroring CHAOS-3551's own end-to-end proof: "PROCEED" alone does not
    show the model round loop actually executes a subject-bearing tool
    against the committed cohort and returns a real answer.

    Before this fix: TERMINATE not_found, no model round, no answer.
    """

    output = await run_preflight_orchestrator(
        question=_REVIEW2_COHORT_BYSTANDER_QUESTION,
        entities=[(ORG_ID, _REVIEW2_WEB_APP), (ORG_ID, _REVIEW2_API_GATEWAY)],
        script_id="chaos-3574-bystander-cohort-render",
    )

    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [
        ("committed_cohort_v1_render", None)
    ]
    assert output.result.state is RunState.COMPLETED
    assert output.result.error is None, (
        f"a resolved cohort beside an unrelated unresolvable bystander must "
        f"not refuse -- got {output.result.error!r}"
    )
    assert output.result.answer is not None
    # The model round loop actually ran a subject-bearing tool against the
    # committed scope, rather than the commit being decorative.
    assert [call.tool_id for call in output.calls] == [ToolID.STATUS_SNAPSHOT]
    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.resolved_scope is not None
    assert sorted(resolution.resolved_scope.repositories) == [
        "meridian/api-gateway",
        "meridian/web-app",
    ]


@pytest.mark.asyncio
async def test_a_bystander_organization_mention_does_not_discard_a_singular_subject() -> (
    None
):
    """The same LOAD-BEARING gate, for the singular-commit path: ONE real,
    resolved project beside an unresolved organization-adjacent bystander
    must still PROCEED and commit the real subject, not widen or terminate.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for(
            "What's the status of the Ask Dev project compared to the "
            "Orbit organization?"
        ),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "proceeded_committed_subject"
    assert result.committed_resolution is not None
    assert result.all_subjects_committed is True
    assert result.subject_set is not None
    assert any(w for w in result.subject_set.warnings), (
        "the unresolved bystander mention must be disclosed via a warning, "
        "not silently dropped"
    )


@pytest.mark.asyncio
async def test_an_organization_idiom_is_not_mistaken_for_a_named_organization() -> None:
    """CHAOS-3574 review round 2 (CONFIRMED, blocking): the trailing-noun
    regex alone, with no guard on what follows "organization"/"org", fired on
    attributive/idiomatic uses -- "the Atlas organization chart" is not a
    claim that an organization named Atlas exists, "chart" is the actual
    noun being named and "organization" is a modifier. Before this fix this
    reached TERMINATE not_found exactly like a genuine cross-tenant probe;
    after, it falls back to the same graceful ``proceeded_unresolved_bare_name``
    path a plain ambiguous bare word like "Zephyr"/"DORA" already gets --
    "Atlas" is still extracted as an ordinary untyped bare name (unaffected),
    it is only no longer treated as unambiguously naming an organization.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for("What's the status of the Atlas organization chart?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "proceeded_unresolved_bare_name"
    assert result.legacy_guard_required is True


@pytest.mark.asyncio
async def test_a_resolved_cohort_is_unsupported_not_partially_committed() -> None:
    """Two real subjects: v1 ``DevScope`` names one, so neither is guessed.

    Committing only the first of several named subjects is the fabricated
    premise this issue exists to close; the landed v2-to-v1 projector reaches
    the same conclusion independently for a ``subject_set_ref`` cohort frame.

    CHAOS-3301 (D1): the cohort is now genuinely *committed and persisted* as
    a ``dev_subject_set.v1`` — diagnostic ``committed_cohort_v1_only``, not
    ``cohort_unsupported_in_v1`` — but the v1 surface still returns
    ``unsupported`` and ``committed_resolution`` (the single-scope binding)
    stays ``None``: rendering a cohort answer is CHAOS-3297/3298's job.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)]),
        request_for("Compare project Ask Dev and project Nightfall"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.UNSUPPORTED
    assert result.diagnostic == "committed_cohort_v1_only"
    assert result.committed_resolution is None
    # Both resolved honestly; nothing was discarded to make one fit.
    assert result.ledger is not None
    assert all(
        entry.outcome is ResolutionOutcome.EXACT_MATCH
        for entry in result.ledger.latest_by_mention().values()
    )
    # The set itself commits every resolved member, not just the first.
    assert result.subject_set is not None
    assert result.subject_set.original_mention_count == 2
    assert result.subject_set.cohort_complete is True
    assert result.subject_set.unresolved_mention_ids == ()
    committed_ids = {ref.entity_id for ref in result.subject_set.committed_entity_refs}
    assert committed_ids == {
        ASK_DEV_PROJECT.canonical_id,
        NIGHTFALL_PROJECT.canonical_id,
    }


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


def _candidate(
    display_label: str, entity_id: str = "project-acr"
) -> DevResolutionCandidate:
    return DevResolutionCandidate(
        entity_ref=DevEntityRefV2(
            entity_kind=ContractEntityKind.PROJECT,
            entity_id=entity_id,
            display_label=display_label,
            repository_id=None,
        ),
        reason="Closest authorized match for the named subject.",
    )


@pytest.mark.asyncio
async def test_a_needs_clarification_v1_error_names_its_real_candidates() -> None:
    """CHAOS-3388: the v1 wire must not discard the frame's own candidates.

    Before this fix ``project_preflight_error`` always returned the fixed
    "The requested scope is ambiguous." sentence for ``needs_clarification``,
    whatever ``clarification_candidates`` the ledger actually authorized --
    the client-visible half of the wrong-terminal defect: a run that found a
    real close match still told the user nothing more than "ambiguous".
    """

    answer = build_preflight_answer(
        outcome=PublicOutcome.NEEDS_CLARIFICATION,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
        clarification_key="not_found_close_matches",
        clarification_candidates=(
            _candidate("Dev Health Agent Context Runtime (Context Fabric)"),
        ),
    )
    error = project_preflight_error(answer, request_id="request_01")

    assert error.code == "scope_ambiguous"
    assert "Dev Health Agent Context Runtime (Context Fabric)" in error.safe_message
    # The base closest-matches sentence must still be present, not replaced.
    assert "closest matches" in error.safe_message


@pytest.mark.asyncio
async def test_a_needs_clarification_v1_error_names_every_bounded_candidate() -> None:
    answer = build_preflight_answer(
        outcome=PublicOutcome.NEEDS_CLARIFICATION,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
        clarification_key="ambiguous",
        clarification_candidates=(
            _candidate("Atlas", entity_id="project-atlas-1"),
            _candidate("Atlas Migration", entity_id="project-atlas-2"),
        ),
    )
    error = project_preflight_error(answer, request_id="request_01")

    assert "Atlas" in error.safe_message
    assert "Atlas Migration" in error.safe_message


def test_a_needs_clarification_v1_error_without_candidates_is_unchanged() -> None:
    """No candidates (e.g. an uninterpretable question) -> the base sentence."""

    answer = build_preflight_answer(
        outcome=PublicOutcome.NEEDS_CLARIFICATION,
        intent_id=QuestionIntentID.ENTITY_STATUS,
        versions=versions(),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        generated_at=fixed_now(),
        clarification_key="uninterpretable",
    )
    error = project_preflight_error(answer, request_id="request_01")

    assert error.safe_message == answer.frame.direct_answer
    assert "Candidates:" not in error.safe_message


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
async def test_a_team_subject_commits_a_real_v1_team_scope() -> None:
    """CHAOS-3301 superseded this test's premise: TEAM now has a real v1
    ``DirectScope``/``EntityType`` member, so a resolved team *does* reach
    (and succeeds through) the v1 scope constructors — see
    ``committed_resolution_for``'s docstring for why that call no longer
    raises for a team entity.
    """

    from dev_health_ops.api.dev.contracts import DirectScope

    result = await _run(
        _preflight([(ORG_ID, PLATFORM_TEAM)]),
        request_for("How is the Platform team doing?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.outcome is None
    assert result.diagnostic == "proceeded_committed_subject"
    assert result.committed_resolution is not None
    committed_scope = result.committed_resolution.resolved_scope
    assert committed_scope is not None
    assert committed_scope.direct_scope is DirectScope.TEAM
    assert committed_scope.team_ids == [PLATFORM_TEAM.canonical_id]
    assert result.ledger is not None
    entry = next(iter(result.ledger.latest_by_mention().values()))
    assert entry.outcome is ResolutionOutcome.EXACT_MATCH
    assert entry.team_attribution == PLATFORM_TEAM.canonical_id


# ---------------------------------------------------------------------------
# Vocabulary handlers must be total over their enum
# ---------------------------------------------------------------------------


def test_display_labels_cover_every_public_outcome() -> None:
    """Derived from the enum, so a new member fails here until it is labelled.

    ``DENIED`` was missing and is unreachable from the preflight today, which
    is precisely what made it a latent ``KeyError`` rather than a live bug.
    """

    from dev_health_ops.api.dev.preflight_outcomes import _DISPLAY_LABELS

    assert set(_DISPLAY_LABELS) == set(PublicOutcome)


def test_display_labels_match_the_canonical_answer_labels() -> None:
    """The stronger half: a *wrong* label is as broken as a missing one.

    ``DevAnswerV2`` validates ``outcome_display_label`` against its own
    canonical table, so a drifted duplicate here would raise on the first
    answer that used it.
    """

    from dev_health_ops.api.dev.contracts_v2.answer import _OUTCOME_DISPLAY_LABELS
    from dev_health_ops.api.dev.preflight_outcomes import _DISPLAY_LABELS

    assert _DISPLAY_LABELS == _OUTCOME_DISPLAY_LABELS


@pytest.mark.parametrize("outcome", list(PublicOutcome))
def test_every_public_outcome_has_a_usable_label(outcome: PublicOutcome) -> None:
    """Reaching the label must not raise for any member of the vocabulary."""

    from dev_health_ops.api.dev.preflight_outcomes import _DISPLAY_LABELS

    assert _DISPLAY_LABELS[outcome]
