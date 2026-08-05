"""CHAOS-3393: the full orchestrator seam for ``status.portfolio.v1``.

Driven through the real orchestrator (``tests._chaos_3292_preflight.
run_preflight_orchestrator``), never a diagnostic that inspects internals
without exercising the seam -- mirrors ``test_chaos_3295_plan_executor_
acceptance.py``'s own discipline. Proves the plumbing CHAOS-3393 added:
the orchestrator's PLURAL_COHORT/ORGANIZATION_WIDE gate builds
``StepContext.subject_set_scopes`` from the preflight-committed
``DevSubjectSet`` and threads ``subject_set_fingerprint`` through to the
persisted ``DevInvestigationResult``; the final frame carries
``subject_set_ref`` (never ``subject_ref``) and the portfolio's own
per-project rollup.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevToolRequest, DevToolResult, ToolID
from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.health_rules import RuleApplicability
from dev_health_ops.api.dev.health_profile_synthesis import HealthProfileResult
from dev_health_ops.api.dev.investigation_plans.executor import PlanExecutor
from dev_health_ops.api.dev.investigation_plans.steps import StepRegistry
from dev_health_ops.api.dev.investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
    register_wave_3_1_steps,
)
from dev_health_ops.api.dev.portfolio_status_service import (
    MAX_PORTFOLIO_PROJECTS,
    PortfolioProjectFailure,
    PortfolioStatusResult,
)
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    NIGHTFALL_PROJECT,
    ORG_ID,
    fixed_now,
    run_preflight_orchestrator,
)
from tests._chaos_3295_plan_executor import InvestigationRecorder

_NOW = datetime(2026, 8, 5, 12, tzinfo=UTC)


#: A v2-``EvidenceHandle``-shaped id (``ev1_`` + 40 hex chars).
_VALID_EVIDENCE_HANDLE = "ev1_" + "a" * 40


def _rewrite_evidence_ref_ids(value: Any) -> Any:
    """Replace every ``"ev_01"`` (the shared fixture's short, v1-only-shaped
    id) with :data:`_VALID_EVIDENCE_HANDLE`, recursively.

    The shared stock ``dev_tool_result.v1`` fixture's ``evidence``/
    ``metrics`` entries carry ``evidence_ref_id: "ev_01"`` -- valid v1
    ``OpaqueID`` shape, but too short for v2's stricter ``EvidenceHandle``
    grammar. ``terminal_frames.wrap_legacy_answer_as_frame`` re-validates a
    v1 answer's metrics/evidence as v2 types, so this PRE-EXISTING fixture
    gap (found while writing these CHAOS-3393 tests; reported separately,
    not this suite's to fix) fails frame construction for ANY run whose
    canonical answer carries this stock evidence/metric -- silently, since
    no other test asserts on the recorded frame's content. Rewritten
    in-place (never simply dropped -- an empty evidence/metrics list fails
    this run's OWN grounding-floor validation instead) so this suite's
    frame assertions are not blocked by that unrelated defect.
    """

    if isinstance(value, str):
        return _VALID_EVIDENCE_HANDLE if value == "ev_01" else value
    if isinstance(value, dict):
        return {key: _rewrite_evidence_ref_ids(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_rewrite_evidence_ref_ids(item) for item in value]
    return value


def _valid_evidence_registry(calls: list[DevToolRequest]) -> AskDevToolRegistry:
    """Mirrors ``tests._chaos_3292_preflight.recording_registry``, except
    every stock evidence handle is rewritten to a v2-valid shape (see
    :func:`_rewrite_evidence_ref_ids`) and ``metrics`` is cleared.

    ``metrics`` is cleared rather than rewritten: ``_wrap_legacy_metric``
    (CHAOS-3297 stack #3, team-lead ruling) sets ``evidence_classification
    = legacy_v1_unminted`` UNCONDITIONALLY on every v1-sourced metric,
    because the real ``production_runtime.py`` query_metric.v1 tool always
    scrubs ``evidence_ref_ids`` to ``()`` -- carrying both a real handle
    AND that classification trips ``DevMetricRefV2``'s own F10 XOR
    validator (by design: a v1 metric with genuine evidence would be that
    invariant breaking, and the validator is right to reject it loudly).
    The shared stock fixture's canned metric has a real (id-shaped)
    ``evidence_ref_ids``, which is exactly that invariant violation for a
    fixture that never went through the real scrubbing tool -- so this
    suite drops it rather than construct an artificial metric shape that
    real production traffic could never actually produce.
    """

    async def execute(_context: Any, request: DevToolRequest) -> DevToolResult:
        calls.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload = _rewrite_evidence_ref_ids(payload)
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "metrics": [],
            }
        )
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


class _NeverCalledHealth:
    async def evaluate_project(self, **_kwargs: Any) -> HealthProfileResult:
        raise AssertionError("only the portfolio step should run in this suite")

    async def evaluate_team(self, **_kwargs: Any) -> HealthProfileResult:
        raise AssertionError("only the portfolio step should run in this suite")

    async def evaluate_workload(self, **_kwargs: Any) -> HealthProfileResult:
        raise AssertionError("only the portfolio step should run in this suite")


class _NeverCalledDeficiency:
    async def evaluate_project(self, **_kwargs: Any):
        raise AssertionError("only the portfolio step should run in this suite")

    async def evaluate_team(self, **_kwargs: Any):
        raise AssertionError("only the portfolio step should run in this suite")


class _FakePortfolioStatus:
    def __init__(self, result: PortfolioStatusResult) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

    async def evaluate_portfolio(
        self,
        *,
        org_id,
        permission_fingerprint,
        projects,
        now,
        unresolved_mention_ids=(),
        ambiguous_mention_ids=(),
        warnings=(),
        per_project_timeout_seconds=None,
    ):
        self.calls.append(
            {
                "org_id": org_id,
                "permission_fingerprint": permission_fingerprint,
                "projects": tuple(projects),
                "per_project_timeout_seconds": per_project_timeout_seconds,
            }
        )
        return self._result


class _DynamicPortfolioStatus:
    """Evaluates every scope it is actually handed, all cleanly (empty
    findings) -- used where the batch size is determined by the real
    preflight-committed subject set (e.g. an ORGANIZATION_WIDE enumeration)
    rather than fixed by the test."""

    async def evaluate_portfolio(
        self,
        *,
        org_id,
        permission_fingerprint,
        projects,
        now,
        unresolved_mention_ids=(),
        ambiguous_mention_ids=(),
        warnings=(),
        per_project_timeout_seconds=None,
    ):
        return PortfolioStatusResult(
            projects=tuple(
                _health_profile_result(item.project_id) for item in projects
            ),
            counts_by_worst_state={},
            failures=(),
            unresolved_mention_ids=tuple(unresolved_mention_ids),
            ambiguous_mention_ids=tuple(ambiguous_mention_ids),
            warnings=tuple(warnings),
            evaluated_at=now,
        )


def _health_profile_result(subject_id: str) -> HealthProfileResult:
    return HealthProfileResult(
        subject_kind=RuleApplicability.PROJECT,
        subject_id=subject_id,
        observations=(),
        launch_findings=(),
        shadow_findings=(),
        suppressed_findings=(),
        observations_by_rule={},
    )


def _plan_executor(
    portfolio_status: _FakePortfolioStatus | _DynamicPortfolioStatus,
) -> PlanExecutor:
    registry = StepRegistry()
    register_wave_3_1_steps(
        registry,
        project_health=_NeverCalledHealth(),
        team_health=_NeverCalledHealth(),
        team_workload=_NeverCalledHealth(),
        operational_deficiency=_NeverCalledDeficiency(),
        portfolio_status=portfolio_status,
    )
    return PlanExecutor(registry=registry, now=fixed_now)


@pytest.mark.asyncio
async def test_named_project_cohort_batches_both_projects_through_the_portfolio_step():
    portfolio_status = _FakePortfolioStatus(
        PortfolioStatusResult(
            projects=(
                _health_profile_result(ASK_DEV_PROJECT.canonical_id),
                _health_profile_result(NIGHTFALL_PROJECT.canonical_id),
            ),
            counts_by_worst_state={},
            failures=(),
            unresolved_mention_ids=(),
            ambiguous_mention_ids=(),
            warnings=(),
            evaluated_at=_NOW,
        )
    )
    output = await run_preflight_orchestrator(
        question="What is the status of project Ask Dev and project Nightfall?",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)],
        script_id="chaos3393-cohort",
        recorder_factory=InvestigationRecorder,
        plan_registry=WAVE_3_1_PLANS_BY_INTENT,
        plan_executor=_plan_executor(portfolio_status),
        registry_factory=_valid_evidence_registry,
    )

    # The plan executor actually ran, batching BOTH committed projects --
    # never ctx.scope (which stays the single org-level authorized scope).
    assert len(portfolio_status.calls) == 1
    call = portfolio_status.calls[0]
    project_ids = {scope.project_id for scope in call["projects"]}
    assert project_ids == {ASK_DEV_PROJECT.canonical_id, NIGHTFALL_PROJECT.canonical_id}
    # Budget: min(120/N, 15s) sliced across the batch.
    assert call["per_project_timeout_seconds"] == pytest.approx(15.0)

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    assert len(recorder.results) == 1
    result = recorder.results[0]
    assert result.plan_id == "status.portfolio.v1"
    # A cohort/org-wide result carries subject_set_fingerprint, never
    # subject_entity_id (DevInvestigationResult's own XOR invariant).
    assert result.subject_set_fingerprint is not None
    assert result.subject_entity_id is None
    assert len(result.observations) == 1
    observation = result.observations[0]
    assert observation.observed_state is SourceRequirementState.AVAILABLE_CURRENT
    assert observation.content is not None
    assert len(observation.content.portfolio_project_statuses) == 2
    assert {
        status.project_id for status in observation.content.portfolio_project_statuses
    } == {ASK_DEV_PROJECT.canonical_id, NIGHTFALL_PROJECT.canonical_id}
    assert all(
        status.evaluated for status in observation.content.portfolio_project_statuses
    )

    # The final frame: subject_set_ref set, never subject_ref, and the
    # portfolio rollup rides alongside the legacy-loop's own direct_answer.
    assert recorder.frames, "a frame must have been recorded"
    frame = recorder.frames[-1]
    assert frame.subject_set_ref is not None
    assert frame.subject_ref is None
    assert len(frame.portfolio_project_statuses) == 2
    assert frame.portfolio_project_statuses_truncated is False


@pytest.mark.asyncio
async def test_partial_failure_is_disclosed_never_fabricated_or_refused():
    portfolio_status = _FakePortfolioStatus(
        PortfolioStatusResult(
            projects=(_health_profile_result(ASK_DEV_PROJECT.canonical_id),),
            counts_by_worst_state={},
            failures=(
                PortfolioProjectFailure(
                    project_id=NIGHTFALL_PROJECT.canonical_id, error="timeout"
                ),
            ),
            unresolved_mention_ids=(),
            ambiguous_mention_ids=(),
            warnings=(),
            evaluated_at=_NOW,
        )
    )
    output = await run_preflight_orchestrator(
        question="What is the status of project Ask Dev and project Nightfall?",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)],
        script_id="chaos3393-partial-failure",
        recorder_factory=InvestigationRecorder,
        plan_registry=WAVE_3_1_PLANS_BY_INTENT,
        plan_executor=_plan_executor(portfolio_status),
        registry_factory=_valid_evidence_registry,
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    result = recorder.results[0]
    observation = result.observations[0]
    # Never fabricated status, never a hard refusal: the batch still
    # reports AVAILABLE_STALE (queried, not unmeasured) with a disclosed,
    # bounded limitation naming the failed project.
    assert observation.observed_state is SourceRequirementState.AVAILABLE_STALE
    assert observation.limitation is not None
    assert NIGHTFALL_PROJECT.canonical_id in observation.limitation
    assert observation.content is not None
    statuses = {
        status.project_id: status
        for status in observation.content.portfolio_project_statuses
    }
    assert statuses[ASK_DEV_PROJECT.canonical_id].evaluated is True
    failed = statuses[NIGHTFALL_PROJECT.canonical_id]
    assert failed.evaluated is False
    assert failed.failure_reason == "evaluation_timeout"
    # Never the raw exception detail on the wire.
    assert "timeout" != failed.failure_reason


@pytest.mark.asyncio
async def test_organization_wide_truncation_is_disclosed_in_the_answer():
    """CHAOS-3393: DevSubjectSet.warnings (the org-wide enumeration's own
    truncation disclosure) is persisted separately from the v1 answer/
    frame -- wrap_legacy_answer_as_frame's own `limitations` never reads
    it. render_portfolio_summary folds it into direct_answer so a
    truncated batch is disclosed in the actual answer text, not merely
    recorded on a row nothing downstream reads.
    """

    entities = [
        (
            ORG_ID,
            AuthorizedEntity(EntityKind.PROJECT, f"project-{i:02}", f"Project {i:02}"),
        )
        for i in range(MAX_PORTFOLIO_PROJECTS + 5)
    ]
    output = await run_preflight_orchestrator(
        question="What is the portfolio status?",
        entities=entities,
        script_id="chaos3393-org-wide-truncated",
        recorder_factory=InvestigationRecorder,
        plan_registry=WAVE_3_1_PLANS_BY_INTENT,
        plan_executor=_plan_executor(_DynamicPortfolioStatus()),
        registry_factory=_valid_evidence_registry,
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    result = recorder.results[0]
    observation_content = result.observations[0].content
    assert observation_content is not None
    assert len(observation_content.portfolio_project_statuses) == MAX_PORTFOLIO_PROJECTS
    assert recorder.frames, "a frame must have been recorded"
    frame = recorder.frames[-1]
    assert frame.subject_set_ref is not None
    assert "authorized projects" in frame.direct_answer
