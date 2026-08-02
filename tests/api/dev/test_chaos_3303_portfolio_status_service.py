"""Tests for CHAOS-3303's ``PortfolioStatusService``.

Covers the required-implementation test list that applies at this layer:
two-project and organization-scale portfolio batching, deterministic
severity ordering, bounded output, isolated per-project failure, and
preserved unresolved/ambiguous mention disclosure.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    FreshnessState,
    MetricID,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import DimensionState
from dev_health_ops.api.dev.data_health_service import DataHealthResult
from dev_health_ops.api.dev.metrics.definitions import get_metric
from dev_health_ops.api.dev.metrics.service import (
    MetricDataState,
    MetricQueryResult,
    MetricQueryValue,
)
from dev_health_ops.api.dev.portfolio_status_service import (
    MAX_PORTFOLIO_PROJECTS,
    PortfolioProjectScope,
    PortfolioStatusService,
)
from dev_health_ops.api.dev.project_health_service import ProjectHealthService
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    IncidentFact,
    StatusResultState,
    StatusSnapshotResult,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_ORG_ID = "org-1"


def _scope(project_id: str) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.PROJECT,
                entity_id=project_id,
                display_label=project_id,
            )
        ],
        time_range=DevTimeRange(
            start=_NOW - timedelta(days=14), end=_NOW, timezone="UTC"
        ),
        comparison_range=DevTimeRange(
            start=_NOW - timedelta(days=28),
            end=_NOW - timedelta(days=14),
            timezone="UTC",
        ),
    )


def _actual() -> ActualCompletion:
    return ActualCompletion(
        state=CompletionState.READY,
        rule_id="actual-completion",
        rule_version="actual-completion.v4",
        reason_codes=(),
        required_children=(),
        conflicts=(),
        source_ref_ids=(),
        evidence_ref_ids=(),
    )


def _incident(entity_id: str) -> IncidentFact:
    return IncidentFact(
        entity_id=entity_id,
        display_label=entity_id,
        status="open",
        active=True,
        blocking=False,
        observed_at=_NOW,
        source_ref_id="ref-1",
        evidence_ref_ids=(),
    )


def _snapshot(*, incident_count: int) -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=StatusResultState.COMPLETE,
        scope=None,  # type: ignore[arg-type]
        as_of=_NOW,
        declared=None,
        actual=_actual(),
        children=(),
        blockers=(),
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=tuple(_incident(f"inc-{i}") for i in range(incident_count)),
        source_refs=(),
        warnings=(),
    )


@dataclass
class FakePortfolioRuntime:
    """Per-project-id incident counts drive deterministic severity ordering:
    a project with more incidents (closer to the incident_load threshold of
    10.0) sorts as more severe.
    """

    incident_counts: dict[str, int]
    raises_for: frozenset[str] = frozenset()

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        project_id = scope.entity_refs[0].entity_id
        if project_id in self.raises_for:
            raise RuntimeError(f"simulated status_snapshot failure for {project_id}")
        return _snapshot(incident_count=self.incident_counts.get(project_id, 1))

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        return MetricQueryResult(
            definition=get_metric(MetricID.CHANGE_FAILURE_RATE),
            state=MetricDataState.ZERO,
            freshness=FreshnessState.FRESH,
            values=(
                MetricQueryValue(
                    dimensions=(), value=0.0, comparison_value=0.0, series=()
                ),
            ),
            coverage=1.0,
            current_window_start=_NOW,
            current_window_end=_NOW,
            comparison_window_start=_NOW,
            comparison_window_end=_NOW,
            watermark=_NOW,
            source_refs=(),
        )

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        return DataHealthResult(sources=(), complete_eligible=True)

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        # CHAOS-3296's PlanExecutorRuntime protocol member -- neither
        # ProjectHealthService nor PortfolioStatusService mints evidence
        # itself, so this is here only for structural conformance.
        raise AssertionError("not exercised by this suite")


@pytest.mark.asyncio
async def test_evaluate_portfolio_two_projects_returns_both() -> None:
    runtime = FakePortfolioRuntime(incident_counts={"proj-a": 1, "proj-b": 1})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=(
            PortfolioProjectScope(_scope("proj-a")),
            PortfolioProjectScope(_scope("proj-b")),
        ),
        now=_NOW,
    )
    assert {p.subject_id for p in result.projects} == {"proj-a", "proj-b"}
    assert not result.failures


@pytest.mark.asyncio
async def test_evaluate_portfolio_organization_scale_batch() -> None:
    project_ids = [f"proj-{i}" for i in range(MAX_PORTFOLIO_PROJECTS)]
    runtime = FakePortfolioRuntime(incident_counts=dict.fromkeys(project_ids, 1))
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=tuple(PortfolioProjectScope(_scope(pid)) for pid in project_ids),
        now=_NOW,
    )
    assert len(result.projects) == MAX_PORTFOLIO_PROJECTS


@pytest.mark.asyncio
async def test_evaluate_portfolio_rejects_over_bound_batch() -> None:
    project_ids = [f"proj-{i}" for i in range(MAX_PORTFOLIO_PROJECTS + 1)]
    runtime = FakePortfolioRuntime(incident_counts=dict.fromkeys(project_ids, 1))
    service = PortfolioStatusService(ProjectHealthService(runtime))
    with pytest.raises(ValueError, match="bounded maximum"):
        await service.evaluate_portfolio(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            projects=tuple(PortfolioProjectScope(_scope(pid)) for pid in project_ids),
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_portfolio_rejects_duplicate_project_ids() -> None:
    runtime = FakePortfolioRuntime(incident_counts={"proj-a": 1})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    with pytest.raises(ValueError, match="duplicate project_id"):
        await service.evaluate_portfolio(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            projects=(
                PortfolioProjectScope(_scope("proj-a")),
                PortfolioProjectScope(_scope("proj-a")),
            ),
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_portfolio_rejects_duplicate_scope_under_different_labels() -> (
    None
):
    """Codex finding (HIGH, 2026-08-02): project_id used to be a caller-
    asserted label independent of scope, so the SAME committed DevScope
    submitted as two ``PortfolioProjectScope`` entries with different
    asserted ids minted two "different" portfolio subjects with identical
    data. ``project_id`` is now always ``scope.entity_refs[0].entity_id``,
    so there is no label left to vary -- the exact same scope object under
    two entries collides on the duplicate check regardless of how it is
    constructed.
    """

    runtime = FakePortfolioRuntime(incident_counts={"proj-a": 1})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    same_scope = _scope("proj-a")
    with pytest.raises(ValueError, match="duplicate project_id"):
        await service.evaluate_portfolio(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            projects=(
                PortfolioProjectScope(same_scope),
                PortfolioProjectScope(same_scope),
            ),
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_portfolio_isolates_a_failing_project() -> None:
    """A canonical-service exception for one project must not crash the
    batch or silently drop the project -- it appears in ``failures``.
    """

    runtime = FakePortfolioRuntime(
        incident_counts={"proj-a": 1, "proj-b": 1}, raises_for=frozenset({"proj-b"})
    )
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=(
            PortfolioProjectScope(_scope("proj-a")),
            PortfolioProjectScope(_scope("proj-b")),
        ),
        now=_NOW,
    )
    assert {p.subject_id for p in result.projects} == {"proj-a"}
    assert [f.project_id for f in result.failures] == ["proj-b"]


@pytest.mark.asyncio
async def test_evaluate_portfolio_preserves_unresolved_and_ambiguous_mentions() -> None:
    runtime = FakePortfolioRuntime(incident_counts={"proj-a": 1})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=(PortfolioProjectScope(_scope("proj-a")),),
        now=_NOW,
        unresolved_mention_ids=("mention-1",),
        ambiguous_mention_ids=("mention-2",),
        warnings=("cohort_truncated",),
    )
    assert result.unresolved_mention_ids == ("mention-1",)
    assert result.ambiguous_mention_ids == ("mention-2",)
    assert result.warnings == ("cohort_truncated",)


@pytest.mark.asyncio
async def test_evaluate_portfolio_all_provisional_registry_reports_no_elevated_state() -> (
    None
):
    """Codex finding (HIGH, 2026-08-02): every HEALTH_RULE_REGISTRY rule is
    provisional today, so ``launch_findings`` is empty for every project --
    status/ordering/counts_by_worst_state must report no elevated state
    anywhere (never promote calibration-only shadow findings into launch
    authority). The underlying WATCH-level signal for the "risky" project
    (15 incidents, past the incident_load threshold) is NOT lost -- it is
    exactly the separately-labeled calibration payload, still readable off
    each project's own ``shadow_findings``.
    """

    runtime = FakePortfolioRuntime(
        incident_counts={"proj-healthy": 1, "proj-risky": 15}
    )
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=(
            PortfolioProjectScope(_scope("proj-healthy")),
            PortfolioProjectScope(_scope("proj-risky")),
        ),
        now=_NOW,
    )

    # No elevated state anywhere: both projects tie at UNKNOWN (no launch
    # findings), so ordering falls back to project_id alphabetically.
    assert [p.subject_id for p in result.projects] == ["proj-healthy", "proj-risky"]
    assert result.counts_by_worst_state[DimensionState.CRITICAL] == 0
    assert result.counts_by_worst_state[DimensionState.AT_RISK] == 0
    assert result.counts_by_worst_state[DimensionState.WATCH] == 0
    assert result.counts_by_worst_state[DimensionState.UNKNOWN] == 2
    for project in result.projects:
        assert project.launch_findings == ()

    # The calibration payload is still fully present, separately labeled.
    risky = next(p for p in result.projects if p.subject_id == "proj-risky")
    risky_incident_finding = next(
        f for f in risky.shadow_findings if f.rule_id == "health_rule.incident_load.v1"
    )
    assert risky_incident_finding.state == DimensionState.WATCH
    assert risky_incident_finding.shadow_only is True


@pytest.mark.asyncio
async def test_evaluate_portfolio_requires_at_least_one_project() -> None:
    runtime = FakePortfolioRuntime(incident_counts={})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    with pytest.raises(ValueError, match="at least one project"):
        await service.evaluate_portfolio(
            org_id=_ORG_ID, permission_fingerprint="fp", projects=(), now=_NOW
        )


@dataclass
class SingleFlightPortfolioRuntime:
    """Trips ``RuntimeError`` if any two of its methods are ever in flight at
    once -- a production-shaped stand-in for the request-scoped SQLAlchemy
    AsyncSession the real ``_ProductionPlanExecutorRuntime`` shares across
    every canonical-service call it makes (Codex finding, HIGH,
    2026-08-02). ``asyncio.sleep(0)`` is a real yield point (mirroring a
    genuine I/O await), so a concurrent caller (e.g. ``asyncio.gather``)
    interleaves two calls and the second observes ``_in_flight=True`` before
    the first resets it; a strictly sequential caller never does.
    """

    incident_counts: dict[str, int]
    _in_flight: bool = False

    async def _guarded(self):
        if self._in_flight:
            raise RuntimeError(
                "concurrent single-session use detected -- two calls were "
                "in flight against the same request-scoped runtime at once"
            )
        self._in_flight = True
        try:
            await asyncio.sleep(0)
        finally:
            self._in_flight = False

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        await self._guarded()
        project_id = scope.entity_refs[0].entity_id
        return _snapshot(incident_count=self.incident_counts.get(project_id, 1))

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        await self._guarded()
        return MetricQueryResult(
            definition=get_metric(MetricID.CHANGE_FAILURE_RATE),
            state=MetricDataState.ZERO,
            freshness=FreshnessState.FRESH,
            values=(
                MetricQueryValue(
                    dimensions=(), value=0.0, comparison_value=0.0, series=()
                ),
            ),
            coverage=1.0,
            current_window_start=_NOW,
            current_window_end=_NOW,
            comparison_window_start=_NOW,
            comparison_window_end=_NOW,
            watermark=_NOW,
            source_refs=(),
        )

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        await self._guarded()
        return DataHealthResult(sources=(), complete_eligible=True)

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        raise AssertionError("not exercised by this suite")


@pytest.mark.asyncio
async def test_evaluate_portfolio_never_overlaps_single_session_runtime_calls() -> None:
    """The regression proof for the concurrency fix: a runtime that raises
    on overlapping in-flight calls must complete cleanly under
    ``evaluate_portfolio``'s sequential evaluation, across enough projects
    that a concurrent (``asyncio.gather``) implementation would reliably
    interleave and trip the guard.
    """

    project_ids = [f"proj-{i}" for i in range(10)]
    runtime = SingleFlightPortfolioRuntime(
        incident_counts=dict.fromkeys(project_ids, 1)
    )
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=tuple(PortfolioProjectScope(_scope(pid)) for pid in project_ids),
        now=_NOW,
    )
    assert len(result.projects) == len(project_ids)
    assert not result.failures
