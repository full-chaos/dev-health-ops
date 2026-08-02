"""Tests for CHAOS-3303's ``PortfolioStatusService``.

Covers the required-implementation test list that applies at this layer:
two-project and organization-scale portfolio batching, deterministic
severity ordering, bounded output, isolated per-project failure, and
preserved unresolved/ambiguous mention disclosure.
"""

from __future__ import annotations

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


@pytest.mark.asyncio
async def test_evaluate_portfolio_two_projects_returns_both() -> None:
    runtime = FakePortfolioRuntime(incident_counts={"proj-a": 1, "proj-b": 1})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=(
            PortfolioProjectScope("proj-a", _scope("proj-a")),
            PortfolioProjectScope("proj-b", _scope("proj-b")),
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
        projects=tuple(PortfolioProjectScope(pid, _scope(pid)) for pid in project_ids),
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
            projects=tuple(
                PortfolioProjectScope(pid, _scope(pid)) for pid in project_ids
            ),
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
                PortfolioProjectScope("proj-a", _scope("proj-a")),
                PortfolioProjectScope("proj-a", _scope("proj-a")),
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
            PortfolioProjectScope("proj-a", _scope("proj-a")),
            PortfolioProjectScope("proj-b", _scope("proj-b")),
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
        projects=(PortfolioProjectScope("proj-a", _scope("proj-a")),),
        now=_NOW,
        unresolved_mention_ids=("mention-1",),
        ambiguous_mention_ids=("mention-2",),
        warnings=("cohort_truncated",),
    )
    assert result.unresolved_mention_ids == ("mention-1",)
    assert result.ambiguous_mention_ids == ("mention-2",)
    assert result.warnings == ("cohort_truncated",)


@pytest.mark.asyncio
async def test_evaluate_portfolio_orders_worst_severity_first() -> None:
    """Kill site: sort key must rank by worst-dimension severity, not by
    input order or project_id alone. A project with 15 incidents (past the
    incident_load threshold of 10.0, sustained_periods_required=1,
    triggered_state=WATCH) must sort ahead of a healthy one.
    """

    runtime = FakePortfolioRuntime(
        incident_counts={"proj-healthy": 1, "proj-risky": 15}
    )
    service = PortfolioStatusService(ProjectHealthService(runtime))
    result = await service.evaluate_portfolio(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        projects=(
            PortfolioProjectScope("proj-healthy", _scope("proj-healthy")),
            PortfolioProjectScope("proj-risky", _scope("proj-risky")),
        ),
        now=_NOW,
    )
    assert [p.subject_id for p in result.projects] == ["proj-risky", "proj-healthy"]
    risky_incident_finding = next(
        f
        for f in result.projects[0].findings
        if f.rule_id == "health_rule.incident_load.v1"
    )
    assert risky_incident_finding.state == DimensionState.WATCH


@pytest.mark.asyncio
async def test_evaluate_portfolio_requires_at_least_one_project() -> None:
    runtime = FakePortfolioRuntime(incident_counts={})
    service = PortfolioStatusService(ProjectHealthService(runtime))
    with pytest.raises(ValueError, match="at least one project"):
        await service.evaluate_portfolio(
            org_id=_ORG_ID, permission_fingerprint="fp", projects=(), now=_NOW
        )
