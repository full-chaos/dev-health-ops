"""Tests for CHAOS-3303's ``ProjectHealthService``.

A fake ``PlanExecutorRuntime`` double stands in for the exact seam
CHAOS-3295 built and ``production_runtime.py`` already wires -- this
service is proven against that protocol, not against a second bespoke
query path.
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
from dev_health_ops.api.dev.project_health_service import (
    CHANGE_FAILURE_RATE_SUPPORTED_SCOPES,
    ProjectHealthService,
)
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    StatusResultState,
    StatusSnapshotResult,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_ORG_ID = "org-1"


def _project_scope(*, with_comparison: bool = True) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.PROJECT,
                entity_id="proj-1",
                display_label="Project 1",
            )
        ],
        time_range=DevTimeRange(
            start=_NOW - timedelta(days=14), end=_NOW, timezone="UTC"
        ),
        comparison_range=(
            DevTimeRange(
                start=_NOW - timedelta(days=28),
                end=_NOW - timedelta(days=14),
                timezone="UTC",
            )
            if with_comparison
            else None
        ),
    )


def _snapshot() -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=StatusResultState.COMPLETE,
        scope=None,  # type: ignore[arg-type]
        as_of=_NOW,
        declared=None,
        actual=ActualCompletion(
            state=CompletionState.READY,
            rule_id="actual-completion",
            rule_version="actual-completion.v4",
            reason_codes=(),
            required_children=(),
            required_child_total=0,
            required_child_complete=0,
            display_truncated=False,
            conflicts=(),
            source_ref_ids=(),
            evidence_ref_ids=(),
        ),
        children=(),
        blockers=(),
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=(),
        source_refs=(),
        warnings=(),
    )


def _data_health() -> DataHealthResult:
    return DataHealthResult(sources=(), complete_eligible=True)


def _metric_result() -> MetricQueryResult:
    return MetricQueryResult(
        definition=get_metric(MetricID.CHANGE_FAILURE_RATE),
        state=MetricDataState.ZERO,
        freshness=FreshnessState.FRESH,
        values=(
            MetricQueryValue(dimensions=(), value=0.0, comparison_value=0.0, series=()),
        ),
        coverage=1.0,
        current_window_start=_NOW,
        current_window_end=_NOW,
        comparison_window_start=_NOW,
        comparison_window_end=_NOW,
        watermark=_NOW,
        source_refs=(),
    )


@dataclass
class FakeRuntime:
    query_metric_calls: int = 0

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return _snapshot()

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        self.query_metric_calls += 1
        return _metric_result()

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        return _data_health()

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
        # CHAOS-3296's PlanExecutorRuntime protocol member --
        # ProjectHealthService never mints evidence itself, so this is here
        # only for structural conformance.
        raise AssertionError("not exercised by this suite")


@pytest.mark.asyncio
async def test_evaluate_project_rejects_non_project_scope() -> None:
    runtime = FakeRuntime()
    service = ProjectHealthService(runtime)
    non_project_scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.ORGANIZATION,
        time_range=DevTimeRange(
            start=_NOW - timedelta(days=14), end=_NOW, timezone="UTC"
        ),
    )
    with pytest.raises(ValueError, match="project direct scope"):
        await service.evaluate_project(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=non_project_scope,
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_project_requires_comparison_range() -> None:
    runtime = FakeRuntime()
    service = ProjectHealthService(runtime)
    with pytest.raises(ValueError, match="comparison_range"):
        await service.evaluate_project(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=_project_scope(with_comparison=False),
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_project_queries_the_change_failure_rate_metric() -> None:
    runtime = FakeRuntime()
    service = ProjectHealthService(runtime)
    profile = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    assert runtime.query_metric_calls == 1
    # Every shipped rule is provisional today, so the finding is shadow.
    finding = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.change_failure_rate.v1"
    )
    # ZERO state maps to a genuinely measured 0.0 -> healthy, not unknown.
    assert finding.state == DimensionState.HEALTHY


@pytest.mark.asyncio
async def test_change_failure_rate_supported_scopes_excludes_team() -> None:
    assert DirectScope.TEAM not in CHANGE_FAILURE_RATE_SUPPORTED_SCOPES
    assert DirectScope.PROJECT in CHANGE_FAILURE_RATE_SUPPORTED_SCOPES


@pytest.mark.asyncio
async def test_evaluate_project_is_deterministic_across_repeated_calls() -> None:
    runtime = FakeRuntime()
    service = ProjectHealthService(runtime)
    profile_a = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    profile_b = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    assert [f.finding_id for f in profile_a.shadow_findings] == [
        f.finding_id for f in profile_b.shadow_findings
    ]


@pytest.mark.asyncio
async def test_evaluate_project_derives_subject_id_from_scope_not_a_label() -> None:
    """Codex finding (HIGH, 2026-08-02): project_id used to be a caller-
    supplied label independent of `scope` -- the exact same committed
    DevScope submitted as two different asserted labels ("alias-a",
    "alias-b") minted two portfolio "subjects" with identical underlying
    data. There is no longer a project_id parameter to assert through:
    the subject id is always the scope's own (validator-guaranteed unique)
    entity_ref id, proven here for two scopes that are identical except for
    entity_id.
    """

    runtime = FakeRuntime()
    service = ProjectHealthService(runtime)
    scope_a = _project_scope()
    scope_b = scope_a.model_copy(
        update={
            "entity_refs": [
                DevEntityRef(
                    entity_type=EntityType.PROJECT,
                    entity_id="proj-2",
                    display_label="Project 2",
                )
            ]
        }
    )

    profile_a = await service.evaluate_project(
        org_id=_ORG_ID, permission_fingerprint="fp", scope=scope_a, now=_NOW
    )
    profile_b = await service.evaluate_project(
        org_id=_ORG_ID, permission_fingerprint="fp", scope=scope_b, now=_NOW
    )

    assert profile_a.subject_id == "proj-1"
    assert profile_b.subject_id == "proj-2"
