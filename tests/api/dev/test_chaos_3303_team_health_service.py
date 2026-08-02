"""Tests for CHAOS-3303's ``TeamHealthService``.

The load-bearing behavior today: ``native_status_change.py`` fails closed
for every team direct scope (every source it reads is still in
``TEAM_NOT_APPLICABLE_SOURCES``), so a team health evaluation must never
fabricate a healthy/zero finding out of that fail-closed response --
regardless of whether a cohort size is supplied. This is proven with a
fake runtime that mirrors the real fail-closed shape, not by special-casing
team scope inside the service.
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
)
from dev_health_ops.api.dev.contracts_v2.health_rules import DimensionState
from dev_health_ops.api.dev.data_health_service import DataHealthResult
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    StatusResultState,
    StatusSnapshotResult,
)
from dev_health_ops.api.dev.team_health_service import TeamHealthService

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_ORG_ID = "org-1"


def _team_scope(*, team_id: str = "team-1") -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.TEAM,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM, entity_id=team_id, display_label="Team 1"
            )
        ],
        team_ids=[team_id],
        time_range=DevTimeRange(
            start=_NOW - timedelta(days=14), end=_NOW, timezone="UTC"
        ),
    )


def _fail_closed_snapshot() -> StatusSnapshotResult:
    """Mirrors ``ClickHouseStatusChangeSource``'s current team fail-closed branch:
    an INSUFFICIENT_EVIDENCE state with no facts of any kind.
    """

    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=StatusResultState.INSUFFICIENT_EVIDENCE,
        scope=None,  # type: ignore[arg-type]
        as_of=_NOW,
        declared=None,
        actual=ActualCompletion(
            state=CompletionState.INDETERMINATE,
            rule_id="actual-completion",
            rule_version="actual-completion.v4",
            reason_codes=("team_scope_not_yet_attributed",),
            required_children=(),
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
        warnings=("no_authorized_repositories",),
    )


@dataclass
class FakeTeamRuntime:
    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return _fail_closed_snapshot()

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError(
            "TeamHealthService must never query CHANGE_FAILURE_RATE for a "
            "team subject -- the metric does not support DirectScope.TEAM"
        )

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        return DataHealthResult(sources=(), complete_eligible=False)


@pytest.mark.asyncio
async def test_evaluate_team_rejects_non_team_scope() -> None:
    service = TeamHealthService(FakeTeamRuntime())
    project_scope = DevScope(
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
    )
    with pytest.raises(ValueError, match="team direct scope"):
        await service.evaluate_team(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=project_scope,
            team_id="team-1",
            cohort_size=5,
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_team_rejects_mismatched_team_id() -> None:
    service = TeamHealthService(FakeTeamRuntime())
    with pytest.raises(ValueError, match="team_ids must name exactly"):
        await service.evaluate_team(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=_team_scope(team_id="team-1"),
            team_id="team-2",
            cohort_size=5,
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_team_never_queries_change_failure_rate_metric() -> None:
    """CHANGE_FAILURE_RATE has no supported_scopes entry for TEAM -- this
    must be a structural not_applicable, never an attempted, failing call.
    """

    service = TeamHealthService(FakeTeamRuntime())
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        cohort_size=5,
        now=_NOW,
    )
    finding = next(
        f for f in profile.findings if f.rule_id == "health_rule.change_failure_rate.v1"
    )
    assert finding.state == DimensionState.UNKNOWN


@pytest.mark.asyncio
async def test_evaluate_team_without_attribution_never_reports_healthy() -> None:
    """With cohort_size=0 (no resolved team_repo_ownership attribution),
    every applicable rule requiring a minimum cohort must suppress -- never
    a fabricated healthy/at-risk finding for an unattributed team.
    """

    service = TeamHealthService(FakeTeamRuntime())
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        cohort_size=0,
        now=_NOW,
    )
    assert profile.findings
    for finding in profile.findings:
        assert finding.state in (DimensionState.UNKNOWN, DimensionState.NOT_APPLICABLE)


@pytest.mark.asyncio
async def test_evaluate_team_with_attribution_still_honest_given_fail_closed_status_source() -> (
    None
):
    """Even with real attribution (cohort_size >= minimum), today's
    fail-closed team status source means every status-derived dimension
    stays unknown -- this is the current, honest state (see module
    docstring), proven so a future SQL-arm change is visible as a real
    behavior change here, not silently.
    """

    service = TeamHealthService(FakeTeamRuntime())
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        cohort_size=25,
        now=_NOW,
    )
    incident_finding = next(
        f for f in profile.findings if f.rule_id == "health_rule.incident_load.v1"
    )
    assert incident_finding.state == DimensionState.UNKNOWN
