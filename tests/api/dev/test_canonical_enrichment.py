"""CHAOS-3664: CanonicalEnrichmentAccessor.

Every constituent service is a fake here -- this suite proves the
ACCESSOR's own composition/branching logic (which service gets called for
which subject kind, gap disclosure on failure, partial-failure isolation
across metrics), not the constituent services' own internals, which already
have their own test suites.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from dev_health_ops.api.dev.canonical_enrichment import (
    CanonicalEnrichmentAccessor,
    EnrichmentGap,
)
from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
ORG_ID = "org-a"
PERMISSION_FINGERPRINT = "allowed"


def _scope(
    *,
    direct_scope: DirectScope,
    entity_id: str = "subject-1",
    entity_type: EntityType = EntityType.TEAM,
    team_ids: list[str] = [],  # noqa: B006 -- never mutated
) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=direct_scope,
        entity_refs=[
            DevEntityRef(
                entity_type=entity_type, entity_id=entity_id, display_label="Subject"
            )
        ],
        team_ids=list(team_ids),
        time_range=DevTimeRange(
            start=NOW - timedelta(days=14), end=NOW, timezone="UTC"
        ),
    )


def _team_scope(*, team_id: str = "team-1") -> DevScope:
    return _scope(
        direct_scope=DirectScope.TEAM,
        entity_id=team_id,
        entity_type=EntityType.TEAM,
        team_ids=[team_id],
    )


def _project_scope(*, project_id: str = "proj-1") -> DevScope:
    return _scope(
        direct_scope=DirectScope.PROJECT,
        entity_id=project_id,
        entity_type=EntityType.PROJECT,
    )


@dataclass
class _FakeStatusService:
    result: Any = None
    raises: bool = False
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def status_snapshot(self, org_id, permission_fingerprint, request):
        self.calls.append(
            {"org_id": org_id, "permission_fingerprint": permission_fingerprint}
        )
        if self.raises:
            raise RuntimeError("status backend unavailable")
        return self.result if self.result is not None else "status-ok"


@dataclass
class _FakeRuleService:
    """Shared fake shape for health/workload (``evaluate_team``/
    ``evaluate_workload``) -- both take the same keyword arguments."""

    result: Any = "rule-ok"
    raises: bool = False
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def _evaluate(self, *, org_id, permission_fingerprint, scope, team_id, now):
        self.calls.append({"org_id": org_id, "team_id": team_id, "scope": scope})
        if self.raises:
            raise RuntimeError("rule evaluation unavailable")
        return self.result

    async def evaluate_team(self, **kwargs):
        return await self._evaluate(**kwargs)

    async def evaluate_workload(self, **kwargs):
        return await self._evaluate(**kwargs)


@dataclass
class _FakeReadinessService:
    project_result: Any = "readiness-project-ok"
    team_result: Any = "readiness-team-ok"
    raises: bool = False
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def evaluate_project(self, *, org_id, permission_fingerprint, scope, now):
        self.calls.append({"kind": "project", "org_id": org_id})
        if self.raises:
            raise RuntimeError("readiness unavailable")
        return self.project_result

    async def evaluate_team(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        self.calls.append({"kind": "team", "org_id": org_id, "team_id": team_id})
        if self.raises:
            raise RuntimeError("readiness unavailable")
        return self.team_result


class _FakeMetricQueryResult:
    def __init__(self, refs: tuple[Any, ...]) -> None:
        self._refs = refs

    def contract_refs(self, scope: DevScope) -> tuple[Any, ...]:
        return self._refs


@dataclass
class _FakeMetricService:
    definitions: list[Any] = field(default_factory=list)
    failing_metric_ids: frozenset[str] = frozenset()
    query_calls: list[str] = field(default_factory=list)

    def list_metrics(self, scope):
        return tuple(self.definitions)

    async def query(self, org_id, permission_fingerprint, request, *, now=None):
        self.query_calls.append(request.metric_id)
        if request.metric_id in self.failing_metric_ids:
            raise RuntimeError(f"{request.metric_id} unavailable")
        return _FakeMetricQueryResult((f"ref:{request.metric_id}",))


def _accessor(
    *,
    status: _FakeStatusService | None = None,
    health: _FakeRuleService | None = None,
    workload: _FakeRuleService | None = None,
    readiness: _FakeReadinessService | None = None,
    metrics: _FakeMetricService | None = None,
) -> CanonicalEnrichmentAccessor:
    return CanonicalEnrichmentAccessor(
        status=status if status is not None else _FakeStatusService(),
        health=health if health is not None else _FakeRuleService(),
        workload=workload if workload is not None else _FakeRuleService(),
        readiness=readiness if readiness is not None else _FakeReadinessService(),
        metrics=metrics if metrics is not None else _FakeMetricService(),
    )


def _enrich(accessor: CanonicalEnrichmentAccessor, scope: DevScope):
    return asyncio.run(
        accessor.enrich(
            org_id=ORG_ID, permission_fingerprint=PERMISSION_FINGERPRINT, scope=scope
        )
    )


# ---------------------------------------------------------------------------
# status: always attempted, gap on failure
# ---------------------------------------------------------------------------


def test_status_is_returned_on_success() -> None:
    result = _enrich(
        _accessor(status=_FakeStatusService(result="snapshot")), _team_scope()
    )
    assert result.status == "snapshot"


def test_status_failure_is_a_disclosed_gap_not_a_crash() -> None:
    result = _enrich(_accessor(status=_FakeStatusService(raises=True)), _team_scope())
    assert result.status is EnrichmentGap.UNAVAILABLE


# ---------------------------------------------------------------------------
# health/workload: TEAM-only, and the branch happens BEFORE calling the
# service -- proven by asserting the fake was never invoked for a
# non-team/ambiguous scope, not by catching a raised ValueError.
# ---------------------------------------------------------------------------


def test_health_and_workload_are_evaluated_for_a_single_team_scope() -> None:
    health = _FakeRuleService(result="health-ok")
    workload = _FakeRuleService(result="workload-ok")
    result = _enrich(
        _accessor(health=health, workload=workload), _team_scope(team_id="team-9")
    )

    assert result.health == "health-ok"
    assert result.workload == "workload-ok"
    assert health.calls == [
        {"org_id": ORG_ID, "team_id": "team-9", "scope": health.calls[0]["scope"]}
    ]
    assert workload.calls[0]["team_id"] == "team-9"


def test_health_and_workload_are_not_applicable_for_a_project_scope() -> None:
    health = _FakeRuleService()
    workload = _FakeRuleService()
    result = _enrich(_accessor(health=health, workload=workload), _project_scope())

    assert result.health is EnrichmentGap.NOT_APPLICABLE
    assert result.workload is EnrichmentGap.NOT_APPLICABLE
    # The branch happens BEFORE calling the service -- never invoked, never
    # relying on it to raise and catching that instead.
    assert health.calls == []
    assert workload.calls == []


def test_health_failure_is_a_disclosed_gap() -> None:
    result = _enrich(_accessor(health=_FakeRuleService(raises=True)), _team_scope())
    assert result.health is EnrichmentGap.UNAVAILABLE


# ---------------------------------------------------------------------------
# readiness: PROJECT -> evaluate_project, single-team TEAM -> evaluate_team,
# neither -> NOT_APPLICABLE.
# ---------------------------------------------------------------------------


def test_readiness_uses_evaluate_project_for_a_project_scope() -> None:
    readiness = _FakeReadinessService(project_result="readiness-project")
    result = _enrich(_accessor(readiness=readiness), _project_scope())

    assert result.readiness == "readiness-project"
    assert readiness.calls == [{"kind": "project", "org_id": ORG_ID}]


def test_readiness_uses_evaluate_team_for_a_team_scope() -> None:
    readiness = _FakeReadinessService(team_result="readiness-team")
    result = _enrich(_accessor(readiness=readiness), _team_scope(team_id="team-5"))

    assert result.readiness == "readiness-team"
    assert readiness.calls == [{"kind": "team", "org_id": ORG_ID, "team_id": "team-5"}]


def test_readiness_is_not_applicable_for_neither_project_nor_team() -> None:
    readiness = _FakeReadinessService()
    other = _scope(direct_scope=DirectScope.ISSUE, entity_type=EntityType.ISSUE)

    result = _enrich(_accessor(readiness=readiness), other)

    assert result.readiness is EnrichmentGap.NOT_APPLICABLE
    assert readiness.calls == []


# ---------------------------------------------------------------------------
# metrics: every scope-applicable definition queried; one metric's failure
# never blanks the others (partial-failure isolation).
# ---------------------------------------------------------------------------


def test_metrics_pulls_every_scope_applicable_definition() -> None:
    metrics = _FakeMetricService(
        definitions=[
            SimpleNamespace(metric_id="items_completed"),
            SimpleNamespace(metric_id="avg_wip"),
        ]
    )

    result = _enrich(_accessor(metrics=metrics), _team_scope())

    assert set(result.metrics) == {"ref:items_completed", "ref:avg_wip"}
    assert metrics.query_calls == ["items_completed", "avg_wip"]


def test_one_failing_metric_does_not_blank_the_others() -> None:
    """Mutation-relevant property: a per-metric exception must be isolated
    to that metric, never abort the whole round."""

    metrics = _FakeMetricService(
        definitions=[
            SimpleNamespace(metric_id="items_completed"),
            SimpleNamespace(metric_id="avg_wip"),
            SimpleNamespace(metric_id="deployments_count"),
        ],
        failing_metric_ids=frozenset({"avg_wip"}),
    )

    result = _enrich(_accessor(metrics=metrics), _team_scope())

    assert set(result.metrics) == {"ref:items_completed", "ref:deployments_count"}
    # All three were attempted -- the failure did not short-circuit the loop.
    assert metrics.query_calls == ["items_completed", "avg_wip", "deployments_count"]


def test_no_applicable_metrics_returns_an_empty_tuple_not_a_gap() -> None:
    result = _enrich(
        _accessor(metrics=_FakeMetricService(definitions=[])), _team_scope()
    )
    assert result.metrics == ()
