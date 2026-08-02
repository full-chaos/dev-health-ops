"""Tests for CHAOS-3304's ``TeamWorkloadService``.

Mirrors ``test_chaos_3303_team_health_service.py``'s post-03da63aeb
discipline exactly: proven against a fake ``PlanExecutorRuntime``, a fake
``TeamAttributionSource``, and a fake ``TeamWorkloadDataSource``
independently -- cohort_size is always ``len(team_repository_ids(...))``,
never a caller-supplied int, so an unattributed team (zero owned
repositories) suppresses every applicable rule even when every other source
returns otherwise-complete facts.
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
from dev_health_ops.api.dev.native_team_workload import (
    TeamCognitiveLoadResult,
    TeamInvestmentMixResult,
)
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    StatusResultState,
    StatusSnapshotResult,
)
from dev_health_ops.api.dev.team_workload_service import TeamWorkloadService

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_ORG_ID = "org-1"


def _team_scope(*, team_id: str = "team-1", with_comparison: bool = False) -> DevScope:
    comparison_range = None
    if with_comparison:
        comparison_range = DevTimeRange(
            start=_NOW - timedelta(days=28),
            end=_NOW - timedelta(days=14),
            timezone="UTC",
        )
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
        comparison_range=comparison_range,
    )


def _fail_closed_snapshot() -> StatusSnapshotResult:
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


_UNMEASURED_COGNITIVE_LOAD = TeamCognitiveLoadResult(
    after_hours_commit_ratio=None,
    weekend_commit_ratio=None,
    pr_interruption_load=None,
    review_request_load=None,
    context_spread_count=None,
    sample_days=0,
    measured=False,
)

_UNMEASURED_INVESTMENT_MIX = TeamInvestmentMixResult(
    new_value_units=0.0,
    ktlo_units=0.0,
    security_units=0.0,
    infra_units=0.0,
    unclassified_units=0.0,
    total_units=0.0,
    measured=False,
)


@dataclass
class FakeRuntime:
    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return _fail_closed_snapshot()

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError(
            "TeamWorkloadService must never query CHANGE_FAILURE_RATE for a "
            "team subject -- the metric does not support DirectScope.TEAM"
        )

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        return DataHealthResult(sources=(), complete_eligible=False)


@dataclass
class FakeAttributionSource:
    """A configurable ``TeamAttributionSource`` double -- ``repository_ids``
    is the ONLY thing that determines ``cohort_size`` (see
    ``TeamWorkloadService.evaluate_workload``); there is no other lever a
    test (or a caller) has to influence it.
    """

    repository_ids: tuple[str, ...] = ()
    as_of_calls: list[datetime] | None = None

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> list[str]:
        if self.as_of_calls is not None:
            self.as_of_calls.append(as_of)
        return list(self.repository_ids)


@dataclass
class FakeFailingAttributionSource:
    """A ``TeamAttributionSource`` double whose lookup genuinely fails --
    distinct from ``FakeAttributionSource(repository_ids=())``, which
    succeeds with a real, empty answer."""

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> list[str]:
        raise RuntimeError("attribution lookup failed")


_NO_ATTRIBUTION = FakeAttributionSource(repository_ids=())
_FULL_ATTRIBUTION = FakeAttributionSource(
    repository_ids=tuple(f"repo-{i}" for i in range(25))
)


@dataclass
class FakeUnmeasuredWorkloadSource:
    async def cognitive_load(self, *, org_id, team_id, start, end):
        return _UNMEASURED_COGNITIVE_LOAD

    async def active_contributor_count(self, *, org_id, team_id, start, end):
        return None

    async def investment_mix(self, *, org_id, team_id, start, end):
        return _UNMEASURED_INVESTMENT_MIX


@dataclass
class FakeMeasuredWorkloadSource:
    """A team with genuinely measured, attributed cognitive-load and
    investment facts -- and NO active-contributor count resolved (the
    "missing team membership but valid own-history baseline" case)."""

    calls: list[tuple[datetime, datetime]]

    async def cognitive_load(self, *, org_id, team_id, start, end):
        self.calls.append((start, end))
        if len(self.calls) == 1:
            return TeamCognitiveLoadResult(
                after_hours_commit_ratio=0.4,  # above threshold=0.25
                weekend_commit_ratio=0.1,
                pr_interruption_load=12.0,
                review_request_load=15.0,
                context_spread_count=4.0,
                sample_days=14,
                measured=True,
            )
        return TeamCognitiveLoadResult(
            after_hours_commit_ratio=0.1,
            weekend_commit_ratio=0.05,
            pr_interruption_load=6.0,
            review_request_load=8.0,
            context_spread_count=2.0,
            sample_days=14,
            measured=True,
        )

    async def active_contributor_count(self, *, org_id, team_id, start, end):
        return None

    async def investment_mix(self, *, org_id, team_id, start, end):
        return TeamInvestmentMixResult(
            new_value_units=40.0,
            ktlo_units=30.0,
            security_units=10.0,
            infra_units=10.0,
            unclassified_units=10.0,
            total_units=100.0,
            measured=True,
        )


def _project_scope() -> DevScope:
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
    )


@pytest.mark.asyncio
async def test_evaluate_workload_rejects_non_team_scope() -> None:
    service = TeamWorkloadService(
        FakeRuntime(), _NO_ATTRIBUTION, FakeUnmeasuredWorkloadSource()
    )
    with pytest.raises(ValueError, match="team direct scope"):
        await service.evaluate_workload(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=_project_scope(),
            team_id="team-1",
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_workload_rejects_mismatched_team_id() -> None:
    service = TeamWorkloadService(
        FakeRuntime(), _NO_ATTRIBUTION, FakeUnmeasuredWorkloadSource()
    )
    with pytest.raises(ValueError, match="team_ids must name exactly"):
        await service.evaluate_workload(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=_team_scope(team_id="team-1"),
            team_id="team-2",
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_workload_without_attribution_never_reports_healthy() -> None:
    """Zero resolved ``team_repo_ownership`` rows (cohort_size derives to 0):
    every applicable rule requiring a minimum cohort must suppress -- never
    a fabricated healthy/at-risk finding for an unattributed team.
    """

    service = TeamWorkloadService(
        FakeRuntime(), _NO_ATTRIBUTION, FakeUnmeasuredWorkloadSource()
    )
    profile = await service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    assert profile.launch_findings == ()
    assert profile.suppressed_findings == ()
    assert profile.shadow_findings
    for finding in profile.shadow_findings:
        assert finding.state in (DimensionState.UNKNOWN, DimensionState.NOT_APPLICABLE)


@pytest.mark.asyncio
async def test_evaluate_workload_zero_attribution_suppresses_even_with_real_facts() -> (
    None
):
    """Codex finding (HIGH, 2026-08-02), the exact repro shape reproduced
    for the workload service: an unattributed team must stay suppressed
    even when the workload source itself returns otherwise-complete, real
    facts -- cohort_size is genuinely derived from the attribution source,
    never inferable from "the sources look fine".
    """

    source = FakeMeasuredWorkloadSource(calls=[])
    service = TeamWorkloadService(FakeRuntime(), _NO_ATTRIBUTION, source)
    profile = await service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    after_hours = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.after_hours_pressure_sustained.v1"
    )
    assert after_hours.state is DimensionState.UNKNOWN
    assert after_hours.suppressed_reason == "insufficient_cohort"


@pytest.mark.asyncio
async def test_evaluate_workload_unmeasured_source_reports_unknown_not_healthy() -> (
    None
):
    service = TeamWorkloadService(
        FakeRuntime(), _FULL_ATTRIBUTION, FakeUnmeasuredWorkloadSource()
    )
    profile = await service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    for rule_id in (
        "health_rule.after_hours_pressure_sustained.v1",
        "health_rule.review_request_load_pressure.v1",
        "health_rule.pr_interruption_load_pressure.v1",
        "health_rule.investment_allocation_shift.v1",
    ):
        finding = next(f for f in profile.shadow_findings if f.rule_id == rule_id)
        assert finding.state is DimensionState.UNKNOWN


@pytest.mark.asyncio
async def test_evaluate_workload_with_attribution_and_real_facts_reports_measured_findings() -> (
    None
):
    """Positive control: real, attributed team facts and an attribution
    source reporting real owned repositories clearing every applicable
    rule's minimum cohort produce genuinely measured dimensions -- not
    honestly-unknown-by-construction.
    """

    source = FakeMeasuredWorkloadSource(calls=[])
    service = TeamWorkloadService(FakeRuntime(), _FULL_ATTRIBUTION, source)
    profile = await service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(with_comparison=True),
        team_id="team-1",
        now=_NOW,
    )
    after_hours = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.after_hours_pressure_sustained.v1"
    )
    assert after_hours.state is DimensionState.WATCH  # 0.4 >= threshold 0.25

    # No active-contributor count resolved -> burden is not calculable, but
    # the raw pressure was still genuinely measured (never dropped).
    review_load = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.review_request_load_pressure.v1"
    )
    assert review_load.state is DimensionState.UNKNOWN
    assert review_load.suppressed_reason == "missing_denominator"

    investment_shift = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.investment_allocation_shift.v1"
    )
    # Stable mix across both fetched windows in this fixture -> genuinely
    # measured zero shift, not a value judgment about the mix's composition.
    assert investment_shift.state is DimensionState.HEALTHY

    # Both current and comparison windows were fetched, sequentially.
    assert len(source.calls) == 2


@pytest.mark.asyncio
async def test_evaluate_workload_without_comparison_range_skips_comparison_fetch() -> (
    None
):
    source = FakeMeasuredWorkloadSource(calls=[])
    service = TeamWorkloadService(FakeRuntime(), _FULL_ATTRIBUTION, source)
    await service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(with_comparison=False),
        team_id="team-1",
        now=_NOW,
    )
    assert len(source.calls) == 1


@pytest.mark.asyncio
async def test_evaluate_workload_resolves_attribution_at_scope_window_end_not_now() -> (
    None
):
    """Attribution-snapshot discipline: evaluating a two-week-old scope
    three days after the fact must attribute against the roster that was
    true at ``scope.time_range.end`` -- never against today's wall-clock
    ``now``, which could have drifted from it.
    """

    attribution = FakeAttributionSource(
        repository_ids=tuple(f"repo-{i}" for i in range(25)), as_of_calls=[]
    )
    service = TeamWorkloadService(
        FakeRuntime(), attribution, FakeUnmeasuredWorkloadSource()
    )
    scope = _team_scope()
    evaluated_three_days_later = scope.time_range.end + timedelta(days=3)
    await service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=scope,
        team_id="team-1",
        now=evaluated_three_days_later,
    )
    assert attribution.as_of_calls == [scope.time_range.end]
    assert evaluated_three_days_later not in attribution.as_of_calls


@pytest.mark.asyncio
async def test_evaluate_workload_attribution_lookup_failure_is_distinct_from_empty_cohort() -> (
    None
):
    """A failed attribution lookup must never collapse into the
    ``insufficient_cohort`` shape a genuinely empty, successfully resolved
    cohort produces -- see the module docstring's attribution-snapshot
    discipline. Proven both ways: the failure path carries no
    ``suppressed_reason`` and ``cohort_size=None``; the empty-cohort path
    carries ``suppressed_reason="insufficient_cohort"`` and a real
    ``cohort_size=0``.
    """

    failing_service = TeamWorkloadService(
        FakeRuntime(),
        FakeFailingAttributionSource(),
        FakeMeasuredWorkloadSource(calls=[]),
    )
    failure_profile = await failing_service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    assert failure_profile.launch_findings == ()
    assert failure_profile.suppressed_findings == ()
    assert failure_profile.shadow_findings
    for finding in failure_profile.shadow_findings:
        assert finding.state is DimensionState.UNKNOWN
        assert finding.suppressed_reason is None
    for observation in failure_profile.observations:
        assert observation.cohort_size is None

    empty_cohort_service = TeamWorkloadService(
        FakeRuntime(), _NO_ATTRIBUTION, FakeMeasuredWorkloadSource(calls=[])
    )
    empty_cohort_profile = await empty_cohort_service.evaluate_workload(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    after_hours = next(
        f
        for f in empty_cohort_profile.shadow_findings
        if f.rule_id == "health_rule.after_hours_pressure_sustained.v1"
    )
    assert after_hours.state is DimensionState.UNKNOWN
    assert after_hours.suppressed_reason == "insufficient_cohort"
    assert any(
        observation.cohort_size == 0
        for observation in empty_cohort_profile.observations
    )
