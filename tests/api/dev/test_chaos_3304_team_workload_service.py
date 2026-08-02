"""Tests for CHAOS-3304's ``TeamWorkloadService``.

Mirrors ``test_chaos_3303_team_health_service.py``'s post-a37caf322
discipline exactly: proven against a fake ``PlanExecutorRuntime``, a fake
``TeamAttributionSource`` (returning ``TeamAttributionResult``), and a fake
``TeamWorkloadDataSource`` independently -- cohort_size is always
``len(team_repository_ids(...).repository_ids)`` when the lookup succeeded,
never a caller-supplied int, so an unattributed team (zero owned
repositories) suppresses every applicable rule even when every other source
returns otherwise-complete facts. A genuine lookup FAILURE
(``measured=False``) is structurally distinct from a measured-empty cohort
-- see ``TeamAttributionResult``'s own docstring.
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
from dev_health_ops.api.dev.native_status_change import TeamAttributionResult
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
        # TeamWorkloadService never mints evidence itself, so this is here
        # only for structural conformance.
        raise AssertionError("not exercised by this suite")


@dataclass
class FakeAttributionSource:
    """A configurable ``TeamAttributionSource`` double -- ``repository_ids``
    is the ONLY thing that determines ``cohort_size`` (see
    ``TeamWorkloadService.evaluate_workload``); there is no other lever a
    test (or a caller) has to influence it. ``measured=False`` simulates a
    genuine lookup failure, distinct from a measured-empty cohort (matches
    ``TeamAttributionResult``'s own contract, CHAOS-3303 round 2).
    """

    repository_ids: tuple[str, ...] = ()
    measured: bool = True
    as_of_calls: list[datetime] | None = None

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> TeamAttributionResult:
        if self.as_of_calls is not None:
            self.as_of_calls.append(as_of)
        return TeamAttributionResult(
            measured=self.measured, repository_ids=self.repository_ids
        )


_NO_ATTRIBUTION = FakeAttributionSource(repository_ids=())
_FULL_ATTRIBUTION = FakeAttributionSource(
    repository_ids=tuple(f"repo-{i}" for i in range(25))
)
_FAILED_ATTRIBUTION = FakeAttributionSource(measured=False)


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


class _UncallableRuntime:
    """A ``PlanExecutorRuntime`` double that fails loudly if called -- proves
    a ``measured=False`` attribution lookup short-circuits before the
    runtime is ever reached (mirrors ``test_chaos_3303_team_health_service.py``'s
    own ``_UncallableRuntime``).
    """

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError(
            "the runtime must never be called when attribution measurement "
            "itself failed -- cohort_size is unknowable, not merely small"
        )

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("unexpected call")

    def list_metrics(self, scope):
        raise AssertionError("unexpected call")

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError("unexpected call")

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("unexpected call")

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("unexpected call")

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
        raise AssertionError("unexpected call")


class _UncallableWorkloadSource:
    """A ``TeamWorkloadDataSource`` double that fails loudly if called --
    same proof as ``_UncallableRuntime``, for the workload-specific port.
    """

    async def cognitive_load(self, *, org_id, team_id, start, end):
        raise AssertionError("unexpected call")

    async def active_contributor_count(self, *, org_id, team_id, start, end):
        raise AssertionError("unexpected call")

    async def investment_mix(self, *, org_id, team_id, start, end):
        raise AssertionError("unexpected call")


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
    facts -- cohort_size is genuinely derived from the attribution source
    (0, not fabricated), never inferable from "the sources look fine".

    Every finding here actually suppresses as ``missing_attribution``
    rather than ``insufficient_cohort`` (Codex-confirmed finding, round 2,
    2026-08-02): attribution is now the CONTROLLING guard, checked before
    cohort, and every CHAOS-3304 adapter reports ``attribution_present=
    False`` unconditionally (CHAOS-3331) -- so it always fires first,
    regardless of cohort_size. ``cohort_size`` is still asserted directly
    on the observation (never on which specific guard fired) to preserve
    the "genuinely derived, not fabricated" property this test exists to
    prove.
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
    after_hours_observation = next(
        o for o in profile.observations if o.current_value == 0.4
    )
    assert after_hours_observation.cohort_size == 0

    after_hours = next(
        f
        for f in profile.shadow_findings + profile.suppressed_findings
        if f.rule_id == "health_rule.after_hours_pressure_sustained.v1"
    )
    assert after_hours.state is DimensionState.UNKNOWN
    assert after_hours.suppressed_reason == "missing_attribution"


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
async def test_evaluate_workload_with_attribution_and_real_facts_still_gates_on_chaos_3331() -> (
    None
):
    """Even with COMPLETE, well-attributed data -- a full cohort, genuinely
    measured cognitive-load and investment facts, both current and
    comparison windows fetched -- all four CHAOS-3304 findings still
    suppress as ``UNKNOWN``/``missing_attribution``, never a fabricated
    measured result (Codex-confirmed finding, round 2, 2026-08-02): every
    adapter unconditionally reports ``attribution_present=False``
    (CHAOS-3331), and attribution is now the CONTROLLING guard, checked
    before cohort/sample/coverage/denominator -- so it fires first
    regardless of how complete everything else is. This positive-control
    test's ORIGINAL premise (that investment_allocation_shift.v1 was
    CHAOS-3331-exempt and could reach a genuine HEALTHY finding) was
    itself the round-2 Codex finding: that exemption failed open and is
    corrected here, not merely in the source module.
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
    assert profile.launch_findings == ()
    for rule_id in (
        "health_rule.after_hours_pressure_sustained.v1",
        "health_rule.review_request_load_pressure.v1",
        "health_rule.pr_interruption_load_pressure.v1",
        "health_rule.investment_allocation_shift.v1",
    ):
        finding = next(f for f in profile.suppressed_findings if f.rule_id == rule_id)
        assert finding.state is DimensionState.UNKNOWN
        assert finding.suppressed_reason == "missing_attribution"

    # The cohort itself is still genuinely resolved (25, not fabricated) --
    # attribution is what gates the finding, not a missing cohort.
    assert all(observation.cohort_size == 25 for observation in profile.observations)

    # Both current and comparison windows were still fetched, sequentially
    # -- CHAOS-3331 gates the FINDING, not the data collection itself.
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
async def test_evaluate_workload_attribution_failure_is_unmeasured_not_insufficient_cohort() -> (
    None
):
    """A genuine attribution-lookup FAILURE (``measured=False``) must never
    collapse into the shape a genuinely empty, successfully resolved cohort
    produces -- see the module docstring's attribution-snapshot discipline.
    Proven both ways at the ``cohort_size`` level (the honest signal,
    checked directly on each observation rather than via a finding's
    ``suppressed_reason`` -- see below for why): the failure path carries
    no ``suppressed_reason`` at all and ``cohort_size=None`` throughout,
    AND never even calls the runtime/workload source (nothing meaningful
    could be reported regardless of what they'd return); the empty-cohort
    path carries a genuinely resolved ``cohort_size=0``.

    The empty-cohort path's FINDING itself reports
    ``missing_attribution``, not ``insufficient_cohort`` (Codex-confirmed
    finding, round 2, 2026-08-02): attribution is now the CONTROLLING
    guard, checked before cohort, and every CHAOS-3304 adapter reports
    ``attribution_present=False`` unconditionally (CHAOS-3331) -- so it
    always fires first regardless of cohort_size. This is why the
    cohort_size=0-vs-None distinction is asserted on the raw observation,
    not inferred from which reason string a finding happens to carry.
    """

    failing_service = TeamWorkloadService(
        _UncallableRuntime(), _FAILED_ATTRIBUTION, _UncallableWorkloadSource()
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
        assert finding.suppressed_reason != "insufficient_cohort"
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
        for f in empty_cohort_profile.suppressed_findings
        if f.rule_id == "health_rule.after_hours_pressure_sustained.v1"
    )
    assert after_hours.state is DimensionState.UNKNOWN
    assert after_hours.suppressed_reason == "missing_attribution"
    assert any(
        observation.cohort_size == 0
        for observation in empty_cohort_profile.observations
    )
