"""Tests for CHAOS-3303's ``TeamHealthService``.

Proven against a fake ``PlanExecutorRuntime`` AND a fake
``TeamAttributionSource`` independently, not by special-casing team scope
inside the service itself:

* a runtime that returns real, fresh team-scoped facts (mirroring
  ``native_status_change.py``'s now-landed ``team_repo_ownership``
  re-derivation and its real 'team' SQL arms) produces real findings, but
  ONLY when the attribution source also reports real owned repositories;
* a runtime that returns the fail-closed shape (a source genuinely
  unavailable) must never fabricate a healthy/zero finding out of it;
* an attribution source reporting zero owned repositories must suppress
  every applicable rule as insufficient_cohort, even when the runtime
  itself returns otherwise-complete facts -- there is no caller-supplied
  cohort_size left to assert around this (Codex finding, HIGH,
  2026-08-02): cohort_size is always ``len(team_repository_ids(...))``.
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
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    IncidentFact,
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
        # CHAOS-3296's PlanExecutorRuntime protocol member -- TeamHealthService
        # (CHAOS-3302/3303) never mints evidence itself, so this is here only
        # for structural conformance, same posture as the sibling
        # never-called methods above.
        raise AssertionError("not exercised by this suite")


@dataclass
class FakeAttributionSource:
    """A configurable ``TeamAttributionSource`` double -- ``repository_ids``
    is the ONLY thing that determines ``cohort_size`` (see
    ``TeamHealthService.evaluate_team``); there is no other lever a test
    (or a caller) has to influence it. ``measured=False`` simulates a
    genuine lookup failure, distinct from a measured-empty cohort (Codex
    finding, MEDIUM, 2026-08-02).
    """

    repository_ids: tuple[str, ...] = ()
    measured: bool = True
    calls: list[tuple[str, str, datetime]] | None = None

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> TeamAttributionResult:
        if self.calls is not None:
            self.calls.append((org_id, team_id, as_of))
        return TeamAttributionResult(
            measured=self.measured, repository_ids=self.repository_ids
        )


_NO_ATTRIBUTION = FakeAttributionSource(repository_ids=())
_FULL_ATTRIBUTION = FakeAttributionSource(
    repository_ids=tuple(f"repo-{i}" for i in range(25))
)
_FAILED_ATTRIBUTION = FakeAttributionSource(measured=False)


@pytest.mark.asyncio
async def test_evaluate_team_rejects_non_team_scope() -> None:
    service = TeamHealthService(FakeTeamRuntime(), _NO_ATTRIBUTION)
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
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_team_rejects_mismatched_team_id() -> None:
    service = TeamHealthService(FakeTeamRuntime(), _NO_ATTRIBUTION)
    with pytest.raises(ValueError, match="team_ids must name exactly"):
        await service.evaluate_team(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=_team_scope(team_id="team-1"),
            team_id="team-2",
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_team_never_queries_change_failure_rate_metric() -> None:
    """CHANGE_FAILURE_RATE has no supported_scopes entry for TEAM -- this
    must be a structural not_applicable, never an attempted, failing call.
    """

    service = TeamHealthService(FakeTeamRuntime(), _FULL_ATTRIBUTION)
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    finding = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.change_failure_rate.v1"
    )
    assert finding.state == DimensionState.UNKNOWN


@pytest.mark.asyncio
async def test_evaluate_team_without_attribution_never_reports_healthy() -> None:
    """With zero resolved ``team_repo_ownership`` rows (cohort_size derives
    to 0), every applicable rule requiring a minimum cohort must suppress --
    never a fabricated healthy/at-risk finding for an unattributed team.
    """

    service = TeamHealthService(FakeTeamRuntime(), _NO_ATTRIBUTION)
    profile = await service.evaluate_team(
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
async def test_evaluate_team_zero_attribution_suppresses_even_with_real_facts() -> None:
    """Codex finding (HIGH, 2026-08-02), the exact repro shape: an
    unattributed team (zero owned repositories) must stay suppressed even
    when the runtime itself returns otherwise-complete, real facts --
    proving cohort_size is genuinely derived from the attribution source,
    never inferable from "the sources look fine" or any other signal.
    """

    service = TeamHealthService(FakeAttributedTeamRuntime(), _NO_ATTRIBUTION)
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    # health_rule_registry._evaluate_with_registry partitions
    # suppressed_reason before shadow_only (Codex-confirmed finding, round
    # 2, 2026-08-02) -- a genuinely suppressed provisional finding lands in
    # suppressed_findings, not shadow_findings.
    incident_finding = next(
        f
        for f in profile.suppressed_findings
        if f.rule_id == "health_rule.incident_load.v1"
    )
    assert incident_finding.state == DimensionState.UNKNOWN
    assert incident_finding.suppressed_reason == "insufficient_cohort"


@pytest.mark.asyncio
async def test_evaluate_team_with_attribution_stays_honest_when_runtime_fails_closed() -> (
    None
):
    """Even with real attribution (cohort_size >= minimum), a runtime that
    hands back the fail-closed shape (e.g. a genuinely unresolved
    ``team_repo_ownership``-derived read) must keep every status-derived
    dimension unknown -- the service must never treat "cohort size is fine"
    as license to assume the underlying facts are too.
    """

    service = TeamHealthService(FakeTeamRuntime(), _FULL_ATTRIBUTION)
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    incident_finding = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.incident_load.v1"
    )
    assert incident_finding.state == DimensionState.UNKNOWN


def _real_snapshot() -> StatusSnapshotResult:
    """Mirrors what ``native_status_change.py`` now genuinely returns for a
    team with resolved ``team_repo_ownership`` rows: real incident facts and
    an overall ``COMPLETE`` state (``declared_optional`` now includes TEAM,
    so the absent single declared status no longer degrades the result).
    """

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
            conflicts=(),
            source_ref_ids=(),
            evidence_ref_ids=(),
        ),
        children=(),
        blockers=(),
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=(
            IncidentFact(
                entity_id="inc-1",
                display_label="inc-1",
                status="open",
                active=True,
                blocking=False,
                observed_at=_NOW,
                source_ref_id="ref-1",
                evidence_ref_ids=(),
            ),
        ),
        source_refs=(),
        warnings=(),
    )


@dataclass
class FakeAttributedTeamRuntime:
    """A team WITH resolved team_repo_ownership rows: real facts, no gap."""

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return _real_snapshot()

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
async def test_evaluate_team_with_attribution_and_real_facts_reports_real_findings() -> (
    None
):
    """The positive control: once the runtime returns real team-scoped
    facts (mirroring the now-landed native_status_change.py team arms) and
    the attribution source reports real owned repositories clearing every
    applicable rule's minimum cohort, the incident_load dimension is a
    genuinely measured finding -- not honestly-unknown-by-construction like
    the fail-closed/unattributed tests above.
    """

    service = TeamHealthService(FakeAttributedTeamRuntime(), _FULL_ATTRIBUTION)
    profile = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    incident_finding = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.incident_load.v1"
    )
    # 1 incident, sample_count=1 clears minimum_sample=1, value 1.0 <
    # threshold 10.0 -- a real, measured healthy finding.
    assert incident_finding.state == DimensionState.HEALTHY
    assert incident_finding.suppressed_reason is None


@dataclass
class _UncallableRuntime:
    """A runtime that fails the test immediately if any method is called --
    proves ``evaluate_team`` short-circuits on a failed attribution lookup
    rather than proceeding to fetch facts it cannot responsibly attach a
    cohort to.
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


@pytest.mark.asyncio
async def test_evaluate_team_attribution_failure_is_unmeasured_not_insufficient_cohort() -> (
    None
):
    """Codex finding (MEDIUM, 2026-08-02): a genuine attribution-lookup
    FAILURE must be distinct from a measured-empty cohort. Both currently
    suppress via the same UNKNOWN state (evaluate_rule's own no_data/
    not_measured short-circuit fires before the cohort guard even runs --
    see health_rule_registry.evaluate_rule), but a failure must never carry
    ``suppressed_reason="insufficient_cohort"`` (a claim that a real,
    measured zero was found) and the runtime must never even be called.
    """

    service = TeamHealthService(_UncallableRuntime(), _FAILED_ATTRIBUTION)
    profile = await service.evaluate_team(
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
        assert finding.suppressed_reason != "insufficient_cohort"


@pytest.mark.asyncio
async def test_evaluate_team_resolves_attribution_at_scope_end_not_wall_clock_now() -> (
    None
):
    """Codex finding (MEDIUM, 2026-08-02), the exact repro shape: an
    ownership row valid at ``scope.time_range.end`` but since expired by
    wall-clock ``now`` must not wrongly zero the cohort. This service must
    resolve attribution at ``scope.time_range.end``, never ``now`` -- proven
    here with a ``now`` far past the scope window.
    """

    recorded_calls: list[tuple[str, str, datetime]] = []
    attribution = FakeAttributionSource(
        repository_ids=("repo-a",), calls=recorded_calls
    )
    scope = _team_scope()
    much_later_now = scope.time_range.end + timedelta(days=90)
    assert much_later_now != scope.time_range.end

    service = TeamHealthService(FakeAttributedTeamRuntime(), attribution)
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=scope,
        team_id="team-1",
        now=much_later_now,
    )

    assert len(recorded_calls) == 1
    _org_id, _team_id, as_of = recorded_calls[0]
    assert as_of == scope.time_range.end
    assert as_of != much_later_now
