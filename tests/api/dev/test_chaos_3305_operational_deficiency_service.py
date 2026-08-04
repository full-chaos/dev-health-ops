"""Tests for CHAOS-3305's ``OperationalDeficiencyService``.

A fake ``PlanExecutorRuntime`` double stands in for the exact seam
CHAOS-3295 built and ``production_runtime.py`` already wires -- mirrors
``test_chaos_3303_project_health_service.py``'s own fixture pattern, so
this service is proven against the same canonical contract, not a second
bespoke query path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DEFICIENCY_CATEGORIES,
    DeficiencyCategory,
    DeficiencyEvidenceClassification,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationState,
    DimensionObservation,
    DimensionState,
    HealthDimension,
    HealthRuleFinding,
    RuleApplicability,
)
from dev_health_ops.api.dev.data_health_service import (
    NATIVE_EVIDENCE_SOURCES,
    DataHealthResult,
    DataHealthSource,
    DataHealthState,
)
from dev_health_ops.api.dev.health_profile_synthesis import HealthProfileResult
from dev_health_ops.api.dev.metrics.definitions import get_metric
from dev_health_ops.api.dev.metrics.service import (
    MetricDataState,
    MetricQueryResult,
    MetricQueryValue,
)
from dev_health_ops.api.dev.native_status_change import TeamAttributionResult
from dev_health_ops.api.dev.native_team_workload import (
    TeamCognitiveLoadResult,
    TeamInvestmentMixResult,
)
from dev_health_ops.api.dev.operational_deficiency_service import (
    _INVESTMENT_BALANCE_NOT_APPLICABLE_TO_PROJECT_LIMITATION,
    _RULE_DRIVEN_PARTIAL_LIMITATION,
    _RULE_DRIVEN_SHADOW_AND_SUPPRESSED_LIMITATION,
    _RULE_DRIVEN_SHADOW_LIMITATION,
    _RULE_DRIVEN_SUPPRESSED_LIMITATION,
    _TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION,
    OperationalDeficiencyService,
    _dedupe_findings,
    _merge_data_health_sources,
    _mint_deficiency_finding_id,
    _rule_driven_category_result,
    _rule_driven_results,
)
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    ConflictSeverity,
    StatusConflict,
    StatusFact,
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


def _team_scope(
    *, end: datetime | None = None, with_comparison: bool = False
) -> DevScope:
    resolved_end = end or _NOW
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.TEAM,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM, entity_id="team-1", display_label="Team 1"
            )
        ],
        team_ids=["team-1"],
        time_range=DevTimeRange(
            start=resolved_end - timedelta(days=14), end=resolved_end, timezone="UTC"
        ),
        comparison_range=(
            DevTimeRange(
                start=resolved_end - timedelta(days=28),
                end=resolved_end - timedelta(days=14),
                timezone="UTC",
            )
            if with_comparison
            else None
        ),
    )


def _actual_completion(
    *,
    reason_codes: tuple[str, ...] = (),
    required_children: tuple[StatusFact, ...] = (),
    conflicts: tuple[StatusConflict, ...] = (),
) -> ActualCompletion:
    return ActualCompletion(
        state=CompletionState.NOT_READY if reason_codes else CompletionState.READY,
        rule_id="actual-completion",
        rule_version="actual-completion.v4",
        reason_codes=reason_codes,
        required_children=required_children,
        required_child_total=len(required_children),
        required_child_complete=len(required_children),
        display_truncated=False,
        conflicts=conflicts,
        source_ref_ids=(),
        evidence_ref_ids=(),
    )


def _snapshot(
    *,
    state: StatusResultState = StatusResultState.COMPLETE,
    actual: ActualCompletion | None = None,
    blockers: tuple[StatusFact, ...] = (),
) -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=state,
        scope=None,  # type: ignore[arg-type]
        as_of=_NOW,
        declared=None,
        actual=actual or _actual_completion(),
        children=(),
        blockers=blockers,
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=(),
        source_refs=(),
        warnings=(),
    )


def _data_health(sources: tuple[DataHealthSource, ...] = ()) -> DataHealthResult:
    complete_eligible = all(
        not s.required or s.state is DataHealthState.COMPLETE for s in sources
    )
    return DataHealthResult(sources=sources, complete_eligible=complete_eligible)


def _source(
    system: str, state: DataHealthState, *, required: bool = True, coverage: float = 1.0
) -> DataHealthSource:
    return DataHealthSource(
        source_system=system,
        state=state,
        required=required,
        last_successful_at=_NOW,
        watermark=_NOW,
        missing_repository_ids=(),
        missing_entity_ids=(),
        coverage=coverage,
        confidence_impact=None,
        freshness_policy_version="v1",
    )


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
    data_health_result: DataHealthResult
    status_snapshot_result: StatusSnapshotResult
    data_health_scopes: list[DevScope] = field(default_factory=list)

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return self.status_snapshot_result

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        return _metric_result()

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        self.data_health_scopes.append(scope)
        return self.data_health_result

    def mint_evidence(self, **kwargs):
        raise NotImplementedError


def _runtime(
    *,
    sources: tuple[DataHealthSource, ...] = (),
    snapshot: StatusSnapshotResult | None = None,
) -> FakeRuntime:
    return FakeRuntime(
        data_health_result=_data_health(sources),
        status_snapshot_result=snapshot or _snapshot(),
    )


@dataclass
class FakeAttributionSource:
    """A configurable ``TeamAttributionSource`` double -- mirrors
    ``test_chaos_3303_team_health_service.py``'s own fixture exactly
    (a37caf322): ``measured=False`` simulates a genuine lookup failure,
    distinct from a measured-empty cohort. ``repository_ids`` is the only
    thing that determines ``cohort_size`` when ``measured=True``.
    """

    repository_ids: tuple[str, ...] = ()
    measured: bool = True
    calls: list[tuple[str, str, datetime]] = field(default_factory=list)

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> TeamAttributionResult:
        self.calls.append((org_id, team_id, as_of))
        return TeamAttributionResult(
            measured=self.measured, repository_ids=self.repository_ids
        )


_NO_ATTRIBUTION = FakeAttributionSource(repository_ids=())


def _unmeasured_cognitive_load() -> TeamCognitiveLoadResult:
    return TeamCognitiveLoadResult(
        after_hours_commit_ratio=None,
        weekend_commit_ratio=None,
        pr_interruption_load=None,
        review_request_load=None,
        context_spread_count=None,
        sample_days=0,
        measured=False,
    )


def _unmeasured_investment_mix() -> TeamInvestmentMixResult:
    return TeamInvestmentMixResult(
        new_value_units=0.0,
        ktlo_units=0.0,
        security_units=0.0,
        infra_units=0.0,
        unclassified_units=0.0,
        total_units=0.0,
        measured=False,
    )


def _measured_investment_mix(
    *, new_value_units: float, total_units: float
) -> TeamInvestmentMixResult:
    return TeamInvestmentMixResult(
        new_value_units=new_value_units,
        ktlo_units=total_units - new_value_units,
        security_units=0.0,
        infra_units=0.0,
        unclassified_units=0.0,
        total_units=total_units,
        measured=True,
    )


@dataclass
class FakeWorkloadSource:
    """A configurable ``TeamWorkloadDataSource`` double.

    Defaults to entirely unmeasured results for every method -- the
    neutral "no workload data was even attempted" shape every test that
    predates CHAOS-3304's wiring (categories 1-6, never 8) needs
    unchanged. ``investment_mix_results`` supplies genuinely measured
    results IN CALL ORDER (``_investment_balance_profile`` calls
    ``investment_mix`` for the current window first, then the comparison
    window if ``scope.comparison_range`` is set) -- category-8-specific
    tests use this to prove a real, non-``no_data`` shift gets computed
    (see ``dimension_observation_adapters.investment_allocation_shift_
    observation``'s own ``comparison_share is None -> no_data`` branch,
    which this double must clear to reach the ``attribution_present=False``
    guard the CHAOS-3331 tests below assert against). ``raise_on_
    investment_mix`` simulates an expected dependency failure (Codex
    finding, HIGH, round 5) -- ``cognitive_load``/``active_contributor_
    count`` are never called by ``_investment_balance_profile`` at all
    (investment-only path), so this double does not need a failure mode
    for them.
    """

    investment_mix_results: tuple[TeamInvestmentMixResult, ...] = ()
    raise_on_investment_mix: bool = False
    calls: list[tuple[str, str, str, datetime, datetime]] = field(default_factory=list)
    #: A DEDICATED counter, never derived by filtering ``self.calls`` after
    #: the fact (Codex finding, MEDIUM, round 6 -- the second time this
    #: exact class of bug has come back): ``self.calls`` is the complete,
    #: unfiltered, order-preserving log every test asserts against
    #: directly. Deriving ``investment_mix``'s own result index from a
    #: filtered view of that log would silently keep working even if an
    #: unexpected ``cognitive_load``/``active_contributor_count`` call got
    #: interleaved -- exactly the bug class a filtered view exists to hide.
    _investment_mix_call_count: int = field(default=0, init=False, repr=False)

    async def cognitive_load(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> TeamCognitiveLoadResult:
        self.calls.append(("cognitive_load", org_id, team_id, start, end))
        return _unmeasured_cognitive_load()

    async def active_contributor_count(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> int | None:
        self.calls.append(("active_contributor_count", org_id, team_id, start, end))
        return None

    async def investment_mix(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> TeamInvestmentMixResult:
        index = self._investment_mix_call_count
        self._investment_mix_call_count += 1
        self.calls.append(("investment_mix", org_id, team_id, start, end))
        if self.raise_on_investment_mix:
            raise RuntimeError("simulated investment_mix dependency failure")
        if index < len(self.investment_mix_results):
            return self.investment_mix_results[index]
        return _unmeasured_investment_mix()


_NO_WORKLOAD = FakeWorkloadSource()


@pytest.mark.asyncio
async def test_fake_workload_source_call_log_mutation_control_catches_interleaved_call() -> (
    None
):
    """Permanent mutation check (Codex finding, MEDIUM, round 6, 2026-08-02
    -- the second time this exact test-honesty class has come back): proves
    the exact-equality, complete, unfiltered call-log assertion pattern
    every test in this file relies on for ``FakeWorkloadSource`` ACTUALLY
    catches an interleaved, unexpected call -- not merely that a filtered/
    counted view happens to still pass. Directly exercises
    ``FakeWorkloadSource`` (never through ``evaluate_team``, which never
    calls ``cognitive_load`` at all in production) to plant Codex's exact
    repro: one ``cognitive_load`` call sandwiched between two
    ``investment_mix`` calls.

    Also proves the SECOND, independently-tracked half of the round-6 fix:
    ``investment_mix``'s own result-index counter (``_investment_mix_call_
    count``) is untouched by the interleaved ``cognitive_load`` call --
    the second ``investment_mix`` call still returns
    ``investment_mix_results[1]``, not ``investment_mix_results[2]`` (which
    would be the wrong, out-of-bounds-masking result a filtered-``self.
    calls``-derived index would produce if ``cognitive_load`` were ever
    counted as if it were a third ``investment_mix`` call).
    """

    first_result = _measured_investment_mix(new_value_units=80.0, total_units=100.0)
    second_result = _measured_investment_mix(new_value_units=20.0, total_units=100.0)
    workload = FakeWorkloadSource(investment_mix_results=(first_result, second_result))
    start, end = _NOW - timedelta(days=14), _NOW

    actual_first = await workload.investment_mix(
        org_id=_ORG_ID, team_id="team-1", start=start, end=end
    )
    # Codex's exact repro: an unexpected call interleaved between the two
    # genuine investment_mix calls.
    await workload.cognitive_load(
        org_id=_ORG_ID, team_id="team-1", start=start, end=end
    )
    actual_second = await workload.investment_mix(
        org_id=_ORG_ID, team_id="team-1", start=start, end=end
    )

    assert actual_first == first_result
    assert actual_second == second_result

    expected_clean_sequence = [
        ("investment_mix", _ORG_ID, "team-1", start, end),
        ("investment_mix", _ORG_ID, "team-1", start, end),
    ]
    # The exact-equality pattern this file's real assertions use MUST
    # reject the interleaved sequence -- proving it is a real check, not
    # a filtered view that would silently still pass regardless.
    assert workload.calls != expected_clean_sequence
    assert workload.calls == [
        ("investment_mix", _ORG_ID, "team-1", start, end),
        ("cognitive_load", _ORG_ID, "team-1", start, end),
        ("investment_mix", _ORG_ID, "team-1", start, end),
    ]


@dataclass
class RepoAwareFakeRuntime:
    """A ``data_health`` double whose result depends on which repositories
    are actually in the queried scope -- lets the batching/merge tests
    prove real reconciliation across multiple calls, not just "a single
    call happens to work". ``stale_repos`` is the ground truth: any
    repository in it makes the ``work_items`` source STALE (with that
    repository in ``missing_repository_ids``) for whichever batch it
    lands in; every other repository is COMPLETE.
    """

    stale_repos: frozenset[str] = field(default_factory=frozenset)
    scopes_seen: list[DevScope] = field(default_factory=list)

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return _snapshot()

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError("not exercised by the batching tests")

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        self.scopes_seen.append(scope)
        repos = tuple(scope.repositories)
        missing = tuple(sorted(set(repos) & self.stale_repos))
        work_items_state = (
            DataHealthState.STALE if missing else DataHealthState.COMPLETE
        )
        coverage = (len(repos) - len(missing)) / len(repos) if repos else 0.0
        # ALL of NATIVE_EVIDENCE_SOURCES, not just work_items (round-4 fix,
        # same rationale as OmissionAwareFakeRuntime above): only
        # work_items varies with `stale_repos`, every other canonical
        # source stays COMPLETE for every repository in this batch, so it
        # is a genuine "nothing else changed" control rather than an
        # always-omitted source the fail-closed merge would flag.
        other_sources = tuple(
            DataHealthSource(
                source_system=system,
                state=DataHealthState.COMPLETE,
                required=True,
                last_successful_at=_NOW,
                watermark=_NOW,
                missing_repository_ids=(),
                missing_entity_ids=(),
                coverage=1.0,
                confidence_impact=None,
                freshness_policy_version="v1",
            )
            for system in NATIVE_EVIDENCE_SOURCES
            if system != "work_items"
        )
        return DataHealthResult(
            sources=(
                DataHealthSource(
                    source_system="work_items",
                    state=work_items_state,
                    required=True,
                    last_successful_at=_NOW,
                    watermark=_NOW,
                    missing_repository_ids=missing,
                    missing_entity_ids=(),
                    coverage=coverage,
                    confidence_impact=None,
                    freshness_policy_version="v1",
                ),
                *other_sources,
            ),
            complete_eligible=not missing,
        )

    def mint_evidence(self, **kwargs):
        raise NotImplementedError


@dataclass
class OmissionAwareFakeRuntime:
    """A ``data_health`` double that can make specific BATCHES (by 0-based
    call order) omit specific source_systems entirely, or return an empty
    ``sources`` tuple altogether -- reproduces the exact Codex round-3
    repro (a batch returning ``DataHealthResult(sources=(), complete_
    eligible=True)``) plus the asymmetric-set variant (one batch missing
    a source_system another batch reports), without disturbing
    ``RepoAwareFakeRuntime``'s own existing single-source-system tests.

    Emits two source_systems by default -- ``work_items`` and
    ``pull_requests``, both COMPLETE for every repository -- so a batch
    that omits one of them is a genuine partial-set case, not merely
    "the only source it could have reported".
    """

    empty_batch_indices: frozenset[int] = field(default_factory=frozenset)
    omit_source_systems_by_batch_index: dict[int, frozenset[str]] = field(
        default_factory=dict
    )
    scopes_seen: list[DevScope] = field(default_factory=list)

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return _snapshot()

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    def list_metrics(self, scope):
        return ()

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError("not exercised by the omission tests")

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise NotImplementedError

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        batch_index = len(self.scopes_seen)
        self.scopes_seen.append(scope)
        if batch_index in self.empty_batch_indices:
            # The exact Codex repro: an empty-but-"healthy" batch result.
            return DataHealthResult(sources=(), complete_eligible=True)
        omitted = self.omit_source_systems_by_batch_index.get(batch_index, frozenset())
        # ALL of NATIVE_EVIDENCE_SOURCES, not just two -- see
        # _merge_data_health_sources' round-4 fix (Codex finding, HIGH,
        # 2026-08-02): the expected source set is the CANONICAL contract,
        # not the union of what batches happen to report, so a "no
        # omission" positive control must genuinely report every canonical
        # source or it is no longer testing what it claims to.
        all_systems = NATIVE_EVIDENCE_SOURCES
        sources = tuple(
            DataHealthSource(
                source_system=system,
                state=DataHealthState.COMPLETE,
                required=True,
                last_successful_at=_NOW,
                watermark=_NOW,
                missing_repository_ids=(),
                missing_entity_ids=(),
                coverage=1.0,
                confidence_impact=None,
                freshness_policy_version="v1",
            )
            for system in all_systems
            if system not in omitted
        )
        return DataHealthResult(sources=sources, complete_eligible=True)

    def mint_evidence(self, **kwargs):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Omitted/short batch merges must fail closed, never silently healthy
# (Codex finding, HIGH, round 3, 2026-08-02).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evaluate_team_omitted_batch_never_merges_as_healthy() -> None:
    """The exact Codex repro: 21 repos, batch 1 (20 repos) reports
    work_items COMPLETE, batch 2 (1 repo) returns an empty-but-"healthy"
    DataHealthResult -- the merged result must NOT report COMPLETE/
    coverage=1.0/complete_eligible=True; the unmeasured repo must degrade
    the merged picture, never disappear from it.
    """

    repos = tuple(f"repo-{i}" for i in range(21))
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = OmissionAwareFakeRuntime(empty_batch_indices=frozenset({1}))
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    # Unfiltered -- category 8 must never make its own raw-TEAM-scope
    # data_health call alongside these (Codex finding, HIGH, round 5).
    assert len(runtime.scopes_seen) == 2
    data_findings = [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]
    # The omitted batch must produce a real, degraded finding -- never
    # silently healthy (which would mean zero data-integration findings
    # here despite one whole repository never actually being measured).
    assert len(data_findings) >= 1
    assert all(f.coverage < 1.0 for f in data_findings)
    data_status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert data_status.finding_count >= 1


@pytest.mark.asyncio
async def test_evaluate_team_all_batches_empty_never_merges_as_healthy() -> None:
    """Codex finding, HIGH, round 4, 2026-08-02: round 3's fix unioned the
    batches' OWN reported source_systems as the "expected" set -- which
    still silently passed when EVERY batch returned ``sources=()``, since
    the union of nothing is nothing to flag. A single batch (<=20 repos,
    so there is only ONE batch total) that reports a totally empty result
    must NOT merge into a healthy, zero-finding category 1 -- every one of
    NATIVE_EVIDENCE_SOURCES must synthesize an explicit UNAVAILABLE
    placeholder, exactly like the multi-batch omission case above.
    """

    repos = tuple(f"repo-{i}" for i in range(5))  # single batch, all empty
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = OmissionAwareFakeRuntime(empty_batch_indices=frozenset({0}))
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    assert len(runtime.scopes_seen) == 1
    data_findings = [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]
    # Every canonical source must be flagged unavailable -- never silently
    # healthy just because the one-and-only batch reported nothing at all.
    assert len(data_findings) == len(NATIVE_EVIDENCE_SOURCES)
    assert all(f.coverage == 0.0 for f in data_findings)
    assert all(f.severity is DeficiencySeverity.CRITICAL for f in data_findings)
    data_status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert data_status.finding_count == len(NATIVE_EVIDENCE_SOURCES)


@pytest.mark.asyncio
async def test_evaluate_team_asymmetric_source_set_across_batches() -> None:
    """One batch omits ONE source_system (not its whole result) that
    another batch does report -- the omitted source must degrade only
    for the batch that omitted it, while the source both batches agree
    on stays healthy.
    """

    repos = tuple(f"repo-{i}" for i in range(21))
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = OmissionAwareFakeRuntime(
        omit_source_systems_by_batch_index={1: frozenset({"pull_requests"})}
    )
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    data_findings = {
        f.blast_radius: f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    }
    # pull_requests must show up as a genuine, degraded finding (coverage
    # < 1.0 -- the last batch's repo is unmeasured for it); work_items
    # must NOT (every batch agreed it was COMPLETE for every repo).
    assert any("pull_requests" in radius for radius in data_findings)
    assert not any(
        "work_items" in radius and "pull_requests" not in radius
        for radius in data_findings
    )


def test_merge_data_health_sources_required_flag_is_order_independent() -> None:
    """Codex finding, MEDIUM, round 4, 2026-08-02: ``required`` is merged
    INDEPENDENTLY of which batch's state wins the worst-state comparison
    -- the OR of every batch's own ``required`` flag, never copied from
    whichever record the worst-state comparison happens to keep. Proven
    in BOTH batch orders: a required+COMPLETE batch's requiredness must
    never become invisible just because an optional+UNAVAILABLE batch for
    the SAME source_system happens to be evaluated before OR after it.

    Round-5 correction (Codex finding, MEDIUM PLAUSIBLE, 2026-08-02): the
    round-4 fix above still let the optional+UNAVAILABLE batch's WORSE
    STATE win the merge for a source every REQUIRED batch reported
    COMPLETE -- ``required=True``/``state=UNAVAILABLE`` falsely implies the
    required dependency itself failed. The merged state must come from the
    REQUIRED records only when any exist: ``required=True``,
    ``state=COMPLETE`` (not blocking), in BOTH orders. See
    ``test_merge_data_health_sources_optional_failure_never_blocks_when_
    required_batches_complete`` for the full ``complete_eligible``-level
    regression matching Codex's own repro.
    """

    required_complete = DataHealthSource(
        source_system="work_items",
        state=DataHealthState.COMPLETE,
        required=True,
        last_successful_at=_NOW,
        watermark=_NOW,
        missing_repository_ids=(),
        missing_entity_ids=(),
        coverage=1.0,
        confidence_impact=None,
        freshness_policy_version="v1",
    )
    optional_unavailable = DataHealthSource(
        source_system="work_items",
        state=DataHealthState.UNAVAILABLE,
        required=False,
        last_successful_at=None,
        watermark=None,
        missing_repository_ids=("repo-b",),
        missing_entity_ids=(),
        coverage=0.0,
        confidence_impact="insufficient_evidence",
        freshness_policy_version=None,
    )

    for sources_by_batch in (
        ((required_complete,), (optional_unavailable,)),
        ((optional_unavailable,), (required_complete,)),
    ):
        merged = _merge_data_health_sources(
            sources_by_batch, (("repo-a",), ("repo-b",)), total_repositories=2
        )
        work_items = next(s for s in merged if s.source_system == "work_items")
        assert work_items.required is True
        # The optional batch's failure must never block a source every
        # REQUIRED batch satisfied.
        assert work_items.state is DataHealthState.COMPLETE
        assert work_items.missing_repository_ids == ()


def test_merge_data_health_sources_optional_failure_never_blocks_when_required_batches_complete() -> (
    None
):
    """Codex's own round-5 repro, at the ``complete_eligible`` level a
    caller actually reads: a required+COMPLETE batch plus an optional+
    UNAVAILABLE batch for the SAME source_system must aggregate
    eligible=True -- every batch that actually required this source
    completed; the unrelated optional failure must inform coverage/
    warnings for that OTHER, optional-only picture, never block this one.
    """

    required_complete = DataHealthSource(
        source_system="work_items",
        state=DataHealthState.COMPLETE,
        required=True,
        last_successful_at=_NOW,
        watermark=_NOW,
        missing_repository_ids=(),
        missing_entity_ids=(),
        coverage=1.0,
        confidence_impact=None,
        freshness_policy_version="v1",
    )
    optional_unavailable = DataHealthSource(
        source_system="work_items",
        state=DataHealthState.UNAVAILABLE,
        required=False,
        last_successful_at=None,
        watermark=None,
        missing_repository_ids=("repo-b",),
        missing_entity_ids=(),
        coverage=0.0,
        confidence_impact="insufficient_evidence",
        freshness_policy_version=None,
    )

    def _complete_required(system: str) -> DataHealthSource:
        return DataHealthSource(
            source_system=system,
            state=DataHealthState.COMPLETE,
            required=True,
            last_successful_at=_NOW,
            watermark=_NOW,
            missing_repository_ids=(),
            missing_entity_ids=(),
            coverage=1.0,
            confidence_impact=None,
            freshness_policy_version="v1",
        )

    # Every OTHER canonical source_system is trivially COMPLETE in both
    # batches -- this test isolates work_items' own mixed-optionality
    # resolution, never the (already separately-tested) all-canonical-
    # sources-omitted behavior.
    other_systems = [s for s in NATIVE_EVIDENCE_SOURCES if s != "work_items"]
    batch_a = (required_complete, *(_complete_required(s) for s in other_systems))
    batch_b = (optional_unavailable, *(_complete_required(s) for s in other_systems))
    merged = _merge_data_health_sources(
        (batch_a, batch_b), (("repo-a",), ("repo-b",)), total_repositories=2
    )
    complete_eligible = all(
        not source.required or source.state is DataHealthState.COMPLETE
        for source in merged
    )
    assert complete_eligible is True
    work_items = next(s for s in merged if s.source_system == "work_items")
    assert work_items.required is True
    assert work_items.state is DataHealthState.COMPLETE


def test_merge_data_health_sources_optional_only_source_reports_honestly() -> None:
    """A source_system NO batch ever required still falls back to
    aggregating its optional records -- an honest, reportable state, just
    never able to block ``complete_eligible`` on its own (``required``
    stays ``False``).
    """

    optional_stale = DataHealthSource(
        source_system="work_items",
        state=DataHealthState.STALE,
        required=False,
        last_successful_at=_NOW,
        watermark=_NOW,
        missing_repository_ids=("repo-a",),
        missing_entity_ids=(),
        coverage=0.5,
        confidence_impact=None,
        freshness_policy_version="v1",
    )
    merged = _merge_data_health_sources(
        ((optional_stale,),), (("repo-a", "repo-b"),), total_repositories=2
    )
    work_items = next(s for s in merged if s.source_system == "work_items")
    assert work_items.required is False
    assert work_items.state is DataHealthState.STALE
    assert work_items.missing_repository_ids == ("repo-a",)


def test_placeholder_for_omitted_source_required_matches_canonical_table() -> None:
    """The requiredness half of the round-4 fix (Codex finding, MEDIUM,
    2026-08-02): a synthesized omission placeholder must read its
    ``required`` flag from ``_CANONICAL_SOURCE_REQUIRED``, never a blanket
    ``True`` literal that would misreport a genuinely optional
    source_system as required just because a batch failed to mention it.
    Tested directly against ``_placeholder_for_omitted_source`` (the exact
    function the fix touched), not through the merge pipeline -- ``_merge_
    data_health_sources`` only ever routes members of NATIVE_EVIDENCE_
    SOURCES through this function (its ``expected_source_systems`` is
    exactly that set), so every member's requiredness is checked directly
    against the SAME table this function reads, proving they cannot drift
    apart; the fallback branch for a source_system outside that table
    (defensive, unreachable via the merge pipeline as currently wired) is
    proven separately.
    """

    from dev_health_ops.api.dev.operational_deficiency_service import (
        _CANONICAL_SOURCE_REQUIRED,
        _placeholder_for_omitted_source,
    )

    for source_system in NATIVE_EVIDENCE_SOURCES:
        placeholder = _placeholder_for_omitted_source(source_system, ("repo-a",))
        assert placeholder.required == _CANONICAL_SOURCE_REQUIRED[source_system]
        assert placeholder.required is True  # every canonical source today

    # Fail-closed default for a source_system the table has no entry for.
    unrecognized = _placeholder_for_omitted_source("some_future_source", ("repo-a",))
    assert unrecognized.required is True


@pytest.mark.asyncio
async def test_evaluate_team_consistent_batches_produce_no_spurious_findings() -> None:
    """Positive control: when every batch reports the SAME source_system
    set, the omission fail-closed logic must never fire -- no placeholder,
    no synthesized finding, a genuinely healthy team stays healthy.
    """

    repos = tuple(f"repo-{i}" for i in range(21))
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = OmissionAwareFakeRuntime()  # no omissions, no empty batches
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    data_findings = [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]
    assert data_findings == []
    data_status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert data_status.finding_count == 0


# ---------------------------------------------------------------------------
# Team data_health batching: >20-repository teams must never crash or
# silently truncate (Codex finding, HIGH, 2026-08-02).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evaluate_team_data_health_exactly_20_repositories_single_batch() -> None:
    repos = tuple(f"repo-{i}" for i in range(20))
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = RepoAwareFakeRuntime()
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    # Unfiltered: exactly one call total, never a second raw-TEAM-scope
    # data_health call from category 8 (Codex finding, HIGH, round 5).
    assert len(runtime.scopes_seen) == 1
    assert sorted(runtime.scopes_seen[0].repositories) == sorted(repos)


@pytest.mark.asyncio
async def test_evaluate_team_data_health_21_repositories_batches_into_two_calls() -> (
    None
):
    repos = tuple(f"repo-{i}" for i in range(21))
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = RepoAwareFakeRuntime()
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    # Unfiltered call sequence -- exactly the two REPOSITORY batches,
    # never a third, raw-TEAM-scope call from category 8.
    assert len(runtime.scopes_seen) == 2
    assert all(s.direct_scope is DirectScope.REPOSITORY for s in runtime.scopes_seen)
    batch_sizes = sorted(len(s.repositories) for s in runtime.scopes_seen)
    assert batch_sizes == [1, 20]
    all_queried = {repo for s in runtime.scopes_seen for repo in s.repositories}
    assert all_queried == set(
        repos
    )  # every repository queried exactly once, none dropped


@pytest.mark.asyncio
async def test_evaluate_team_data_health_45_repositories_batches_and_merges() -> None:
    repos = tuple(f"repo-{i}" for i in range(45))
    stale = frozenset({"repo-5", "repo-25", "repo-40"})  # one per batch of 20/20/5
    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = RepoAwareFakeRuntime(stale_repos=stale)
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    # Unfiltered: 20 + 20 + 5, never truncated to the first batch, and
    # never a fourth raw-TEAM-scope call from category 8.
    assert len(runtime.scopes_seen) == 3
    assert all(s.direct_scope is DirectScope.REPOSITORY for s in runtime.scopes_seen)
    batch_sizes = sorted(len(s.repositories) for s in runtime.scopes_seen)
    assert batch_sizes == [5, 20, 20]

    data_findings = [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]
    assert len(data_findings) == 1
    finding = data_findings[0]
    assert finding.rule_id == "deficiency_rule.stale_watermark.v1"
    # Exact coverage recomputed from the true total, not a per-batch average.
    assert finding.coverage == pytest.approx((45 - 3) / 45)


@pytest.mark.asyncio
async def test_evaluate_team_data_health_batched_merge_matches_hypothetical_single_call() -> (
    None
):
    """The merged, batched result must be byte-identical in substance to
    what a single, hypothetical unbounded call would have produced --
    proving the batching/merge machinery is lossless, not an
    approximation.
    """

    repos = tuple(f"repo-{i}" for i in range(45))
    stale = frozenset({"repo-5", "repo-25", "repo-40"})

    # The hypothetical: what a single call over ALL 45 repos would report,
    # computed with the exact same ground-truth logic RepoAwareFakeRuntime
    # uses, just never actually issued (DevScope forbids it).
    hypothetical_missing = tuple(sorted(set(repos) & stale))
    hypothetical_coverage = (len(repos) - len(hypothetical_missing)) / len(repos)

    attribution = FakeAttributionSource(repository_ids=repos)
    runtime = RepoAwareFakeRuntime(stale_repos=stale)
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    finding = next(
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert finding.coverage == pytest.approx(hypothetical_coverage)
    assert finding.severity is DeficiencySeverity.AT_RISK  # stale_watermark


@pytest.mark.asyncio
async def test_evaluate_team_data_health_batching_never_calls_runtime_when_no_repos() -> (
    None
):
    runtime = RepoAwareFakeRuntime()
    service = OperationalDeficiencyService(
        runtime, FakeAttributionSource(measured=False), _NO_WORKLOAD
    )
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    assert runtime.scopes_seen == []


# ---------------------------------------------------------------------------
# Scope validation, mirroring ProjectHealthService/TeamHealthService.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evaluate_project_rejects_non_project_scope() -> None:
    service = OperationalDeficiencyService(_runtime(), _NO_ATTRIBUTION, _NO_WORKLOAD)
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
    service = OperationalDeficiencyService(_runtime(), _NO_ATTRIBUTION, _NO_WORKLOAD)
    with pytest.raises(ValueError, match="comparison_range"):
        await service.evaluate_project(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=_project_scope(with_comparison=False),
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_evaluate_team_rejects_mismatched_team_ids() -> None:
    service = OperationalDeficiencyService(_runtime(), _NO_ATTRIBUTION, _NO_WORKLOAD)
    scope = _team_scope()
    with pytest.raises(ValueError, match="scope.team_ids"):
        await service.evaluate_team(
            org_id=_ORG_ID,
            permission_fingerprint="fp",
            scope=scope,
            team_id="other-team",
            now=_NOW,
        )


# ---------------------------------------------------------------------------
# Team attribution: snapshot resolved at scope.time_range.end (never `now`),
# and lookup failure kept structurally distinct from a genuinely empty
# cohort.
# ---------------------------------------------------------------------------

_RULE_DRIVEN_CATEGORY_SET = {
    DeficiencyCategory.DELIVERY_FLOW,
    DeficiencyCategory.REVIEW_CI,
    DeficiencyCategory.DEPLOYMENT_RELIABILITY,
    DeficiencyCategory.OWNERSHIP_CODE_RISK,
}


@pytest.mark.asyncio
async def test_evaluate_team_resolves_attribution_at_scope_end_not_now() -> None:
    """Boundary control: `now` genuinely differs from `scope.time_range.end`
    -- the attribution lookup must use the scope's own committed window
    end, never the evaluation-time `now` (Codex finding, same class as
    3303/3304: ownership can expire between scope end and `now`, wrongly
    suppressing real historical findings as insufficient_cohort).
    """

    scope_end = _NOW - timedelta(days=3)
    scope = _team_scope(end=scope_end)
    attribution = FakeAttributionSource(repository_ids=("repo-1",))
    service = OperationalDeficiencyService(_runtime(), attribution, _NO_WORKLOAD)

    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=scope,
        team_id="team-1",
        now=_NOW,
    )

    # Exactly one lookup, complete unfiltered log: category 8's own
    # _investment_balance_profile takes the already-resolved cohort_size
    # as a parameter and never re-resolves attribution itself (Codex
    # finding, HIGH, round 5 -- composing the WHOLE TeamWorkloadService
    # made a second, independent lookup here).
    assert attribution.calls == [(_ORG_ID, "team-1", scope_end)]
    assert scope_end != _NOW


@pytest.mark.asyncio
async def test_evaluate_team_attribution_failure_is_unmeasured_not_insufficient_cohort() -> (
    None
):
    """A genuine attribution-lookup failure must never be presented as
    "insufficient_cohort" -- that phrase claims attribution was checked
    and found too small, which is a different fact from "attribution could
    not be checked at all".
    """

    attribution = FakeAttributionSource(measured=False)
    service = OperationalDeficiencyService(_runtime(), attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    for status in inventory.category_statuses:
        if status.category in _RULE_DRIVEN_CATEGORY_SET:
            assert status.evaluated is False
            assert status.finding_count == 0
            # The exact unmeasured-attribution limitation, not the generic
            # shadow-calibration or insufficient-cohort text a merely-empty
            # (but successfully resolved) cohort would carry.
            assert status.limitation == _TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION
    # Category 2 (planning & relationships) IS independent of team
    # attribution -- status_snapshot is queried regardless (see the
    # dedicated test below). Category 1 (data & integration) is NOT
    # (Codex finding, HIGH): on attribution failure there is no verified
    # repository set to scope data_health to, so it must never be queried
    # at all -- never-queried, not the org-wide fallback a raw TEAM scope
    # would otherwise trigger.
    data_status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert data_status.evaluated is False  # never queried, not org-wide


@pytest.mark.asyncio
async def test_evaluate_team_data_health_is_scoped_to_attributed_repositories() -> None:
    """Codex finding, HIGH, 2026-08-02: data_health must be queried against
    an explicit DirectScope.REPOSITORY scope built from the SAME
    attribution snapshot used for cohort_size -- never the raw TEAM scope
    (which NativeDataHealthReader would resolve to zero explicit
    repositories and fall back to querying every repository in the org).
    """

    attribution = FakeAttributionSource(repository_ids=("repo-a", "repo-b"))
    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    # Unfiltered: exactly one data_health call total -- category 8 must
    # never make its own second, raw-TEAM-scope call (Codex finding, HIGH,
    # round 5).
    assert len(runtime.data_health_scopes) == 1
    called_scope = runtime.data_health_scopes[0]
    assert called_scope.direct_scope is DirectScope.REPOSITORY
    assert sorted(called_scope.repositories) == ["repo-a", "repo-b"]


@pytest.mark.asyncio
async def test_evaluate_team_data_health_never_called_when_attribution_unmeasured() -> (
    None
):
    attribution = FakeAttributionSource(measured=False)
    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    assert runtime.data_health_scopes == []


@pytest.mark.asyncio
async def test_evaluate_team_data_health_never_called_when_cohort_genuinely_empty() -> (
    None
):
    """The batched ``_team_scoped_data_health`` path (category 1) is never
    invoked for a genuinely empty (but measured) cohort -- an empty
    ``repository_ids`` short-circuits before any batch is issued. Category
    8's own ``_investment_balance_profile`` never touches ``self._runtime``
    at all (it only calls ``self._workload_source.investment_mix``, which
    takes ``team_id`` directly, no ``DevScope``) -- so this is a genuinely
    UNFILTERED, complete assertion: zero ``data_health`` calls, period
    (Codex finding, HIGH, round 5: a prior composition via
    ``TeamWorkloadService`` made its own extra, raw-TEAM-scope call here).
    """

    attribution = FakeAttributionSource(repository_ids=())
    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    assert runtime.data_health_scopes == []


@pytest.mark.asyncio
async def test_evaluate_team_status_snapshot_always_called_with_raw_team_scope() -> (
    None
):
    """Category 2 genuinely IS attribution-independent: status_snapshot's
    own ClickHouseStatusChangeSource re-derives team_repo_ownership
    internally, so the raw TEAM scope is safe to pass through directly,
    regardless of attribution outcome.
    """

    attribution = FakeAttributionSource(measured=False)
    runtime = _runtime()
    service = OperationalDeficiencyService(runtime, attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    planning_status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.PLANNING_RELATIONSHIPS
    )
    assert planning_status.evaluated is True


@pytest.mark.asyncio
async def test_evaluate_team_empty_cohort_stays_evaluated_true() -> None:
    """A genuinely resolved, empty cohort (the lookup succeeded and
    returned zero repositories) is a different fact from a lookup failure
    -- the rule-driven categories are still `evaluated=True` (the check
    ran; it just found nothing to attribute), not the unmeasured branch a
    failure produces.
    """

    attribution = FakeAttributionSource(repository_ids=())
    service = OperationalDeficiencyService(_runtime(), attribution, _NO_WORKLOAD)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(),
        team_id="team-1",
        now=_NOW,
    )
    for status in inventory.category_statuses:
        if status.category in _RULE_DRIVEN_CATEGORY_SET:
            assert status.evaluated is True
            assert status.limitation != _TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION


# ---------------------------------------------------------------------------
# Category 1: data & integration.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_data_integration_finding_for_unconfigured_required_source() -> None:
    runtime = _runtime(sources=(_source("work_items", DataHealthState.UNCONFIGURED),))
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    data_findings = [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]
    assert len(data_findings) == 1
    assert data_findings[0].severity is DeficiencySeverity.CRITICAL
    assert data_findings[0].rule_id == "deficiency_rule.unconfigured_required_source.v1"
    status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert status.evaluated is True
    assert status.finding_count == 1


@pytest.mark.asyncio
async def test_data_integration_optional_unconfigured_source_is_not_a_deficiency() -> (
    None
):
    """Guardrail: no missing optional integration is treated as a failure."""

    runtime = _runtime(
        sources=(_source("acr", DataHealthState.UNCONFIGURED, required=False),)
    )
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    assert not [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]


@pytest.mark.asyncio
async def test_data_integration_complete_source_is_not_a_deficiency() -> None:
    runtime = _runtime(sources=(_source("work_items", DataHealthState.COMPLETE),))
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    assert not [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.DATA_INTEGRATION
    ]


@pytest.mark.asyncio
async def test_data_integration_distinguishes_stale_missing_and_unconfigured() -> None:
    runtime = _runtime(
        sources=(
            _source("work_items", DataHealthState.STALE),
            _source("pull_requests", DataHealthState.NO_DATA),
            _source("reviews", DataHealthState.UNCONFIGURED),
        )
    )
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    kinds_by_rule = {f.rule_id: f.severity for f in inventory.findings}
    assert (
        kinds_by_rule["deficiency_rule.stale_watermark.v1"]
        is DeficiencySeverity.AT_RISK
    )
    assert (
        kinds_by_rule["deficiency_rule.missing_subject_coverage.v1"]
        is DeficiencySeverity.WATCH
    )
    assert (
        kinds_by_rule["deficiency_rule.unconfigured_required_source.v1"]
        is DeficiencySeverity.CRITICAL
    )
    assert len(inventory.findings) >= 3  # never collapsed into one finding


@pytest.mark.asyncio
async def test_data_integration_never_queried_is_unevaluated() -> None:
    runtime = _runtime(sources=())
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert status.evaluated is False
    assert status.limitation is not None


# ---------------------------------------------------------------------------
# Category 2: planning & relationships.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_planning_relationships_open_blocker_finding() -> None:
    blocker = StatusFact(
        entity_type="issue",
        entity_id="BLOCK-1",
        display_label="Blocking issue",
        status="open",
        observed_at=_NOW,
        source_ref_id="ref-1",
        evidence_ref_ids=("evidence-1",),
        required=True,
    )
    snapshot = _snapshot(
        state=StatusResultState.COMPLETE,
        actual=_actual_completion(reason_codes=("open_blocker",)),
        blockers=(blocker,),
    )
    runtime = _runtime(snapshot=snapshot)
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    planning = [
        f
        for f in inventory.findings
        if f.category is DeficiencyCategory.PLANNING_RELATIONSHIPS
    ]
    assert len(planning) == 1
    assert planning[0].rule_id == "deficiency_rule.unresolved_blocking_dependency.v1"
    assert planning[0].evidence_ref_ids == ("evidence-1",)
    assert planning[0].evidence_classification is None


@pytest.mark.asyncio
async def test_planning_relationships_missing_declared_status_has_no_evidence() -> None:
    snapshot = _snapshot(
        state=StatusResultState.COMPLETE,
        actual=_actual_completion(reason_codes=("declared_status_missing",)),
    )
    runtime = _runtime(snapshot=snapshot)
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    finding = next(
        f
        for f in inventory.findings
        if f.rule_id == "deficiency_rule.missing_declared_status.v1"
    )
    assert finding.evidence_ref_ids == ()
    from dev_health_ops.api.dev.contracts_v2.deficiency import (
        DeficiencyEvidenceClassification,
    )

    assert (
        finding.evidence_classification
        is DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE
    )
    assert finding.severity is DeficiencySeverity.WATCH


@pytest.mark.asyncio
async def test_planning_relationships_declared_complete_conflict() -> None:
    conflict = StatusConflict(
        code="declared_complete_conflicts_with_observed_work",
        message="Declared completion conflicts with required work.",
        severity=ConflictSeverity.BLOCKING,
        source_ref_ids=(),
        evidence_ref_ids=("evidence-2",),
    )
    snapshot = _snapshot(
        state=StatusResultState.COMPLETE,
        actual=_actual_completion(
            reason_codes=("open_blocker",), conflicts=(conflict,)
        ),
    )
    runtime = _runtime(snapshot=snapshot)
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    finding = next(
        f
        for f in inventory.findings
        if f.rule_id == "deficiency_rule.declared_complete_conflict.v1"
    )
    assert finding.severity is DeficiencySeverity.CRITICAL
    assert finding.evidence_ref_ids == ("evidence-2",)


@pytest.mark.asyncio
async def test_planning_relationships_insufficient_evidence_is_unevaluated() -> None:
    snapshot = _snapshot(state=StatusResultState.INSUFFICIENT_EVIDENCE)
    runtime = _runtime(snapshot=snapshot)
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.PLANNING_RELATIONSHIPS
    )
    assert status.evaluated is False


# ---------------------------------------------------------------------------
# Categories 3-6: rule-registry-driven, shadow_only exclusion.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rule_driven_categories_produce_no_findings_while_all_rules_are_provisional() -> (
    None
):
    """Every HEALTH_RULE_REGISTRY rule shipped today is provisional --
    shadow_only findings must never appear in the inventory's status/counts.
    """

    runtime = _runtime(sources=(_source("work_items", DataHealthState.COMPLETE),))
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    rule_driven = {
        DeficiencyCategory.DELIVERY_FLOW,
        DeficiencyCategory.REVIEW_CI,
        DeficiencyCategory.DEPLOYMENT_RELIABILITY,
        DeficiencyCategory.OWNERSHIP_CODE_RISK,
    }
    for status in inventory.category_statuses:
        if status.category in rule_driven:
            assert status.evaluated is True
            assert status.finding_count == 0
            assert status.limitation is not None
    assert not [f for f in inventory.findings if f.category in rule_driven]


def _health_rule_finding(
    *,
    rule_id: str = "health_rule.incident_load.v1",
    dimension: HealthDimension = HealthDimension.RELIABILITY_RELEASE,
    shadow_only: bool = True,
    suppressed_reason: str | None = None,
    state: DimensionState = DimensionState.UNKNOWN,
    calibration_state: CalibrationState = CalibrationState.PROVISIONAL,
) -> HealthRuleFinding:
    return HealthRuleFinding(
        schema_version="health_rule_finding.v1",
        finding_id="11111111-1111-1111-1111-111111111111",
        rule_id=rule_id,
        rule_version=rule_id,
        dimension=dimension,
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        state=state,
        fact_kind="observed",
        shadow_only=shadow_only,
        evidence_source_classes=(),
        remediation_template="x",
        calibration_state=calibration_state,
        evaluated_at=_NOW,
        suppressed_reason=suppressed_reason,
    )


def test_rule_driven_category_status_discloses_suppressed_only_findings() -> None:
    """Codex/CHAOS-3304 interaction (2026-08-02): health_rule_registry's
    partition order now checks suppressed_reason before shadow_only, so a
    rule that is both provisional AND guardrail-suppressed lands in
    ``suppressed``, never ``shadow``. A category whose only non-launch
    findings are all suppressed must still disclose a limitation -- never
    the accidental ``limitation=None`` a shadow-only check would produce,
    which reads as "genuinely nothing to disclose" and hides a real,
    silenced finding.
    """

    suppressed_finding = _health_rule_finding(suppressed_reason="insufficient_sample")
    status, deficiencies = _rule_driven_category_result(
        DeficiencyCategory.DEPLOYMENT_RELIABILITY,
        launch=(),
        shadow=(),
        suppressed=(suppressed_finding,),
        observations_by_rule={},
        org_id=_ORG_ID,
    )
    assert status.evaluated is True
    assert status.finding_count == 0
    assert deficiencies == ()
    assert status.limitation == _RULE_DRIVEN_SUPPRESSED_LIMITATION


def test_rule_driven_category_status_discloses_mixed_shadow_and_suppressed() -> None:
    shadow_finding = _health_rule_finding(
        rule_id="health_rule.change_failure_rate.v1", suppressed_reason=None
    )
    suppressed_finding = _health_rule_finding(suppressed_reason="insufficient_sample")
    status, _ = _rule_driven_category_result(
        DeficiencyCategory.DEPLOYMENT_RELIABILITY,
        launch=(),
        shadow=(shadow_finding,),
        suppressed=(suppressed_finding,),
        observations_by_rule={},
        org_id=_ORG_ID,
    )
    assert status.limitation == _RULE_DRIVEN_SHADOW_AND_SUPPRESSED_LIMITATION


def test_rule_driven_category_status_partial_when_launch_and_suppressed_coexist() -> (
    None
):
    """Codex finding, MEDIUM, round 4, 2026-08-02: the suppressed-only
    wording ("every applicable rule was guardrail-suppressed") is
    literally false the moment a DIFFERENT rule in the same category also
    cleared every guardrail and launched. A mixed category must disclose
    the weaker, honest "not every rule contributed" claim instead.
    """

    launch_finding = _health_rule_finding(
        rule_id="health_rule.change_failure_rate.v1",
        shadow_only=False,
        suppressed_reason=None,
        state=DimensionState.WATCH,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
    )
    suppressed_finding = _health_rule_finding(suppressed_reason="insufficient_sample")
    status, deficiencies = _rule_driven_category_result(
        DeficiencyCategory.DEPLOYMENT_RELIABILITY,
        launch=(launch_finding,),
        shadow=(),
        suppressed=(suppressed_finding,),
        observations_by_rule={},
        org_id=_ORG_ID,
    )
    assert status.limitation == _RULE_DRIVEN_PARTIAL_LIMITATION
    assert status.finding_count == 1
    assert len(deficiencies) == 1


def test_rule_driven_category_status_partial_when_launch_and_shadow_coexist() -> None:
    """Same claim, the shadow-only-coexists-with-launch variant."""

    launch_finding = _health_rule_finding(
        rule_id="health_rule.change_failure_rate.v1",
        shadow_only=False,
        suppressed_reason=None,
        state=DimensionState.WATCH,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
    )
    shadow_finding = _health_rule_finding(suppressed_reason=None)
    status, deficiencies = _rule_driven_category_result(
        DeficiencyCategory.DEPLOYMENT_RELIABILITY,
        launch=(launch_finding,),
        shadow=(shadow_finding,),
        suppressed=(),
        observations_by_rule={},
        org_id=_ORG_ID,
    )
    assert status.limitation == _RULE_DRIVEN_PARTIAL_LIMITATION
    assert status.finding_count == 1
    assert len(deficiencies) == 1


def test_rule_driven_category_status_shadow_only_unchanged() -> None:
    """Positive control: the pre-3304 shadow-only case still gets the
    original, more specific limitation text -- this fix must not blur
    the two cases together.
    """

    shadow_finding = _health_rule_finding(suppressed_reason=None)
    status, _ = _rule_driven_category_result(
        DeficiencyCategory.DEPLOYMENT_RELIABILITY,
        launch=(),
        shadow=(shadow_finding,),
        suppressed=(),
        observations_by_rule={},
        org_id=_ORG_ID,
    )
    assert status.limitation == _RULE_DRIVEN_SHADOW_LIMITATION


def _real_observation(
    *, rule_id: str, coverage: float, sample_count: int | None
) -> DimensionObservation:
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        cohort_size=10,
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        data_semantics="measured_zero",
        sample_count=sample_count,
        coverage=coverage,
        current_value=1.0,
        comparison_value=None,
        denominator_present=False,
        attribution_present=True,
        window_index=0,
        observed_at=_NOW,
    )


def _unavailable_observation(*, rule_id: str) -> DimensionObservation:
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        cohort_size=None,
        observed_states=(SourceRequirementState.UNAVAILABLE,),
        data_semantics="not_measured",
        sample_count=None,
        coverage=0.0,
        current_value=None,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=0,
        observed_at=_NOW,
    )


def test_rule_driven_results_does_not_overwrite_primary_observation_with_workload_noise() -> (
    None
):
    """Codex finding, HIGH, round 6, 2026-08-02: the exact repro. A rule
    shared by BOTH profiles (e.g. ``incident_load``, TEAM-applicable, so
    ``_investment_balance_profile``'s own ``synthesize_health_profile``
    call also evaluates it and produces a synthesized-UNAVAILABLE
    observation for it, since that profile's sources never carry
    ``status_snapshot``) must keep the PRIMARY profile's own REAL
    observation -- never get silently overwritten by workload_profile's
    unrelated noise for a rule this function never harvested a finding
    from. A wholesale ``{**health, **workload}`` dict merge corrupts the
    real launch finding's ``observed_state``/``coverage``/``sample_count``
    with the unavailable observation's, and ``_deficiency_from_health_rule_
    finding``'s hardcoded ``data_semantics="measured_zero"`` combined with
    an UNAVAILABLE (unmeasured) ``observed_state`` raises
    ``ValidationError`` -- taking down the WHOLE inventory over an
    unrelated category-8 evaluation, not merely losing one field.
    """

    launch_rule_id = "health_rule.incident_load.v1"
    investment_rule_id = "health_rule.investment_allocation_shift.v1"
    real_observation = _real_observation(
        rule_id=launch_rule_id, coverage=0.9, sample_count=7
    )
    launch_finding = _health_rule_finding(
        rule_id=launch_rule_id,
        dimension=HealthDimension.RELIABILITY_RELEASE,
        shadow_only=False,
        suppressed_reason=None,
        state=DimensionState.WATCH,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
    )
    health_profile = HealthProfileResult(
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        observations=(real_observation,),
        launch_findings=(launch_finding,),
        shadow_findings=(),
        suppressed_findings=(),
        observations_by_rule={launch_rule_id: real_observation},
    )

    # workload_profile: the byproduct of _investment_balance_profile
    # reusing synthesize_health_profile -- it ALSO evaluates incident_load
    # (TEAM-applicable) with no status_snapshot source, producing a
    # synthesized-unavailable observation for the SAME rule_id, plus its
    # own genuine investment_mix-sourced shadow finding.
    unavailable_incident_load = _unavailable_observation(rule_id=launch_rule_id)
    incident_load_noise_finding = _health_rule_finding(
        rule_id=launch_rule_id,
        dimension=HealthDimension.RELIABILITY_RELEASE,
        shadow_only=True,
        suppressed_reason=None,
        state=DimensionState.UNKNOWN,
    )
    investment_observation = _real_observation(
        rule_id=investment_rule_id, coverage=0.5, sample_count=None
    )
    investment_finding = _health_rule_finding(
        rule_id=investment_rule_id,
        dimension=HealthDimension.INVESTMENT_BALANCE,
        shadow_only=True,
        suppressed_reason=None,
        state=DimensionState.UNKNOWN,
    )
    workload_profile = HealthProfileResult(
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        observations=(unavailable_incident_load, investment_observation),
        launch_findings=(),
        shadow_findings=(incident_load_noise_finding, investment_finding),
        suppressed_findings=(),
        observations_by_rule={
            launch_rule_id: unavailable_incident_load,
            investment_rule_id: investment_observation,
        },
    )

    # Must not raise -- the exact Codex repro.
    statuses, findings = _rule_driven_results(
        health_profile, org_id=_ORG_ID, workload_profile=workload_profile
    )

    reliability_status = next(
        s for s in statuses if s.category is DeficiencyCategory.DEPLOYMENT_RELIABILITY
    )
    assert reliability_status.evaluated is True
    assert reliability_status.finding_count == 1
    reliability_finding = next(
        f for f in findings if f.category is DeficiencyCategory.DEPLOYMENT_RELIABILITY
    )
    # The REAL primary observation survived -- never overwritten by
    # workload_profile's unrelated unavailable noise for the same rule_id.
    assert reliability_finding.coverage == 0.9
    assert reliability_finding.sample_count == 7
    assert (
        reliability_finding.observed_state is SourceRequirementState.AVAILABLE_CURRENT
    )
    assert reliability_finding.data_semantics == "measured_zero"

    investment_status = next(
        s for s in statuses if s.category is DeficiencyCategory.INVESTMENT_BALANCE
    )
    assert investment_status.evaluated is True
    assert investment_status.finding_count == 0  # shadow-only, no launch


# ---------------------------------------------------------------------------
# Categories 7 & 8: always unevaluated this version.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_is_unevaluated_and_investment_balance_not_applicable_for_project() -> (
    None
):
    """Category 7 (capacity/cognitive load) has no rule bound via this
    pipeline for either scope -- see ``_CAPACITY_LIMITATION``. Category 8
    (investment balance) DOES have a bound rule now (CHAOS-3304 merged),
    but that rule is TEAM-only -- a project subject reports an honest
    "not applicable to this scope" limitation, distinct from "nobody
    built this yet" (see ``test_evaluate_team_investment_balance_*``
    below for the TEAM-scope-evaluated case).
    """

    runtime = _runtime()
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    capacity = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.CAPACITY_COGNITIVE_LOAD
    )
    assert capacity.evaluated is False
    assert capacity.finding_count == 0
    assert capacity.limitation is not None

    investment = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.INVESTMENT_BALANCE
    )
    assert investment.evaluated is False
    assert investment.finding_count == 0
    assert (
        investment.limitation
        == _INVESTMENT_BALANCE_NOT_APPLICABLE_TO_PROJECT_LIMITATION
    )


@pytest.mark.asyncio
async def test_evaluate_team_investment_balance_wired_but_missing_attribution_suppressed() -> (
    None
):
    """CHAOS-3304's ``investment_allocation_shift.v1`` is CHAOS-3331-blocked:
    ``investment_allocation_shift_observation`` reports
    ``attribution_present=False`` unconditionally even for genuinely
    measured, non-``no_data`` investment-mix facts (see
    ``dimension_observation_adapters.py``). A real, computed shift must
    therefore land in ``suppressed_findings`` with reason
    ``missing_attribution`` -- never a launch finding -- and category 8
    must report ``evaluated=True`` with the partial/suppressed limitation,
    not the old "not yet merged" wording. Both windows need genuinely
    measured, non-zero-total investment mix data (current AND comparison)
    to clear the adapter's own ``comparison_share is None -> no_data``
    short-circuit and reach the attribution guard at all.
    """

    workload = FakeWorkloadSource(
        investment_mix_results=(
            _measured_investment_mix(new_value_units=80.0, total_units=100.0),
            _measured_investment_mix(new_value_units=20.0, total_units=100.0),
        )
    )
    attribution = FakeAttributionSource(repository_ids=("repo-1", "repo-2"))
    runtime = _runtime()
    service = OperationalDeficiencyService(runtime, attribution, workload)
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(with_comparison=True),
        team_id="team-1",
        now=_NOW,
    )
    investment = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.INVESTMENT_BALANCE
    )
    assert investment.evaluated is True
    # Suppressed, never a launch finding -- CHAOS-3331 is not resolved.
    assert investment.finding_count == 0
    assert investment.limitation is not None
    assert not any(
        f.category is DeficiencyCategory.INVESTMENT_BALANCE for f in inventory.findings
    )
    # The workload source was genuinely invoked for TEAM scope (structural
    # wiring, not merely a status-text claim) -- the COMPLETE, unfiltered,
    # ordered call log, both windows, exact arguments (Codex finding,
    # MEDIUM, round 6: a filtered/counted view would keep passing even if
    # an unexpected cognitive_load/active_contributor_count call got
    # interleaved -- this equality check cannot).
    assert workload.calls == [
        ("investment_mix", _ORG_ID, "team-1", _NOW - timedelta(days=14), _NOW),
        (
            "investment_mix",
            _ORG_ID,
            "team-1",
            _NOW - timedelta(days=28),
            _NOW - timedelta(days=14),
        ),
    ]


@pytest.mark.asyncio
async def test_evaluate_team_investment_balance_category_in_rule_driven_categories() -> (
    None
):
    """Structural assertion (Codex round-4 finding #2): INVESTMENT_BALANCE
    must be a member of the rule-driven-categories partition, not merely
    described as wired in prose.
    """

    from dev_health_ops.api.dev.operational_deficiency_service import (
        _RULE_DRIVEN_CATEGORIES,
    )

    assert DeficiencyCategory.INVESTMENT_BALANCE in _RULE_DRIVEN_CATEGORIES


@pytest.mark.asyncio
async def test_evaluate_team_investment_balance_unavailable_when_attribution_fails() -> (
    None
):
    """Mirrors the other rule-driven categories' existing attribution-
    unavailable behavior (``_TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION``):
    a failed team_repository_ids lookup must never let category 8 report
    whatever the (unmeasured) workload profile happened to compute.
    """

    workload = FakeWorkloadSource(
        investment_mix_results=(
            _measured_investment_mix(new_value_units=80.0, total_units=100.0),
            _measured_investment_mix(new_value_units=20.0, total_units=100.0),
        )
    )
    runtime = _runtime()
    service = OperationalDeficiencyService(
        runtime, FakeAttributionSource(measured=False), workload
    )
    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(with_comparison=True),
        team_id="team-1",
        now=_NOW,
    )
    investment = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.INVESTMENT_BALANCE
    )
    assert investment.evaluated is False
    assert investment.limitation == _TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION
    # The investment-mix source must never even be called when attribution
    # is unmeasured -- nothing meaningful could be reported regardless.
    assert workload.calls == []


@pytest.mark.asyncio
async def test_evaluate_team_investment_balance_failure_isolated_from_other_categories() -> (
    None
):
    """Codex finding, HIGH, round 5, 2026-08-02: an EXPECTED dependency
    failure (the investment-mix source raising) must degrade ONLY category
    8 to an honest unevaluated/limitation status -- it must never propagate
    out of ``evaluate_team`` and lose categories 1-7. Category 1 gets a
    real, non-empty ``DataHealthSource`` set here specifically so this test
    can prove the REST of the inventory survives intact, not merely that
    the call didn't crash.
    """

    workload = FakeWorkloadSource(raise_on_investment_mix=True)
    attribution = FakeAttributionSource(repository_ids=("repo-1", "repo-2"))
    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, attribution, workload)

    inventory = await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(with_comparison=True),
        team_id="team-1",
        now=_NOW,
    )

    investment = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.INVESTMENT_BALANCE
    )
    assert investment.evaluated is False
    assert investment.limitation is not None
    assert not any(
        f.category is DeficiencyCategory.INVESTMENT_BALANCE for f in inventory.findings
    )
    # Category 1 (and the rest of the inventory) survived the raise --
    # the stale work_items source still produced its real finding.
    data_status = next(
        s
        for s in inventory.category_statuses
        if s.category is DeficiencyCategory.DATA_INTEGRATION
    )
    assert data_status.evaluated is True
    assert data_status.finding_count >= 1
    assert len(inventory.category_statuses) == len(DEFICIENCY_CATEGORIES)


@pytest.mark.asyncio
async def test_evaluate_team_investment_balance_measured_empty_cohort_makes_no_team_scoped_runtime_call() -> (
    None
):
    """Codex's exact round-5 repro: a measured-EMPTY team (zero attributed
    repositories, so zero REPOSITORY data_health batches) must still never
    trigger a single raw-TEAM-scope ``data_health``/``status_snapshot``
    call from category 8 -- ``_investment_balance_profile`` never touches
    ``self._runtime`` at all, so this holds structurally, not by luck of
    what the fake happens to return.
    """

    workload = FakeWorkloadSource(
        investment_mix_results=(
            _measured_investment_mix(new_value_units=80.0, total_units=100.0),
            _measured_investment_mix(new_value_units=20.0, total_units=100.0),
        )
    )
    attribution = FakeAttributionSource(repository_ids=())  # measured, empty
    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, attribution, workload)

    await service.evaluate_team(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_team_scope(with_comparison=True),
        team_id="team-1",
        now=_NOW,
    )

    assert runtime.data_health_scopes == []
    # The investment-mix source still ran, unaffected by the empty cohort
    # -- the complete, unfiltered, ordered call log, both windows.
    assert workload.calls == [
        ("investment_mix", _ORG_ID, "team-1", _NOW - timedelta(days=14), _NOW),
        (
            "investment_mix",
            _ORG_ID,
            "team-1",
            _NOW - timedelta(days=28),
            _NOW - timedelta(days=14),
        ),
    ]


# ---------------------------------------------------------------------------
# Inventory-level invariants: full category coverage, ordering.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inventory_covers_every_taxonomy_category_exactly_once() -> None:
    runtime = _runtime()
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    categories = [s.category for s in inventory.category_statuses]
    assert set(categories) == set(DEFICIENCY_CATEGORIES)
    assert len(categories) == len(set(categories))


@pytest.mark.asyncio
async def test_inventory_findings_are_ordered_worst_first() -> None:
    runtime = _runtime(
        sources=(
            _source("work_items", DataHealthState.NO_DATA),  # watch
            _source("pull_requests", DataHealthState.UNAVAILABLE),  # critical
        )
    )
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    severities = [f.severity for f in inventory.findings]
    order = {
        DeficiencySeverity.CRITICAL: 0,
        DeficiencySeverity.AT_RISK: 1,
        DeficiencySeverity.WATCH: 2,
    }
    assert [order[s] for s in severities] == sorted(order[s] for s in severities)


@pytest.mark.asyncio
async def test_evaluate_project_is_deterministic_across_repeated_calls() -> None:
    """Same ``now`` twice -- the baseline determinism proof."""

    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    inventory_a = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    inventory_b = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    assert inventory_a.inventory_id == inventory_b.inventory_id
    assert [f.finding_id for f in inventory_a.findings] == [
        f.finding_id for f in inventory_b.findings
    ]


@pytest.mark.asyncio
async def test_finding_ids_are_replay_stable_across_different_evaluation_times() -> (
    None
):
    """Codex finding, 2026-08-02: the same underlying deficiency,
    re-evaluated one second apart, must mint the SAME finding_id --
    identity is what the finding is about, never when it was last
    evaluated. This is the real regression the same-``now``-twice test
    above cannot catch (proven RED against the pre-fix
    ``_mint_deficiency_finding_id``, which folded ``evaluated_at`` into
    the uuid5 payload: same inputs one second apart minted two different
    ids).
    """

    runtime = _runtime(sources=(_source("work_items", DataHealthState.STALE),))
    service = OperationalDeficiencyService(runtime, _NO_ATTRIBUTION, _NO_WORKLOAD)
    later = _NOW + timedelta(seconds=1)
    inventory_a = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=_NOW,
    )
    inventory_b = await service.evaluate_project(
        org_id=_ORG_ID,
        permission_fingerprint="fp",
        scope=_project_scope(),
        now=later,
    )
    assert inventory_a.findings and inventory_b.findings
    assert [f.finding_id for f in inventory_a.findings] == [
        f.finding_id for f in inventory_b.findings
    ]
    # evaluated_at is still real, per-evaluation metadata -- it is not
    # frozen out of the finding, only out of the finding's identity.
    assert inventory_a.findings[0].evaluated_at == _NOW
    assert inventory_b.findings[0].evaluated_at == later


# ---------------------------------------------------------------------------
# Dedup: one canonical finding per observation, with a load-bearing-guard
# mutation control (Four Verification Rules #2/#4).
# ---------------------------------------------------------------------------


def _dummy_finding(finding_id: str) -> DeficiencyFinding:
    """A minimal, structurally valid finding -- only ``finding_id`` varies,
    since these dedup tests exercise ``_dedupe_findings``/the mint
    function's key discipline, not the finding's own field content.
    """

    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=finding_id,
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.stale_watermark.v1",
        rule_version="deficiency_rule.stale_watermark.v1",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        severity=DeficiencySeverity.AT_RISK,
        fact_kind="observed",
        observed_state=SourceRequirementState.AVAILABLE_STALE,
        data_semantics="measured_zero",
        sample_count=None,
        coverage=1.0,
        current_window_days=1,
        comparison_window_days=None,
        evidence_ref_ids=(),
        evidence_classification=DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE,
        blast_radius="Required source is stale.",
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template="Investigate.",
            verification_condition="Resolves once re-evaluated fresh.",
        ),
        limitations=(),
        evaluated_at=_NOW,
    )


def test_dedupe_collapses_identical_finding_ids() -> None:
    duplicate_id = "66666666-6666-6666-6666-666666666666"
    findings = (
        _dummy_finding(duplicate_id),
        _dummy_finding(duplicate_id),
    )
    deduped = _dedupe_findings(findings)
    assert len(deduped) == 1


def test_dedupe_key_is_deterministic_over_identical_inputs() -> None:
    """The dedupe guard is only load-bearing because minting is
    deterministic. A mutation that made minting non-deterministic (e.g.
    injecting a random component) would silently defeat _dedupe_findings
    without touching it at all -- this control kills exactly that mutation
    by asserting two calls with identical inputs mint identical ids and,
    conversely, that a differing discriminator (the axis a duplicate-
    observation bug would fail to vary) mints a different id.
    """

    same_a = _mint_deficiency_finding_id(
        org_id=_ORG_ID,
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.stale_watermark.v1",
        subject_kind="project",
        subject_id="proj-1",
        discriminator="work_items",
    )
    same_b = _mint_deficiency_finding_id(
        org_id=_ORG_ID,
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.stale_watermark.v1",
        subject_kind="project",
        subject_id="proj-1",
        discriminator="work_items",
    )
    different_discriminator = _mint_deficiency_finding_id(
        org_id=_ORG_ID,
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.stale_watermark.v1",
        subject_kind="project",
        subject_id="proj-1",
        discriminator="pull_requests",
    )
    assert same_a == same_b
    assert same_a != different_discriminator


def test_dedupe_mutation_control_wrong_key_produces_duplicates() -> None:
    """Plant the defect a correct dedupe key exists to catch: key by
    object identity instead of the deterministic finding_id. The OLD
    (correct) dedupe collapses two content-identical findings to one; a
    broken key that ignores content lets the duplicate through -- proving
    _dedupe_findings' use of finding_id, not object identity, is what
    makes deduplication work at all.
    """

    duplicate_id = "77777777-7777-7777-7777-777777777777"
    findings = (
        _dummy_finding(duplicate_id),
        _dummy_finding(duplicate_id),
    )

    # Correct behavior (the guard under test).
    assert len(_dedupe_findings(findings)) == 1

    # The mutation: dedupe by object identity instead of finding_id.
    broken_seen: dict[int, object] = {}
    for finding in findings:
        broken_seen.setdefault(id(finding), finding)
    assert len(broken_seen) == 2  # old test's assumption fails under the mutation
