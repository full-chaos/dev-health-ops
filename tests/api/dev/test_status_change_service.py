from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts import (
    ClaimKind,
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    FreshnessState,
    MetricID,
)
from dev_health_ops.api.dev.metrics.service import (
    MetricQueryService,
    MetricSourceRef,
    MetricSourceState,
    RawMetricResult,
    RawMetricRow,
)
from dev_health_ops.api.dev.status_change_service import (
    CHANGE_CONTRACT_VERSION,
    STATUS_RULE_VERSION,
    ChangeCategory,
    ChangeSummaryRequest,
    ChangeWindow,
    CIFact,
    CompletionState,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    RawChangeSummary,
    RawStatusSnapshot,
    SourceReference,
    StatusChangeService,
    StatusFact,
    StatusResultState,
    StatusSnapshotRequest,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


def _scope(kind: DirectScope = DirectScope.ISSUE) -> DevScope:
    entity_type = (
        EntityType.PROJECT if kind is DirectScope.PROJECT else EntityType.ISSUE
    )
    entity_id = "project-1" if kind is DirectScope.PROJECT else "issue-1"
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=kind,
        repositories=["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=entity_type,
                entity_id=entity_id,
                display_label=entity_id,
                repository_id="repo-a",
            )
        ],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


def _source_ref(freshness: FreshnessState = FreshnessState.FRESH) -> SourceReference:
    return SourceReference(
        ref_id="source:work-items",
        source_system="work_items",
        source_version="work-items.v1",
        freshness=freshness,
        watermark=NOW,
        evidence_ref_ids=("ev-work-items",),
    )


def _fact(
    entity_id: str,
    status: str,
    *,
    required: bool | None = False,
    label: str | None = None,
) -> StatusFact:
    return StatusFact(
        entity_type="issue",
        entity_id=entity_id,
        display_label=label or entity_id,
        status=status,
        observed_at=NOW,
        source_ref_id="source:work-items",
        evidence_ref_ids=(f"ev-{entity_id}",),
        required=required,
    )


def _deployment(status: str = "success") -> DeploymentFact:
    return DeploymentFact(
        entity_id="deployment-1",
        display_label="Production deployment",
        status=status,
        environment="production",
        required=True,
        observed_at=NOW,
        source_ref_id="source:work-items",
        evidence_ref_ids=("ev-deployment-1",),
    )


class Source:
    def __init__(self, snapshot: RawStatusSnapshot) -> None:
        self.snapshot = snapshot
        self.change_calls: list[tuple[ChangeWindow, ChangeWindow, int]] = []

    async def status_snapshot(self, **_kwargs: object) -> RawStatusSnapshot:
        return self.snapshot

    async def change_summary(
        self,
        *,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
        **_kwargs: object,
    ) -> RawChangeSummary:
        self.change_calls.append((current, comparison, limit))
        return RawChangeSummary(
            changes=(
                ObservedChange(
                    change_id="change-b",
                    category=ChangeCategory.STATUS,
                    entity_type="issue",
                    entity_id="issue-1",
                    display_label="Issue 1",
                    before="in_progress",
                    after="done",
                    observed_at=NOW - timedelta(hours=1),
                    claim_kind=ClaimKind.OBSERVED,
                    relationship_chain=(),
                    metric_id=None,
                    metric_value=None,
                    metric_comparison_value=None,
                    source_ref_ids=("source:work-items",),
                    evidence_ref_ids=("ev-transition",),
                ),
            ),
            source_refs=(_source_ref(),),
        )


class MetricSource:
    async def watermark(self, org_id, definition, scope):
        return f"{org_id}:{definition.metric_id.value}:{scope.time_range.end}"

    async def query(
        self,
        org_id,
        definition,
        scope,
        *,
        comparison,
        include_series,
        max_series_points,
    ):
        del org_id, include_series, max_series_points
        latest_day = date(2026, 7, 20) if comparison else date(2026, 7, 27)
        watermark = datetime.combine(latest_day, datetime.min.time(), tzinfo=UTC)
        return RawMetricResult(
            rows=(
                RawMetricRow(
                    dimensions=(), value=4.0 if comparison else 6.0, series=()
                ),
            ),
            watermark=watermark,
            latest_materialized_day=latest_day,
            source_state=MetricSourceState.AVAILABLE,
            covered_days=7,
            expected_days=7,
            source_refs=(
                MetricSourceRef(
                    ref_id=f"metric-source:{definition.metric_id.value}",
                    source_table=definition.source_table,
                    source_version=definition.source_version,
                    watermark=watermark,
                    query_version=definition.query_version,
                ),
            ),
        )


@pytest.mark.asyncio
async def test_completed_parent_with_incomplete_required_child_is_not_ready() -> None:
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(_fact("issue-child", "in_progress", required=True),),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.NOT_READY
    assert result.actual.rule_version == STATUS_RULE_VERSION
    assert result.actual.reason_codes == ("required_child_incomplete",)
    assert result.actual.required_children[0].entity_id == "issue-child"
    assert result.actual.conflicts[0].code == (
        "declared_complete_conflicts_with_observed_work"
    )
    assert result.actual.evidence_ref_ids == (
        "ev-deployment-1",
        "ev-issue-1",
        "ev-issue-child",
    )
    # CHAOS-3297 s2, case 5: one incomplete required child under a Done
    # parent -- the complete denominator/numerator (1 total, 0 complete)
    # must be reported, not just the presence of the incomplete child.
    assert result.actual.required_child_total == 1
    assert result.actual.required_child_complete == 0
    assert result.actual.display_truncated is False


@pytest.mark.asyncio
async def test_project_completion_does_not_require_a_declared_project_status() -> None:
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=None,
                children=(_fact("issue-child", "done", required=True),),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PROJECT), as_of=NOW),
    )

    assert result.actual.state is CompletionState.READY
    assert "declared_status_missing" not in result.actual.reason_codes


@pytest.mark.asyncio
async def test_completed_parent_with_unknown_child_requirement_is_indeterminate() -> (
    None
):
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(_fact("issue-child", "in_progress", required=None),),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("child_requirement_unknown",)
    assert result.actual.required_children == ()
    assert result.children[0].required is None
    assert result.actual.conflicts == ()
    # CHAOS-3297 s2, case 2: the requirement itself is unknown, so the
    # denominator is honestly zero -- never fabricated as "0 required, so
    # vacuously complete". Callers must gate on reason_codes/state, not on
    # a bare 0/0 count.
    assert result.actual.required_child_total == 0
    assert result.actual.required_child_complete == 0
    assert result.actual.display_truncated is False


@pytest.mark.asyncio
async def test_green_ci_with_unknown_required_skip_semantics_cannot_prove_completion() -> (
    None
):
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                ci=(
                    CIFact(
                        entity_id="ci-1",
                        display_label="CI 1",
                        conclusion="success",
                        required=True,
                        skipped_required_work=None,
                        observed_at=NOW,
                        source_ref_id="source:work-items",
                        evidence_ref_ids=("ev-ci-1",),
                    ),
                ),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope())
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_ci_skip_state_unknown",)


@pytest.mark.asyncio
async def test_stale_source_is_partial_and_never_ready() -> None:
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                deployments=(_deployment(),),
                source_refs=(_source_ref(FreshnessState.STALE),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope())
    )

    assert result.state is StatusResultState.PARTIAL
    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_source_not_fresh",)


def test_change_windows_must_be_explicit_equal_duration() -> None:
    with pytest.raises(ValueError, match="equal duration"):
        ChangeSummaryRequest(
            scope=_scope(),
            current_start=NOW - timedelta(days=7),
            current_end=NOW,
            comparison_start=NOW - timedelta(days=10),
            comparison_end=NOW - timedelta(days=7),
        )


@pytest.mark.asyncio
async def test_change_summary_is_reproducible_and_tenant_scoped() -> None:
    source = Source(
        RawStatusSnapshot(
            declared=_fact("issue-1", "done"), source_refs=(_source_ref(),)
        )
    )
    service = StatusChangeService(source)
    request = ChangeSummaryRequest(
        scope=_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    first = await service.change_summary("org-a", "permission-v1", request)
    second = await service.change_summary("org-a", "permission-v1", request)

    assert first == second
    assert first.contract_version == CHANGE_CONTRACT_VERSION
    assert first.changes[0].claim_kind is ClaimKind.OBSERVED
    assert source.change_calls[0] == source.change_calls[1]
    with pytest.raises(ValueError, match="authenticated organization"):
        await service.change_summary("org-b", "permission-v1", request)


@pytest.mark.asyncio
async def test_change_summary_reserves_bound_for_registered_metric_deltas() -> None:
    source = Source(
        RawStatusSnapshot(
            declared=_fact("issue-1", "done"), source_refs=(_source_ref(),)
        )
    )
    service = StatusChangeService(
        source, metric_service=MetricQueryService(MetricSource())
    )
    request = ChangeSummaryRequest(
        scope=_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
        max_items=5,
    )

    result = await service.change_summary("org-a", "permission-v1", request)

    assert len(result.changes) == 5
    assert all(change.category is ChangeCategory.METRIC for change in result.changes)
    assert {change.metric_id for change in result.changes} == {
        MetricID.DEPLOYMENTS_COUNT,
        MetricID.CHANGE_FAILURE_RATE,
        MetricID.INVESTMENT_ALLOCATION_PCT,
        MetricID.CYCLOMATIC_PER_KLOC,
        MetricID.COMPOUNDING_RISK_SCORE,
    }
    assert all(change.metric_value == 6.0 for change in result.changes)
    assert all(change.metric_comparison_value == 4.0 for change in result.changes)
    assert all(change.source_ref_ids for change in result.changes)
    returned_source_ids = {ref.ref_id for ref in result.source_refs}
    assert all(
        set(change.source_ref_ids) <= returned_source_ids for change in result.changes
    )
    assert (
        len([ref for ref in result.source_refs if ref.ref_id.startswith("metric-")])
        == 5
    )


@pytest.mark.asyncio
async def test_order_and_bounds_are_server_owned() -> None:
    children = tuple(
        _fact(f"child-{index:03d}", "done", required=True, label=f"Child {100 - index}")
        for index in range(100)
    )
    source = Source(
        RawStatusSnapshot(
            declared=_fact("issue-1", "done"),
            children=children,
            blockers=(
                _fact("b", "open", label="Zulu"),
                _fact("a", "open", label="Alpha"),
            ),
            source_refs=(_source_ref(),),
        )
    )
    service = StatusChangeService(source)

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=10),
    )

    assert len(result.actual.required_children) == 10
    assert [fact.display_label for fact in result.blockers] == ["Alpha", "Zulu"]


@pytest.mark.asyncio
async def test_completion_assesses_required_children_beyond_display_bound() -> None:
    children = tuple(
        _fact(
            f"child-{index:03d}",
            "in_progress" if index == 100 else "done",
            required=True,
        )
        for index in range(101)
    )
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=children,
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert "required_child_incomplete" in result.actual.reason_codes
    assert len(result.actual.required_children) == 100
    # CHAOS-3297 s2, case 4a: the display list is bounded to 100, but the
    # denominator/numerator must reflect the full 101-child assessment set
    # -- this is the exact defect this stack fixes (the denominator was
    # previously discarded along with the truncated display list).
    assert result.actual.required_child_total == 101
    assert result.actual.required_child_complete == 100
    assert result.actual.display_truncated is True


@pytest.mark.asyncio
async def test_children_exactly_at_bound_with_no_truncation_signal_keeps_the_denominator() -> (
    None
):
    """CHAOS-3297 s2 round 3 (codex MEDIUM): exactly
    MAX_STATUS_ASSESSMENT_ITEMS legitimate children, with the source
    reporting no truncation, is a real, complete denominator -- it must
    never be confused with a truncated one just because the count happens
    to equal the bound. Truncation is a claim only the source can make
    (``children_source_truncated``/``membership_source_truncated``), never
    a length heuristic."""
    children = tuple(
        _fact(f"child-{index:04d}", "done", required=True) for index in range(1_000)
    )
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=children,
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.READY
    assert "assessment_source_limit_reached" not in result.actual.reason_codes
    assert result.warnings == ()
    assert result.actual.required_child_total == 1_000
    assert result.actual.required_child_complete == 1_000
    # The display bound is a separate, always-legitimate concern from
    # source truncation -- 1,000 real children still only display 100.
    assert result.actual.display_truncated is True
    assert len(result.actual.required_children) == 100


@pytest.mark.asyncio
async def test_explicit_children_source_truncation_signal_withholds_the_denominator() -> (
    None
):
    """The service test that used to encode the exactly-1,000-children
    ambiguity (CHAOS-3297 s2 round 3, codex MEDIUM): truncation must be
    driven by the source's own provenance flag, not inferred from a
    count. A small, otherwise-unremarkable required-child set with
    ``children_source_truncated=True`` must still withhold the
    denominator and force non-READY, exactly like the 1,000-item case
    used to (incorrectly) via the length heuristic."""
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(_fact("child-a", "done", required=True),),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
                children_source_truncated=True,
            )
        )
    )

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.warnings == ("status assessment source bound reached",)
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None
    assert result.actual.display_truncated is False


@pytest.mark.asyncio
async def test_membership_source_truncation_signal_also_withholds_the_denominator() -> (
    None
):
    """CHAOS-3297 s2 round 3 (codex HIGH): membership truncation is a
    distinct root cause one hop upstream of the children fetch itself
    (native_status_change._WORK_UNIT_MEMBERS_SQL mixes issue and PR
    members in one limited query) -- it must gate the denominator exactly
    like a direct children-source truncation, not just fire a reason code
    that happens to force INDETERMINATE for unrelated reasons."""
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(_fact("child-a", "done", required=True),),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
                membership_source_truncated=True,
            )
        )
    )

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None


# ---------------------------------------------------------------------------
# CHAOS-3408: required-children/blockers have NO single declared/children
# completion tree for an ORGANIZATION or TEAM subject (a deliberate CHAOS-
# 3303 design -- native_status_change.TEAM_NOT_APPLICABLE_SOURCES -- never a
# data gap). ``RawStatusSnapshot.children`` is therefore always structurally
# empty for these two scopes, and treating that the same as a genuinely-
# computed real zero produced a fabricated-looking "0 of 0 required items
# are complete" for every org-wide/team-wide readiness question. The
# denominator must be WITHHELD (None) here, exactly like a truncated source
# -- but distinguishably so: never carrying
# ``assessment_source_limit_reached`` (this is not a truncation, and must
# never be confused with one downstream).
# ---------------------------------------------------------------------------


def _org_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.ORGANIZATION,
        repositories=["repo-a"],
        entity_refs=[],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


def _team_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.TEAM,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM,
                entity_id="team-platform",
                display_label="Platform",
            )
        ],
        team_ids=["team-platform"],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_factory", [_org_scope, _team_scope])
async def test_organization_and_team_scope_withhold_the_required_child_denominator(
    scope_factory,
) -> None:
    """The live repro: an organization-wide (or team-wide) readiness
    question must never report a fabricated "0 of 0 required items are
    complete" -- the denominator is withheld (None), exactly as if the
    source had reported a real truncation, but WITHOUT the truncation
    reason code (this is a structural non-applicability, not a source
    limit)."""

    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=None,
                children=(),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(scope_factory(), as_of=NOW)
    )

    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None
    # Never confused with a real source truncation downstream (the
    # "assessment hit a display limit" copy would be actively misleading
    # here -- nothing was ever attempted, let alone limited).
    assert "assessment_source_limit_reached" not in result.actual.reason_codes


@pytest.mark.asyncio
async def test_project_scope_with_genuinely_zero_required_children_is_a_real_zero() -> (
    None
):
    """No regression: PROJECT/ISSUE/WORK_UNIT scope's required-child
    concept IS applicable and IS queried -- a real query that genuinely
    finds zero required children must still report a REAL ``0``, never
    withheld, and this must stay distinguishable from CHAOS-3408's
    organization/team non-applicability."""

    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("project-1", "done"),
                children=(),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PROJECT), as_of=NOW),
    )

    assert result.actual.required_child_total == 0
    assert result.actual.required_child_complete == 0


def _many_blockers(n: int) -> tuple[StatusFact, ...]:
    return tuple(_fact(f"blocker-{i:04d}", "resolved") for i in range(n))


def _many_pull_requests(n: int) -> tuple[PullRequestFact, ...]:
    return tuple(
        PullRequestFact(
            entity_id=f"pr-{i:04d}",
            display_label=f"PR {i}",
            state="merged",
            review_state="APPROVED",
            changes_requested=0,
            merged=True,
            observed_at=NOW,
            source_ref_id="source:work-items",
            evidence_ref_ids=(f"ev-pr-{i:04d}",),
            required=False,
        )
        for i in range(n)
    )


def _many_ci(n: int) -> tuple[CIFact, ...]:
    return tuple(
        CIFact(
            entity_id=f"ci-{i:04d}",
            display_label=f"CI {i}",
            conclusion="success",
            required=False,
            skipped_required_work=None,
            observed_at=NOW,
            source_ref_id="source:work-items",
            evidence_ref_ids=(f"ev-ci-{i:04d}",),
        )
        for i in range(n)
    )


def _many_deployments(n: int) -> tuple[DeploymentFact, ...]:
    return tuple(
        DeploymentFact(
            entity_id=f"deployment-{i:04d}",
            display_label=f"Deployment {i}",
            status="success",
            environment="production",
            required=True,
            observed_at=NOW,
            source_ref_id="source:work-items",
            evidence_ref_ids=(f"ev-deployment-{i:04d}",),
        )
        for i in range(n)
    )


def _many_incidents(n: int) -> tuple[IncidentFact, ...]:
    return tuple(
        IncidentFact(
            entity_id=f"incident-{i:04d}",
            display_label=f"Incident {i}",
            status="resolved",
            active=False,
            blocking=False,
            observed_at=NOW,
            source_ref_id="source:work-items",
            evidence_ref_ids=(f"ev-incident-{i:04d}",),
        )
        for i in range(n)
    )


def _blockers_snapshot(n: int, *, truncated: bool) -> RawStatusSnapshot:
    return RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        blockers=_many_blockers(n),
        deployments=(_deployment(),),
        source_refs=(_source_ref(),),
        blockers_source_truncated=truncated,
    )


def _pull_requests_snapshot(n: int, *, truncated: bool) -> RawStatusSnapshot:
    return RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        pull_requests=_many_pull_requests(n),
        deployments=(_deployment(),),
        source_refs=(_source_ref(),),
        pull_requests_source_truncated=truncated,
    )


def _ci_snapshot(n: int, *, truncated: bool) -> RawStatusSnapshot:
    return RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        ci=_many_ci(n),
        deployments=(_deployment(),),
        source_refs=(_source_ref(),),
        ci_source_truncated=truncated,
    )


def _deployments_snapshot(n: int, *, truncated: bool) -> RawStatusSnapshot:
    return RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        deployments=_many_deployments(n),
        source_refs=(_source_ref(),),
        deployments_source_truncated=truncated,
    )


def _incidents_snapshot(n: int, *, truncated: bool) -> RawStatusSnapshot:
    return RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        incidents=_many_incidents(n),
        deployments=(_deployment(),),
        source_refs=(_source_ref(),),
        incidents_source_truncated=truncated,
    )


_CATEGORY_SNAPSHOT_BUILDERS = {
    "blockers_source_truncated": _blockers_snapshot,
    "pull_requests_source_truncated": _pull_requests_snapshot,
    "ci_source_truncated": _ci_snapshot,
    "deployments_source_truncated": _deployments_snapshot,
    "incidents_source_truncated": _incidents_snapshot,
}


@pytest.mark.asyncio
@pytest.mark.parametrize("flag_name", sorted(_CATEGORY_SNAPSHOT_BUILDERS))
async def test_category_truncation_is_explicit_provenance_only_not_a_length_count(
    flag_name: str,
) -> None:
    """CHAOS-3297 s2 round 5 (codex MEDIUM): finish what round 3 started
    for every remaining assessment category (blockers, pull_requests, ci,
    deployments, incidents) -- ``assessment_source_limit_reached`` comes
    ONLY from that category's own explicit truncation flag now, never from
    ``len(category) >= MAX_STATUS_ASSESSMENT_ITEMS``. Parametrized: exactly
    MAX_STATUS_ASSESSMENT_ITEMS (1,000) legitimate items with no signal
    must NOT report truncation (the removed equality heuristic's false
    positive); a small result set with the explicit flag set MUST.
    """
    build = _CATEGORY_SNAPSHOT_BUILDERS[flag_name]

    result_at_cap = await StatusChangeService(
        Source(build(1_000, truncated=False))
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )
    assert "assessment_source_limit_reached" not in result_at_cap.actual.reason_codes

    result_truncated = await StatusChangeService(
        Source(build(1, truncated=True))
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )
    assert "assessment_source_limit_reached" in result_truncated.actual.reason_codes


def test_rejects_naive_as_of_and_out_of_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        StatusSnapshotRequest(_scope(), as_of=datetime(2026, 7, 28))
    with pytest.raises(ValueError, match="between 1 and 100"):
        StatusSnapshotRequest(_scope(), max_items=101)


@pytest.mark.asyncio
async def test_fresh_complete_fixture_is_ready() -> None:
    raw = RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        deployments=(_deployment(),),
        source_refs=(_source_ref(),),
    )
    assert replace(raw, warnings=("fixture",)).warnings == ("fixture",)
    result = await StatusChangeService(Source(raw)).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )
    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.READY


@pytest.mark.asyncio
async def test_merged_delivery_without_release_evidence_is_indeterminate() -> None:
    raw = RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        source_refs=(_source_ref(),),
    )

    result = await StatusChangeService(Source(raw)).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.state is StatusResultState.INSUFFICIENT_EVIDENCE
    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_release_evidence_missing",)


@pytest.mark.asyncio
async def test_unsuccessful_required_release_is_not_ready() -> None:
    raw = RawStatusSnapshot(
        declared=_fact("issue-1", "done"),
        deployments=(_deployment("failed"),),
        source_refs=(_source_ref(),),
    )

    result = await StatusChangeService(Source(raw)).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.actual.reason_codes == ("required_deployment_not_succeeded",)


@pytest.mark.asyncio
async def test_known_denominator_reports_total_and_complete_counts() -> None:
    """CHAOS-3297 s2, case 1: a straightforward, well-below-bound required
    child set -- the complete/total counts must match what a human counting
    the raw facts would get."""
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(
                    _fact("child-a", "done", required=True),
                    _fact("child-b", "done", required=True),
                    _fact("child-c", "in_progress", required=True),
                ),
                deployments=(_deployment(),),
                source_refs=(_source_ref(),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.required_child_total == 3
    assert result.actual.required_child_complete == 2
    assert result.actual.display_truncated is False
    assert "required_child_incomplete" in result.actual.reason_codes


@pytest.mark.asyncio
async def test_stale_denominator_counts_are_honest_but_state_withholds_trust() -> None:
    """CHAOS-3297 s2, case 3: a stale source can still yield a
    complete-looking 1/1 count. The count itself must stay honest (not
    zeroed out just because the source is stale), but the result state
    must never present that count as COMPLETE -- callers gate on
    ``state``/``reason_codes``, never on the ratio alone."""
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(_fact("issue-child", "done", required=True),),
                deployments=(_deployment(),),
                source_refs=(_source_ref(FreshnessState.STALE),),
            )
        )
    )

    result = await service.status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.required_child_total == 1
    assert result.actual.required_child_complete == 1
    assert result.actual.display_truncated is False
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.state is StatusResultState.PARTIAL
    assert result.state is not StatusResultState.COMPLETE
