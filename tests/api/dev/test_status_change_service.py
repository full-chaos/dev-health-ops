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
    ObservedChange,
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
async def test_assessment_source_bound_never_false_passes_completion() -> None:
    children = tuple(
        _fact(f"child-{index:04d}", "done", required=True) for index in range(1_000)
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

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.warnings == ("status assessment source bound reached",)
    # CHAOS-3297 s2 round 2 (codex HIGH): when the SOURCE itself hit its
    # bound (1,000 items), the 1,000 children the source did return could
    # look deceptively "complete" (1000/1000) even though the true total
    # is unknown -- withhold the denominator entirely rather than publish
    # a count that might be an undercount by exactly the omitted rows.
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None
    assert result.actual.display_truncated is True


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
