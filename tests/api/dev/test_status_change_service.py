from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts import (
    ClaimKind,
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    FreshnessState,
)
from dev_health_ops.api.dev.status_change_service import (
    CHANGE_CONTRACT_VERSION,
    STATUS_RULE_VERSION,
    ChangeCategory,
    ChangeSummaryRequest,
    ChangeWindow,
    CIFact,
    CompletionState,
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


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.ISSUE,
        repositories=["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue-1",
                display_label="Issue 1",
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
    entity_id: str, status: str, *, required: bool = False, label: str | None = None
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


@pytest.mark.asyncio
async def test_completed_parent_with_incomplete_required_child_is_not_ready() -> None:
    service = StatusChangeService(
        Source(
            RawStatusSnapshot(
                declared=_fact("issue-1", "done"),
                children=(_fact("issue-child", "in_progress", required=True),),
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
    assert result.actual.evidence_ref_ids == ("ev-issue-1", "ev-issue-child")


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


def test_rejects_naive_as_of_and_out_of_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        StatusSnapshotRequest(_scope(), as_of=datetime(2026, 7, 28))
    with pytest.raises(ValueError, match="between 1 and 100"):
        StatusSnapshotRequest(_scope(), max_items=101)


def test_fresh_complete_fixture_is_ready() -> None:
    raw = RawStatusSnapshot(
        declared=_fact("issue-1", "done"), source_refs=(_source_ref(),)
    )
    assert replace(raw, warnings=("fixture",)).warnings == ("fixture",)
