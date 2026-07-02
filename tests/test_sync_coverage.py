from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from dev_health_ops.api.services.sync_coverage import (
    CoverageInterval,
    EffectiveScope,
    UnitWindow,
    build_coverage_summary_payload,
    classify_staleness,
    failed_ranges_not_superseded,
    merge_intervals,
    subtract_intervals,
)
from dev_health_ops.models.integrations import IntegrationSource
from dev_health_ops.models.settings import ScheduledJob, SyncConfiguration


def _dt(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


def _source(source_id: uuid.UUID) -> IntegrationSource:
    return IntegrationSource(
        id=source_id,
        org_id="org-1",
        integration_id=uuid.uuid4(),
        provider="github",
        source_type="repository",
        external_id="acme/repo",
        name="repo",
        full_name="acme/repo",
        metadata_={},
        is_enabled=True,
    )


def _config() -> SyncConfiguration:
    return SyncConfiguration(
        org_id="org-1",
        name="coverage",
        provider="github",
        sync_targets=["git"],
        sync_options={"schedule_cron": "0 * * * *"},
        integration_id=uuid.uuid4(),
        planner_managed=True,
    )


def _window(
    since: datetime,
    before: datetime,
    *,
    source_id: uuid.UUID,
    dataset_key: str = "commits",
    status: str = "success",
    run_time: datetime | None = None,
) -> UnitWindow:
    return UnitWindow(
        since=since,
        before=before,
        source_id=str(source_id),
        dataset_key=dataset_key,
        run_id=str(uuid.uuid4()),
        status=status,
        run_time=run_time or before,
    )


def _summary(
    windows: list[UnitWindow],
    *,
    backfill_requested: list[CoverageInterval] | None = None,
    active_run_ids: set[str] | None = None,
    now: datetime = _dt(2, 1),
) -> dict:
    source_id = uuid.UUID(windows[0].source_id) if windows else uuid.uuid4()
    config = _config()
    scope = EffectiveScope(
        integration_id=config.integration_id,
        sources=(_source(source_id),),
        dataset_keys=("commits",),
    )
    schedule = ScheduledJob(
        org_id="org-1",
        name="sync-coverage",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        sync_config_id=config.id,
    )
    return build_coverage_summary_payload(
        config=config,
        scope=scope,
        windows=windows,
        backfill_requested=backfill_requested or [],
        active_run_ids=active_run_ids or set(),
        active_schedule=schedule,
        has_schedule_row=True,
        generated_at=now,
    )


def test_merge_intervals_collapses_overlap_and_adjacency():
    merged = merge_intervals(
        [
            CoverageInterval(_dt(1), _dt(2), source_ids=("a",)),
            CoverageInterval(_dt(2), _dt(3), source_ids=("b",)),
            CoverageInterval(_dt(5), _dt(6)),
        ]
    )

    assert [(item.since, item.before) for item in merged] == [
        (_dt(1), _dt(3)),
        (_dt(5), _dt(6)),
    ]
    assert merged[0].source_ids == ("a", "b")


def test_subtract_intervals_returns_partial_gap():
    gaps = subtract_intervals(
        [CoverageInterval(_dt(1), _dt(4))],
        [CoverageInterval(_dt(1), _dt(2)), CoverageInterval(_dt(3), _dt(4))],
    )

    assert [(gap.since, gap.before) for gap in gaps] == [(_dt(2), _dt(3))]


def test_complete_summary_is_healthy():
    source_id = uuid.uuid4()
    summary = _summary([_window(_dt(1), _dt(2), source_id=source_id)])

    assert summary["overall"]["health"] == "healthy"
    assert summary["datasets"][0]["status"] == "healthy"
    assert summary["datasets"][0]["gaps"] == []


def test_failed_window_without_later_success_marks_failed():
    source_id = uuid.uuid4()
    summary = _summary([_window(_dt(1), _dt(2), source_id=source_id, status="failed")])

    assert summary["overall"]["health"] == "failed"
    assert summary["overall"]["failed_range_count"] == 1
    assert summary["datasets"][0]["failed_ranges"][0]["since"] == _dt(1)


def test_backfill_intent_exposes_gap_without_failed_unit():
    source_id = uuid.uuid4()
    summary = _summary(
        [_window(_dt(1), _dt(2), source_id=source_id)],
        backfill_requested=[
            CoverageInterval(_dt(1), _dt(3), source_ids=(str(source_id),))
        ],
    )

    assert summary["overall"]["health"] == "gaps"
    assert [
        (gap["since"], gap["before"]) for gap in summary["datasets"][0]["gaps"]
    ] == [(_dt(2), _dt(3))]


def test_stale_classification_uses_schedule_grace():
    stale = classify_staleness(_dt(1), now=_dt(3), schedule_interval=timedelta(hours=1))

    assert stale.status == "stale"


def test_empty_legacy_summary_is_insufficient_data():
    summary = _summary([])

    assert summary["data_basis"] == "legacy"
    assert summary["overall"]["health"] == "insufficient_data"
    assert summary["datasets"][0]["status"] == "insufficient_data"


def test_failed_retry_superseded_by_later_success_is_healthy():
    source_id = uuid.uuid4()
    failed = _window(
        _dt(1), _dt(2), source_id=source_id, status="failed", run_time=_dt(2)
    )
    success = _window(
        _dt(1), _dt(2), source_id=source_id, status="success", run_time=_dt(3)
    )

    assert failed_ranges_not_superseded([failed], [success]) == []
    assert _summary([failed, success])["overall"]["health"] == "healthy"


def test_full_resync_window_can_cover_entire_requested_range():
    source_id = uuid.uuid4()
    summary = _summary([_window(_dt(1), _dt(31), source_id=source_id)])

    dataset = summary["datasets"][0]
    assert dataset["covered_ranges"][0]["since"] == _dt(1)
    assert dataset["covered_ranges"][0]["before"] == _dt(31)
    assert summary["overall"]["health"] == "healthy"
