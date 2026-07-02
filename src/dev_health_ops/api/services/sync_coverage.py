from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any

from croniter import croniter as Croniter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.integrations import (
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.models.settings import JobStatus, ScheduledJob, SyncConfiguration
from dev_health_ops.sync.datasets import supported_datasets

HISTORY_LOOKBACK_DAYS = 180
STALE_MINIMUM_GRACE = timedelta(hours=6)
STALE_FALLBACK_GRACE = timedelta(hours=48)
INTERVAL_ADJACENCY_TOLERANCE = timedelta(microseconds=1)

TERMINAL_UNIT_STATUSES = {
    SyncRunUnitStatus.SUCCESS.value,
    SyncRunUnitStatus.FAILED.value,
}
ACTIVE_RUN_STATUSES = {
    SyncRunStatus.PLANNED.value,
    SyncRunStatus.DISPATCHING.value,
    SyncRunStatus.RUNNING.value,
}


@dataclass(frozen=True)
class CoverageInterval:
    since: datetime
    before: datetime
    source_ids: tuple[str, ...] = ()
    run_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class UnitWindow:
    since: datetime
    before: datetime
    source_id: str
    dataset_key: str
    run_id: str
    status: str
    run_time: datetime


@dataclass(frozen=True)
class StaleClassification:
    status: str
    stale_after: datetime | None


@dataclass(frozen=True)
class EffectiveScope:
    integration_id: uuid.UUID | None
    sources: tuple[IntegrationSource, ...]
    dataset_keys: tuple[str, ...]


@dataclass
class _DatasetCoverage:
    dataset_key: str
    requested: list[CoverageInterval] = field(default_factory=list)
    covered: list[CoverageInterval] = field(default_factory=list)
    gaps: list[CoverageInterval] = field(default_factory=list)
    stale_ranges: list[CoverageInterval] = field(default_factory=list)
    failed_ranges: list[CoverageInterval] = field(default_factory=list)
    covered_through: datetime | None = None
    status: str = "insufficient_data"


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def merge_intervals(
    intervals: Iterable[CoverageInterval],
    *,
    tolerance: timedelta = INTERVAL_ADJACENCY_TOLERANCE,
) -> list[CoverageInterval]:
    normalized = sorted(
        (
            CoverageInterval(
                ensure_utc(interval.since),
                ensure_utc(interval.before),
                tuple(sorted(set(interval.source_ids))),
                tuple(sorted(set(interval.run_ids))),
            )
            for interval in intervals
            if ensure_utc(interval.since) < ensure_utc(interval.before)
        ),
        key=lambda item: (item.since, item.before),
    )
    merged: list[CoverageInterval] = []
    for interval in normalized:
        if not merged:
            merged.append(interval)
            continue
        last = merged[-1]
        if interval.since <= last.before + tolerance:
            merged[-1] = CoverageInterval(
                since=last.since,
                before=max(last.before, interval.before),
                source_ids=tuple(
                    sorted(set(last.source_ids).union(interval.source_ids))
                ),
                run_ids=tuple(sorted(set(last.run_ids).union(interval.run_ids))),
            )
            continue
        merged.append(interval)
    return merged


def subtract_intervals(
    requested: Iterable[CoverageInterval], covered: Iterable[CoverageInterval]
) -> list[CoverageInterval]:
    gaps: list[CoverageInterval] = []
    covered_merged = merge_intervals(covered)
    for req in merge_intervals(requested):
        cursor = req.since
        for cov in covered_merged:
            if cov.before <= cursor:
                continue
            if cov.since >= req.before:
                break
            if cov.since > cursor:
                gaps.append(
                    CoverageInterval(
                        since=cursor,
                        before=min(cov.since, req.before),
                        source_ids=req.source_ids,
                        run_ids=req.run_ids,
                    )
                )
            cursor = max(cursor, cov.before)
            if cursor >= req.before:
                break
        if cursor < req.before:
            gaps.append(
                CoverageInterval(
                    since=cursor,
                    before=req.before,
                    source_ids=req.source_ids,
                    run_ids=req.run_ids,
                )
            )
    return merge_intervals(gaps)


def failed_ranges_not_superseded(
    failed: Iterable[UnitWindow], successful: Iterable[UnitWindow]
) -> list[CoverageInterval]:
    success_windows = list(successful)
    ranges: list[CoverageInterval] = []
    for failure in failed:
        later_cover = [
            CoverageInterval(
                since=success.since,
                before=success.before,
                source_ids=(success.source_id,),
                run_ids=(success.run_id,),
            )
            for success in success_windows
            if success.source_id == failure.source_id
            and success.dataset_key == failure.dataset_key
            and success.run_time >= failure.run_time
        ]
        remaining = subtract_intervals(
            [
                CoverageInterval(
                    since=failure.since,
                    before=failure.before,
                    source_ids=(failure.source_id,),
                    run_ids=(failure.run_id,),
                )
            ],
            later_cover,
        )
        ranges.extend(remaining)
    return merge_intervals(ranges)


def classify_staleness(
    covered_through: datetime | None,
    *,
    now: datetime | None = None,
    schedule_interval: timedelta | None = None,
    paused: bool = False,
    scheduled: bool = True,
) -> StaleClassification:
    if paused:
        return StaleClassification(status="paused", stale_after=None)
    if not scheduled:
        return StaleClassification(status="not_scheduled", stale_after=None)
    if covered_through is None:
        return StaleClassification(status="insufficient_data", stale_after=None)
    current = ensure_utc(now or datetime.now(timezone.utc))
    grace = STALE_FALLBACK_GRACE
    if schedule_interval is not None:
        grace = max(schedule_interval * 2, STALE_MINIMUM_GRACE)
    stale_after = ensure_utc(covered_through) + grace
    if stale_after < current:
        return StaleClassification(status="stale", stale_after=stale_after)
    return StaleClassification(status="healthy", stale_after=stale_after)


def _range_to_dict(interval: CoverageInterval) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "since": interval.since,
        "before": interval.before,
    }
    if interval.source_ids:
        payload["source_ids"] = list(interval.source_ids)
    if interval.run_ids:
        payload["run_ids"] = list(interval.run_ids)
    return payload


def _unit_window_from_row(unit: SyncRunUnit, run: SyncRun) -> UnitWindow | None:
    since_at = unit.since_at
    before_at = unit.before_at
    if since_at is None or before_at is None:
        return None
    run_time = run.completed_at or run.started_at or run.created_at
    return UnitWindow(
        since=ensure_utc(since_at),
        before=ensure_utc(before_at),
        source_id=str(unit.source_id),
        dataset_key=str(unit.dataset_key),
        run_id=str(unit.sync_run_id),
        status=str(unit.status),
        run_time=ensure_utc(run_time),
    )


def _dataset_keys_for_config(config: SyncConfiguration) -> tuple[str, ...]:
    targets = {
        str(target) for target in (config.sync_targets or []) if target is not None
    }
    if not targets:
        return ()
    return tuple(
        spec.dataset_key
        for spec in supported_datasets(str(config.provider))
        if targets.intersection(spec.legacy_targets)
    )


def _planner_source_for_config(
    source: IntegrationSource, config: SyncConfiguration
) -> bool:
    metadata = dict(source.metadata_ or {})
    return metadata.get("planner_managed_sync_config_id") == str(config.id)


async def resolve_effective_scope(
    session: AsyncSession, org_id: str, config: SyncConfiguration
) -> EffectiveScope:
    integration_id = config.integration_id
    if integration_id is None:
        return EffectiveScope(None, (), _dataset_keys_for_config(config))

    source_stmt = select(IntegrationSource).where(
        IntegrationSource.org_id == org_id,
        IntegrationSource.integration_id == integration_id,
        IntegrationSource.is_enabled.is_(True),
    )
    source_rows = list((await session.execute(source_stmt)).scalars().all())
    if config.source_id is not None:
        sources = tuple(
            source for source in source_rows if source.id == config.source_id
        )
        dataset_keys = _dataset_keys_for_config(config)
    elif bool(config.planner_managed):
        sources = tuple(
            source
            for source in source_rows
            if _planner_source_for_config(source, config)
        )
        dataset_keys = ()
    else:
        sources = tuple(source_rows)
        dataset_keys = _dataset_keys_for_config(config)

    if not dataset_keys:
        dataset_stmt = select(IntegrationDataset.dataset_key).where(
            IntegrationDataset.org_id == org_id,
            IntegrationDataset.integration_id == integration_id,
            IntegrationDataset.is_enabled.is_(True),
        )
        dataset_keys = tuple((await session.execute(dataset_stmt)).scalars().all())
    return EffectiveScope(integration_id, sources, tuple(sorted(set(dataset_keys))))


def _schedule_interval(job: ScheduledJob | None, now: datetime) -> timedelta | None:
    if job is None:
        return None
    try:
        itr = Croniter(job.schedule_cron, now)
        next_one = ensure_utc(itr.get_next(datetime))
        next_two = ensure_utc(itr.get_next(datetime))
    except Exception:
        return None
    return next_two - next_one


async def _active_schedule(
    session: AsyncSession, org_id: str, config: SyncConfiguration
) -> ScheduledJob | None:
    stmt = (
        select(ScheduledJob)
        .where(
            ScheduledJob.org_id == org_id,
            ScheduledJob.sync_config_id == config.id,
            ScheduledJob.job_type == "sync",
            ScheduledJob.status == JobStatus.ACTIVE.value,
        )
        .order_by(
            ScheduledJob.next_run_at.asc().nullslast(), ScheduledJob.created_at.desc()
        )
    )
    return (await session.execute(stmt)).scalars().first()


async def _has_schedule_row(
    session: AsyncSession, org_id: str, config: SyncConfiguration
) -> bool:
    stmt = select(ScheduledJob.id).where(
        ScheduledJob.org_id == org_id,
        ScheduledJob.sync_config_id == config.id,
        ScheduledJob.job_type == "sync",
    )
    return (await session.execute(stmt)).scalar_one_or_none() is not None


async def _terminal_unit_windows(
    session: AsyncSession,
    org_id: str,
    scope: EffectiveScope,
    truncated_before: datetime,
) -> list[UnitWindow]:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return []
    source_ids = [source.id for source in scope.sources]
    base_filters = [
        SyncRunUnit.org_id == org_id,
        SyncRunUnit.integration_id == scope.integration_id,
        SyncRunUnit.source_id.in_(source_ids),
        SyncRunUnit.dataset_key.in_(scope.dataset_keys),
        SyncRunUnit.status.in_(TERMINAL_UNIT_STATUSES),
        SyncRunUnit.since_at.is_not(None),
        SyncRunUnit.before_at.is_not(None),
        SyncRun.org_id == org_id,
    ]
    recent_stmt = (
        select(SyncRunUnit, SyncRun)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.updated_at >= truncated_before)
    )
    latest_success_stmt = (
        select(SyncRunUnit, SyncRun)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.status == SyncRunUnitStatus.SUCCESS.value)
        .order_by(
            SyncRunUnit.source_id, SyncRunUnit.dataset_key, SyncRunUnit.before_at.desc()
        )
    )
    rows: list[tuple[SyncRunUnit, SyncRun]] = [
        (unit, run) for unit, run in (await session.execute(recent_stmt)).all()
    ]
    seen = {(unit.id, run.id) for unit, run in rows}
    for unit, run in (await session.execute(latest_success_stmt)).all():
        key = (unit.id, run.id)
        if key not in seen:
            rows.append((unit, run))
            seen.add(key)
    windows: list[UnitWindow] = []
    for unit, run in rows:
        window = _unit_window_from_row(unit, run)
        if window is not None:
            windows.append(window)
    return windows


async def _active_run_ids(
    session: AsyncSession, org_id: str, scope: EffectiveScope
) -> set[str]:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return set()
    stmt = (
        select(SyncRun.id)
        .join(SyncRunUnit, SyncRunUnit.sync_run_id == SyncRun.id)
        .where(
            SyncRun.org_id == org_id,
            SyncRun.integration_id == scope.integration_id,
            SyncRun.status.in_(ACTIVE_RUN_STATUSES),
            SyncRunUnit.org_id == org_id,
            SyncRunUnit.source_id.in_([source.id for source in scope.sources]),
            SyncRunUnit.dataset_key.in_(scope.dataset_keys),
        )
        .distinct()
    )
    return {str(run_id) for run_id in (await session.execute(stmt)).scalars().all()}


def _backfill_interval(job: BackfillJob) -> CoverageInterval:
    since = datetime.combine(job.since_date, time.min, tzinfo=timezone.utc)
    before = datetime.combine(job.before_date, time.max, tzinfo=timezone.utc)
    return CoverageInterval(since=since, before=before, run_ids=(str(job.id),))


async def _backfill_requested_ranges(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    scope: EffectiveScope,
    truncated_before: datetime,
) -> list[CoverageInterval]:
    if not scope.sources:
        return []
    stmt = select(BackfillJob).where(
        BackfillJob.org_id == org_id,
        BackfillJob.sync_config_id == config.id,
        BackfillJob.created_at >= truncated_before,
    )
    ranges: list[CoverageInterval] = []
    for job in (await session.execute(stmt)).scalars().all():
        task_id = str(job.celery_task_id or "")
        if "sync_run:" in task_id:
            continue
        interval = _backfill_interval(job)
        ranges.append(
            CoverageInterval(
                since=interval.since,
                before=interval.before,
                source_ids=tuple(str(source.id) for source in scope.sources),
                run_ids=interval.run_ids,
            )
        )
    return ranges


def _intervals_from_windows(windows: Iterable[UnitWindow]) -> list[CoverageInterval]:
    return [
        CoverageInterval(
            since=window.since,
            before=window.before,
            source_ids=(window.source_id,),
            run_ids=(window.run_id,),
        )
        for window in windows
    ]


def _status_from_parts(
    *,
    failed_count: int,
    gap_count: int,
    stale_status: str,
    has_data: bool,
    running: bool,
) -> str:
    if failed_count:
        return "failed"
    if gap_count:
        return "gaps"
    if not has_data:
        return "insufficient_data"
    if stale_status in {"paused", "not_scheduled"}:
        return stale_status
    if running:
        return "running"
    if stale_status == "stale":
        return "stale"
    return "healthy"


def build_coverage_summary_payload(
    *,
    config: SyncConfiguration,
    scope: EffectiveScope,
    windows: Sequence[UnitWindow],
    backfill_requested: Sequence[CoverageInterval],
    active_run_ids: set[str],
    active_schedule: ScheduledJob | None,
    has_schedule_row: bool,
    generated_at: datetime | None = None,
    lookback_days: int = HISTORY_LOOKBACK_DAYS,
) -> dict[str, Any]:
    now = ensure_utc(generated_at or datetime.now(timezone.utc))
    schedule_interval = _schedule_interval(active_schedule, now)
    paused = not bool(config.is_active)
    scheduled = active_schedule is not None and has_schedule_row

    by_dataset: dict[str, list[UnitWindow]] = defaultdict(list)
    for window in windows:
        by_dataset[window.dataset_key].append(window)

    backfill_by_dataset: dict[str, list[CoverageInterval]] = defaultdict(list)
    for interval in backfill_requested:
        for dataset_key in scope.dataset_keys:
            backfill_by_dataset[dataset_key].append(interval)

    datasets: list[_DatasetCoverage] = []
    for dataset_key in scope.dataset_keys:
        dataset_windows = by_dataset.get(dataset_key, [])
        successes = [
            window
            for window in dataset_windows
            if window.status == SyncRunUnitStatus.SUCCESS.value
        ]
        failures = [
            window
            for window in dataset_windows
            if window.status == SyncRunUnitStatus.FAILED.value
        ]
        requested = merge_intervals(
            [
                *_intervals_from_windows(dataset_windows),
                *backfill_by_dataset[dataset_key],
            ]
        )
        covered = merge_intervals(_intervals_from_windows(successes))
        failed_ranges = failed_ranges_not_superseded(failures, successes)
        gaps = subtract_intervals(requested, covered)
        covered_through = max((interval.before for interval in covered), default=None)
        stale = classify_staleness(
            covered_through,
            now=now,
            schedule_interval=schedule_interval,
            paused=paused,
            scheduled=scheduled,
        )
        stale_ranges = []
        if stale.status == "stale" and covered_through is not None:
            stale_ranges = [CoverageInterval(since=covered_through, before=now)]
        status = _status_from_parts(
            failed_count=len(failed_ranges),
            gap_count=len(gaps),
            stale_status=stale.status,
            has_data=bool(requested or covered),
            running=bool(active_run_ids),
        )
        datasets.append(
            _DatasetCoverage(
                dataset_key=dataset_key,
                requested=requested,
                covered=covered,
                gaps=gaps,
                stale_ranges=stale_ranges,
                failed_ranges=failed_ranges,
                covered_through=covered_through,
                status=status,
            )
        )

    source_payloads: list[dict[str, Any]] = []
    for source in scope.sources:
        source_windows = [
            window for window in windows if window.source_id == str(source.id)
        ]
        successes = [
            window
            for window in source_windows
            if window.status == SyncRunUnitStatus.SUCCESS.value
        ]
        failures = [
            window
            for window in source_windows
            if window.status == SyncRunUnitStatus.FAILED.value
        ]
        requested = merge_intervals(_intervals_from_windows(source_windows))
        covered = merge_intervals(_intervals_from_windows(successes))
        gaps = subtract_intervals(requested, covered)
        failed_ranges = failed_ranges_not_superseded(failures, successes)
        covered_through = max((interval.before for interval in covered), default=None)
        stale = classify_staleness(
            covered_through,
            now=now,
            schedule_interval=schedule_interval,
            paused=paused,
            scheduled=scheduled,
        )
        status = _status_from_parts(
            failed_count=len(failed_ranges),
            gap_count=len(gaps),
            stale_status=stale.status,
            has_data=bool(requested or covered),
            running=bool(active_run_ids),
        )
        source_payloads.append(
            {
                "source_id": str(source.id),
                "source_name": source.full_name or source.name,
                "status": status,
                "covered_through": covered_through,
                "gap_count": len(gaps),
                "failed_range_count": len(failed_ranges),
            }
        )

    failed_count = sum(len(dataset.failed_ranges) for dataset in datasets)
    gap_count = sum(len(dataset.gaps) for dataset in datasets)
    stale_count = sum(1 for dataset in datasets if dataset.status == "stale")
    has_data = any(dataset.requested or dataset.covered for dataset in datasets)
    if not has_data:
        overall_health = "insufficient_data"
    elif failed_count:
        overall_health = "failed"
    elif gap_count:
        overall_health = "gaps"
    elif stale_count:
        overall_health = "stale"
    else:
        overall_health = "healthy"

    successful_windows = [
        window for window in windows if window.status == SyncRunUnitStatus.SUCCESS.value
    ]
    latest_successful_run_at = max(
        (window.run_time for window in successful_windows), default=None
    )
    latest_covered_through = max(
        (dataset.covered_through for dataset in datasets if dataset.covered_through),
        default=None,
    )
    data_basis = "planner" if windows else "legacy"
    truncated_before = now - timedelta(days=lookback_days)
    return {
        "config_id": str(config.id),
        "provider": str(config.provider),
        "generated_at": now,
        "data_basis": data_basis,
        "history_lookback_days": lookback_days,
        "truncated_before": truncated_before,
        "overall": {
            "health": overall_health,
            "latest_successful_run_at": latest_successful_run_at,
            "latest_covered_through": latest_covered_through,
            "next_scheduled_run_at": active_schedule.next_run_at
            if active_schedule
            else None,
            "gap_count": gap_count,
            "stale_dataset_count": stale_count,
            "failed_range_count": failed_count,
        },
        "datasets": [
            {
                "dataset_key": dataset.dataset_key,
                "status": dataset.status,
                "covered_through": dataset.covered_through,
                "requested_ranges": [
                    _range_to_dict(item) for item in dataset.requested
                ],
                "covered_ranges": [_range_to_dict(item) for item in dataset.covered],
                "gaps": [_range_to_dict(item) for item in dataset.gaps],
                "stale_ranges": [_range_to_dict(item) for item in dataset.stale_ranges],
                "failed_ranges": [
                    _range_to_dict(item) for item in dataset.failed_ranges
                ],
            }
            for dataset in datasets
        ],
        "sources": source_payloads,
    }


async def build_sync_coverage_summary(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    *,
    lookback_days: int = HISTORY_LOOKBACK_DAYS,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    now = ensure_utc(generated_at or datetime.now(timezone.utc))
    truncated_before = now - timedelta(days=lookback_days)
    scope = await resolve_effective_scope(session, org_id, config)
    schedule = await _active_schedule(session, org_id, config)
    has_schedule = await _has_schedule_row(session, org_id, config)
    windows = await _terminal_unit_windows(session, org_id, scope, truncated_before)
    active_runs = await _active_run_ids(session, org_id, scope)
    backfill_requested = await _backfill_requested_ranges(
        session, org_id, config, scope, truncated_before
    )
    return build_coverage_summary_payload(
        config=config,
        scope=scope,
        windows=windows,
        backfill_requested=backfill_requested,
        active_run_ids=active_runs,
        active_schedule=schedule,
        has_schedule_row=has_schedule,
        generated_at=now,
        lookback_days=lookback_days,
    )
