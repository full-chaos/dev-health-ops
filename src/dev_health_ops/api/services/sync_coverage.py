from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any

from croniter import croniter as Croniter
from sqlalchemy import func, select
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
REQUESTED_UNIT_STATUSES = TERMINAL_UNIT_STATUSES | {
    SyncRunUnitStatus.PLANNED.value,
    SyncRunUnitStatus.DISPATCHING.value,
    SyncRunUnitStatus.RUNNING.value,
    SyncRunUnitStatus.RETRYING.value,
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
    # Dataset keys this interval is scoped to. Empty means "every dataset key
    # in scope" (the legacy/fallback behavior) -- set by
    # ``_backfill_requested_ranges`` when a backfill job's linked SyncRun lets
    # us resolve the exact (source_id, dataset_key) pairs it planned units
    # for. Never populated on windows/covered/gap intervals -- only on raw
    # backfill-requested intervals before they are split per pair in
    # ``build_coverage_summary_payload``.
    dataset_keys: tuple[str, ...] = ()


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


@dataclass
class _PairCoverage:
    source_id: str
    dataset_key: str
    requested: list[CoverageInterval] = field(default_factory=list)
    covered: list[CoverageInterval] = field(default_factory=list)
    gaps: list[CoverageInterval] = field(default_factory=list)
    stale_ranges: list[CoverageInterval] = field(default_factory=list)
    failed_ranges: list[CoverageInterval] = field(default_factory=list)
    covered_through: datetime | None = None
    status: str = "insufficient_data"


def ensure_utc(value: datetime) -> datetime:
    """Return ``value`` as an aware UTC datetime, treating naive values as UTC."""

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def merge_intervals(
    intervals: Iterable[CoverageInterval],
    *,
    tolerance: timedelta = INTERVAL_ADJACENCY_TOLERANCE,
) -> list[CoverageInterval]:
    """Sort, normalize, and merge overlapping or adjacent coverage intervals."""

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
    """Return requested sub-ranges that are not covered by covered intervals."""

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
    """Return failed unit windows that no later successful unit fully covers."""

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
    """Classify whether coverage is stale under schedule-aware grace rules."""

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
    """Resolve the source and dataset scope covered by a sync configuration."""

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
        SyncRunUnit.status.in_(REQUESTED_UNIT_STATUSES),
        SyncRunUnit.since_at.is_not(None),
        SyncRunUnit.before_at.is_not(None),
        SyncRun.org_id == org_id,
    ]
    recent_stmt = (
        select(SyncRunUnit, SyncRun)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.updated_at >= truncated_before)
    )
    latest_success_ranked = (
        select(
            SyncRunUnit.id.label("unit_id"),
            func.row_number()
            .over(
                partition_by=(SyncRunUnit.source_id, SyncRunUnit.dataset_key),
                order_by=(
                    SyncRunUnit.before_at.desc(),
                    SyncRunUnit.updated_at.desc(),
                ),
            )
            .label("row_num"),
        )
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.status == SyncRunUnitStatus.SUCCESS.value)
        .subquery()
    )
    latest_success_stmt = (
        select(SyncRunUnit, SyncRun)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .join(latest_success_ranked, latest_success_ranked.c.unit_id == SyncRunUnit.id)
        .where(
            *base_filters,
            SyncRunUnit.status == SyncRunUnitStatus.SUCCESS.value,
            latest_success_ranked.c.row_num == 1,
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
) -> set[tuple[str, str]]:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return set()
    stmt = (
        select(SyncRunUnit.source_id, SyncRunUnit.dataset_key)
        .select_from(SyncRun)
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
    return {
        (str(source_id), str(dataset_key))
        for source_id, dataset_key in (await session.execute(stmt)).all()
    }


def _backfill_interval(job: BackfillJob) -> CoverageInterval:
    since = datetime.combine(job.since_date, time.min, tzinfo=timezone.utc)
    before = datetime.combine(job.before_date, time.max, tzinfo=timezone.utc)
    return CoverageInterval(since=since, before=before, run_ids=(str(job.id),))


def _backfill_job_sync_run_id(job: BackfillJob) -> str | None:
    """Extract the linked SyncRun id from a backfill job's celery_task_id.

    Mirrors the identical helper in ``api/admin/routers/sync.py`` and
    ``workers/sync_reconciler.py`` (duplicated locally -- pulling it in would
    create a router import cycle from this service module).
    """
    task_id = str(job.celery_task_id or "")
    marker = "sync_run:"
    if marker not in task_id:
        return None
    return task_id.rsplit(marker, 1)[-1] or None


async def _backfill_job_run_pairs(
    session: AsyncSession, org_id: str, run_id: uuid.UUID
) -> set[tuple[str, str]]:
    """Return the distinct (source_id, dataset_key) pairs a SyncRun planned units for."""
    stmt = (
        select(SyncRunUnit.source_id, SyncRunUnit.dataset_key)
        .where(
            SyncRunUnit.org_id == org_id,
            SyncRunUnit.sync_run_id == run_id,
        )
        .distinct()
    )
    return {
        (str(source_id), str(dataset_key))
        for source_id, dataset_key in (await session.execute(stmt)).all()
    }


async def _backfill_requested_ranges(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    scope: EffectiveScope,
    truncated_before: datetime,
) -> list[CoverageInterval]:
    """Return backfill-driven requested intervals.

    Pair-aware: each job's linked SyncRun (resolved via the ``sync_run:<uuid>``
    suffix of ``celery_task_id``) tells us exactly which (source_id,
    dataset_key) pairs the backfill actually planned units for, so the job's
    date range is only counted as "requested" for those pairs. Without this,
    a backfill that only plans units for a subset of the in-scope pairs
    (unsupported datasets for a provider, sources added after the backfill
    ran, work-item family composite keys) permanently "requests" coverage on
    every other in-scope pair too -- a gap no future backfill on that pair
    can ever clear (CHAOS-2869).

    Fallback: if the marker is absent/unparseable, the linked SyncRun row no
    longer exists, or the run has zero SyncRunUnit rows, we fall back to the
    legacy all-pairs-in-scope behavior for that job. This keeps pre-marker
    legacy jobs working as before; such rows naturally age out of the
    ``HISTORY_LOOKBACK_DAYS`` lookback over time.
    """
    if not scope.sources:
        return []
    stmt = select(BackfillJob).where(
        BackfillJob.org_id == org_id,
        BackfillJob.sync_config_id == config.id,
        BackfillJob.created_at >= truncated_before,
    )
    scope_source_ids = tuple(str(source.id) for source in scope.sources)
    ranges: list[CoverageInterval] = []
    for job in (await session.execute(stmt)).scalars().all():
        interval = _backfill_interval(job)
        pairs: set[tuple[str, str]] = set()
        run_id_str = _backfill_job_sync_run_id(job)
        if run_id_str is not None:
            try:
                run_uuid = uuid.UUID(run_id_str)
            except ValueError:
                run_uuid = None
            if run_uuid is not None:
                pairs = await _backfill_job_run_pairs(session, org_id, run_uuid)
        if pairs:
            for source_id, dataset_key in pairs:
                if source_id not in scope_source_ids:
                    continue
                ranges.append(
                    CoverageInterval(
                        since=interval.since,
                        before=interval.before,
                        source_ids=(source_id,),
                        dataset_keys=(dataset_key,),
                        run_ids=interval.run_ids,
                    )
                )
        else:
            ranges.append(
                CoverageInterval(
                    since=interval.since,
                    before=interval.before,
                    source_ids=scope_source_ids,
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


def _rollup_stale_status(statuses: Iterable[str]) -> str:
    status_set = set(statuses)
    if "paused" in status_set:
        return "paused"
    if "not_scheduled" in status_set:
        return "not_scheduled"
    if "stale" in status_set:
        return "stale"
    return "healthy"


def _data_basis_for_config(config: SyncConfiguration, scope: EffectiveScope) -> str:
    if config.integration_id is not None and scope.integration_id is not None:
        return "planner"
    return "legacy"


def build_coverage_summary_payload(
    *,
    config: SyncConfiguration,
    scope: EffectiveScope,
    windows: Sequence[UnitWindow],
    backfill_requested: Sequence[CoverageInterval],
    active_pairs: set[tuple[str, str]],
    active_schedule: ScheduledJob | None,
    has_schedule_row: bool,
    generated_at: datetime | None = None,
    lookback_days: int = HISTORY_LOOKBACK_DAYS,
) -> dict[str, Any]:
    """Build the API coverage payload from persisted unit and backfill windows.

    Interval math is evaluated per ``(source_id, dataset_key)`` before summaries
    roll up to dataset, source, and overall levels.
    """

    now = ensure_utc(generated_at or datetime.now(timezone.utc))
    schedule_interval = _schedule_interval(active_schedule, now)
    paused = not bool(config.is_active)
    scheduled = active_schedule is not None and has_schedule_row

    by_pair: dict[tuple[str, str], list[UnitWindow]] = defaultdict(list)
    for window in windows:
        by_pair[(window.source_id, window.dataset_key)].append(window)

    scope_source_ids = {str(source.id) for source in scope.sources}
    scope_dataset_keys = set(scope.dataset_keys)
    backfill_by_pair: dict[tuple[str, str], list[CoverageInterval]] = defaultdict(list)
    for interval in backfill_requested:
        interval_source_ids = interval.source_ids or tuple(sorted(scope_source_ids))
        # Empty dataset_keys means "legacy/unresolved backfill" -- spread it
        # across every dataset in scope (the pre-fix, all-pairs fallback).
        # Non-empty dataset_keys means the backfill's SyncRun told us exactly
        # which pairs it planned units for, so we only apply it there.
        interval_dataset_keys = interval.dataset_keys or scope.dataset_keys
        for source_id in interval_source_ids:
            if source_id not in scope_source_ids:
                continue
            for dataset_key in interval_dataset_keys:
                if dataset_key not in scope_dataset_keys:
                    continue
                backfill_by_pair[(source_id, dataset_key)].append(
                    CoverageInterval(
                        since=interval.since,
                        before=interval.before,
                        source_ids=(source_id,),
                        run_ids=interval.run_ids,
                    )
                )

    pair_coverages: list[_PairCoverage] = []
    for source in scope.sources:
        source_id = str(source.id)
        for dataset_key in scope.dataset_keys:
            pair_windows = by_pair.get((source_id, dataset_key), [])
            successes = [
                window
                for window in pair_windows
                if window.status == SyncRunUnitStatus.SUCCESS.value
            ]
            failures = [
                window
                for window in pair_windows
                if window.status == SyncRunUnitStatus.FAILED.value
            ]
            requested = merge_intervals(
                [
                    *_intervals_from_windows(pair_windows),
                    *backfill_by_pair[(source_id, dataset_key)],
                ]
            )
            covered = merge_intervals(_intervals_from_windows(successes))
            failed_ranges = failed_ranges_not_superseded(failures, successes)
            gaps = subtract_intervals(requested, covered)
            covered_through = max(
                (interval.before for interval in covered), default=None
            )
            stale = classify_staleness(
                covered_through,
                now=now,
                schedule_interval=schedule_interval,
                paused=paused,
                scheduled=scheduled,
            )
            stale_ranges = []
            if stale.status == "stale" and covered_through is not None:
                stale_ranges = [
                    CoverageInterval(
                        since=covered_through,
                        before=now,
                        source_ids=(source_id,),
                    )
                ]
            status = _status_from_parts(
                failed_count=len(failed_ranges),
                gap_count=len(gaps),
                stale_status=stale.status,
                has_data=bool(requested or covered),
                running=(source_id, dataset_key) in active_pairs,
            )
            pair_coverages.append(
                _PairCoverage(
                    source_id=source_id,
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

    datasets: list[_DatasetCoverage] = []
    for dataset_key in scope.dataset_keys:
        pairs = [pair for pair in pair_coverages if pair.dataset_key == dataset_key]
        requested = merge_intervals(
            interval for pair in pairs for interval in pair.requested
        )
        covered = merge_intervals(
            interval for pair in pairs for interval in pair.covered
        )
        gaps = merge_intervals(interval for pair in pairs for interval in pair.gaps)
        failed_ranges = merge_intervals(
            interval for pair in pairs for interval in pair.failed_ranges
        )
        stale_ranges = merge_intervals(
            interval for pair in pairs for interval in pair.stale_ranges
        )
        covered_through = max(
            (pair.covered_through for pair in pairs if pair.covered_through),
            default=None,
        )
        status = _status_from_parts(
            failed_count=len(failed_ranges),
            gap_count=len(gaps),
            stale_status=_rollup_stale_status(pair.status for pair in pairs),
            has_data=bool(requested or covered),
            running=any(pair.status == "running" for pair in pairs)
            and not any(pair.status == "stale" for pair in pairs),
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
        source_id = str(source.id)
        pairs = [pair for pair in pair_coverages if pair.source_id == source_id]
        covered_through = max(
            (pair.covered_through for pair in pairs if pair.covered_through),
            default=None,
        )
        gap_count = sum(len(pair.gaps) for pair in pairs)
        failed_range_count = sum(len(pair.failed_ranges) for pair in pairs)
        status = _status_from_parts(
            failed_count=failed_range_count,
            gap_count=gap_count,
            stale_status=_rollup_stale_status(pair.status for pair in pairs),
            has_data=any(pair.requested or pair.covered for pair in pairs),
            running=any(pair.status == "running" for pair in pairs)
            and not any(pair.status == "stale" for pair in pairs),
        )
        source_payloads.append(
            {
                "source_id": source_id,
                "source_name": source.full_name or source.name,
                "status": status,
                "covered_through": covered_through,
                "gap_count": gap_count,
                "failed_range_count": failed_range_count,
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
    data_basis = _data_basis_for_config(config, scope)
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
    """Query persisted sync state and return a config-scoped coverage summary."""

    now = ensure_utc(generated_at or datetime.now(timezone.utc))
    truncated_before = now - timedelta(days=lookback_days)
    scope = await resolve_effective_scope(session, org_id, config)
    schedule = await _active_schedule(session, org_id, config)
    has_schedule = await _has_schedule_row(session, org_id, config)
    windows = await _terminal_unit_windows(session, org_id, scope, truncated_before)
    active_pairs = await _active_run_ids(session, org_id, scope)
    backfill_requested = await _backfill_requested_ranges(
        session, org_id, config, scope, truncated_before
    )
    return build_coverage_summary_payload(
        config=config,
        scope=scope,
        windows=windows,
        backfill_requested=backfill_requested,
        active_pairs=active_pairs,
        active_schedule=schedule,
        has_schedule_row=has_schedule,
        generated_at=now,
        lookback_days=lookback_days,
    )
