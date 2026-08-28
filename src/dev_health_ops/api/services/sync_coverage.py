from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from time import monotonic
from typing import Any, Literal, Protocol

from croniter import croniter as Croniter
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, func, select, union, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, load_only
from sqlalchemy.sql.dml import Update

from dev_health_ops.metrics.prometheus import (
    SYNC_COVERAGE_DATASETS_EXCLUDED_BY_INTENT_TOTAL,
    SYNC_COVERAGE_FOLDED_KEY_RESOLUTIONS_TOTAL,
)
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
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from dev_health_ops.sync.datasets import supported_datasets
from dev_health_ops.sync.family_flags import (
    FOLD_FAMILIES,
    dataset_keys_from_flags,
)

logger = logging.getLogger(__name__)

HISTORY_LOOKBACK_DAYS = 3650
# Bump whenever the persisted payload shape changes. The read path filters on
# this value, so a stale-shaped row becomes unreadable and is rebuilt instead of
# being served as current. Version 1 stored ``backfill_windows`` as bare
# calendar dates with no source/dataset scope; version 2 stores exact UTC
# instants plus scope, which is what BackfillSelectorRequest requires.
SYNC_COVERAGE_PROJECTION_VERSION = 2
STALE_MINIMUM_GRACE = timedelta(hours=6)
STALE_FALLBACK_GRACE = timedelta(hours=48)
INTERVAL_ADJACENCY_TOLERANCE = timedelta(microseconds=1)

# Code-owned safety limits for the background projection builder. These are
# deliberately not environment overrides: deployment configuration must not be
# able to silently remove the memory bound. Requests beyond the active-build
# limit are rejected immediately rather than retained in an unbounded waiter
# queue. Query and derived-allocation budgets reject pathological scopes before
# the driver or interval builder can materialize unbounded collections.
MAX_CONCURRENT_COVERAGE_BUILDS = 2
MAX_COVERAGE_QUERY_ROWS = 40_000
MAX_COVERAGE_SOURCES = 5_000
MAX_COVERAGE_DATASETS = 100
MAX_COVERAGE_PAIRS = 20_000
MAX_COVERAGE_UNIT_WINDOWS = 30_000
MAX_COVERAGE_BACKFILL_PAIR_INTERVALS = 20_000
MAX_COVERAGE_PROJECTION_ROWS = 1_000_000

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


class SyncCoverageBusyError(RuntimeError):
    """Raised when the process-local coverage capacity is saturated."""


class SyncCoveragePendingError(RuntimeError):
    """Raised when the background-built coverage projection is not ready."""


class SyncCoverageComplexityError(RuntimeError):
    """Raised before a coverage request exceeds a code-owned allocation cap."""

    def __init__(self, *, stage: str, limit: int, observed: int) -> None:
        self.stage = stage
        self.limit = limit
        self.observed = observed
        super().__init__(
            f"sync coverage complexity limit exceeded during {stage}: "
            f"observed {observed} items (limit {limit})"
        )


@dataclass
class _CoverageQueryBudget:
    """Shared row budget across potentially multi-row coverage queries."""

    limit: int = MAX_COVERAGE_QUERY_ROWS
    consumed: int = 0

    @property
    def sql_limit(self) -> int:
        """Return a LIMIT that includes one overflow sentinel row."""

        return max(self.limit - self.consumed, 0) + 1

    def consume(self, count: int, *, stage: str) -> None:
        observed = self.consumed + count
        if observed > self.limit:
            raise SyncCoverageComplexityError(
                stage=stage,
                limit=self.limit,
                observed=observed,
            )
        self.consumed = observed


async def _bounded_query_rows(
    session: AsyncSession,
    statement: Any,
    budget: _CoverageQueryBudget,
    *,
    stage: str,
    stage_limit: int | None = None,
) -> list[Any]:
    """Execute with an overflow sentinel so allocation is bounded in SQL."""

    sql_limit = budget.sql_limit
    if stage_limit is not None:
        sql_limit = min(sql_limit, stage_limit + 1)
    rows = list((await session.execute(statement.limit(sql_limit))).all())
    if stage_limit is not None and len(rows) > stage_limit:
        raise SyncCoverageComplexityError(
            stage=stage,
            limit=stage_limit,
            observed=len(rows),
        )
    budget.consume(len(rows), stage=stage)
    return rows


class _CoverageAdmissionController:
    """Bound active builds and reject excess work without retaining waiters."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("coverage admission capacity must be positive")
        self.capacity = capacity
        self._active = 0
        self._held: ContextVar[bool] = ContextVar(
            f"sync_coverage_admission_held_{id(self)}", default=False
        )

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        if self._held.get():
            yield
            return
        # This check/increment contains no await, so it is atomic with respect
        # to other coroutines on this process's event loop. Unlike Semaphore,
        # saturation creates no waiter objects retaining HTTP/session state.
        if self._active >= self.capacity:
            raise SyncCoverageBusyError("sync coverage capacity is busy; retry shortly")
        self._active += 1
        token = self._held.set(True)
        try:
            yield
        finally:
            self._held.reset(token)
            self._active -= 1


_coverage_admission = _CoverageAdmissionController(MAX_CONCURRENT_COVERAGE_BUILDS)


@asynccontextmanager
async def sync_coverage_admission_slot() -> AsyncIterator[None]:
    """Acquire the route-wide coverage slot before any database checkout."""

    async with _coverage_admission.slot():
        yield


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
class _CoverageUnitRow:
    """The narrow persisted projection required by coverage interval math.

    SyncRunUnit.result/error and SyncRun.result/error can be large. Hydrating the
    full ORM entities for every unit in a large lookback made one coverage
    request consume hundreds of MiB and allowed concurrent admin page requests
    to OOM-kill the API container.
    """

    unit_id: uuid.UUID
    sync_run_id: uuid.UUID
    source_id: uuid.UUID
    dataset_key: str
    processor_flags: Mapping[str, object] | None
    since_at: datetime
    before_at: datetime
    status: str
    run_time: datetime


class _DatasetKeyUnit(Protocol):
    """Structural input shared by projected rows and ORM-backed unit tests."""

    @property
    def dataset_key(self) -> str: ...

    @property
    def processor_flags(self) -> Mapping[str, object] | None: ...


class _BackfillJobLike(Protocol):
    @property
    def id(self) -> uuid.UUID: ...

    @property
    def celery_task_id(self) -> str | None: ...

    @property
    def since_date(self) -> date: ...

    @property
    def before_date(self) -> date: ...


def _coverage_unit_row(row: Any) -> _CoverageUnitRow:
    """Convert one narrow SQL result row without hydrating ORM entities."""

    return _CoverageUnitRow(
        unit_id=row.unit_id,
        sync_run_id=row.sync_run_id,
        source_id=row.source_id,
        dataset_key=str(row.dataset_key),
        processor_flags=row.processor_flags,
        since_at=row.since_at,
        before_at=row.before_at,
        status=str(row.status),
        run_time=row.run_time,
    )


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


@dataclass
class _CompactPairState:
    """Streaming reducer state for one source/dataset pair."""

    requested: list[CoverageInterval] = field(default_factory=list)
    covered: list[CoverageInterval] = field(default_factory=list)
    failed: list[CoverageInterval] = field(default_factory=list)


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


def merge_intervals_by_source_scope(
    intervals: Iterable[CoverageInterval],
    *,
    tolerance: timedelta = INTERVAL_ADJACENCY_TOLERANCE,
) -> list[CoverageInterval]:
    """Merge intervals only when their source scopes match exactly.

    Dataset coverage is displayed as a union across sources. That remains
    useful for reporting, but a row-level backfill action must keep the source
    scope that produced the gap. Otherwise adjacent gaps from two sources turn
    into one broad actionable range.
    """

    by_source_scope: dict[tuple[str, ...], list[CoverageInterval]] = defaultdict(list)
    for interval in intervals:
        source_scope = tuple(sorted(set(interval.source_ids)))
        by_source_scope[source_scope].append(interval)

    return sorted(
        (
            merged
            for scoped_intervals in by_source_scope.values()
            for merged in merge_intervals(scoped_intervals, tolerance=tolerance)
        ),
        key=lambda interval: (interval.since, interval.before, interval.source_ids),
    )


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
                run_ids=(success.run_id,) if success.run_id else (),
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
                    run_ids=(failure.run_id,) if failure.run_id else (),
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


def _unit_window_from_row(
    unit: _CoverageUnitRow, dataset_key: str | None = None
) -> UnitWindow:
    """Build a ``UnitWindow`` for one row, optionally under an effective
    dataset key (CHAOS-2721 work-item-family expansion) instead of the raw
    persisted ``unit.dataset_key``. Run id/source id/status/timestamps always
    reflect the raw unit -- only the reported dataset key changes.
    """
    return UnitWindow(
        since=ensure_utc(unit.since_at),
        before=ensure_utc(unit.before_at),
        source_id=str(unit.source_id),
        dataset_key=dataset_key if dataset_key is not None else str(unit.dataset_key),
        run_id=str(unit.sync_run_id),
        status=str(unit.status),
        run_time=ensure_utc(unit.run_time),
    )


# CHAOS-2721 collapses the enabled work-item-family datasets (work-items,
# work-item-labels, work-item-projects, work-item-history, work-item-comments)
# into ONE composite SyncRunUnit per (source, window) with canonical
# dataset_key="work-items" and boolean family_dataset_<key> processor flags
# per enabled child dataset. CHAOS-4078 adds two more non-atomic folds: the
# PR-social family (prs/pr-reviews/pr-comments -> prs) and the TestOps family
# (cicd/tests -> cicd). Coverage math must expand a persisted unit into its
# effective child dataset keys before doing interval/status math, or a later
# successful composite run never supersedes stale child-dataset gaps or
# failures. This mirrors the identical expansion in
# ``workers/sync_units.py::_watermark_dataset_keys``/``_family_dataset_audit_metadata``.
_WORK_ITEMS_CANONICAL_DATASET_KEY = "work-items"

# canonical_dataset_key -> member dataset keys, for every collapsible family.
_CANONICAL_FAMILY_DATASETS: dict[str, tuple[str, ...]] = dict(FOLD_FAMILIES)
# member dataset key -> the canonical key its family folds onto.
_FAMILY_CHILD_CANONICAL: dict[str, str] = {
    member: canonical for canonical, members in FOLD_FAMILIES for member in members
}


def _effective_dataset_keys(
    dataset_key: str, processor_flags: Mapping[str, object] | None
) -> list[str]:
    """Expand a raw ``dataset_key``/``processor_flags`` pair into effective
    coverage dataset keys.

    Only a canonical composite key (``"work-items"``, ``"prs"``, ``"cicd"``)
    is ever expanded -- a raw, non-composite ``dataset_key`` is returned as-is
    even if stray ``family_dataset_*`` flags are present, since a plain unit
    never carries a real family collapse. For a canonical key, returns the
    enabled family child keys (canonical order) decoded from
    ``processor_flags`` when any are true; otherwise falls back to the raw
    ``dataset_key`` (missing/false/unknown flags never advance coverage for a
    dataset that was not actually run).
    """
    members = _CANONICAL_FAMILY_DATASETS.get(str(dataset_key))
    if members is None:
        return [str(dataset_key)]
    family_keys = dataset_keys_from_flags(members, processor_flags)
    if not family_keys:
        return [str(dataset_key)]
    SYNC_COVERAGE_FOLDED_KEY_RESOLUTIONS_TOTAL.labels(
        canonical_dataset_key=str(dataset_key)
    ).inc(len(family_keys))
    return family_keys


def _effective_dataset_keys_for_unit(unit: _DatasetKeyUnit) -> list[str]:
    """Expand a projected sync unit into its effective coverage dataset keys."""
    return _effective_dataset_keys(unit.dataset_key, unit.processor_flags)


def _is_family_child_dataset_key(dataset_key: str) -> bool:
    """True when ``dataset_key`` is a child of any collapsible family (the
    work-item family, or the CHAOS-4078 PR-social/TestOps folds)."""
    return dataset_key in _FAMILY_CHILD_CANONICAL


def _query_dataset_keys_for_scope(dataset_keys: Sequence[str]) -> tuple[str, ...]:
    """Expand scope dataset keys with each requested key's canonical family key.

    Persisted ``SyncRunUnit`` rows carry the collapsed composite key
    (``"work-items"``, ``"prs"``, or ``"cicd"``) for every enabled member of
    that family. A scope covering any family child key (e.g.
    ``work-item-comments`` or ``pr-comments``) must therefore also query for
    rows keyed under its canonical identity, or the composite row is
    invisible to per-dataset coverage queries entirely. Non-family scopes are
    returned unchanged.
    """
    keys = set(dataset_keys)
    for key in dataset_keys:
        canonical = _FAMILY_CHILD_CANONICAL.get(key)
        if canonical is not None:
            keys.add(canonical)
    return tuple(sorted(keys))


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


async def resolve_effective_scope(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    budget: _CoverageQueryBudget,
) -> EffectiveScope:
    """Resolve the source and dataset scope covered by a sync configuration."""

    integration_id = config.integration_id
    if integration_id is None:
        return EffectiveScope(None, (), _dataset_keys_for_config(config))

    source_filters = [
        IntegrationSource.org_id == org_id,
        IntegrationSource.integration_id == integration_id,
        IntegrationSource.is_enabled.is_(True),
    ]
    if config.source_id is not None:
        source_filters.append(IntegrationSource.id == config.source_id)
    elif bool(config.planner_managed):
        source_filters.append(
            IntegrationSource.metadata_["planner_managed_sync_config_id"].as_string()
            == str(config.id)
        )
    source_stmt = (
        select(IntegrationSource)
        .options(
            load_only(
                IntegrationSource.id,
                IntegrationSource.name,
                IntegrationSource.full_name,
            )
        )
        .where(*source_filters)
    )
    source_rows = [
        row[0]
        for row in await _bounded_query_rows(
            session,
            source_stmt,
            budget,
            stage="effective_sources",
            stage_limit=MAX_COVERAGE_SOURCES,
        )
    ]
    sources = tuple(source_rows)
    target_scoped = config.source_id is not None or not bool(config.planner_managed)
    target_dataset_keys = _dataset_keys_for_config(config) if target_scoped else ()

    # Both the enabled keys AND whether any intent row exists at all come from
    # one query. Selecting only `is_enabled IS TRUE` rows makes an empty result
    # ambiguous between "this integration was never seeded" and "the operator
    # switched everything off" -- and those two demand opposite behaviour
    # (Codex adversarial review, round 2).
    dataset_stmt = select(
        IntegrationDataset.dataset_key, IntegrationDataset.is_enabled
    ).where(
        IntegrationDataset.org_id == org_id,
        IntegrationDataset.integration_id == integration_id,
    )
    dataset_rows = await _bounded_query_rows(
        session,
        dataset_stmt,
        budget,
        stage="effective_datasets",
        stage_limit=MAX_COVERAGE_DATASETS,
    )
    intent_rows_exist = bool(dataset_rows)
    enabled_dataset_keys = tuple(row[0] for row in dataset_rows if row[1])

    # Mirrors resolveScope in internal/synccoverage/repository.go; the two must
    # stay in lockstep. A target-scoped config (source-scoped child, or any
    # non-planner-managed config) derives its datasets from sync_targets alone,
    # which says nothing about whether the dataset is still enabled. The planner
    # is is_enabled-authoritative on every path (planner.py::
    # _load_enabled_datasets filters is_enabled IS TRUE and only then narrows by
    # the requested keys), so without this intersection coverage advertises gap
    # windows and backfill buttons for datasets the planner would refuse to plan
    # (CHAOS-4106).
    #
    # The intersection deliberately does NOT run for a planner-managed parent.
    # There dataset_keys already come from the enabled rows -- exactly what the
    # planner reads -- and intersecting with target keys would drop "blame" and
    # "security", neither of which is derivable from an operator-selectable
    # target, leaving coverage blind to two datasets that really are syncing.
    #
    # A config with no sync_targets at all keeps the pre-existing fallback of
    # scoping to every enabled dataset. Only a non-empty selection is
    # intersected, so "every selected dataset is disabled" resolves to an empty
    # scope rather than inverting into the fallback and advertising everything.
    #
    # An integration with NO integration_datasets rows at all is left alone: an
    # unseeded intent plane is not a statement of intent, and reading it as one
    # would blank an otherwise working config's coverage. But rows that EXIST
    # and are all disabled ARE a statement of intent -- the operator switched
    # everything off -- and must narrow to an empty scope. Hence
    # `intent_rows_exist` rather than `enabled_dataset_keys` as the test.
    if not target_scoped or not target_dataset_keys:
        dataset_keys = enabled_dataset_keys
    elif not intent_rows_exist:
        dataset_keys = target_dataset_keys
    else:
        enabled_set = set(enabled_dataset_keys)
        dataset_keys = tuple(key for key in target_dataset_keys if key in enabled_set)
        excluded = sorted(set(target_dataset_keys) - enabled_set)
        if excluded:
            SYNC_COVERAGE_DATASETS_EXCLUDED_BY_INTENT_TOTAL.labels(
                provider=str(config.provider)
            ).inc(len(excluded))
            logger.warning(
                "sync_coverage_scope_excluded_user_disabled_datasets",
                extra={
                    "org_id": org_id,
                    "sync_config_id": str(config.id),
                    "integration_id": str(integration_id),
                    "provider": str(config.provider),
                    "excluded_dataset_keys": ",".join(excluded),
                    "excluded_count": len(excluded),
                    "reason": (
                        "integration_datasets.is_enabled is false; coverage must "
                        "not advertise backfill windows the planner would refuse"
                    ),
                },
            )
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


@dataclass(frozen=True)
class _CoverageLookbackSelection:
    effective_days: int
    requested_unit_window_count: int
    effective_unit_window_count: int
    unit_window_limit: int


def _terminal_unit_base_filters(
    org_id: str,
    scope: EffectiveScope,
) -> list[Any]:
    source_ids = [source.id for source in scope.sources]
    query_dataset_keys = _query_dataset_keys_for_scope(scope.dataset_keys)
    return [
        SyncRunUnit.org_id == org_id,
        SyncRunUnit.integration_id == scope.integration_id,
        SyncRunUnit.source_id.in_(source_ids),
        SyncRunUnit.dataset_key.in_(query_dataset_keys),
        SyncRunUnit.status.in_(REQUESTED_UNIT_STATUSES),
        SyncRunUnit.since_at.is_not(None),
        SyncRunUnit.before_at.is_not(None),
        SyncRun.org_id == org_id,
    ]


def _family_window_weight(scope: EffectiveScope, canonical_key: str) -> int:
    """Weight of ONE persisted unit row keyed under ``canonical_key``: the
    number of effective coverage windows ``_effective_dataset_keys_for_unit``
    will expand it into, bounded by how many of THAT family's own members
    this scope actually asks about (never less than 1).

    CHAOS-4078: this must be computed per canonical family, not as a single
    flat scalar applied to every composite row. A scope spanning both a
    work-items child (e.g. ``work-item-comments``) and a PR-social child
    (e.g. ``pr-comments``) must weight a ``work-items`` row only by the
    work-items members in scope, and a ``prs`` row only by the PR-social
    members in scope -- summing across families would overcount one and
    undercount the other.
    """
    members = _CANONICAL_FAMILY_DATASETS.get(canonical_key, ())
    return max(sum(1 for key in scope.dataset_keys if key in members), 1)


async def _weighted_unit_window_count(
    session: AsyncSession,
    scope: EffectiveScope,
    unit_rows: Any,
    *,
    limit: int,
) -> int:
    bounded_units = unit_rows.limit(limit + 1).subquery()
    # One WHEN branch per canonical family (work-items, prs, cicd) -- a
    # composite row expands into as many coverage windows as that family has
    # members in scope. A non-family dataset_key (or a family with none of
    # its members in scope) falls through to the ELSE weight of 1, matching
    # _effective_dataset_keys' own raw-key fallback.
    weight_cases = tuple(
        (
            bounded_units.c.dataset_key == canonical_key,
            _family_window_weight(scope, canonical_key),
        )
        for canonical_key in _CANONICAL_FAMILY_DATASETS
    )
    stmt = select(
        func.coalesce(
            func.sum(case(*weight_cases, else_=1)),
            0,
        )
    ).select_from(bounded_units)
    return int((await session.execute(stmt)).scalar_one())


def _latest_success_unit_rows(
    org_id: str,
    scope: EffectiveScope,
) -> Any:
    base_filters = _terminal_unit_base_filters(org_id, scope)
    latest_success_ranked = (
        select(
            SyncRunUnit.id.label("unit_id"),
            SyncRunUnit.dataset_key.label("dataset_key"),
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
        .select_from(SyncRunUnit)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.status == SyncRunUnitStatus.SUCCESS.value)
        .subquery()
    )
    return select(
        latest_success_ranked.c.unit_id,
        latest_success_ranked.c.dataset_key,
    ).where(latest_success_ranked.c.row_num == 1)


async def _recent_backfill_run_ids_for_lookback(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    truncated_before: datetime,
    *,
    limit: int,
) -> set[uuid.UUID] | None:
    stmt = select(BackfillJob.celery_task_id).where(
        BackfillJob.org_id == org_id,
        BackfillJob.sync_config_id == config.id,
        BackfillJob.created_at >= truncated_before,
    )
    rows = list((await session.execute(stmt.limit(limit + 1))).all())
    if len(rows) > limit:
        return None
    run_ids: set[uuid.UUID] = set()
    for job in rows:
        run_id_str = _backfill_job_sync_run_id(job)
        if run_id_str is None:
            continue
        try:
            run_ids.add(uuid.UUID(run_id_str))
        except ValueError:
            continue
    return run_ids


async def _coverage_unit_window_count(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    scope: EffectiveScope,
    truncated_before: datetime,
    *,
    limit: int,
) -> int:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return 0
    base_filters = _terminal_unit_base_filters(org_id, scope)
    recent_units = (
        select(
            SyncRunUnit.id.label("unit_id"),
            SyncRunUnit.dataset_key.label("dataset_key"),
        )
        .select_from(SyncRunUnit)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.updated_at >= truncated_before)
    )
    recent_backfill_run_ids = await _recent_backfill_run_ids_for_lookback(
        session,
        org_id,
        config,
        truncated_before,
        limit=limit,
    )
    if recent_backfill_run_ids is None:
        return limit + 1
    unit_queries: list[Any] = [
        recent_units,
        _latest_success_unit_rows(org_id, scope),
    ]
    if recent_backfill_run_ids:
        backfill_units = (
            select(
                SyncRunUnit.id.label("unit_id"),
                SyncRunUnit.dataset_key.label("dataset_key"),
            )
            .select_from(SyncRunUnit)
            .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
            .where(
                *base_filters,
                SyncRunUnit.sync_run_id.in_(recent_backfill_run_ids),
            )
        )
        unit_queries.append(backfill_units)
    unit_rows = union(*unit_queries)
    return await _weighted_unit_window_count(
        session,
        scope,
        unit_rows,
        limit=limit,
    )


async def _select_coverage_lookback(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    scope: EffectiveScope,
    *,
    requested_days: int,
    generated_at: datetime,
) -> _CoverageLookbackSelection:
    """Choose the largest window whose expanded unit set fits the memory cap."""

    unit_window_limit = MAX_COVERAGE_UNIT_WINDOWS
    counts: dict[int, int] = {}

    async def count_for(days: int) -> int:
        if days not in counts:
            counts[days] = await _coverage_unit_window_count(
                session,
                org_id,
                config,
                scope,
                generated_at - timedelta(days=days),
                limit=unit_window_limit,
            )
        return counts[days]

    requested_count = await count_for(requested_days)
    if requested_count <= unit_window_limit:
        return _CoverageLookbackSelection(
            effective_days=requested_days,
            requested_unit_window_count=requested_count,
            effective_unit_window_count=requested_count,
            unit_window_limit=unit_window_limit,
        )

    minimum_count = await count_for(1)
    if minimum_count > unit_window_limit:
        raise SyncCoverageComplexityError(
            stage="expanded_unit_windows",
            limit=unit_window_limit,
            observed=minimum_count,
        )

    low = 1
    high = requested_days - 1
    effective_days = 1
    effective_count = minimum_count
    while low <= high:
        candidate_days = (low + high) // 2
        candidate_count = await count_for(candidate_days)
        if candidate_count <= unit_window_limit:
            effective_days = candidate_days
            effective_count = candidate_count
            low = candidate_days + 1
        else:
            high = candidate_days - 1

    return _CoverageLookbackSelection(
        effective_days=effective_days,
        requested_unit_window_count=requested_count,
        effective_unit_window_count=effective_count,
        unit_window_limit=unit_window_limit,
    )


async def _terminal_unit_windows(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    scope: EffectiveScope,
    truncated_before: datetime,
    budget: _CoverageQueryBudget,
) -> list[UnitWindow]:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return []
    base_filters = _terminal_unit_base_filters(org_id, scope)
    # Deliberately project only the columns used by interval math. Selecting
    # the ORM entities hydrates SyncRunUnit.result/error and SyncRun.result/error
    # for every unit in the lookback; those JSON/text payloads are unrelated to
    # coverage and can push concurrent requests beyond the API's memory limit.
    unit_columns = (
        SyncRunUnit.id.label("unit_id"),
        SyncRunUnit.sync_run_id,
        SyncRunUnit.source_id,
        SyncRunUnit.dataset_key,
        SyncRunUnit.processor_flags,
        SyncRunUnit.since_at,
        SyncRunUnit.before_at,
        SyncRunUnit.status,
        func.coalesce(
            SyncRun.completed_at,
            SyncRun.started_at,
            SyncRun.created_at,
        ).label("run_time"),
    )
    recent_stmt = (
        select(*unit_columns)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(*base_filters, SyncRunUnit.updated_at >= truncated_before)
    )
    recent_backfill_stmt = select(BackfillJob.celery_task_id).where(
        BackfillJob.org_id == org_id,
        BackfillJob.sync_config_id == config.id,
        BackfillJob.created_at >= truncated_before,
    )
    recent_backfill_run_ids: set[uuid.UUID] = set()
    for job in await _bounded_query_rows(
        session,
        recent_backfill_stmt,
        budget,
        stage="recent_backfill_jobs",
    ):
        run_id_str = _backfill_job_sync_run_id(job)
        if run_id_str is None:
            continue
        try:
            recent_backfill_run_ids.add(uuid.UUID(run_id_str))
        except ValueError:
            continue
    backfill_unit_stmt = None
    if recent_backfill_run_ids:
        backfill_unit_stmt = (
            select(*unit_columns)
            .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
            .where(*base_filters, SyncRunUnit.sync_run_id.in_(recent_backfill_run_ids))
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
        select(*unit_columns)
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .join(latest_success_ranked, latest_success_ranked.c.unit_id == SyncRunUnit.id)
        .where(
            *base_filters,
            SyncRunUnit.status == SyncRunUnitStatus.SUCCESS.value,
            latest_success_ranked.c.row_num == 1,
        )
    )
    rows = [
        _coverage_unit_row(row)
        for row in await _bounded_query_rows(
            session,
            recent_stmt,
            budget,
            stage="recent_units",
        )
    ]
    seen = {unit.unit_id for unit in rows}
    for row in await _bounded_query_rows(
        session,
        latest_success_stmt,
        budget,
        stage="latest_success_units",
    ):
        key = row.unit_id
        if key not in seen:
            rows.append(_coverage_unit_row(row))
            seen.add(key)
    if backfill_unit_stmt is not None:
        for row in await _bounded_query_rows(
            session,
            backfill_unit_stmt,
            budget,
            stage="recent_backfill_units",
        ):
            key = row.unit_id
            if key not in seen:
                rows.append(_coverage_unit_row(row))
                seen.add(key)
    windows: list[UnitWindow] = []
    for unit in rows:
        for effective_key in _effective_dataset_keys_for_unit(unit):
            if effective_key not in scope.dataset_keys:
                continue
            if len(windows) >= MAX_COVERAGE_UNIT_WINDOWS:
                raise SyncCoverageComplexityError(
                    stage="expanded_unit_windows",
                    limit=MAX_COVERAGE_UNIT_WINDOWS,
                    observed=len(windows) + 1,
                )
            windows.append(_unit_window_from_row(unit, effective_key))
    return windows


async def _stream_compact_unit_windows(
    session: AsyncSession,
    org_id: str,
    scope: EffectiveScope,
    truncated_before: datetime,
    *,
    generated_at: datetime,
) -> tuple[list[UnitWindow], datetime | None, int]:
    """Reduce exact retained history without materializing the raw row set.

    The query streams narrow scalar columns in run-time order. Requested and
    covered intervals are appended during the scan and coalesced once per pair;
    unresolved failures retain chronological replay semantics. This keeps the
    worker linear for the common success-heavy case and out of the API process.
    """

    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return [], None, 0
    source_ids = [source.id for source in scope.sources]
    query_dataset_keys = _query_dataset_keys_for_scope(scope.dataset_keys)
    run_time = func.coalesce(
        SyncRun.completed_at,
        SyncRun.started_at,
        SyncRun.created_at,
    ).label("run_time")
    stmt = (
        select(
            SyncRunUnit.source_id,
            SyncRunUnit.dataset_key,
            SyncRunUnit.processor_flags,
            SyncRunUnit.since_at,
            SyncRunUnit.before_at,
            SyncRunUnit.status,
            run_time,
            SyncRunUnit.id,
        )
        .join(SyncRun, SyncRun.id == SyncRunUnit.sync_run_id)
        .where(
            SyncRunUnit.org_id == org_id,
            SyncRunUnit.integration_id == scope.integration_id,
            SyncRunUnit.source_id.in_(source_ids),
            SyncRunUnit.dataset_key.in_(query_dataset_keys),
            SyncRunUnit.status.in_(REQUESTED_UNIT_STATUSES),
            SyncRunUnit.since_at.is_not(None),
            SyncRunUnit.before_at.is_not(None),
            SyncRunUnit.before_at >= truncated_before,
            SyncRun.org_id == org_id,
        )
        .order_by(run_time.asc(), SyncRunUnit.id.asc())
        .execution_options(yield_per=1_000)
    )
    states: dict[tuple[str, str], _CompactPairState] = defaultdict(_CompactPairState)
    latest_successful_run_at: datetime | None = None
    row_count = 0
    stream = await session.stream(stmt)
    async for row in stream:
        row_count += 1
        if row_count > MAX_COVERAGE_PROJECTION_ROWS:
            raise SyncCoverageComplexityError(
                stage="projection_rows",
                limit=MAX_COVERAGE_PROJECTION_ROWS,
                observed=row_count,
            )
        since = max(ensure_utc(row.since_at), truncated_before)
        before = ensure_utc(row.before_at)
        if since >= before:
            continue
        for effective_key in _effective_dataset_keys(
            str(row.dataset_key), row.processor_flags
        ):
            if effective_key not in scope.dataset_keys:
                continue
            pair = (str(row.source_id), effective_key)
            state = states[pair]
            interval = CoverageInterval(since=since, before=before)
            state.requested.append(interval)
            if str(row.status) == SyncRunUnitStatus.SUCCESS.value:
                state.covered.append(interval)
                state.failed = subtract_intervals(state.failed, [interval])
                successful_at = ensure_utc(row.run_time)
                latest_successful_run_at = max(
                    latest_successful_run_at or successful_at, successful_at
                )
            elif str(row.status) == SyncRunUnitStatus.FAILED.value:
                state.failed = merge_intervals([*state.failed, interval])

    # Reuse the established payload builder with a compact semantic equivalent
    # of the raw windows. Successful intervals precede unresolved failures so
    # the latter remain visible exactly as they do after chronological replay.
    success_time = datetime.min.replace(tzinfo=timezone.utc)
    failure_time = generated_at + timedelta(microseconds=1)
    windows: list[UnitWindow] = []
    for (source_id, dataset_key), state in states.items():
        for interval in merge_intervals(state.requested):
            if len(windows) >= MAX_COVERAGE_UNIT_WINDOWS:
                raise SyncCoverageComplexityError(
                    stage="compact_unit_windows",
                    limit=MAX_COVERAGE_UNIT_WINDOWS,
                    observed=len(windows) + 1,
                )
            windows.append(
                UnitWindow(
                    since=interval.since,
                    before=interval.before,
                    source_id=source_id,
                    dataset_key=dataset_key,
                    run_id="",
                    status=SyncRunUnitStatus.PLANNED.value,
                    run_time=success_time,
                )
            )
        for interval in merge_intervals(state.covered):
            if len(windows) >= MAX_COVERAGE_UNIT_WINDOWS:
                raise SyncCoverageComplexityError(
                    stage="compact_unit_windows",
                    limit=MAX_COVERAGE_UNIT_WINDOWS,
                    observed=len(windows) + 1,
                )
            windows.append(
                UnitWindow(
                    since=interval.since,
                    before=interval.before,
                    source_id=source_id,
                    dataset_key=dataset_key,
                    run_id="",
                    status=SyncRunUnitStatus.SUCCESS.value,
                    run_time=success_time,
                )
            )
        for interval in state.failed:
            if len(windows) >= MAX_COVERAGE_UNIT_WINDOWS:
                raise SyncCoverageComplexityError(
                    stage="compact_unit_windows",
                    limit=MAX_COVERAGE_UNIT_WINDOWS,
                    observed=len(windows) + 1,
                )
            windows.append(
                UnitWindow(
                    since=interval.since,
                    before=interval.before,
                    source_id=source_id,
                    dataset_key=dataset_key,
                    run_id="",
                    status=SyncRunUnitStatus.FAILED.value,
                    run_time=failure_time,
                )
            )
    return windows, latest_successful_run_at, row_count


async def _active_run_ids(
    session: AsyncSession,
    org_id: str,
    scope: EffectiveScope,
    budget: _CoverageQueryBudget,
) -> set[tuple[str, str]]:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return set()
    query_dataset_keys = _query_dataset_keys_for_scope(scope.dataset_keys)
    stmt = (
        select(
            SyncRunUnit.source_id,
            SyncRunUnit.dataset_key,
            SyncRunUnit.processor_flags,
        )
        .select_from(SyncRun)
        .join(SyncRunUnit, SyncRunUnit.sync_run_id == SyncRun.id)
        .where(
            SyncRun.org_id == org_id,
            SyncRun.integration_id == scope.integration_id,
            SyncRun.status.in_(ACTIVE_RUN_STATUSES),
            SyncRunUnit.org_id == org_id,
            SyncRunUnit.source_id.in_([source.id for source in scope.sources]),
            SyncRunUnit.dataset_key.in_(query_dataset_keys),
        )
    )
    # No SQL-level .distinct(): SyncRunUnit.processor_flags is a plain JSON
    # column (not JSONB), which Postgres cannot compare for DISTINCT. Dedup
    # happens naturally by building a set of expanded (source_id,
    # effective_dataset_key) pairs below.
    pairs: set[tuple[str, str]] = set()
    for source_id, dataset_key, processor_flags in await _bounded_query_rows(
        session,
        stmt,
        budget,
        stage="active_units",
    ):
        for effective_key in _effective_dataset_keys(dataset_key, processor_flags):
            if effective_key not in scope.dataset_keys:
                continue
            pairs.add((str(source_id), effective_key))
    return pairs


def _backfill_interval(job: _BackfillJobLike) -> CoverageInterval:
    since = datetime.combine(job.since_date, time.min, tzinfo=timezone.utc)
    before = datetime.combine(job.before_date, time.max, tzinfo=timezone.utc)
    return CoverageInterval(since=since, before=before, run_ids=(str(job.id),))


def _backfill_job_sync_run_id(job: _BackfillJobLike) -> str | None:
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


async def _backfill_job_run_pair_windows(
    session: AsyncSession,
    org_id: str,
    run_id: uuid.UUID,
    budget: _CoverageQueryBudget,
) -> dict[tuple[str, str], list[CoverageInterval]]:
    """Return each (source_id, dataset_key) pair's merged unit-window union for a SyncRun.

    An empty dict means the run has zero (valid) SyncRunUnit rows -- callers
    MUST treat that as "this job requested nothing", not as "unresolvable"
    (see ``_backfill_requested_ranges``): a run that legitimately planned zero
    units must not be conflated with a marker we simply couldn't parse.

    Pairs are keyed by effective dataset key (CHAOS-2721 work-item-family
    expansion via ``_effective_dataset_keys_for_unit``), not the raw persisted
    ``dataset_key``, so a collapsed composite unit's window is attributed to
    each of its actually-enabled child datasets rather than the invisible
    canonical "work-items" key.
    """
    stmt = select(
        SyncRunUnit.source_id,
        SyncRunUnit.dataset_key,
        SyncRunUnit.processor_flags,
        SyncRunUnit.since_at,
        SyncRunUnit.before_at,
    ).where(
        SyncRunUnit.org_id == org_id,
        SyncRunUnit.sync_run_id == run_id,
        SyncRunUnit.since_at.is_not(None),
        SyncRunUnit.before_at.is_not(None),
    )
    raw_windows: dict[tuple[str, str], list[CoverageInterval]] = defaultdict(list)
    for (
        source_id,
        dataset_key,
        processor_flags,
        since_at,
        before_at,
    ) in await _bounded_query_rows(
        session,
        stmt,
        budget,
        stage="linked_backfill_units",
    ):
        if since_at is None or before_at is None:
            continue
        interval = CoverageInterval(
            since=ensure_utc(since_at), before=ensure_utc(before_at)
        )
        for effective_key in _effective_dataset_keys(dataset_key, processor_flags):
            pair = (str(source_id), effective_key)
            raw_windows[pair].append(interval)
    return {pair: merge_intervals(intervals) for pair, intervals in raw_windows.items()}


async def _resolve_backfill_job_pair_windows(
    session: AsyncSession,
    org_id: str,
    job: _BackfillJobLike,
    budget: _CoverageQueryBudget,
) -> dict[tuple[str, str], list[CoverageInterval]] | None:
    """Resolve a backfill job's linked-run pair windows, or ``None`` if unresolvable.

    ``None`` means the ``sync_run:<uuid>`` marker is absent or unparseable --
    callers should fall back to legacy all-pairs-in-scope behavior. A resolved
    but empty dict means the run exists (or at least its id parsed) but has no
    units -- callers must contribute nothing for that job, NOT fall back.
    """
    run_id_str = _backfill_job_sync_run_id(job)
    if run_id_str is None:
        return None
    try:
        run_uuid = uuid.UUID(run_id_str)
    except ValueError:
        return None
    return await _backfill_job_run_pair_windows(session, org_id, run_uuid, budget)


def _clip_intervals(
    intervals: Iterable[CoverageInterval], since: datetime, before: datetime
) -> list[CoverageInterval]:
    """Intersect each interval with ``[since, before)``, dropping empty results."""
    clipped: list[CoverageInterval] = []
    for interval in intervals:
        start = max(interval.since, since)
        end = min(interval.before, before)
        if start < end:
            clipped.append(CoverageInterval(since=start, before=end))
    return clipped


async def _backfill_requested_ranges(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    scope: EffectiveScope,
    truncated_before: datetime,
    budget: _CoverageQueryBudget,
) -> list[CoverageInterval]:
    """Return backfill-driven requested intervals.

    Pair-aware: each job's linked SyncRun (resolved via the ``sync_run:<uuid>``
    suffix of ``celery_task_id``) tells us exactly which (source_id,
    dataset_key) pairs the backfill actually planned units for, and the
    ACTUAL unit windows for each pair (clipped to the job's own date range) --
    not the job's full date range -- are what gets counted as "requested".
    Without this, a backfill that only plans units for a subset of the range
    or a subset of the in-scope pairs (unsupported datasets for a provider,
    sources added after the backfill ran, work-item family composite keys)
    permanently "requests" coverage the run never actually attempted -- a gap
    no future backfill on that pair can ever clear (CHAOS-2869).

    Three resolution states, handled distinctly so a run that legitimately
    planned zero units is never conflated with an unresolvable marker:

    * Marker absent/unparseable -> fall back to the legacy all-pairs-in-scope
      behavior for that job (pre-marker legacy jobs keep working as before;
      such rows naturally age out of the ``HISTORY_LOOKBACK_DAYS`` lookback).
    * Marker resolves but the run has zero SyncRunUnit rows -> contribute
      NOTHING for that job (it requested nothing, so nothing is "requested").
    * Marker resolves with units -> pair-scoped intervals clipped to each
      pair's actual unit-window union.
    """
    if not scope.sources:
        return []
    stmt = select(
        BackfillJob.id,
        BackfillJob.celery_task_id,
        BackfillJob.since_date,
        BackfillJob.before_date,
    ).where(
        BackfillJob.org_id == org_id,
        BackfillJob.sync_config_id == config.id,
        BackfillJob.before_date >= truncated_before.date(),
    )
    scope_source_ids = tuple(str(source.id) for source in scope.sources)
    ranges: list[CoverageInterval] = []
    for job in await _bounded_query_rows(
        session,
        stmt,
        budget,
        stage="backfill_jobs",
    ):
        interval = _backfill_interval(job)
        interval = CoverageInterval(
            since=max(interval.since, truncated_before),
            before=interval.before,
            run_ids=interval.run_ids,
        )
        pair_windows = await _resolve_backfill_job_pair_windows(
            session,
            org_id,
            job,
            budget,
        )
        if pair_windows is None:
            ranges.append(
                CoverageInterval(
                    since=interval.since,
                    before=interval.before,
                    source_ids=scope_source_ids,
                    run_ids=interval.run_ids,
                )
            )
            continue
        for (source_id, dataset_key), windows in pair_windows.items():
            if source_id not in scope_source_ids:
                continue
            for clipped in _clip_intervals(windows, interval.since, interval.before):
                ranges.append(
                    CoverageInterval(
                        since=clipped.since,
                        before=clipped.before,
                        source_ids=(source_id,),
                        dataset_keys=(dataset_key,),
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
            run_ids=(window.run_id,) if window.run_id else (),
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


def _canonical_backfill_windows(
    pair_coverages: Sequence[_PairCoverage],
) -> list[dict[str, Any]]:
    """Return exact, source/dataset-scoped actionable coverage windows.

    Boundaries are emitted verbatim, at whatever instant the coverage interval
    actually has. An earlier revision emitted a suggestion only when both
    boundaries fell on exact UTC midnight; coverage intervals derive from sync
    run unit windows, which start whenever a sync happened, so that gate
    matched 0 of 138 real intervals in a populated org and the feature could
    never produce a suggestion (CHAOS-3915).

    Sub-day and off-midnight windows are safe to advertise because the planner
    honours them: ``_backfill_windows`` chunks on whole days but
    ``_chunk_to_window`` keeps the requested instants at the outer edges, so a
    02:46:06.501450 boundary is planned as 02:46:06.501450, and a window inside
    a single day still yields one unit rather than none.

    Intervals no wider than ``INTERVAL_ADJACENCY_TOLERANCE`` are dropped. They
    are not gaps: they are the seam between two ranges the merge step already
    treats as adjacent, and they show up as ``23:59:59.999999 -> 00:00:00``
    where one day-bounded window meets the next. On real data 66 of 114
    candidate windows were exactly one microsecond wide, so advertising them
    would hand the operator 66 buttons that each plan a run covering no time
    at all. This subsumes the empty-interval case, which
    BackfillSelectorRequest would reject outright with a 422.
    """

    candidates_by_scope: dict[
        tuple[datetime, datetime, str, str], set[Literal["gap", "failed"]]
    ] = defaultdict(set)
    for pair in pair_coverages:
        for interval in pair.gaps:
            since = ensure_utc(interval.since)
            before = ensure_utc(interval.before)
            if before - since <= INTERVAL_ADJACENCY_TOLERANCE:
                continue
            candidates_by_scope[(since, before, pair.source_id, pair.dataset_key)].add(
                "gap"
            )
        for interval in pair.failed_ranges:
            since = ensure_utc(interval.since)
            before = ensure_utc(interval.before)
            if before - since <= INTERVAL_ADJACENCY_TOLERANCE:
                continue
            candidates_by_scope[(since, before, pair.source_id, pair.dataset_key)].add(
                "failed"
            )

    candidates = [
        {
            "since": since,
            "before": before,
            "source_ids": [source_id],
            "dataset_keys": [dataset_key],
            "reasons": sorted(reasons),
        }
        for (
            since,
            before,
            source_id,
            dataset_key,
        ), reasons in candidates_by_scope.items()
    ]
    return sorted(
        candidates,
        key=lambda item: (
            item["since"],
            item["before"],
            item["dataset_keys"],
            item["source_ids"],
            item["reasons"],
        ),
    )


def _sync_coverage_lock_name(org_id: str, sync_config_id: uuid.UUID) -> str:
    return f"sync-coverage:{org_id}:{sync_config_id}"


def _sync_coverage_lock_statement(org_id: str, sync_config_id: uuid.UUID) -> Any:
    return select(
        func.pg_advisory_xact_lock(
            func.hashtextextended(_sync_coverage_lock_name(org_id, sync_config_id), 0)
        )
    )


def _sync_coverage_invalidation_statement(
    org_id: str,
    *,
    sync_config_id: uuid.UUID | None = None,
    integration_id: uuid.UUID | None = None,
) -> Update:
    """Build the transaction-local write that makes a projection unreadable."""

    if (sync_config_id is None) == (integration_id is None):
        raise ValueError("exactly one coverage invalidation selector is required")
    statement = update(SyncCoverageProjection).where(
        SyncCoverageProjection.org_id == org_id
    )
    if sync_config_id is not None:
        statement = statement.where(
            SyncCoverageProjection.sync_config_id == sync_config_id
        )
    else:
        config_ids = select(SyncConfiguration.id).where(
            SyncConfiguration.org_id == org_id,
            SyncConfiguration.integration_id == integration_id,
        )
        statement = statement.where(
            SyncCoverageProjection.sync_config_id.in_(config_ids)
        )
    return statement.values(invalidated_at=func.now())


async def invalidate_sync_coverage_projection(
    session: AsyncSession,
    org_id: str,
    *,
    sync_config_id: uuid.UUID | None = None,
    integration_id: uuid.UUID | None = None,
) -> None:
    """Serialize with rebuilds and invalidate in the mutation transaction."""

    if (sync_config_id is None) == (integration_id is None):
        raise ValueError("exactly one coverage invalidation selector is required")
    config_ids = (
        [sync_config_id]
        if sync_config_id is not None
        else list(
            (
                await session.execute(
                    select(SyncConfiguration.id)
                    .where(
                        SyncConfiguration.org_id == org_id,
                        SyncConfiguration.integration_id == integration_id,
                    )
                    .order_by(SyncConfiguration.id)
                )
            ).scalars()
        )
    )
    if session.get_bind().dialect.name == "postgresql":
        for config_id in config_ids:
            await session.execute(_sync_coverage_lock_statement(org_id, config_id))

    await session.execute(
        _sync_coverage_invalidation_statement(
            org_id,
            sync_config_id=sync_config_id,
            integration_id=integration_id,
        )
    )


def invalidate_sync_coverage_projection_sync(
    session: Session,
    org_id: str,
    *,
    sync_config_id: uuid.UUID | None = None,
    integration_id: uuid.UUID | None = None,
) -> None:
    """Synchronous invalidation variant for terminal sync transactions."""

    if (sync_config_id is None) == (integration_id is None):
        raise ValueError("exactly one coverage invalidation selector is required")
    config_ids = (
        [sync_config_id]
        if sync_config_id is not None
        else list(
            session.execute(
                select(SyncConfiguration.id)
                .where(
                    SyncConfiguration.org_id == org_id,
                    SyncConfiguration.integration_id == integration_id,
                )
                .order_by(SyncConfiguration.id)
            ).scalars()
        )
    )
    if session.get_bind().dialect.name == "postgresql":
        for config_id in config_ids:
            session.execute(_sync_coverage_lock_statement(org_id, config_id))
    session.execute(
        _sync_coverage_invalidation_statement(
            org_id,
            sync_config_id=sync_config_id,
            integration_id=integration_id,
        )
    )


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
    latest_successful_run_at_override: datetime | None = None,
    is_truncated: bool = False,
    not_enabled_dataset_keys: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Build the API coverage payload from persisted unit and backfill windows.

    Interval math is evaluated per ``(source_id, dataset_key)`` before summaries
    roll up to dataset, source, and overall levels.

    ``not_enabled_dataset_keys`` (CHAOS-3399) are datasets the provider
    supports but that have no enabled ``IntegrationDataset`` row -- i.e. never
    planned, in contrast to a dataset that IS in ``scope.dataset_keys`` but
    produced zero rows. They are appended to the returned ``datasets`` list
    with a distinct ``"not_enabled"`` status and empty ranges; they never
    participate in ``scope``/pair interval math or the overall/source
    rollups above, so passing this is inert for every existing caller that
    omits it.
    """

    now = ensure_utc(generated_at or datetime.now(timezone.utc))
    schedule_interval = _schedule_interval(active_schedule, now)
    paused = not bool(config.is_active)
    scheduled = active_schedule is not None and has_schedule_row

    if len(windows) > MAX_COVERAGE_UNIT_WINDOWS:
        raise SyncCoverageComplexityError(
            stage="expanded_unit_windows",
            limit=MAX_COVERAGE_UNIT_WINDOWS,
            observed=len(windows),
        )
    pair_count = len(scope.sources) * len(scope.dataset_keys)
    if pair_count > MAX_COVERAGE_PAIRS:
        raise SyncCoverageComplexityError(
            stage="source_dataset_pairs",
            limit=MAX_COVERAGE_PAIRS,
            observed=pair_count,
        )

    by_pair: dict[tuple[str, str], list[UnitWindow]] = defaultdict(list)
    for window in windows:
        by_pair[(window.source_id, window.dataset_key)].append(window)

    scope_source_ids = {str(source.id) for source in scope.sources}
    scope_dataset_keys = set(scope.dataset_keys)
    backfill_by_pair: dict[tuple[str, str], list[CoverageInterval]] = defaultdict(list)
    expanded_backfill_intervals = 0
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
                expanded_backfill_intervals += 1
                if expanded_backfill_intervals > MAX_COVERAGE_BACKFILL_PAIR_INTERVALS:
                    raise SyncCoverageComplexityError(
                        stage="expanded_backfill_pair_intervals",
                        limit=MAX_COVERAGE_BACKFILL_PAIR_INTERVALS,
                        observed=expanded_backfill_intervals,
                    )
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
                (
                    clipped_before
                    for interval in covered
                    if ensure_utc(interval.since)
                    < (clipped_before := min(ensure_utc(interval.before), now))
                ),
                default=None,
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

    pairs_by_dataset: dict[str, list[_PairCoverage]] = defaultdict(list)
    pairs_by_source: dict[str, list[_PairCoverage]] = defaultdict(list)
    for pair in pair_coverages:
        pairs_by_dataset[pair.dataset_key].append(pair)
        pairs_by_source[pair.source_id].append(pair)

    datasets: list[_DatasetCoverage] = []
    for dataset_key in scope.dataset_keys:
        pairs = pairs_by_dataset[dataset_key]
        requested = merge_intervals(
            interval for pair in pairs for interval in pair.requested
        )
        covered = merge_intervals(
            interval for pair in pairs for interval in pair.covered
        )
        gaps = merge_intervals_by_source_scope(
            interval for pair in pairs for interval in pair.gaps
        )
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
        pairs = pairs_by_source[source_id]
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
    if latest_successful_run_at_override is not None:
        latest_successful_run_at = latest_successful_run_at_override
    latest_covered_through = max(
        (dataset.covered_through for dataset in datasets if dataset.covered_through),
        default=None,
    )
    data_basis = _data_basis_for_config(config, scope)
    truncated_before = now - timedelta(days=lookback_days)
    coverage_ranges = [interval for dataset in datasets for interval in dataset.covered]
    coverage_since = min((interval.since for interval in coverage_ranges), default=None)
    coverage_through = max(
        (interval.before for interval in coverage_ranges), default=None
    )
    return {
        "config_id": str(config.id),
        "provider": str(config.provider),
        "generated_at": now,
        "data_basis": data_basis,
        "history_lookback_days": lookback_days,
        "truncated_before": truncated_before,
        "coverage_since": coverage_since,
        "coverage_through": coverage_through,
        "is_truncated": is_truncated,
        "truncation_reason": "lookback_limit" if is_truncated else None,
        "projection_version": SYNC_COVERAGE_PROJECTION_VERSION,
        "projection_complete": True,
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
        ]
        + [
            {
                "dataset_key": dataset_key,
                "status": "not_enabled",
                "covered_through": None,
                "requested_ranges": [],
                "covered_ranges": [],
                "gaps": [],
                "stale_ranges": [],
                "failed_ranges": [],
            }
            for dataset_key in sorted(not_enabled_dataset_keys)
        ],
        "sources": source_payloads,
        "backfill_windows": _canonical_backfill_windows(pair_coverages),
    }


async def _projection_source_updated_at(
    session: AsyncSession,
    org_id: str,
    scope: EffectiveScope,
    truncated_before: datetime,
) -> datetime | None:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return None
    stmt = select(func.max(SyncRunUnit.updated_at)).where(
        SyncRunUnit.org_id == org_id,
        SyncRunUnit.integration_id == scope.integration_id,
        SyncRunUnit.source_id.in_([source.id for source in scope.sources]),
        SyncRunUnit.dataset_key.in_(_query_dataset_keys_for_scope(scope.dataset_keys)),
        SyncRunUnit.before_at >= truncated_before,
        SyncRunUnit.status.in_(REQUESTED_UNIT_STATUSES),
    )
    value = (await session.execute(stmt)).scalar_one_or_none()
    return ensure_utc(value) if value is not None else None


async def _projection_backfill_updated_at(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    truncated_before: datetime,
) -> datetime | None:
    stmt = select(func.max(BackfillJob.updated_at)).where(
        BackfillJob.org_id == org_id,
        BackfillJob.sync_config_id == config.id,
        BackfillJob.before_date >= truncated_before.date(),
    )
    value = (await session.execute(stmt)).scalar_one_or_none()
    return ensure_utc(value) if value is not None else None


async def _has_coverage_before(
    session: AsyncSession,
    org_id: str,
    scope: EffectiveScope,
    truncated_before: datetime,
) -> bool:
    if scope.integration_id is None or not scope.sources or not scope.dataset_keys:
        return False
    stmt = select(SyncRunUnit.id).where(
        SyncRunUnit.org_id == org_id,
        SyncRunUnit.integration_id == scope.integration_id,
        SyncRunUnit.source_id.in_([source.id for source in scope.sources]),
        SyncRunUnit.dataset_key.in_(_query_dataset_keys_for_scope(scope.dataset_keys)),
        SyncRunUnit.since_at < truncated_before,
        SyncRunUnit.status == SyncRunUnitStatus.SUCCESS.value,
    )
    return (await session.execute(stmt.limit(1))).scalar_one_or_none() is not None


def _not_enabled_dataset_keys(
    config: SyncConfiguration, scope: EffectiveScope
) -> frozenset[str]:
    """Datasets the provider supports but that have no enabled row (CHAOS-3399).

    Only computed for integration-level scope (a parent/planner-managed or
    legacy whole-integration config): a source-scoped child config's
    ``dataset_keys`` is an intentional subset of the integration's, not "every
    dataset that could be enabled", so flagging the rest as not-enabled there
    would be noise -- the parent config already surfaces it.
    """
    if scope.integration_id is None or config.source_id is not None:
        return frozenset()
    supported_keys = {spec.dataset_key for spec in supported_datasets(config.provider)}
    return frozenset(supported_keys - set(scope.dataset_keys))


async def rebuild_sync_coverage_projection(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    *,
    lookback_days: int = HISTORY_LOOKBACK_DAYS,
    generated_at: datetime | None = None,
    scope: EffectiveScope | None = None,
    budget: _CoverageQueryBudget | None = None,
) -> dict[str, Any]:
    """Stream raw facts into one exact, atomically replaceable summary row."""

    now = ensure_utc(generated_at or datetime.now(timezone.utc))
    truncated_before = now - timedelta(days=lookback_days)
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(_sync_coverage_lock_statement(org_id, config.id))
    query_budget = budget or _CoverageQueryBudget()
    effective_scope = scope or await resolve_effective_scope(
        session, org_id, config, query_budget
    )
    source_updated_at = await _projection_source_updated_at(
        session, org_id, effective_scope, truncated_before
    )
    backfill_updated_at = await _projection_backfill_updated_at(
        session, org_id, config, truncated_before
    )
    schedule = await _active_schedule(session, org_id, config)
    has_schedule = await _has_schedule_row(session, org_id, config)
    (
        windows,
        latest_successful_run_at,
        raw_row_count,
    ) = await _stream_compact_unit_windows(
        session,
        org_id,
        effective_scope,
        truncated_before,
        generated_at=now,
    )
    active_pairs = await _active_run_ids(session, org_id, effective_scope, query_budget)
    backfill_requested = await _backfill_requested_ranges(
        session,
        org_id,
        config,
        effective_scope,
        truncated_before,
        query_budget,
    )
    payload = build_coverage_summary_payload(
        config=config,
        scope=effective_scope,
        windows=windows,
        backfill_requested=backfill_requested,
        active_pairs=active_pairs,
        active_schedule=schedule,
        has_schedule_row=has_schedule,
        generated_at=now,
        lookback_days=lookback_days,
        latest_successful_run_at_override=latest_successful_run_at,
        is_truncated=await _has_coverage_before(
            session, org_id, effective_scope, truncated_before
        ),
        not_enabled_dataset_keys=_not_enabled_dataset_keys(config, effective_scope),
    )
    projection = (
        await session.execute(
            select(SyncCoverageProjection).where(
                SyncCoverageProjection.org_id == org_id,
                SyncCoverageProjection.sync_config_id == config.id,
                SyncCoverageProjection.history_lookback_days == lookback_days,
            )
        )
    ).scalar_one_or_none()
    if projection is None:
        projection = SyncCoverageProjection(
            org_id=org_id,
            sync_config_id=config.id,
            history_lookback_days=lookback_days,
            projection_version=SYNC_COVERAGE_PROJECTION_VERSION,
            generated_at=now,
            source_updated_at=source_updated_at,
            backfill_updated_at=backfill_updated_at,
            invalidated_at=None,
            payload=jsonable_encoder(payload),
        )
        session.add(projection)
    else:
        projection.projection_version = SYNC_COVERAGE_PROJECTION_VERSION
        projection.generated_at = now
        projection.source_updated_at = source_updated_at
        projection.backfill_updated_at = backfill_updated_at
        projection.invalidated_at = None
        projection.payload = jsonable_encoder(payload)
    await session.flush()
    logger.info(
        "sync_coverage_projection_rebuilt",
        extra={
            "org_id": org_id,
            "sync_config_id": str(config.id),
            "history_lookback_days": lookback_days,
            "raw_unit_row_count": raw_row_count,
            "compact_window_count": len(windows),
            "projection_version": SYNC_COVERAGE_PROJECTION_VERSION,
        },
    )
    return payload


async def build_sync_coverage_summary(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    *,
    lookback_days: int = HISTORY_LOOKBACK_DAYS,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Return the latest O(1) durable projection, including during refresh.

    Invalidation marks the existing payload as needing replacement; it does not
    erase the last completed coverage state. Serving that payload with an
    explicit refresh marker keeps navigation and reloads truthful while the
    background builder prepares its successor. A genuinely cold config still
    raises ``SyncCoveragePendingError``.
    """

    started_at = monotonic()
    log_context = {
        "org_id": org_id,
        "sync_config_id": str(config.id),
        "history_lookback_days": lookback_days,
    }
    logger.info("sync_coverage_summary_waiting", extra=log_context)
    projection = (
        await session.execute(
            select(SyncCoverageProjection).where(
                SyncCoverageProjection.org_id == org_id,
                SyncCoverageProjection.sync_config_id == config.id,
                SyncCoverageProjection.history_lookback_days == lookback_days,
                SyncCoverageProjection.projection_version
                == SYNC_COVERAGE_PROJECTION_VERSION,
            )
        )
    ).scalar_one_or_none()
    if projection is None:
        logger.info("sync_coverage_projection_pending", extra=log_context)
        raise SyncCoveragePendingError("Coverage is being prepared. Retry shortly.")
    payload = dict(projection.payload)
    payload["projection_refreshing"] = projection.invalidated_at is not None
    logger.info(
        "sync_coverage_summary_completed",
        extra={
            **log_context,
            "elapsed_seconds": round(monotonic() - started_at, 3),
            "gap_count": payload["overall"]["gap_count"],
            "failed_range_count": payload["overall"]["failed_range_count"],
            "projection_version": payload["projection_version"],
            "projection_refreshing": payload["projection_refreshing"],
        },
    )
    return payload
