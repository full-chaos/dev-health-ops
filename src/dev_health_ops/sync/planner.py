"""Sync planner contract (CHAOS-2511).

FROZEN CONTRACT — this module's public types and signatures are the interface
between the planner (Wave 1, CHAOS-2511) and the dispatcher/unit-worker
(Wave 2, CHAOS-2512/2513). Implement BEHIND these signatures; do not change the
DTO shapes without updating every consumer.

Responsibilities (CHAOS-2511):
  * Load enabled sources + enabled datasets for an integration.
  * Skip unsupported provider/dataset pairs (see ``sync.datasets``).
  * Resolve incremental windows from per-(source, dataset) watermarks.
  * Resolve backfill windows via ``backfill.chunker``.
  * Assign cost class per dataset.
  * Persist the FULL plan (SyncRun + all SyncRunUnit rows, status=planned)
    BEFORE any dispatch. Dispatch is a separate, idempotent step.

Invariants:
  * Disabled source -> zero units. Disabled dataset -> zero units.
  * Backfill units carry mode="backfill" and must never update watermarks.
  * total_units on the persisted SyncRun equals len(unit_ids).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from typing import TYPE_CHECKING

from dev_health_ops.backfill.chunker import chunk_date_range
from dev_health_ops.models import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.datasets import WatermarkBehavior, get_dataset_spec
from dev_health_ops.sync.watermarks import get_watermark

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class WatermarkKey:
    """Generalized watermark identity (CHAOS-2509)."""

    org_id: str
    source_id: str
    dataset_key: str


@dataclass(frozen=True)
class SyncPlanRequest:
    """Input to :func:`plan_sync_run`.

    ``source_ids`` / ``dataset_keys`` of ``None`` mean "all enabled". Explicit
    tuples filter to the given subset (still intersected with enabled rows).
    """

    integration_id: str
    mode: str  # one of models.integrations.SyncRunMode
    triggered_by: str
    source_ids: tuple[str, ...] | None = None
    dataset_keys: tuple[str, ...] | None = None
    since: datetime | None = None
    before: datetime | None = None


@dataclass(frozen=True)
class PlannedUnit:
    """Frozen description of one execution unit prior to persistence.

    Mirrors the ``SyncRunUnit`` columns. Celery payloads carry the persisted
    ``unit_id`` ONLY (never this object, never credentials).
    """

    org_id: str
    integration_id: str
    source_id: str
    provider: str
    dataset_key: str
    cost_class: str
    mode: str
    window_start: datetime | None
    window_end: datetime | None
    processor_flags: Mapping[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class SyncRunPlan:
    """Result of :func:`plan_sync_run` — the persisted run + its unit ids."""

    sync_run_id: str
    total_units: int
    unit_ids: tuple[str, ...]


def plan_sync_run(session: Session, request: SyncPlanRequest) -> SyncRunPlan:
    """Expand an integration into persisted SyncRun + SyncRunUnit rows.

    Persists everything with status=planned and returns the run id + unit ids.
    Implemented in CHAOS-2511. The dispatcher (CHAOS-2512) consumes the result
    via :func:`dev_health_ops.workers.sync_units.dispatch_sync_run`.
    """

    integration = _load_integration(session, request.integration_id)
    mode = _validate_mode(request.mode)
    sources = _load_enabled_sources(session, integration, request.source_ids)
    datasets = _load_enabled_datasets(session, integration, request.dataset_keys)
    now = datetime.now(timezone.utc)

    planned_units = _build_planned_units(
        session=session,
        request=request,
        integration=integration,
        sources=sources,
        datasets=datasets,
        mode=mode,
        now=now,
    )

    sync_run = SyncRun(
        org_id=integration.org_id,
        integration_id=integration.id,
        triggered_by=request.triggered_by,
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=len(planned_units),
        completed_units=0,
        failed_units=0,
    )
    session.add(sync_run)
    session.flush()

    unit_rows = [
        SyncRunUnit(
            org_id=unit.org_id,
            sync_run_id=sync_run.id,
            integration_id=integration.id,
            source_id=_coerce_uuid(unit.source_id, "source_id"),
            provider=unit.provider,
            dataset_key=unit.dataset_key,
            cost_class=unit.cost_class,
            mode=unit.mode,
            since_at=unit.window_start,
            before_at=unit.window_end,
            status=SyncRunUnitStatus.PLANNED.value,
            attempts=0,
        )
        for unit in planned_units
    ]
    session.add_all(unit_rows)
    session.flush()

    return SyncRunPlan(
        sync_run_id=str(sync_run.id),
        total_units=len(unit_rows),
        unit_ids=tuple(str(unit.id) for unit in unit_rows),
    )


def _load_integration(session: Session, integration_id: str) -> Integration:
    integration_uuid = _coerce_uuid(integration_id, "integration_id")
    integration = session.get(Integration, integration_uuid)
    if integration is None:
        raise ValueError(f"Integration not found: {integration_id}")
    return integration


def _load_enabled_sources(
    session: Session,
    integration: Integration,
    source_ids: tuple[str, ...] | None,
) -> list[IntegrationSource]:
    query = session.query(IntegrationSource).filter(
        IntegrationSource.org_id == integration.org_id,
        IntegrationSource.integration_id == integration.id,
        IntegrationSource.is_enabled.is_(True),
    )
    if source_ids is not None:
        source_uuids = tuple(
            _coerce_uuid(source_id, "source_id") for source_id in source_ids
        )
        if not source_uuids:
            return []
        query = query.filter(IntegrationSource.id.in_(source_uuids))
    return list(query.order_by(IntegrationSource.full_name, IntegrationSource.id).all())


def _load_enabled_datasets(
    session: Session,
    integration: Integration,
    dataset_keys: tuple[str, ...] | None,
) -> list[IntegrationDataset]:
    query = session.query(IntegrationDataset).filter(
        IntegrationDataset.org_id == integration.org_id,
        IntegrationDataset.integration_id == integration.id,
        IntegrationDataset.is_enabled.is_(True),
    )
    if dataset_keys is not None:
        if not dataset_keys:
            return []
        query = query.filter(IntegrationDataset.dataset_key.in_(dataset_keys))
    return list(query.order_by(IntegrationDataset.dataset_key).all())


def _build_planned_units(
    *,
    session: Session,
    request: SyncPlanRequest,
    integration: Integration,
    sources: list[IntegrationSource],
    datasets: list[IntegrationDataset],
    mode: str,
    now: datetime,
) -> list[PlannedUnit]:
    planned_units: list[PlannedUnit] = []
    for source in sources:
        provider = source.provider
        for dataset in datasets:
            spec = get_dataset_spec(provider, dataset.dataset_key)
            if spec is None or not spec.supported:
                continue

            windows = _resolve_windows(
                session=session,
                request=request,
                mode=mode,
                org_id=integration.org_id,
                source_id=str(source.id),
                dataset_key=dataset.dataset_key,
                watermark_behavior=spec.watermark_behavior,
                now=now,
            )
            for window_start, window_end in windows:
                planned_units.append(
                    PlannedUnit(
                        org_id=integration.org_id,
                        integration_id=str(integration.id),
                        source_id=str(source.id),
                        provider=provider,
                        dataset_key=dataset.dataset_key,
                        cost_class=spec.default_cost_class.value,
                        mode=mode,
                        window_start=window_start,
                        window_end=window_end,
                        processor_flags=dict(spec.processor_flags),
                    )
                )
    return planned_units


def _resolve_windows(
    *,
    session: Session,
    request: SyncPlanRequest,
    mode: str,
    org_id: str,
    source_id: str,
    dataset_key: str,
    watermark_behavior: WatermarkBehavior,
    now: datetime,
) -> tuple[tuple[datetime | None, datetime | None], ...]:
    if mode == SyncRunMode.INCREMENTAL.value:
        window_start = None
        if watermark_behavior == WatermarkBehavior.INCREMENTAL:
            window_start = get_watermark(session, org_id, source_id, dataset_key)
        return ((window_start, _request_before_or_now(request, now)),)

    if mode == SyncRunMode.BACKFILL.value:
        return _backfill_windows(request)

    return ((None, _request_before_or_now(request, now)),)


def _backfill_windows(
    request: SyncPlanRequest,
) -> tuple[tuple[datetime | None, datetime | None], ...]:
    if request.since is None or request.before is None:
        raise ValueError("Backfill sync planning requires since and before")

    since = _as_utc(request.since)
    before = _as_utc(request.before)
    if since > before:
        raise ValueError("Backfill since must be before or equal to before")

    chunks = chunk_date_range(since=since.date(), before=before.date())
    return tuple(
        _chunk_to_window(chunk_since, chunk_before, since, before)
        for chunk_since, chunk_before in chunks
    )


def _chunk_to_window(
    chunk_since,
    chunk_before,
    requested_since: datetime,
    requested_before: datetime,
) -> tuple[datetime, datetime]:
    window_start = (
        requested_since
        if chunk_since == requested_since.date()
        else datetime.combine(chunk_since, time.min, tzinfo=timezone.utc)
    )
    window_end = (
        requested_before
        if chunk_before == requested_before.date()
        else datetime.combine(chunk_before, time.max, tzinfo=timezone.utc)
    )
    return window_start, window_end


def _request_before_or_now(request: SyncPlanRequest, now: datetime) -> datetime:
    if request.before is None:
        return now
    return _as_utc(request.before)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _validate_mode(mode: str) -> str:
    valid_modes = {item.value for item in SyncRunMode}
    if mode not in valid_modes:
        raise ValueError(f"Unsupported sync run mode: {mode}")
    return mode


def _coerce_uuid(value: str, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: {value}") from exc


def map_datasets_to_legacy_targets(
    provider: str, dataset_keys: Iterable[str]
) -> frozenset[str]:
    """Fan-in seam: union the legacy post-sync targets for completed datasets.

    ``finalize_sync_run`` (CHAOS-2512) calls this to translate the dataset keys
    of successful units back into the legacy ``sync_targets`` vocabulary that
    ``_dispatch_post_sync_tasks`` understands, so metrics fan-out stays unchanged.
    Registry-owned mapping — do NOT hand-roll string mapping in finalize.
    """

    targets: set[str] = set()
    for dataset_key in dataset_keys:
        spec = get_dataset_spec(provider, dataset_key)
        if spec is not None:
            targets.update(spec.legacy_targets)
    return frozenset(targets)
