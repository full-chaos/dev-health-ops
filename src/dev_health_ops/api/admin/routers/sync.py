from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol, cast

from croniter import croniter as Croniter
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    JOB_RUN_STATUS_LABELS,
    BackfillRequest,
    JobRunResponse,
    SyncConfigBatchCreate,
    SyncConfigBatchResponse,
    SyncConfigCreate,
    SyncConfigRepositorySelection,
    SyncConfigRepositorySelectionUpdate,
    SyncConfigResponse,
    SyncConfigUpdate,
    SyncCoverageRange,
    SyncCoverageSummaryResponse,
    SyncRunJobEnrichment,
)
from dev_health_ops.api.services.configuration import (
    IntegrationCredentialsService,
    SyncConfigurationService,
)
from dev_health_ops.api.services.licensing import TierLimitService
from dev_health_ops.api.services.sync_coverage import (
    HISTORY_LOOKBACK_DAYS,
    build_sync_coverage_summary,
    ensure_utc,
)
from dev_health_ops.models import SyncRun
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.sync.datasets import (
    DatasetKey,
    supported_datasets,
    supported_legacy_targets,
)
from dev_health_ops.sync.error_sanitize import sanitize_error_text
from dev_health_ops.sync.execution_trigger import (
    create_sync_execution_trigger,
    ensure_pending_sync_job_run,
    mark_job_run_failed,
    merge_job_run_result,
)
from dev_health_ops.sync.trigger_routing import (
    mark_sync_run_failed,
)
from dev_health_ops.utils.datetime import validate_timezone_name
from dev_health_ops.workers.sync_units import dispatch_sync_run

from .common import get_session

router = APIRouter()

logger = logging.getLogger(__name__)


def _mark_job_run_failed(sync_session, run_id: str, error: BaseException | str) -> None:
    mark_job_run_failed(sync_session, run_id, error)


def _merge_job_run_result(
    sync_session, run_id: str, result: dict[str, Any] | None = None
) -> None:
    merge_job_run_result(sync_session, run_id, result)


def _mark_backfill_job_failed(
    sync_session,
    backfill_job_id: str,
    error: BaseException | str,
    completed_at: datetime,
) -> None:
    """Terminalize a ``BackfillJob`` as failed.

    ``error`` is sanitized here, at the sink (CHAOS-2766 codex review
    finding), for the same reason as ``mark_job_run_failed``: a Celery/broker
    enqueue-failure exception can embed the broker/result-backend URL
    including credentials, and ``error_message`` surfaces through admin
    backfill-job responses.
    """
    from dev_health_ops.models.backfill import BackfillJob as BackfillJobModel

    bf_job = (
        sync_session.query(BackfillJobModel)
        .filter(BackfillJobModel.id == uuid.UUID(str(backfill_job_id)))
        .one_or_none()
    )
    if bf_job is None:
        return
    bf_job.status = "failed"
    bf_job.error_message = sanitize_error_text(error)
    bf_job.completed_at = completed_at
    # The dispatch never enqueued (or its task id is meaningless), so drop any
    # pre-dispatch sync_run marker: the run is separately terminalized as failed.
    bf_job.celery_task_id = None
    sync_session.flush()


def _sync_config_integration_credential_id(
    sync_session, config, org_id: str
) -> uuid.UUID | None:
    """Resolve the credential a config's LINKED integration actually uses.

    ``SyncConfiguration`` carries no credential of its own (CHAOS-2762) --
    ``Integration.credential_id`` (reached via ``config.integration_id``) is
    the single sanctioned surface. Returns ``None`` when the config has no
    linked integration (a pre-planner legacy row; the trigger/backfill
    endpoints already reject those with "no linked integration" downstream)
    OR when the linked integration does not belong to ``org_id``: an
    out-of-org ``integration_id`` (corrupt data / manual tampering) must never
    leak another org's credential, so it is treated as no-credential -- the
    same org-scoping ``sync/planner.py``'s ``_load_integration`` enforces.
    """
    integration_id = getattr(config, "integration_id", None)
    if integration_id is None:
        return None
    integration = (
        sync_session.query(Integration)
        .filter(Integration.id == integration_id, Integration.org_id == org_id)
        .one_or_none()
    )
    return getattr(integration, "credential_id", None) if integration else None


def _preflight_planner_credential(sync_session, config) -> None:
    org_id = str(getattr(config, "org_id"))
    credential_id = _sync_config_integration_credential_id(sync_session, config, org_id)
    if credential_id is None:
        return
    from dev_health_ops.models.settings import IntegrationCredential

    credential = (
        sync_session.query(IntegrationCredential)
        .filter(
            IntegrationCredential.id == credential_id,
            IntegrationCredential.org_id == org_id,
        )
        .one_or_none()
    )
    if credential is None:
        raise HTTPException(status_code=400, detail="Credential not found")
    if not bool(credential.is_active):
        raise HTTPException(status_code=409, detail="Credential is inactive")
    if credential.last_test_success is False:
        detail = credential.last_test_error or "Credential preflight failed"
        raise HTTPException(status_code=409, detail=detail)


def _ensure_pending_sync_job_run(
    sync_session,
    config,
    org_id: str,
    triggered_by: str,
    result: dict[str, Any] | None = None,
) -> str:
    return ensure_pending_sync_job_run(
        sync_session, config, org_id, triggered_by, result
    )


async def _active_repo_usage_count_for_limit(session: AsyncSession, org_id: str) -> int:
    active_configs = await session.execute(
        select(SyncConfiguration).where(
            SyncConfiguration.org_id == org_id,
            SyncConfiguration.is_active.is_(True),
        )
    )
    active_config_rows = list(active_configs.scalars().all())
    planner_parent_ids = [
        config.id
        for config in active_config_rows
        if config.parent_id is None
        and bool(config.planner_managed)
        and config.integration_id is not None
    ]
    planner_integration_ids = [
        config.integration_id
        for config in active_config_rows
        if config.id in planner_parent_ids
    ]
    legacy_count = sum(
        1 for config in active_config_rows if config.id not in planner_parent_ids
    )
    if not planner_integration_ids:
        return legacy_count

    source_count = await session.scalar(
        select(func.count(IntegrationSource.id)).where(
            IntegrationSource.org_id == org_id,
            IntegrationSource.integration_id.in_(planner_integration_ids),
            IntegrationSource.is_enabled.is_(True),
        )
    )
    return legacy_count + int(source_count or 0)


def _repo_limit_advisory_lock_key(org_id: str) -> int:
    try:
        org_int = uuid.UUID(org_id).int
    except ValueError:
        org_int = uuid.uuid5(uuid.NAMESPACE_URL, org_id).int
    return org_int & ((1 << 63) - 1)


async def _acquire_repo_limit_create_lock(session: AsyncSession, org_id: str) -> None:
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": _repo_limit_advisory_lock_key(org_id)},
    )


class _MutableSyncConfiguration(Protocol):
    sync_targets: list[str]
    sync_options: dict[str, Any]
    is_active: bool


async def _integration_credential_id_for_config(
    session: AsyncSession, config: object, org_id: str
) -> uuid.UUID | None:
    """Async counterpart of ``_sync_config_integration_credential_id``.

    Resolves the ``credential_id`` of a config's linked ``Integration`` --
    the single sanctioned surface (CHAOS-2762) -- for building API responses.
    Scoped to ``org_id`` so an out-of-org ``integration_id`` (corrupt data)
    can never leak another org's credential UUID into this response; treated
    as no-credential, same as the planner's org-scoped integration lookup.
    """
    integration_id = getattr(config, "integration_id", None)
    if integration_id is None:
        return None
    result = await session.execute(
        select(Integration.credential_id).where(
            Integration.id == integration_id, Integration.org_id == org_id
        )
    )
    return result.scalar_one_or_none()


async def _integration_credential_ids_for_configs(
    session: AsyncSession, configs: Sequence[object], org_id: str
) -> dict[str, uuid.UUID | None]:
    """Batch variant of ``_integration_credential_id_for_config`` for list responses.

    ONE query for the whole page via ``Integration.id.in_(...)`` -- callers
    must use this (not a per-row ``_integration_credential_id_for_config``
    call in a loop) when building a list response, or credential resolution
    becomes an N+1 over ``Integration``. Also scoped to ``org_id`` (see
    ``_integration_credential_id_for_config``) so an out-of-org
    ``integration_id`` never leaks another org's credential.
    """
    integration_ids = {
        getattr(config, "integration_id")
        for config in configs
        if getattr(config, "integration_id", None) is not None
    }
    if not integration_ids:
        return {}
    result = await session.execute(
        select(Integration.id, Integration.credential_id).where(
            Integration.org_id == org_id, Integration.id.in_(integration_ids)
        )
    )
    return {
        str(integration_id): credential_id
        for integration_id, credential_id in result.all()
    }


def _sync_config_to_response(
    config: object,
    children_count: int | None = None,
    *,
    credential_id: uuid.UUID | str | None = None,
) -> SyncConfigResponse:
    return SyncConfigResponse.model_validate(
        {
            "id": str(getattr(config, "id")),
            "name": getattr(config, "name"),
            "provider": getattr(config, "provider"),
            "credential_id": str(credential_id) if credential_id is not None else None,
            "sync_targets": list(getattr(config, "sync_targets") or []),
            "sync_options": dict(getattr(config, "sync_options") or {}),
            "is_active": getattr(config, "is_active"),
            "parent_id": (
                str(getattr(config, "parent_id"))
                if getattr(config, "parent_id") is not None
                else None
            ),
            "children_count": children_count,
            "last_sync_at": getattr(config, "last_sync_at"),
            "last_sync_success": getattr(config, "last_sync_success"),
            "last_sync_error": getattr(config, "last_sync_error"),
            "created_at": getattr(config, "created_at"),
            "updated_at": getattr(config, "updated_at"),
        }
    )


def _aware_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _latest_datetime(*values: datetime | None) -> datetime | None:
    aware_values = [aware for value in values if (aware := _aware_datetime(value))]
    return max(aware_values) if aware_values else None


def _elapsed_seconds(
    started_at: datetime | None, completed_at: datetime | None
) -> int | None:
    started = _aware_datetime(started_at)
    completed = _aware_datetime(completed_at)
    if started is None or completed is None:
        return None
    return max(0, int((completed - started).total_seconds()))


def _items_synced_from_result(result: dict[str, Any] | None) -> int:
    if not result:
        return 0
    for key in ("items_synced", "rows_ingested", "rows", "items", "count"):
        if key in result:
            try:
                return int(result.get(key) or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def _planner_job_run_sync_run_id(run: object) -> uuid.UUID | None:
    result = getattr(run, "result", None)
    if not isinstance(result, dict):
        return None
    sync_run_id = result.get("sync_run_id")
    if sync_run_id is None:
        return None
    try:
        return uuid.UUID(str(sync_run_id))
    except ValueError:
        return None


def _planner_job_run_status(run_status: str) -> int:
    if run_status == SyncRunStatus.PLANNED.value:
        return JobRunStatus.PENDING.value
    if run_status in {
        SyncRunStatus.DISPATCHING.value,
        SyncRunStatus.RUNNING.value,
    }:
        return JobRunStatus.RUNNING.value
    if run_status == SyncRunStatus.SUCCESS.value:
        return JobRunStatus.SUCCESS.value
    if run_status in {
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }:
        return JobRunStatus.FAILED.value
    return JobRunStatus.RUNNING.value


async def _planner_sync_runs_for_job_runs(
    session: AsyncSession, runs: Sequence[object], org_id: str
) -> dict[str, SyncRun]:
    sync_run_ids = {
        sync_run_id
        for run in runs
        if (sync_run_id := _planner_job_run_sync_run_id(run)) is not None
    }
    if not sync_run_ids:
        return {}
    result = await session.execute(
        select(SyncRun).where(
            SyncRun.id.in_(sync_run_ids),
            SyncRun.org_id == org_id,
        )
    )
    return {str(sync_run.id): sync_run for sync_run in result.scalars().all()}


async def _planner_sync_run_units_for_job_runs(
    session: AsyncSession, runs: Sequence[object], org_id: str
) -> dict[str, list[SyncRunUnit]]:
    sync_run_ids = {
        sync_run_id
        for run in runs
        if (sync_run_id := _planner_job_run_sync_run_id(run)) is not None
    }
    if not sync_run_ids:
        return {}
    result = await session.execute(
        select(SyncRunUnit).where(
            SyncRunUnit.sync_run_id.in_(sync_run_ids),
            SyncRunUnit.org_id == org_id,
        )
    )
    units_by_run: dict[str, list[SyncRunUnit]] = {}
    for unit in result.scalars().all():
        units_by_run.setdefault(str(unit.sync_run_id), []).append(unit)
    return units_by_run


def _sync_run_unit_range(
    units: Sequence[SyncRunUnit], *, success_only: bool = False
) -> SyncCoverageRange | None:
    scoped_units = [
        unit
        for unit in units
        if unit.since_at is not None
        and unit.before_at is not None
        and (not success_only or unit.status == SyncRunUnitStatus.SUCCESS.value)
    ]
    if not scoped_units:
        return None
    return SyncCoverageRange(
        since=min(
            ensure_utc(unit.since_at)
            for unit in scoped_units
            if unit.since_at is not None
        ),
        before=max(
            ensure_utc(unit.before_at)
            for unit in scoped_units
            if unit.before_at is not None
        ),
        source_ids=sorted({str(unit.source_id) for unit in scoped_units}),
        run_ids=sorted({str(unit.sync_run_id) for unit in scoped_units}),
    )


def _sync_run_job_enrichment(
    sync_run: SyncRun | None, units: Sequence[SyncRunUnit]
) -> SyncRunJobEnrichment | None:
    if sync_run is None:
        return None
    sync_run_id = getattr(sync_run, "id", None)
    mode = getattr(sync_run, "mode", None)
    triggered_by = getattr(sync_run, "triggered_by", None)
    if sync_run_id is None or mode is None or triggered_by is None:
        return None
    return SyncRunJobEnrichment(
        mode=str(mode),
        triggered_by=str(triggered_by),
        requested_range=_sync_run_unit_range(units),
        covered_range=_sync_run_unit_range(units, success_only=True),
        total_units=int(sync_run.total_units),
        completed_units=int(sync_run.completed_units),
        failed_units=int(sync_run.failed_units),
        sync_run_id=str(sync_run_id),
    )


def _job_run_response(
    run: object,
    planner_sync_run: SyncRun | None = None,
    planner_sync_run_units: Sequence[SyncRunUnit] = (),
) -> JobRunResponse:
    status_value = int(getattr(run, "status"))
    started_at = getattr(run, "started_at")
    completed_at = getattr(run, "completed_at")
    duration_seconds = getattr(run, "duration_seconds")
    error = getattr(run, "error")
    result = getattr(run, "result")
    items_synced = _items_synced_from_result(
        result if isinstance(result, dict) else None
    )

    if planner_sync_run is not None:
        # Planner-managed JobRun rows are visibility anchors for Job History.
        # The linked SyncRun owns execution lifecycle and runtime stats.
        sync_result = getattr(planner_sync_run, "result")
        result = {
            **(result if isinstance(result, dict) else {}),
            **(sync_result if isinstance(sync_result, dict) else {}),
            "sync_run_status": str(getattr(planner_sync_run, "status")),
            "total_units": int(getattr(planner_sync_run, "total_units")),
            "completed_units": int(getattr(planner_sync_run, "completed_units")),
            "failed_units": int(getattr(planner_sync_run, "failed_units")),
        }
        status_value = _planner_job_run_status(str(getattr(planner_sync_run, "status")))
        started_at = getattr(planner_sync_run, "started_at")
        completed_at = getattr(planner_sync_run, "completed_at")
        duration_seconds = _elapsed_seconds(started_at, completed_at)
        error = getattr(planner_sync_run, "error") or error
        items_synced = int(getattr(planner_sync_run, "completed_units"))

    return JobRunResponse.model_validate(
        {
            "id": str(getattr(run, "id")),
            "job_id": str(getattr(run, "job_id")),
            "status": JOB_RUN_STATUS_LABELS.get(status_value, "failed"),
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": duration_seconds,
            "items_synced": items_synced,
            "result": result,
            "error": error,
            "triggered_by": getattr(run, "triggered_by"),
            "sync_run": _sync_run_job_enrichment(
                planner_sync_run, planner_sync_run_units
            ),
            "created_at": getattr(run, "created_at"),
        }
    )


def _backfill_job_response(job: object, run_counts: dict[str, Any] | None = None):
    from dev_health_ops.api.schemas.backfill import BackfillJobResponse

    run_counts = run_counts or {}
    total_chunks = int(run_counts.get("total_chunks", getattr(job, "total_chunks")))
    completed_chunks = int(
        run_counts.get("completed_chunks", getattr(job, "completed_chunks"))
    )
    progress_pct = (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0.0
    job_updated_at = getattr(job, "updated_at")
    effective_updated_at = _latest_datetime(
        job_updated_at,
        run_counts.get("updated_at"),
    )
    return BackfillJobResponse(
        id=str(getattr(job, "id")),
        sync_config_id=str(getattr(job, "sync_config_id")),
        status=str(run_counts.get("status", getattr(job, "status"))),
        since_date=getattr(job, "since_date"),
        before_date=getattr(job, "before_date"),
        total_chunks=total_chunks,
        completed_chunks=completed_chunks,
        failed_chunks=int(
            run_counts.get("failed_chunks", getattr(job, "failed_chunks"))
        ),
        progress_pct=progress_pct,
        error_message=run_counts.get("error_message", getattr(job, "error_message")),
        started_at=getattr(job, "started_at"),
        completed_at=run_counts.get("completed_at", getattr(job, "completed_at")),
        created_at=getattr(job, "created_at"),
        # Response-level updated_at is effective liveness: the raw BackfillJob
        # row-write timestamp OR the latest linked fanout unit activity.
        updated_at=effective_updated_at or job_updated_at,
    )


def _backfill_job_sync_run_id(job: object) -> str | None:
    task_id = str(getattr(job, "celery_task_id") or "")
    marker = "sync_run:"
    if marker not in task_id:
        return None
    return task_id.rsplit(marker, 1)[-1] or None


async def _backfill_job_run_counts(
    session: AsyncSession, job: object
) -> dict[str, Any] | None:
    sync_run_id = _backfill_job_sync_run_id(job)
    if sync_run_id is None:
        return None
    try:
        run_uuid = uuid.UUID(sync_run_id)
    except ValueError:
        return None
    stmt = select(SyncRun).where(
        SyncRun.id == run_uuid,
        SyncRun.org_id == str(getattr(job, "org_id")),
    )
    result = await session.execute(stmt)
    run = result.scalar_one_or_none()
    if run is None:
        return None
    activity_stmt = select(
        func.max(SyncRunUnit.updated_at),
        func.max(SyncRunUnit.last_heartbeat_at),
    ).where(
        SyncRunUnit.sync_run_id == run_uuid,
        SyncRunUnit.org_id == str(getattr(job, "org_id")),
    )
    activity_result = await session.execute(activity_stmt)
    latest_unit_updated_at, latest_unit_heartbeat_at = activity_result.one()
    return {
        "status": getattr(run, "status"),
        "total_chunks": int(getattr(run, "total_units")),
        "completed_chunks": int(getattr(run, "completed_units")),
        "failed_chunks": int(getattr(run, "failed_units")),
        "completed_at": getattr(run, "completed_at"),
        "error_message": getattr(run, "error"),
        "updated_at": _latest_datetime(
            latest_unit_updated_at,
            latest_unit_heartbeat_at,
        ),
    }


# Canonical mapping of provider → supported sync targets.
# Jira/Linear only support work-items; Git/CI/CD come from code hosts.
PROVIDER_SYNC_TARGETS: dict[str, list[str]] = {
    provider: supported_legacy_targets(provider)
    for provider in ("github", "gitlab", "jira", "linear", "launchdarkly")
}

NON_REPO_SYNC_PROVIDERS = {"jira", "linear"}
DEFAULT_SYNC_CRON = "0 * * * *"
DEFAULT_GITLAB_URL = "https://gitlab.com"


def _sync_options_with_top_level_fields(
    sync_options: dict[str, Any] | None,
    *,
    schedule_cron: str | None = None,
    timezone: str | None = None,
    initial_sync_depth: int | None = None,
) -> dict[str, Any]:
    merged = dict(sync_options or {})
    if schedule_cron is not None:
        merged["schedule_cron"] = schedule_cron
    if timezone is not None:
        merged["timezone"] = timezone
    if initial_sync_depth is not None:
        merged["initial_sync_depth"] = initial_sync_depth
    return merged


def _config_has_explicit_schedule(config: object) -> bool:
    """True when the config carries a user-set schedule_cron (not manual-only)."""
    sync_options = dict(getattr(config, "sync_options") or {})
    return bool(sync_options.get("schedule_cron"))


def _schedule_cron_for_config(config: object) -> str:
    sync_options = dict(getattr(config, "sync_options") or {})
    return str(sync_options.get("schedule_cron") or DEFAULT_SYNC_CRON)


async def _upsert_scheduled_job(
    session: AsyncSession, config: object, org_id: str
) -> None:
    sync_config_id = getattr(config, "id")
    stmt = select(ScheduledJob).where(
        ScheduledJob.org_id == org_id,
        ScheduledJob.sync_config_id == sync_config_id,
        ScheduledJob.job_type == "sync",
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    sync_options = dict(getattr(config, "sync_options") or {})
    provider = str(getattr(config, "provider")).lower()
    tz = str(sync_options.get("timezone") or "UTC")
    job_config = {
        "provider": provider,
        "sync_config_id": str(sync_config_id),
    }
    # Manual-only configs (no explicit schedule_cron) must not auto-sync
    # (CHAOS-2297). Keep the job row (manual triggers anchor JobRuns to it)
    # but park it PAUSED; the stored cron is only a non-null placeholder.
    status = (
        JobStatus.ACTIVE.value
        if getattr(config, "is_active") and _config_has_explicit_schedule(config)
        else JobStatus.PAUSED.value
    )
    if job is None:
        session.add(
            ScheduledJob(
                name=f"sync-config-{sync_config_id}",
                job_type="sync",
                schedule_cron=_schedule_cron_for_config(config),
                org_id=org_id,
                provider=provider,
                job_config=job_config,
                sync_config_id=sync_config_id,
                tz=tz,
                status=status,
            )
        )
        return

    job.schedule_cron = _schedule_cron_for_config(config)
    job.provider = provider
    job.timezone = tz
    job.job_config = job_config
    job.status = status


def _planner_dataset_keys(provider: str, sync_targets: list[str]) -> list[str]:
    targets = {str(target) for target in sync_targets if target is not None}
    if provider.lower() in {"github", "gitlab"} and "git" in targets:
        targets.add(DatasetKey.BLAME.value)
    return [
        spec.dataset_key
        for spec in supported_datasets(provider)
        if targets.intersection(spec.legacy_targets)
    ]


def _planner_source_rows(
    payload: SyncConfigBatchCreate,
    parent_options: dict[str, Any],
    gitlab_projects: dict[str, tuple[int, str]],
    org_id: str,
    integration_id: uuid.UUID,
    config_id: uuid.UUID,
) -> list[IntegrationSource]:
    provider = payload.provider.lower()
    rows: list[IntegrationSource] = []
    owner = str(
        payload.sync_options.get("owner")
        or payload.sync_options.get("group")
        or payload.name
    )
    for repo_name in payload.repos:
        if provider == "gitlab":
            project_id, full_name = gitlab_projects[repo_name]
            source_type = "project"
            external_id = str(project_id)
            source_name = full_name.rsplit("/", 1)[-1] if full_name else str(project_id)
            metadata = {
                "path_with_namespace": full_name,
                "planner_managed_sync_config_id": str(config_id),
            }
        else:
            source_type = "repository" if provider == "github" else "source"
            if provider == "github" and "/" in repo_name:
                source_owner, source_name = repo_name.split("/", 1)
                full_name = repo_name
            else:
                source_owner = owner
                source_name = repo_name
                full_name = (
                    f"{owner}/{repo_name}" if provider == "github" else repo_name
                )
            external_id = full_name
            metadata = {
                "planner_managed_sync_config_id": str(config_id),
            }
            if provider == "github":
                metadata["owner"] = source_owner
        rows.append(
            IntegrationSource(
                org_id=org_id,
                integration_id=integration_id,
                provider=payload.provider,
                source_type=source_type,
                external_id=external_id,
                name=source_name,
                full_name=full_name,
                metadata_=metadata,
                is_enabled=True,
            )
        )
    return rows


def _repo_selection_owner(config: SyncConfiguration) -> str:
    sync_options = dict(getattr(config, "sync_options") or {})
    return str(sync_options.get("owner") or sync_options.get("group") or "")


def _repo_selection_from_sources(
    config: SyncConfiguration, sources: Sequence[IntegrationSource]
) -> SyncConfigRepositorySelection:
    enabled_sources = [source for source in sources if bool(source.is_enabled)]
    return SyncConfigRepositorySelection(
        owner=_repo_selection_owner(config),
        repos=[str(source.full_name) for source in enabled_sources],
        sync_all_repos=bool((getattr(config, "sync_options") or {}).get("all_repos")),
    )


def _is_planner_source_for_config(
    source: IntegrationSource, config: SyncConfiguration
) -> bool:
    metadata = dict(getattr(source, "metadata_", None) or {})
    return metadata.get("planner_managed_sync_config_id") == str(getattr(config, "id"))


async def _planner_sources_for_config(
    session: AsyncSession, org_id: str, config: SyncConfiguration
) -> list[IntegrationSource]:
    integration_id = getattr(config, "integration_id", None)
    if integration_id is None:
        return []
    result = await session.execute(
        select(IntegrationSource).where(
            IntegrationSource.org_id == org_id,
            IntegrationSource.integration_id == integration_id,
            IntegrationSource.provider == getattr(config, "provider"),
        )
    )
    return [
        source
        for source in result.scalars().all()
        if _is_planner_source_for_config(source, config)
    ]


async def _legacy_child_repositories_for_config(
    session: AsyncSession, org_id: str, config: SyncConfiguration
) -> list[str]:
    result = await session.execute(
        select(SyncConfiguration).where(
            SyncConfiguration.org_id == org_id,
            SyncConfiguration.parent_id == getattr(config, "id"),
        )
    )
    repos: list[str] = []
    for child in result.scalars().all():
        sync_options = dict(getattr(child, "sync_options") or {})
        repo = sync_options.get("repo") or sync_options.get("project_id")
        if repo is None:
            continue
        owner = sync_options.get("owner") or sync_options.get("group")
        if owner and "/" not in str(repo):
            repos.append(f"{owner}/{repo}")
        else:
            repos.append(str(repo))
    return repos


async def _repository_selection_for_config(
    session: AsyncSession, org_id: str, config: SyncConfiguration
) -> SyncConfigRepositorySelection:
    sources = await _planner_sources_for_config(session, org_id, config)
    if sources:
        return _repo_selection_from_sources(config, sources)
    repos = await _legacy_child_repositories_for_config(session, org_id, config)
    sync_options = dict(getattr(config, "sync_options") or {})
    if not repos and sync_options.get("repo") is not None:
        repo = str(sync_options["repo"])
        owner = _repo_selection_owner(config)
        repos = [f"{owner}/{repo}" if owner and "/" not in repo else repo]
    return SyncConfigRepositorySelection(
        owner=_repo_selection_owner(config),
        repos=repos,
        sync_all_repos=bool(sync_options.get("all_repos")),
    )


async def _replace_planner_repository_selection(
    session: AsyncSession,
    org_id: str,
    config: SyncConfiguration,
    payload: SyncConfigRepositorySelectionUpdate,
) -> SyncConfigRepositorySelection:
    integration_id = getattr(config, "integration_id", None)
    if integration_id is None:
        raise HTTPException(
            status_code=409,
            detail="Repository selection edits require a planner-managed sync config",
        )

    existing_sources = await _planner_sources_for_config(session, org_id, config)
    enabled_existing = [
        source for source in existing_sources if bool(source.is_enabled)
    ]
    await _acquire_repo_limit_create_lock(session, org_id)
    current_count = await _active_repo_usage_count_for_limit(session, org_id)
    requested_count = current_count - len(enabled_existing) + len(payload.repos)

    def _check_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), requested_count)

    allowed, reason = await session.run_sync(_check_limit)
    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=reason
            or f"Repo limit exceeded (selecting {len(payload.repos)} repos)",
        )

    sync_options = dict(getattr(config, "sync_options") or {})
    provider = str(getattr(config, "provider")).lower()
    if provider == "gitlab":
        sync_options["group"] = payload.owner
    else:
        sync_options["owner"] = payload.owner
    sync_options.pop("all_repos", None)
    mutable_config = cast(_MutableSyncConfiguration, config)
    mutable_config.sync_options = sync_options

    existing_credential_id = await _integration_credential_id_for_config(
        session, config, org_id
    )
    batch_payload = SyncConfigBatchCreate(
        name=str(getattr(config, "name")),
        provider=str(getattr(config, "provider")),
        credential_id=str(existing_credential_id)
        if existing_credential_id is not None
        else None,
        sync_targets=list(getattr(config, "sync_targets") or []),
        sync_options=sync_options,
        repos=payload.repos,
    )
    gitlab_projects: dict[str, tuple[int, str]] = {}
    if provider == "gitlab" and payload.repos:
        gitlab_projects, effective_gitlab_url = await _resolve_gitlab_batch_projects(
            session, org_id, batch_payload
        )
        if effective_gitlab_url != DEFAULT_GITLAB_URL:
            sync_options["gitlab_url"] = effective_gitlab_url
            mutable_config.sync_options = sync_options

    desired_rows = _planner_source_rows(
        batch_payload,
        sync_options,
        gitlab_projects,
        org_id,
        integration_id,
        getattr(config, "id"),
    )
    existing_by_external_id = {
        str(source.external_id): source for source in existing_sources
    }
    desired_external_ids = {str(row.external_id) for row in desired_rows}

    for source in existing_sources:
        if str(source.external_id) not in desired_external_ids:
            source.is_enabled = False

    for desired in desired_rows:
        existing = existing_by_external_id.get(str(desired.external_id))
        if existing is None:
            session.add(desired)
            continue
        existing.source_type = desired.source_type
        existing.name = desired.name
        existing.full_name = desired.full_name
        existing.metadata_ = desired.metadata_
        existing.is_enabled = True
        existing.last_seen_at = datetime.now(timezone.utc)

    await session.flush()
    refreshed_sources = await _planner_sources_for_config(session, org_id, config)
    return _repo_selection_from_sources(config, refreshed_sources)


async def _assert_single_planner_parent_for_integration(
    session: AsyncSession,
    org_id: str,
    integration_id: uuid.UUID,
) -> None:
    count_stmt = select(func.count(SyncConfiguration.id)).where(
        SyncConfiguration.org_id == org_id,
        SyncConfiguration.planner_managed.is_(True),
        SyncConfiguration.integration_id == integration_id,
        SyncConfiguration.parent_id.is_(None),
    )
    planner_parent_count = (await session.execute(count_stmt)).scalar_one()
    if planner_parent_count > 1:
        raise RuntimeError(
            "Planner-managed integration invariant violated: "
            f"integration {integration_id} is linked to {planner_parent_count} "
            "planner-managed parent sync configurations"
        )


def _non_git_source_rows(
    provider: str,
    sync_options: dict[str, Any],
    name: str,
    org_id: str,
    integration_id: uuid.UUID,
    config_id: uuid.UUID,
) -> list[IntegrationSource]:
    """Materialize the single planner source for a non-git provider config.

    Non-git providers (jira, linear, launchdarkly) carry no repo list and have
    no source-discovery path (``sync/discovery.py`` only maps github/gitlab).
    The planner builds units as enabled sources x enabled datasets, so a config
    with zero sources plans zero units. Resolve the source ``external_id`` from
    ``sync_options`` (the run-time key the dataset adapters consume, e.g. the
    Jira project key) so every non-git config materializes exactly one
    planner-tagged source.
    """
    explicit_external_id = (
        sync_options.get("project_id")
        or sync_options.get("project_key")
        or sync_options.get("team_id")
        or sync_options.get("repo")
    )
    is_linear_org_wide = provider.lower() == "linear" and not explicit_external_id
    external_id = str(
        explicit_external_id or (provider.lower() if is_linear_org_wide else name)
    )
    source_type = "project" if provider.lower() in {"jira", "linear"} else "source"
    full_name = str(sync_options.get("full_name") or external_id)
    metadata: dict[str, Any] = {"planner_managed_sync_config_id": str(config_id)}
    if is_linear_org_wide:
        metadata["org_wide_placeholder"] = True
    return [
        IntegrationSource(
            org_id=org_id,
            integration_id=integration_id,
            provider=provider,
            source_type=source_type,
            external_id=external_id,
            name=name,
            full_name=full_name,
            metadata_=metadata,
            is_enabled=True,
        )
    ]


async def _create_planner_managed_config(
    session: AsyncSession,
    org_id: str,
    *,
    name: str,
    provider: str,
    credential_id: str | None,
    sync_targets: list[str],
    parent_options: dict[str, Any],
    schedule_cron: str | None,
    timezone: str | None,
    build_source_rows: Callable[[uuid.UUID, uuid.UUID], list[IntegrationSource]],
) -> tuple[SyncConfiguration, Integration]:
    """Single integration-native write path for ALL providers.

    Creates the ``Integration``, its planner-managed ``SyncConfiguration``
    (``integration_id`` set, ``planner_managed=True``), the enabled
    ``IntegrationDataset`` rows, the caller-built ``IntegrationSource`` rows, and
    the scheduled-job anchor. There is no bare ``SyncConfiguration`` insert path
    anywhere, so a config can never be created unlinked from its integration and
    the planner can always route it.

    The credential is stamped ONLY on ``Integration.credential_id`` (CHAOS-2762):
    ``SyncConfiguration`` carries no credential column of its own, so there is
    exactly one place a credential attaches to sync work.
    """
    credential_uuid = uuid.UUID(credential_id) if credential_id else None
    integration = Integration(
        org_id=org_id,
        provider=provider,
        credential_id=credential_uuid,
        name=name,
        config=parent_options,
        is_active=True,
        schedule_cron=schedule_cron,
        timezone=timezone,
    )
    session.add(integration)
    await session.flush()

    parent = SyncConfiguration(
        name=name,
        provider=provider,
        org_id=org_id,
        sync_targets=sync_targets,
        sync_options=parent_options,
        is_active=True,
        integration_id=integration.id,
        planner_managed=True,
    )
    session.add(parent)
    await session.flush()
    await _assert_single_planner_parent_for_integration(session, org_id, integration.id)

    source_rows = build_source_rows(integration.id, parent.id)
    dataset_rows = [
        IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key=dataset_key,
            is_enabled=True,
            options={"legacy_targets": list(sync_targets)},
        )
        for dataset_key in _planner_dataset_keys(provider, sync_targets)
    ]
    session.add_all([*source_rows, *dataset_rows])
    await _upsert_scheduled_job(session, parent, org_id)
    await session.flush()
    return parent, integration


@router.get("/sync-targets")
async def get_provider_sync_targets() -> dict[str, list[str]]:
    return PROVIDER_SYNC_TARGETS


@router.get(
    "/sync-configs",
    response_model=list[SyncConfigResponse],
    description=(
        "List sync configurations. "
        "Child sync configs (rows with a non-null ``parent_id`` or linked to a "
        "migrated integration via ``integration_id``/``source_id``) "
        "are **deprecated** and hidden by default when the "
        "``HIDE_MIGRATED_CHILD_CONFIGS`` feature flag is enabled. "
        "Pass ``?include_migrated=true`` to include them (support/rollback)."
    ),
)
async def list_sync_configs(
    active_only: bool = False,
    parent_only: bool = False,
    include_migrated: bool = Query(
        default=False,
        description=(
            "Include deprecated child/migrated sync configs in the response. "
            "Defaults to False when HIDE_MIGRATED_CHILD_CONFIGS is enabled. "
            "Set to true for support or rollback access."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[SyncConfigResponse]:
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=active_only)
    if parent_only:
        configs = [c for c in configs if c.parent_id is None]

    # HIDE_MIGRATED_CHILD_CONFIGS: when enabled, filter out deprecated child
    # configs from the default list response. A config is considered a
    # "migrated child" when any of the following are true:
    #   - parent_id is set (legacy child config), OR
    #   - source_id is set (linked to an integration-era source).
    # The parent SyncConfiguration gets integration_id set by the
    # migration and is the rollback anchor, so it is NOT hidden.
    # Callers may pass ?include_migrated=true to bypass this filter for
    # support or rollback access.
    _hide_migrated = os.getenv("HIDE_MIGRATED_CHILD_CONFIGS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if _hide_migrated and not include_migrated:
        configs = [
            c
            for c in configs
            if (
                getattr(c, "parent_id", None) is None
                and getattr(c, "source_id", None) is None
            )
        ]

    # Build children count map without lazy-loading relationships
    from sqlalchemy import func, select

    children_counts: dict[str, int] = {}
    parent_id_col = getattr(SyncConfiguration, "parent_id")
    sync_configuration_id_col = getattr(SyncConfiguration, "id")
    parent_ids = [
        getattr(config, "id")
        for config in configs
        if getattr(config, "parent_id") is None
    ]
    if parent_ids:
        stmt = (
            select(parent_id_col, func.count(sync_configuration_id_col))
            .where(parent_id_col.in_(parent_ids))
            .group_by(parent_id_col)
        )
        rows = (await session.execute(stmt)).all()
        children_counts = {str(pid): cnt for pid, cnt in rows}

    credential_ids_by_integration = await _integration_credential_ids_for_configs(
        session, configs, org_id
    )

    results = []
    for c in configs:
        cc = children_counts.get(str(getattr(c, "id")))
        integration_id = getattr(c, "integration_id", None)
        credential_id = (
            credential_ids_by_integration.get(str(integration_id))
            if integration_id is not None
            else None
        )
        results.append(
            _sync_config_to_response(c, children_count=cc, credential_id=credential_id)
        )
    return results


def _gitlab_group_from_options(sync_options: dict[str, Any]) -> str:
    """Extract the GitLab group/namespace from batch sync options.

    The web form submits the namespace under ``owner`` (shared field with
    GitHub); API callers may use ``group`` directly.
    """
    group = sync_options.get("group") or sync_options.get("owner") or ""
    # Strip CR/LF so the user-provided group can't forge log lines when it is
    # interpolated into warnings (CodeQL: log injection).
    return str(group).replace("\r", "").replace("\n", "").strip()


async def _list_gitlab_group_projects(
    *, gitlab_url: str, token: str, group: str
) -> list[Any]:
    from dev_health_ops.providers.gitlab.code_client import GitLabCodeClient

    async with GitLabCodeClient(private_token=token, base_url=gitlab_url) as client:
        return await client.list_projects(group_name=group)


async def _resolve_gitlab_batch_projects(
    session: AsyncSession,
    org_id: str,
    payload: SyncConfigBatchCreate,
) -> tuple[dict[str, tuple[int, str]], str]:
    """Resolve batch ``repos`` entries to GitLab ``(project_id, child_name)``.

    Returns the resolved mapping plus the effective ``gitlab_url``, resolved
    with the same precedence the credential resolver uses:
    ``sync_options.gitlab_url`` → decrypted credential ``url`` → credential
    ``config.url`` → ``https://gitlab.com``. The caller persists that URL into
    parent/child options so children sync against the same instance used for
    name resolution.

    Name entries are resolved by listing the group's projects through the
    stored credential; unknown or ambiguous names raise a 400. When a listing
    is available, numeric entries that match a listed project *name* are
    treated as names (a project literally named ``007`` must not be coerced
    to project id 7); numeric entries matching no listed name keep project-id
    semantics.
    """
    entries = list(dict.fromkeys(payload.repos))
    named = [r for r in entries if not r.strip().isdigit()]
    numeric = [r for r in entries if r.strip().isdigit()]
    group = _gitlab_group_from_options(payload.sync_options)

    resolved: dict[str, tuple[int, str]] = {}
    by_id: dict[int, str] = {}
    by_key: dict[str, list[tuple[int, str]]] = {}

    if named:
        if not payload.credential_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    "GitLab batch create requires credential_id to resolve "
                    "project names to project ids"
                ),
            )
        if not group:
            raise HTTPException(
                status_code=400,
                detail=(
                    "GitLab batch create requires a group (sync_options.group "
                    "or sync_options.owner) to resolve project names"
                ),
            )

    # Resolve the effective GitLab instance URL up front (even for all-numeric
    # batches) so self-hosted URLs stored on the credential are not lost.
    option_url = str(payload.sync_options.get("gitlab_url") or "").strip()
    gitlab_url = option_url or DEFAULT_GITLAB_URL
    token: str | None = None
    if payload.credential_id:
        creds_svc = IntegrationCredentialsService(session, org_id)
        decrypted, credential = await creds_svc.get_decrypted_credentials_by_id(
            payload.credential_id
        )
        if credential is None or decrypted is None:
            raise HTTPException(status_code=400, detail="Credential not found")
        token = decrypted.get("token")
        cred_config: dict[str, Any] = getattr(credential, "config") or {}
        gitlab_url = str(
            option_url
            or decrypted.get("url")
            or cred_config.get("url")
            or DEFAULT_GITLAB_URL
        )

    if named and not token:
        raise HTTPException(status_code=400, detail="GitLab credential missing token")

    # List the group's projects when name entries require it, and also
    # opportunistically when numeric entries could shadow project names.
    should_list = bool(named) or (bool(numeric) and bool(token) and bool(group))
    if should_list:
        try:
            projects = await _list_gitlab_group_projects(
                gitlab_url=gitlab_url, token=str(token), group=group
            )
        except Exception as exc:
            if named:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to list GitLab projects for group '{group}': {exc}",
                )
            # All-numeric batch: keep historical id semantics when the listing
            # is unavailable, but log so shadowed names are diagnosable.
            logger.warning(
                "GitLab batch create: failed to list projects for group %r to "
                "cross-check numeric entries; treating them as project ids: %s",
                group,
                exc,
            )
            projects = []

        for project in projects:
            entry = (int(project.id), project.full_name or project.name)
            by_id[entry[0]] = entry[1]
            for key in {project.name, project.full_name}:
                if key:
                    by_key.setdefault(key, []).append(entry)

        # Numeric entries matching a listed project name are names, not ids.
        named_like = named + [r for r in numeric if r in by_key]

        missing = [r for r in named if r not in by_key]
        ambiguous = [r for r in named_like if len(by_key.get(r, [])) > 1]
        if missing or ambiguous:
            parts = []
            if missing:
                parts.append(f"not found in group '{group}': {', '.join(missing)}")
            if ambiguous:
                parts.append(
                    "ambiguous (multiple projects share this name, use the "
                    f"full path): {', '.join(ambiguous)}"
                )
            raise HTTPException(
                status_code=400,
                detail=f"Could not resolve GitLab projects — {'; '.join(parts)}",
            )

        for repo in named_like:
            resolved[repo] = by_key[repo][0]

    for repo in entries:
        if repo in resolved:
            continue
        project_id = int(repo.strip())
        child_name = by_id.get(
            project_id, f"{group}/{project_id}" if group else str(project_id)
        )
        resolved[repo] = (project_id, child_name)

    return resolved, gitlab_url


@router.post(
    "/sync-configs/batch",
    response_model=SyncConfigBatchResponse,
    status_code=201,
    description=(
        "Create a parent sync config backed by the integration/source/dataset "
        "model. **Deprecated (CHAOS-2520):** the legacy child-per-repo sync "
        "configs are removed in favour of that model (CHAOS-2507). The "
        "integration planner is now the only routing path, so this endpoint "
        "always creates the parent config plus its integration/source/dataset "
        "rows and returns zero children."
    ),
)
async def batch_create_sync_configs(
    payload: SyncConfigBatchCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigBatchResponse:
    """Create a parent sync config backed by the integration/source/dataset model.

    Per-source options are provider-shaped:

    - **github**: each planner source row carries ``repo`` (plus ``owner``
      inherited from the parent options); the source name is ``{owner}/{repo}``.
    - **gitlab**: each planner source row carries an integer ``project_id`` plus
      ``group`` (and ``gitlab_url`` when present in the parent options), which the
      unitized GitLab dataset adapter requires to address a project.
      ``payload.repos`` entries may be either numeric GitLab
      project ids or project names, which are resolved to ids by listing the
      group's projects via the stored credential. Name entries therefore
      require ``credential_id`` and a ``group``/``owner`` in ``sync_options``;
      unknown or ambiguous names are rejected with a 400. When a credential
      and group are available the listing also cross-checks numeric entries:
      a numeric entry matching a listed project *name* is resolved as a name
      (so a project literally named ``007`` is not coerced to project id 7),
      and only entries matching no listed name keep id semantics. Without a
      credential, an all-numeric ``repos`` list is the escape hatch: entries
      are used as project ids as-is, with no listing call. The effective
      ``gitlab_url`` (``sync_options.gitlab_url`` → credential ``url`` →
      ``https://gitlab.com``) is persisted into parent and child options when
      it is not the public default, so self-hosted sources sync against the
      same instance used for resolution. The source name is the project's
      ``path_with_namespace`` when known.

    .. deprecated:: CHAOS-2520
        Child sync configs are removed. The integration planner is the only
        routing path; new integrations use the integration/source/dataset model.
        This endpoint always creates the parent config only (zero children) plus
        the integration/source/dataset rows it routes through.
    """
    await _acquire_repo_limit_create_lock(session, org_id)
    current_count = await _active_repo_usage_count_for_limit(session, org_id)
    new_count = len(payload.repos)

    def _check_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), current_count + new_count)

    allowed, reason = await session.run_sync(_check_limit)
    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=reason or f"Repo limit exceeded (adding {new_count} repos)",
        )

    provider = payload.provider.lower()

    parent_options = dict(payload.sync_options)
    parent_options.pop("repo", None)  # parent has no single repo
    if payload.schedule_cron is not None:
        parent_options["schedule_cron"] = payload.schedule_cron
    if payload.timezone is not None:
        parent_options["timezone"] = payload.timezone
    if payload.initial_sync_depth is not None:
        parent_options["initial_sync_depth"] = payload.initial_sync_depth

    # Resolve GitLab repos (and the effective instance URL) before creating
    # the parent so a self-hosted gitlab_url derived from the credential is
    # persisted into both parent and child options — otherwise children with
    # a valid project_id would later sync against the gitlab.com default.
    gitlab_projects: dict[str, tuple[int, str]] = {}
    if provider == "gitlab" and payload.repos:
        gitlab_projects, effective_gitlab_url = await _resolve_gitlab_batch_projects(
            session, org_id, payload
        )
        if effective_gitlab_url != DEFAULT_GITLAB_URL:
            parent_options["gitlab_url"] = effective_gitlab_url

    parent, integration = await _create_planner_managed_config(
        session,
        org_id,
        name=payload.name,
        provider=payload.provider,
        credential_id=payload.credential_id,
        sync_targets=payload.sync_targets,
        parent_options=parent_options,
        schedule_cron=payload.schedule_cron,
        timezone=payload.timezone,
        build_source_rows=lambda integration_id, config_id: _planner_source_rows(
            payload,
            parent_options,
            gitlab_projects,
            org_id,
            integration_id,
            config_id,
        ),
    )

    return SyncConfigBatchResponse(
        parent=_sync_config_to_response(
            parent, children_count=0, credential_id=integration.credential_id
        ),
        children=[],
        total_created=0,
    )


@router.post("/sync-configs", response_model=SyncConfigResponse, status_code=201)
async def create_sync_config(
    payload: SyncConfigCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:

    # Fix 1 (HIGH): Enforce repo limit before creating a new sync config.
    await _acquire_repo_limit_create_lock(session, org_id)
    current_count = await _active_repo_usage_count_for_limit(session, org_id)

    def _check_repo_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), current_count + 1)

    allowed, reason = await session.run_sync(_check_repo_limit)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason or "Repo limit exceeded")

    # Fix 5 (LOW): Validate initial_sync_depth against tier limits.
    sync_options = _sync_options_with_top_level_fields(
        payload.sync_options,
        schedule_cron=payload.schedule_cron,
        timezone=payload.timezone,
        initial_sync_depth=payload.initial_sync_depth,
    )

    initial_sync_depth = sync_options.get("initial_sync_depth")
    if initial_sync_depth is not None:

        def _check_backfill_depth(sync_session) -> tuple[bool, str | None]:
            tier_svc = TierLimitService(sync_session)
            return tier_svc.check_backfill_limit(
                uuid.UUID(org_id), int(initial_sync_depth)
            )

        depth_allowed, depth_reason = await session.run_sync(_check_backfill_depth)
        if not depth_allowed:
            raise HTTPException(
                status_code=403,
                detail=depth_reason or "initial_sync_depth exceeds tier limit",
            )

    # Fix 3 (MEDIUM) & Fix 4 (MEDIUM): Validate schedule_cron interval and gate
    # scheduled jobs behind the "scheduled_jobs" feature (Team+ only).
    schedule_cron = sync_options.get("schedule_cron")
    if schedule_cron:
        # Fix 4: Gate scheduled_jobs feature — Community tier cannot set schedules.
        async def _check_scheduled_jobs_feature(
            payload: SyncConfigCreate = payload,
            session: AsyncSession = session,
            org_id: str = org_id,
        ) -> None:
            from dev_health_ops.licensing.gating import _check_org_feature_async

            feature = "scheduled_jobs"
            if not await _check_org_feature_async(
                feature, {"session": session, "org_id": org_id}
            ):
                from dev_health_ops.licensing import has_feature

                if not has_feature(feature, log_denial=False):
                    raise HTTPException(
                        status_code=403,
                        detail="scheduled_jobs feature requires Team tier or higher",
                    )

        await _check_scheduled_jobs_feature()

        # Fix 3: Validate the cron interval against the tier's min_sync_interval_hours.
        try:
            itr = Croniter(schedule_cron)
            next1 = itr.get_next(float)
            next2 = itr.get_next(float)
            interval_hours = (next2 - next1) / 3600.0
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid cron expression: {exc}"
            )

        try:
            validate_timezone_name(sync_options.get("timezone"))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        def _get_min_interval(sync_session) -> float | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")
            return float(val) if val is not None else None

        min_interval = await session.run_sync(_get_min_interval)
        if min_interval is not None and interval_hours < min_interval:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Sync interval {interval_hours:.2f}h is below the minimum "
                    f"{min_interval}h allowed for your tier"
                ),
            )

    # github/gitlab specify repos either explicitly (via POST /sync-configs/batch)
    # or token-wide via all_repos. A plain create with neither would materialize
    # zero sources and plan zero units while still returning 202 (a silent no-op),
    # so reject it and steer the caller to the right path.
    if payload.provider.lower() in {"github", "gitlab"} and not bool(
        sync_options.get("all_repos")
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "github/gitlab sync configs require repository selection via "
                "POST /sync-configs/batch, or sync_options.all_repos=true"
            ),
        )

    def _build_sources(
        integration_id: uuid.UUID, config_id: uuid.UUID
    ) -> list[IntegrationSource]:
        if payload.provider.lower() in {"github", "gitlab"}:
            return []
        return _non_git_source_rows(
            payload.provider,
            sync_options,
            payload.name,
            org_id,
            integration_id,
            config_id,
        )

    config, integration = await _create_planner_managed_config(
        session,
        org_id,
        name=payload.name,
        provider=payload.provider,
        credential_id=payload.credential_id,
        sync_targets=payload.sync_targets,
        parent_options=sync_options,
        schedule_cron=payload.schedule_cron,
        timezone=payload.timezone,
        build_source_rows=_build_sources,
    )
    return _sync_config_to_response(config, credential_id=integration.credential_id)


@router.get("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def get_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    credential_id = await _integration_credential_id_for_config(session, config, org_id)
    return _sync_config_to_response(config, credential_id=credential_id)


@router.get(
    "/sync-configs/{config_id}/repositories",
    response_model=SyncConfigRepositorySelection,
)
async def get_sync_config_repositories(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigRepositorySelection:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    return await _repository_selection_for_config(session, org_id, config)


@router.get(
    "/sync-configs/{config_id}/coverage",
    response_model=SyncCoverageSummaryResponse,
)
async def get_sync_config_coverage(
    config_id: str,
    history_lookback_days: int = Query(
        default=HISTORY_LOOKBACK_DAYS,
        ge=1,
        le=3650,
    ),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncCoverageSummaryResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    payload = await build_sync_coverage_summary(
        session,
        org_id,
        config,
        lookback_days=history_lookback_days,
    )
    return SyncCoverageSummaryResponse.model_validate(payload)


@router.put(
    "/sync-configs/{config_id}/repositories",
    response_model=SyncConfigRepositorySelection,
)
async def replace_sync_config_repositories(
    config_id: str,
    payload: SyncConfigRepositorySelectionUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigRepositorySelection:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    if str(getattr(config, "provider", "")).lower() not in {"github", "gitlab"}:
        raise HTTPException(
            status_code=400,
            detail="Repository selection is only supported for GitHub and GitLab configs",
        )
    return await _replace_planner_repository_selection(session, org_id, config, payload)


@router.patch("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def update_sync_config(
    config_id: str,
    payload: SyncConfigUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    # Fix 3 (MEDIUM) & Fix 4 (MEDIUM): Validate schedule_cron when updating sync_options.
    # PATCH semantics for schedule fields: an explicitly provided null clears the
    # stored value, while an omitted field leaves it untouched. Top-level fields
    # own these keys and override any (possibly stale) copies nested inside
    # payload.sync_options, so a stale client payload can never resurrect an old
    # schedule.
    provided_fields = payload.model_fields_set
    top_level_schedule_fields = {
        "schedule_cron": payload.schedule_cron,
        "timezone": payload.timezone,
        "initial_sync_depth": payload.initial_sync_depth,
    }
    sync_options = dict(payload.sync_options or {})
    cleared_keys: set[str] = set()
    for key, value in top_level_schedule_fields.items():
        if key not in provided_fields:
            continue
        if value is None:
            cleared_keys.add(key)
            sync_options.pop(key, None)
        else:
            sync_options[key] = value
    sync_options_provided = payload.sync_options is not None or bool(
        provided_fields & top_level_schedule_fields.keys()
    )

    schedule_cron = sync_options.get("schedule_cron")
    if schedule_cron:
        # Fix 4: Gate scheduled_jobs feature — Community tier cannot set schedules.
        from dev_health_ops.licensing.gating import _check_org_feature_async

        feature = "scheduled_jobs"
        if not await _check_org_feature_async(
            feature, {"session": session, "org_id": org_id}
        ):
            from dev_health_ops.licensing import has_feature

            if not has_feature(feature, log_denial=False):
                raise HTTPException(
                    status_code=403,
                    detail="scheduled_jobs feature requires Team tier or higher",
                )

        # Fix 3: Validate the cron interval against the tier's min_sync_interval_hours.
        try:
            itr = Croniter(schedule_cron)
            next1 = itr.get_next(float)
            next2 = itr.get_next(float)
            interval_hours = (next2 - next1) / 3600.0
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid cron expression: {exc}"
            )

        try:
            validate_timezone_name(sync_options.get("timezone"))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        def _get_min_interval(sync_session) -> float | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")
            return float(val) if val is not None else None

        min_interval = await session.run_sync(_get_min_interval)
        if min_interval is not None and interval_hours < min_interval:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Sync interval {interval_hours:.2f}h is below the minimum "
                    f"{min_interval}h allowed for your tier"
                ),
            )

    mutable_config = cast(_MutableSyncConfiguration, config)
    if payload.sync_targets is not None:
        mutable_config.sync_targets = payload.sync_targets
    if sync_options_provided:
        merged_options = {
            **dict(getattr(config, "sync_options") or {}),
            **sync_options,
        }
        for key in cleared_keys:
            merged_options.pop(key, None)
        mutable_config.sync_options = merged_options
    if payload.is_active is not None:
        mutable_config.is_active = payload.is_active
    await session.flush()
    updated = config

    await _upsert_scheduled_job(session, updated, org_id)

    # Cascade shared settings to children when updating a parent config
    if getattr(updated, "parent_id") is None:
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.parent_id == getattr(updated, "id")
        )
        result = await session.execute(stmt)
        children = result.scalars().all()
        for child in children:
            mutable_child = cast(_MutableSyncConfiguration, child)
            if payload.sync_targets is not None:
                mutable_child.sync_targets = payload.sync_targets
            if payload.is_active is not None:
                mutable_child.is_active = payload.is_active
            # Propagate schedule/timezone/depth from sync_options if provided
            if sync_options_provided:
                child_sync_options = dict(getattr(child, "sync_options") or {})
                child_changed = False
                for key in ("schedule_cron", "timezone", "initial_sync_depth"):
                    if key in cleared_keys:
                        if key in child_sync_options:
                            del child_sync_options[key]
                            child_changed = True
                    elif key in sync_options:
                        child_sync_options[key] = sync_options[key]
                        child_changed = True
                if child_changed:
                    mutable_child.sync_options = child_sync_options
        if children:
            for child in children:
                await _upsert_scheduled_job(session, child, org_id)
            await session.flush()

    credential_id = await _integration_credential_id_for_config(
        session, updated, org_id
    )
    return _sync_config_to_response(updated, credential_id=credential_id)


@router.delete("/sync-configs/{config_id}", status_code=204)
async def delete_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> None:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    await svc.delete(
        str(getattr(config, "name")), provider=str(getattr(config, "provider"))
    )


@router.post("/sync-configs/{config_id}/trigger", status_code=202)
async def trigger_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    if str(getattr(config, "org_id", "")) != org_id:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    if not bool(getattr(config, "is_active", False)):
        raise HTTPException(
            status_code=409,
            detail="Sync configuration is paused and cannot be triggered",
        )

    # Fix 6 (LOW-MEDIUM): Check work items count against tier limit before triggering.
    if "work-items" in (config.sync_targets or []):

        def _get_max_work_items(sync_session) -> int | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "max_work_items")
            return int(val) if val is not None else None

        max_work_items = await session.run_sync(_get_max_work_items)
        if max_work_items is not None:
            try:
                import os

                from dev_health_ops.api.queries.client import (
                    get_global_client,
                    query_dicts,
                )

                ch_uri = os.getenv("CLICKHOUSE_URI", "")
                if ch_uri:
                    client = await get_global_client(ch_uri)
                    rows = await query_dicts(
                        client,
                        "SELECT count() AS cnt FROM work_items WHERE org_id = %(org_id)s",
                        {"org_id": org_id},
                    )
                    current_count = int((rows[0].get("cnt") or 0) if rows else 0)
                    if current_count >= max_work_items:
                        raise HTTPException(
                            status_code=403,
                            detail=(
                                f"Work items limit exceeded: {current_count}/{max_work_items}. "
                                "Upgrade your tier to sync more work items."
                            ),
                        )
            except HTTPException:
                raise
            except Exception:
                # ClickHouse unavailable — allow the sync to proceed rather than block it.
                pass

    await session.run_sync(
        lambda sync_session: _preflight_planner_credential(sync_session, config)
    )
    try:
        trigger = await session.run_sync(
            lambda sync_session: create_sync_execution_trigger(
                sync_session,
                config,
                org_id,
                triggered_by="manual",
                mode="incremental",
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_text(exc))
    if trigger is None:
        raise HTTPException(
            status_code=400,
            detail="Sync configuration has no linked integration",
        )
    await session.commit()
    try:
        dispatch_result = getattr(dispatch_sync_run, "apply_async")(
            args=(trigger.sync_run_id,), queue="sync"
        )
    except Exception as exc:
        # Bind to a plain local before closing over it in the lambdas below:
        # `except ... as exc` implicitly deletes `exc` at the end of THIS
        # block, which a closure captures by reference, not by value (ruff
        # F821 catches this). The raw exception is passed straight through
        # to the sink (_mark_job_run_failed -> sanitize_error_text), not
        # pre-formatted into a string here -- a Celery/broker enqueue
        # failure can embed the broker/result-backend URL, credentials
        # included (CHAOS-2766 codex review finding).
        dispatch_exc = exc
        await session.run_sync(
            lambda s: mark_sync_run_failed(
                s, trigger.sync_run_id, "dispatch enqueue failed"
            )
        )
        await session.run_sync(
            lambda s: _mark_job_run_failed(s, trigger.job_run_id, dispatch_exc)
        )
        await session.commit()
        raise HTTPException(
            status_code=503,
            detail=f"Task queue unavailable: {sanitize_error_text(dispatch_exc)}",
        )
    dispatch_task_id = str(getattr(dispatch_result, "id", "") or "")
    await session.run_sync(
        lambda s: _merge_job_run_result(
            s,
            trigger.job_run_id,
            {"dispatch_task_id": dispatch_task_id} if dispatch_task_id else None,
        )
    )
    await session.commit()
    return {
        "status": "triggered",
        "config_id": str(config.id),
        "sync_run_id": trigger.sync_run_id,
        "run_id": trigger.job_run_id,
        "total_units": trigger.total_units,
    }


@router.post("/sync-configs/{config_id}/backfill", status_code=202)
async def trigger_sync_config_backfill(
    config_id: str,
    payload: BackfillRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    if not bool(getattr(config, "is_active", False)):
        raise HTTPException(
            status_code=409,
            detail="Sync configuration is paused and cannot be backfilled",
        )

    requested_days = (payload.before - payload.since).days

    def _check_backfill_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_backfill_limit(uuid.UUID(org_id), requested_days)

    allowed, reason = await session.run_sync(_check_backfill_limit)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason or "Backfill not allowed")

    from dev_health_ops.models.backfill import BackfillJob as BackfillJobModel

    await session.run_sync(
        lambda sync_session: _preflight_planner_credential(sync_session, config)
    )
    try:
        # Planner ValueErrors (e.g. SyncPlanUnitCapExceededError when the
        # window expands past the org's run unit cap) are client-fixable:
        # surface them as 400 like the /trigger endpoint does, instead of
        # letting the generic handler below report 503.
        try:
            trigger = await session.run_sync(
                lambda sync_session: create_sync_execution_trigger(
                    sync_session,
                    config,
                    org_id,
                    triggered_by="backfill",
                    mode="backfill",
                    since=datetime.combine(
                        payload.since, datetime.min.time(), tzinfo=timezone.utc
                    ),
                    before=datetime.combine(
                        payload.before, datetime.max.time(), tzinfo=timezone.utc
                    ),
                    initial_job_result={"planner_managed": True},
                )
            )
        except ValueError as plan_exc:
            raise HTTPException(status_code=400, detail=sanitize_error_text(plan_exc))
        if trigger is None:
            raise HTTPException(
                status_code=400,
                detail="Sync configuration has no linked integration",
            )

        backfill_job = BackfillJobModel(
            org_id=org_id,
            sync_config_id=uuid.UUID(config_id),
            status="pending",
            since_date=payload.since,
            before_date=payload.before,
            total_chunks=0,
        )
        session.add(backfill_job)
        await session.flush()
        backfill_job_id = str(backfill_job.id)
        backfill_job.celery_task_id = f"sync_run:{trigger.sync_run_id}"

        await session.commit()

        try:
            result = getattr(dispatch_sync_run, "apply_async")(
                args=(trigger.sync_run_id,), queue="sync"
            )
        except Exception as e:
            # Bind to a plain local before closing over it below -- see the
            # matching comment in trigger_sync_config above (ruff F821: a
            # closure over a bare `except ... as e` name is unreliable,
            # since the name is implicitly deleted at block exit). Same
            # sink-sanitizes-not-caller rationale (CHAOS-2766 codex review
            # finding).
            dispatch_exc = e
            completed_at = datetime.now(timezone.utc)
            await session.run_sync(
                lambda sync_session: _mark_backfill_job_failed(
                    sync_session, backfill_job_id, dispatch_exc, completed_at
                )
            )
            await session.run_sync(
                lambda sync_session: _mark_job_run_failed(
                    sync_session, trigger.job_run_id, dispatch_exc
                )
            )
            await session.run_sync(
                lambda sync_session: mark_sync_run_failed(
                    sync_session, trigger.sync_run_id, "dispatch enqueue failed"
                )
            )
            await session.commit()
            raise HTTPException(
                status_code=503,
                detail=f"Task queue unavailable: {sanitize_error_text(dispatch_exc)}",
            )
        backfill_job.celery_task_id = f"{result.id}|sync_run:{trigger.sync_run_id}"
        await session.commit()
        return {
            "status": "accepted",
            "config_id": str(config.id),
            "task_id": result.id,
            "backfill_job_id": backfill_job_id,
            "sync_run_id": trigger.sync_run_id,
            "mode": "fanout",
            "since": payload.since.isoformat(),
            "before": payload.before.isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Task queue unavailable: {sanitize_error_text(e)}",
        )


@router.get("/sync-configs/{config_id}/jobs", response_model=list[JobRunResponse])
async def list_sync_config_jobs(
    config_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[JobRunResponse]:
    svc = SyncConfigurationService(session, org_id)
    existing = await svc.get_by_id(config_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    scheduled_job_id_col = getattr(ScheduledJob, "id")
    scheduled_job_org_id = getattr(ScheduledJob, "org_id")
    scheduled_job_sync_config_id = getattr(ScheduledJob, "sync_config_id")
    scheduled_job_type = getattr(ScheduledJob, "job_type")
    job_stmt = select(scheduled_job_id_col).where(
        scheduled_job_org_id == org_id,
        scheduled_job_sync_config_id == uuid.UUID(config_id),
        scheduled_job_type == "sync",
    )
    job_result = await session.execute(job_stmt)
    job_ids = list(job_result.scalars().all())

    if not job_ids:
        return []

    job_run_job_id = getattr(JobRun, "job_id")
    job_run_created_at = getattr(JobRun, "created_at")
    runs_stmt = (
        select(JobRun)
        .where(job_run_job_id.in_(job_ids))
        .order_by(job_run_created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    runs_result = await session.execute(runs_stmt)
    runs = list(runs_result.scalars().all())
    planner_sync_runs = await _planner_sync_runs_for_job_runs(session, runs, org_id)
    planner_sync_run_units = await _planner_sync_run_units_for_job_runs(
        session, runs, org_id
    )

    return [
        _job_run_response(
            run,
            planner_sync_runs.get(str(sync_run_id)) if sync_run_id else None,
            planner_sync_run_units.get(str(sync_run_id), ()) if sync_run_id else (),
        )
        for run in runs
        for sync_run_id in [_planner_job_run_sync_run_id(run)]
    ]


@router.get("/backfill-jobs")
async def list_backfill_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.schemas.backfill import BackfillJobListResponse
    from dev_health_ops.api.services.backfill import BackfillJobService

    svc = BackfillJobService(session, org_id)
    jobs, total = await svc.list_jobs(limit=limit, offset=offset)
    items = []
    for job in jobs:
        run_counts = await _backfill_job_run_counts(session, job)
        items.append(_backfill_job_response(job, run_counts))
    return BackfillJobListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/backfill-jobs/{job_id}")
async def get_backfill_job(
    job_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.services.backfill import BackfillJobService

    svc = BackfillJobService(session, org_id)
    job = await svc.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    run_counts = await _backfill_job_run_counts(session, job)
    return _backfill_job_response(job, run_counts)
