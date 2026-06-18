from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
from dev_health_ops.workers.queues import sync_queue_for_provider
from dev_health_ops.workers.sync_batch import _is_batch_eligible, dispatch_batch_sync
from dev_health_ops.workers.sync_runtime import run_sync_config
from dev_health_ops.workers.task_utils import (
    _as_datetime,
    _as_datetime_or_none,
    _as_dict,
    _as_str,
    _as_uuid,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dev_health_ops.models.settings import ScheduledJob, SyncConfiguration

logger = logging.getLogger(__name__)

# Staleness escape for the `is_running` flag: workers crash and queues get
# purged, so a running marker older than this TTL must not block re-dispatch
# forever. Generous on purpose -- normal syncs finish well within it.
STALE_RUNNING_TTL_SECONDS = 2 * 60 * 60  # 2 hours

DEFAULT_SYNC_CRON = "0 * * * *"


def _ensure_utc(value: datetime | None) -> datetime | None:
    """Coerce a DB timestamp to an aware UTC datetime (SQLite returns naive)."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _running_marker_is_stale(job: ScheduledJob, now: datetime) -> bool:
    """True when an `is_running` flag is too old to be trusted.

    `last_run_at` is stamped when the worker actually starts the run; fall
    back to `updated_at` if it was never set. A missing marker is treated as
    stale so a wedged flag can never block dispatch indefinitely.
    """
    marker = _ensure_utc(_as_datetime_or_none(job.last_run_at)) or _ensure_utc(
        _as_datetime_or_none(job.updated_at)
    )
    if marker is None:
        return True
    return (now - marker).total_seconds() > STALE_RUNNING_TTL_SECONDS


def _session_dialect_name(session: Session) -> str:
    bind = session.get_bind()
    return getattr(getattr(bind, "dialect", None), "name", "")


def _new_sync_scheduled_job(config: SyncConfiguration, cron_expr: str) -> ScheduledJob:
    from dev_health_ops.models.settings import JobStatus, ScheduledJob

    provider = _as_str(config.provider).lower()
    sync_config_id = _as_uuid(config.id)
    return ScheduledJob(
        name=f"sync-config-{sync_config_id}",
        job_type="sync",
        schedule_cron=cron_expr,
        org_id=_as_str(config.org_id),
        provider=provider,
        job_config={
            "provider": provider,
            "sync_config_id": str(sync_config_id),
        },
        sync_config_id=sync_config_id,
        tz=str(_as_dict(config.sync_options).get("timezone") or "UTC"),
        status=JobStatus.ACTIVE.value,
    )


def _scheduled_job_insert_values(
    config: SyncConfiguration, cron_expr: str
) -> dict[str, object]:
    job = _new_sync_scheduled_job(config, cron_expr)
    created_at = datetime.now(timezone.utc)
    return {
        "id": uuid.uuid4(),
        "org_id": job.org_id,
        "name": job.name,
        "job_type": job.job_type,
        "provider": job.provider,
        "schedule_cron": job.schedule_cron,
        "timezone": job.timezone,
        "job_config": job.job_config,
        "sync_config_id": job.sync_config_id,
        "status": job.status,
        "is_running": False,
        "run_count": 0,
        "failure_count": 0,
        "created_at": created_at,
        "updated_at": created_at,
    }


def _ensure_due_job_marker(
    session: Session,
    config: SyncConfiguration,
    cron_expr: str,
) -> ScheduledJob | None:
    """Create and lock the ScheduledJob marker for a due sync config."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from dev_health_ops.models.settings import ScheduledJob

    job_query = session.query(ScheduledJob).filter(
        ScheduledJob.sync_config_id == _as_uuid(config.id),
        ScheduledJob.org_id == _as_str(config.org_id),
        ScheduledJob.job_type == "sync",
    )

    if _session_dialect_name(session) == "postgresql":
        session.execute(
            pg_insert(ScheduledJob)
            .values(_scheduled_job_insert_values(config, cron_expr))
            .on_conflict_do_nothing(constraint="uq_scheduled_job_org_sync_config_type")
        )
        job = job_query.with_for_update(skip_locked=True).one_or_none()
        if job is None and job_query.with_entities(ScheduledJob.id).first() is not None:
            return None
        return job

    job = job_query.one_or_none()
    if job is None:
        job = _new_sync_scheduled_job(config, cron_expr)
        session.add(job)
        session.flush()
    return job


def _maybe_dispatch_config(
    session: Session, config: SyncConfiguration, now: datetime
) -> bool:
    """Dispatch a single sync config if due. Returns True when dispatched.

    Dispatch-time idempotency: `config.last_sync_at` only advances when a run
    COMPLETES, so due-ness alone re-dispatches the same config every beat tick
    while its first task sits in the queue (CHAOS-2270). To prevent that, the
    `ScheduledJob.next_run_at` column is stamped at dispatch time with the next
    cron occurrence and acts as a "do not re-dispatch before" marker. The
    marker is self-expiring: if the dispatched task is lost (worker crash,
    queue purge), the config becomes dispatchable again at the next cron
    occurrence -- at most one cron interval of delay, no manual cleanup.
    """
    from croniter import croniter

    from dev_health_ops.models.settings import JobStatus, ScheduledJob

    if not organization_exists_sync(session, config.org_id):
        return False

    # Manual-only configs (no explicit schedule_cron in sync_options) are
    # never auto-dispatched (CHAOS-2297). The config is the source of truth:
    # stored ScheduledJob rows carry DEFAULT_SYNC_CRON as a non-null
    # placeholder, and legacy rows may still be marked ACTIVE.
    config_cron = str(_as_dict(config.sync_options).get("schedule_cron") or "")
    if not config_cron:
        return False

    sync_config_id = _as_uuid(config.id)
    org_id = _as_str(config.org_id)
    job_query = session.query(ScheduledJob).filter(
        ScheduledJob.sync_config_id == sync_config_id,
        ScheduledJob.org_id == org_id,
        ScheduledJob.job_type == "sync",
    )
    if _session_dialect_name(session) == "postgresql":
        job = job_query.with_for_update(skip_locked=True).one_or_none()
        if job is None and job_query.with_entities(ScheduledJob.id).first() is not None:
            return False
    else:
        job = job_query.one_or_none()

    # PAUSED/DISABLED jobs (manual-only, org teardown) are never dispatched.
    if job is not None and int(job.status) != JobStatus.ACTIVE.value:
        return False

    if job is not None and bool(job.is_running):
        if not _running_marker_is_stale(job, now):
            return False
        logger.warning(
            "Sync job %s for config %s has is_running set for more than %ss; "
            "treating the marker as stale and re-evaluating dispatch",
            job.id,
            config.id,
            STALE_RUNNING_TTL_SECONDS,
        )

    # Idempotency gate: a previous tick already dispatched this config and
    # stamped when it should next be considered.
    if job is not None:
        next_allowed = _ensure_utc(_as_datetime_or_none(job.next_run_at))
        if next_allowed is not None and next_allowed > now:
            return False

    cron_expr = _as_str(job.schedule_cron) if job is not None else config_cron
    last_sync = (
        config.last_sync_at
        if isinstance(config.last_sync_at, datetime)
        else _as_datetime(config.created_at)
    )
    next_run = croniter(cron_expr, last_sync).get_next(datetime)

    if not next_run <= now:
        return False

    if job is None:
        job = _ensure_due_job_marker(session, config, cron_expr)
        if job is None:
            return False

        next_allowed = _ensure_utc(_as_datetime_or_none(job.next_run_at))
        if next_allowed is not None and next_allowed > now:
            return False

        if int(job.status) != JobStatus.ACTIVE.value:
            return False

        if bool(job.is_running):
            if not _running_marker_is_stale(job, now):
                return False
            logger.warning(
                "Sync job %s for config %s has is_running set for more than %ss; "
                "treating the marker as stale and re-evaluating dispatch",
                job.id,
                config.id,
                STALE_RUNNING_TTL_SECONDS,
            )

    marker = croniter(cron_expr, now).get_next(datetime)
    if isinstance(marker, datetime):
        job.next_run_at = marker
    session.flush()

    dispatch_kwargs = {
        "config_id": str(sync_config_id),
        "org_id": org_id,
        "triggered_by": "schedule",
    }
    sync_queue = sync_queue_for_provider(_as_str(config.provider))
    is_batch = _is_batch_eligible(config)

    session.commit()

    # Migrated-trigger routing (CHAOS-2516): when the feature flag is enabled
    # for this org AND the config was migrated, route through the fan-out
    # planner instead of the legacy per-config tasks.
    from dev_health_ops.sync.planner import plan_sync_run
    from dev_health_ops.sync.trigger_routing import (
        is_migrated_trigger_routing_enabled,
        mark_sync_run_failed,
        plan_request_for_config,
    )
    from dev_health_ops.workers.sync_units import dispatch_sync_run

    use_planner = is_migrated_trigger_routing_enabled(session, org_id)
    if use_planner:
        request = plan_request_for_config(
            config, triggered_by="schedule", mode="incremental"
        )
        if request is not None:
            logger.info(
                "Routing config %s through fan-out planner (migrated trigger routing)",
                config.id,
            )
            try:
                plan = plan_sync_run(session, request)
                session.commit()
            except Exception:
                # Stale migrated link or a transient planner/DB error must not
                # suppress this scheduled sync. next_run_at was already committed
                # above, so roll back the failed plan attempt and fall through to
                # the legacy per-config path -- this tick still dispatches exactly
                # once instead of going dark until the next cron occurrence.
                logger.exception(
                    "Fan-out planner failed for config %s; "
                    "falling back to legacy dispatch",
                    config.id,
                )
                session.rollback()
            else:
                try:
                    getattr(dispatch_sync_run, "apply_async")(
                        args=(plan.sync_run_id,), queue="sync"
                    )
                except Exception:
                    # The run + units are committed but the dispatch enqueue
                    # failed. There is no periodic sweeper for stranded PLANNED
                    # runs (the intra-dispatch reclaim only runs once
                    # dispatch_sync_run executes), so mark this run FAILED rather
                    # than leave it silently PLANNED with no queued dispatcher,
                    # then fall through to the legacy path so this tick still
                    # attempts a sync.
                    logger.exception(
                        "Fan-out dispatch enqueue failed for config %s "
                        "(sync_run=%s); marking run failed and falling back",
                        config.id,
                        plan.sync_run_id,
                    )
                    mark_sync_run_failed(
                        session, plan.sync_run_id, "dispatch enqueue failed"
                    )
                else:
                    return True

    # Per-provider routing (CHAOS-2299): "is Linear stuck?" must be one LLEN.
    if is_batch:
        getattr(dispatch_batch_sync, "apply_async")(
            kwargs=dispatch_kwargs,
            queue=sync_queue,
        )
    else:
        getattr(run_sync_config, "apply_async")(
            kwargs=dispatch_kwargs,
            queue=sync_queue,
        )

    return True


@celery_app.task(
    bind=True, name="dev_health_ops.workers.tasks.dispatch_scheduled_syncs"
)
def dispatch_scheduled_syncs(self) -> dict:
    """Check active sync configs and dispatch any that are due."""
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import SyncConfiguration

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    skipped = 0
    errors = 0

    try:
        with get_postgres_session_sync() as session:
            configs = (
                session.query(SyncConfiguration)
                .filter(SyncConfiguration.is_active.is_(True))
                .all()
            )

            for config in configs:
                try:
                    if _maybe_dispatch_config(session, config, now):
                        dispatched.append(str(config.id))
                    else:
                        skipped += 1
                except Exception:
                    # One bad config (e.g. a malformed cron expression) must
                    # not abort dispatch for the remaining configs.
                    logger.exception(
                        "Failed to evaluate sync config %s for dispatch; skipping",
                        config.id,
                    )
                    errors += 1

    except Exception:
        logger.exception("dispatch_scheduled_syncs failed")

    logger.info(
        "Scheduled sync dispatch: dispatched=%d skipped=%d errors=%d",
        len(dispatched),
        skipped,
        errors,
    )
    return {"dispatched": dispatched, "skipped": skipped, "errors": errors}
