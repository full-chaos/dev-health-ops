from __future__ import annotations

import logging
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

    job = (
        session.query(ScheduledJob)
        .filter(
            ScheduledJob.sync_config_id == config.id,
            ScheduledJob.org_id == config.org_id,
            ScheduledJob.job_type == "sync",
        )
        .one_or_none()
    )

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

    # Per-provider routing (CHAOS-2299): "is Linear stuck?" must be one LLEN.
    sync_queue = sync_queue_for_provider(_as_str(config.provider))
    if _is_batch_eligible(config):
        dispatch_batch_sync.apply_async(
            kwargs={
                "config_id": str(config.id),
                "org_id": config.org_id,
                "triggered_by": "schedule",
            },
            queue=sync_queue,
        )
    else:
        run_sync_config.apply_async(
            kwargs={
                "config_id": str(config.id),
                "org_id": config.org_id,
                "triggered_by": "schedule",
            },
            queue=sync_queue,
        )

    # Stamp the dispatch marker. Create the ScheduledJob row if missing
    # (mirrors run_sync_config, which would otherwise create it on pickup --
    # too late to dedupe dispatches while the task waits in the queue).
    if job is None:
        provider = _as_str(config.provider).lower()
        job = ScheduledJob(
            name=f"sync-config-{_as_uuid(config.id)}",
            job_type="sync",
            schedule_cron=cron_expr,
            org_id=_as_str(config.org_id),
            provider=provider,
            job_config={
                "provider": provider,
                "sync_config_id": str(config.id),
            },
            sync_config_id=_as_uuid(config.id),
            tz=str(_as_dict(config.sync_options).get("timezone") or "UTC"),
        )
        session.add(job)

    marker = croniter(cron_expr, now).get_next(datetime)
    if isinstance(marker, datetime):
        job.next_run_at = marker
    session.flush()

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
