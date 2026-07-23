from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dev_health_ops.sync.canonical_incident_gate import (
    CANONICAL_INCIDENT_FEATURE_KEY,
    CanonicalIncidentFeatureDisabledError,
    is_canonical_incident_feature_enabled_sync,
    require_canonical_incident_feature_sync,
    sync_targets_require_canonical_incident_feature,
)
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
from dev_health_ops.workers.task_utils import (
    _as_datetime,
    _as_datetime_or_none,
    _as_dict,
    _as_str,
    _as_uuid,
    cron_next_run,
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

# This is intentionally only a bounded, callable reconciliation slice.  The
# active scheduler paths do not invoke it while the Go scheduler remains
# shadow-only; an operator-owned consumer can opt in after that hand-off is
# reviewed.
DEFAULT_PENDING_OCCURRENCE_RECONCILE_LIMIT = 100


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


def _complete_pending_scheduled_sync_occurrence(
    session: Session,
    *,
    occurrence_id: str,
    org_id: str,
    sync_config_id: uuid.UUID,
    scheduled_job_id: uuid.UUID,
) -> bool:
    """Plan one Go-authored occurrence after config, job, occurrence locks.

    The initial scan is deliberately unlocked so reconciliation stays bounded
    and deterministic.  Before doing any work, this function reacquires the
    authoritative rows in the same order as the scheduler hand-off contract:
    configuration, scheduled job, then occurrence.  PostgreSQL consumers skip
    a row another consumer owns instead of waiting behind a potentially slow
    planner transaction.
    """
    from dev_health_ops.models.settings import (
        ScheduledJob,
        ScheduledSyncOccurrence,
        SyncConfiguration,
    )
    from dev_health_ops.sync.execution_trigger import (
        create_scheduled_sync_execution_trigger,
    )

    config_query = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.id == sync_config_id,
            SyncConfiguration.org_id == org_id,
        )
        .populate_existing()
    )
    job_query = session.query(ScheduledJob).filter(
        ScheduledJob.id == scheduled_job_id,
        ScheduledJob.sync_config_id == sync_config_id,
        ScheduledJob.org_id == org_id,
        ScheduledJob.job_type == "sync",
    )
    occurrence_query = session.query(ScheduledSyncOccurrence).filter(
        ScheduledSyncOccurrence.occurrence_id == occurrence_id,
        ScheduledSyncOccurrence.org_id == org_id,
        ScheduledSyncOccurrence.sync_config_id == sync_config_id,
        ScheduledSyncOccurrence.scheduled_job_id == scheduled_job_id,
    )

    if _session_dialect_name(session) == "postgresql":
        config = config_query.with_for_update(skip_locked=True).one_or_none()
        if config is None:
            return False
        job = job_query.with_for_update(skip_locked=True).one_or_none()
        if job is None:
            return False
        occurrence = occurrence_query.with_for_update(skip_locked=True).one_or_none()
    else:
        config = config_query.one_or_none()
        if config is None:
            return False
        job = job_query.one_or_none()
        if job is None:
            return False
        occurrence = occurrence_query.one_or_none()

    if occurrence is None:
        return False
    if occurrence.job_run_id is not None or occurrence.sync_run_id is not None:
        return False

    create_scheduled_sync_execution_trigger(
        session,
        config,
        job,
        org_id,
        scheduled_for=occurrence.scheduled_for,
        triggered_by="schedule",
        mode="incremental",
    )
    return True


def reconcile_pending_scheduled_sync_occurrences(
    session: Session,
    *,
    limit: int = DEFAULT_PENDING_OCCURRENCE_RECONCILE_LIMIT,
) -> dict[str, int]:
    """Complete a deterministic, bounded batch of unplanned Go occurrences.

    This helper is deliberately dormant: it is not registered with Celery Beat,
    called by :func:`dispatch_scheduled_syncs`, or exposed to the Go scheduler.
    It lets a separately authorized Python consumer materialize the existing
    stable occurrence identities without reconsidering ``next_run_at``.
    """
    from dev_health_ops.models.settings import ScheduledSyncOccurrence

    if limit <= 0:
        return {"scanned": 0, "completed": 0, "skipped": 0, "errors": 0}

    candidates = (
        session.query(
            ScheduledSyncOccurrence.occurrence_id,
            ScheduledSyncOccurrence.org_id,
            ScheduledSyncOccurrence.sync_config_id,
            ScheduledSyncOccurrence.scheduled_job_id,
        )
        .filter(
            ScheduledSyncOccurrence.job_run_id.is_(None),
            ScheduledSyncOccurrence.sync_run_id.is_(None),
        )
        .order_by(
            ScheduledSyncOccurrence.org_id,
            ScheduledSyncOccurrence.sync_config_id,
            ScheduledSyncOccurrence.scheduled_for,
            ScheduledSyncOccurrence.occurrence_id,
        )
        .limit(limit)
        .all()
    )
    completed = 0
    skipped = 0
    errors = 0

    for occurrence_id, org_id, sync_config_id, scheduled_job_id in candidates:
        try:
            # A malformed/ineligible occurrence must not roll back a prior
            # completion or prevent the rest of this bounded batch from being
            # reconciled.
            with session.begin_nested():
                if _complete_pending_scheduled_sync_occurrence(
                    session,
                    occurrence_id=str(occurrence_id),
                    org_id=str(org_id),
                    sync_config_id=_as_uuid(sync_config_id),
                    scheduled_job_id=_as_uuid(scheduled_job_id),
                ):
                    completed += 1
                else:
                    skipped += 1
        except Exception:
            logger.exception(
                "pending_scheduled_sync_occurrence_reconciliation_failed",
                extra={"occurrence_id": str(occurrence_id), "org_id": str(org_id)},
            )
            errors += 1

    return {
        "scanned": len(candidates),
        "completed": completed,
        "skipped": skipped,
        "errors": errors,
    }


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

    from dev_health_ops.models.settings import (
        JobStatus,
        ScheduledJob,
        SyncConfiguration,
    )

    config_query = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.id == _as_uuid(config.id),
            SyncConfiguration.org_id == _as_str(config.org_id),
        )
        .populate_existing()
    )
    if _session_dialect_name(session) == "postgresql":
        locked_config = config_query.with_for_update(skip_locked=True).one_or_none()
        if locked_config is None:
            return False
    else:
        locked_config = config_query.one_or_none()
        if locked_config is None:
            return False
    config = locked_config

    if not organization_exists_sync(session, config.org_id):
        return False
    sync_targets = [str(target) for target in (config.sync_targets or [])]
    if sync_targets_require_canonical_incident_feature(
        sync_targets
    ) and not is_canonical_incident_feature_enabled_sync(session, config.org_id):
        logger.warning(
            "sync_scheduler.feature_disabled",
            extra={
                "config_id": str(config.id),
                "org_id": str(config.org_id),
                "feature_key": CANONICAL_INCIDENT_FEATURE_KEY,
            },
        )
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
    tz_name = (
        _as_str(job.timezone)
        if job is not None
        else _as_str(_as_dict(config.sync_options).get("timezone"))
    ) or "UTC"
    last_sync = (
        config.last_sync_at
        if isinstance(config.last_sync_at, datetime)
        else _as_datetime(config.created_at)
    )
    next_run = cron_next_run(cron_expr, last_sync, tz_name)

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

    marker = cron_next_run(cron_expr, now, tz_name)
    if isinstance(marker, datetime):
        job.next_run_at = marker
    session.flush()

    from dev_health_ops.sync.execution_trigger import (
        ScheduledSyncOccurrenceIneligibleError,
        create_scheduled_sync_execution_trigger,
    )
    from dev_health_ops.workers.sync_units import (
        terminalize_feature_disabled_plan,
    )

    logger.info("Routing config %s through fan-out planner", config.id)
    try:
        trigger = create_scheduled_sync_execution_trigger(
            session,
            config,
            job,
            org_id,
            scheduled_for=next_run,
            triggered_by="schedule",
            mode="incremental",
        )
        if not trigger.dispatch_required:
            session.commit()
            logger.warning(
                "sync_scheduler.pagerduty_sync_disabled",
                extra={
                    "config_id": str(config.id),
                    "org_id": str(config.org_id),
                    "job_run_id": trigger.job_run_id,
                    "sync_run_id": trigger.sync_run_id,
                    "reason": trigger.terminal_reason,
                },
            )
            return False
    except ScheduledSyncOccurrenceIneligibleError as exc:
        logger.warning("Skipping sync config %s: %s", config.id, exc)
        session.rollback()
        return False
    except Exception:
        logger.exception("Fan-out planner failed for config %s", config.id)
        session.rollback()
        return False

    try:
        if sync_targets_require_canonical_incident_feature(sync_targets):
            require_canonical_incident_feature_sync(session, config.org_id)
    except CanonicalIncidentFeatureDisabledError as exc:
        try:
            transition = terminalize_feature_disabled_plan(
                session,
                trigger.sync_run_id,
                exc,
            )
            session.commit()
        except Exception:
            session.rollback()
            logger.exception(
                "sync_scheduler.feature_denial_terminalization_failed",
                extra={
                    "config_id": str(config.id),
                    "org_id": str(config.org_id),
                    "feature_key": CANONICAL_INCIDENT_FEATURE_KEY,
                    "sync_run_id": trigger.sync_run_id,
                },
            )
            raise
        logger.warning(
            "sync_scheduler.feature_disabled_before_enqueue",
            extra={
                "config_id": str(config.id),
                "org_id": str(config.org_id),
                "feature_key": CANONICAL_INCIDENT_FEATURE_KEY,
                "sync_run_id": trigger.sync_run_id,
                "failed_units": transition.failed_units,
            },
        )
        return False

    try:
        session.commit()
    except Exception:
        logger.exception("Fan-out planner commit failed for config %s", config.id)
        session.rollback()
        return False

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
