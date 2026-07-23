from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import func, or_, true, update

from dev_health_ops.providers.utils import env_flag
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

    from dev_health_ops.models.settings import (
        ScheduledJob,
        ScheduledSyncOccurrence,
        SyncConfiguration,
    )

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
_PENDING_OCCURRENCE_RECONCILE_SCAN_MULTIPLIER = 2
_PENDING_OCCURRENCE_RECONCILE_MAX_ATTEMPTS = 5
_PENDING_OCCURRENCE_RECONCILE_BACKOFF_CAP_SECONDS = 15 * 60


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
    config: SyncConfiguration,
    job: ScheduledJob,
    occurrence: ScheduledSyncOccurrence,
) -> str:
    """Plan one pre-locked Go-authored occurrence.

    The caller locks configuration, scheduled job, then occurrence. That lets
    a replica skip a contended occurrence prefix before applying its limit.
    """
    from dev_health_ops.models.settings import (
        SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED,
    )
    from dev_health_ops.sync.execution_trigger import (
        create_scheduled_sync_execution_trigger,
    )

    if not _scheduled_occurrence_identity_is_valid(occurrence, config, job):
        return "identity_conflict"
    if occurrence.job_run_id is not None or occurrence.sync_run_id is not None:
        return "already_completed"

    create_scheduled_sync_execution_trigger(
        session,
        config,
        job,
        str(config.org_id),
        scheduled_for=occurrence.scheduled_for,
        triggered_by="schedule",
        mode="incremental",
    )
    occurrence.reconcile_attempt_count = 0
    occurrence.reconcile_next_attempt_at = None
    occurrence.reconcile_error_code = None
    occurrence.reconcile_error_at = None
    occurrence.reconcile_status = SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED
    session.flush()
    return "completed"


def _scheduled_occurrence_identity_is_valid(
    occurrence: ScheduledSyncOccurrence,
    config: SyncConfiguration,
    job: ScheduledJob,
) -> bool:
    """Verify the immutable Go/Python occurrence identity before planning."""
    from dev_health_ops.sync.execution_trigger import (
        SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
        scheduled_sync_occurrence_identity,
    )

    try:
        scheduled_for = _ensure_utc(occurrence.scheduled_for)
        if scheduled_for is None:
            return False
        expected_id = scheduled_sync_occurrence_identity(config.id, scheduled_for)
    except (AttributeError, TypeError, ValueError):
        return False
    return (
        occurrence.identity_version == SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION
        and occurrence.occurrence_id == expected_id
        and occurrence.org_id == config.org_id == job.org_id
        and occurrence.sync_config_id == config.id == job.sync_config_id
        and occurrence.scheduled_job_id == job.id
        and str(job.job_type) == "sync"
    )


def _pending_occurrence_backoff_seconds(attempt_count: int) -> int:
    """Return a bounded exponential retry delay for a failed occurrence."""
    return min(
        _PENDING_OCCURRENCE_RECONCILE_BACKOFF_CAP_SECONDS,
        60 * (2 ** min(max(attempt_count - 1, 0), 4)),
    )


def _defer_pending_scheduled_sync_occurrence(
    session: Session,
    *,
    occurrence_id: str,
    expected_attempt_count: int,
    error_code: str,
    now: datetime,
) -> bool:
    """CAS a pending occurrence into retry (or terminal quarantine) state."""
    from dev_health_ops.models.settings import (
        SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
        SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED,
        SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
        ScheduledSyncOccurrence,
    )

    attempt_count = expected_attempt_count + 1
    exhausted = attempt_count >= _PENDING_OCCURRENCE_RECONCILE_MAX_ATTEMPTS
    result = session.execute(
        update(ScheduledSyncOccurrence)
        .where(
            ScheduledSyncOccurrence.occurrence_id == occurrence_id,
            ScheduledSyncOccurrence.reconcile_status.in_(
                (
                    SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
                    SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
                )
            ),
            ScheduledSyncOccurrence.reconcile_attempt_count == expected_attempt_count,
            ScheduledSyncOccurrence.job_run_id.is_(None),
            ScheduledSyncOccurrence.sync_run_id.is_(None),
        )
        .values(
            reconcile_attempt_count=attempt_count,
            reconcile_next_attempt_at=(
                None
                if exhausted
                else now
                + timedelta(seconds=_pending_occurrence_backoff_seconds(attempt_count))
            ),
            reconcile_error_code="retry_exhausted" if exhausted else error_code,
            reconcile_error_at=now,
            reconcile_status=(
                SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED
                if exhausted
                else SCHEDULED_OCCURRENCE_RECONCILE_RETRY
            ),
        )
    )
    return bool(getattr(result, "rowcount", 0))


def _quarantine_pending_scheduled_sync_occurrence(
    session: Session,
    *,
    occurrence_id: str,
    expected_attempt_count: int,
    now: datetime,
) -> bool:
    """CAS an identity-conflicting occurrence into permanent quarantine."""
    from dev_health_ops.models.settings import (
        SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
        SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED,
        SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
        ScheduledSyncOccurrence,
    )

    result = session.execute(
        update(ScheduledSyncOccurrence)
        .where(
            ScheduledSyncOccurrence.occurrence_id == occurrence_id,
            ScheduledSyncOccurrence.reconcile_status.in_(
                (
                    SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
                    SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
                )
            ),
            ScheduledSyncOccurrence.reconcile_attempt_count == expected_attempt_count,
            ScheduledSyncOccurrence.job_run_id.is_(None),
            ScheduledSyncOccurrence.sync_run_id.is_(None),
        )
        .values(
            reconcile_next_attempt_at=None,
            reconcile_error_code="identity_conflict",
            reconcile_error_at=now,
            reconcile_status=SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED,
        )
    )
    return bool(getattr(result, "rowcount", 0))


def reconcile_pending_scheduled_sync_occurrences(
    session: Session,
    *,
    limit: int = DEFAULT_PENDING_OCCURRENCE_RECONCILE_LIMIT,
) -> dict[str, int]:
    """Complete a deterministic, bounded batch of unplanned Go occurrences.

    The helper is only invoked by the separately gated Celery consumer; it is
    not called by :func:`dispatch_scheduled_syncs` or exposed to the Go
    scheduler. It materializes existing stable occurrence identities without
    reconsidering ``next_run_at``.
    """
    from dev_health_ops.models.settings import (
        SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
        SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
        ScheduledJob,
        ScheduledSyncOccurrence,
        SyncConfiguration,
    )
    from dev_health_ops.sync.execution_trigger import (
        ScheduledSyncOccurrenceIneligibleError,
    )

    if limit <= 0:
        return _empty_pending_occurrence_reconcile_counts()
    claim_limit = min(limit, DEFAULT_PENDING_OCCURRENCE_RECONCILE_LIMIT)
    now = datetime.now(timezone.utc)
    due_occurrences = session.query(ScheduledSyncOccurrence.occurrence_id).filter(
        ScheduledSyncOccurrence.sync_config_id == SyncConfiguration.id,
        ScheduledSyncOccurrence.scheduled_job_id == ScheduledJob.id,
        ScheduledSyncOccurrence.job_run_id.is_(None),
        ScheduledSyncOccurrence.sync_run_id.is_(None),
        ScheduledSyncOccurrence.reconcile_status.in_(
            (
                SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
                SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
            )
        ),
        or_(
            ScheduledSyncOccurrence.reconcile_next_attempt_at.is_(None),
            ScheduledSyncOccurrence.reconcile_next_attempt_at <= now,
        ),
    )
    due_occurrence_exists = due_occurrences.exists()
    earliest_due_occurrence = due_occurrences.with_entities(
        func.min(ScheduledSyncOccurrence.scheduled_for)
    ).scalar_subquery()
    pairs = (
        session.query(SyncConfiguration, ScheduledJob)
        # The correlated due predicate pairs these rows through the occurrence
        # IDs. Do not assume config/job coherence here: a mismatched persisted
        # pair must be locked and quarantined, not silently left pending.
        .join(
            ScheduledJob,
            true(),
        )
        .filter(due_occurrence_exists)
        .order_by(
            earliest_due_occurrence,
            SyncConfiguration.org_id,
            SyncConfiguration.id,
            ScheduledJob.id,
        )
        # Claim config/job pairs before limiting. The occurrence query below
        # keeps the global lock order config -> job -> occurrence.
        .with_for_update(of=(SyncConfiguration, ScheduledJob), skip_locked=True)
        .limit(claim_limit)
        .all()
    )
    completed = 0
    retried = 0
    quarantined = 0
    already_completed = 0
    errors = 0
    scanned = 0
    scan_limit = claim_limit * _PENDING_OCCURRENCE_RECONCILE_SCAN_MULTIPLIER

    for config, job in pairs:
        while completed < claim_limit and scanned < scan_limit:
            remaining = min(claim_limit - completed, scan_limit - scanned)
            candidates = (
                session.query(ScheduledSyncOccurrence)
                .filter(
                    ScheduledSyncOccurrence.sync_config_id == config.id,
                    ScheduledSyncOccurrence.scheduled_job_id == job.id,
                    ScheduledSyncOccurrence.job_run_id.is_(None),
                    ScheduledSyncOccurrence.sync_run_id.is_(None),
                    ScheduledSyncOccurrence.reconcile_status.in_(
                        (
                            SCHEDULED_OCCURRENCE_RECONCILE_PENDING,
                            SCHEDULED_OCCURRENCE_RECONCILE_RETRY,
                        )
                    ),
                    or_(
                        ScheduledSyncOccurrence.reconcile_next_attempt_at.is_(None),
                        ScheduledSyncOccurrence.reconcile_next_attempt_at <= now,
                    ),
                )
                .order_by(
                    ScheduledSyncOccurrence.scheduled_for,
                    ScheduledSyncOccurrence.occurrence_id,
                )
                # Apply SKIP LOCKED before LIMIT so a locked prefix cannot
                # starve a later occurrence in this claimed pair.
                .with_for_update(of=ScheduledSyncOccurrence, skip_locked=True)
                .limit(remaining)
                .all()
            )
            if not candidates:
                break

            for occurrence in candidates:
                occurrence_id = str(occurrence.occurrence_id)
                attempt_count = int(occurrence.reconcile_attempt_count)
                scanned += 1
                try:
                    # One bad occurrence must not roll back a prior completion
                    # or prevent later rows in the bounded scan from advancing.
                    with session.begin_nested():
                        outcome = _complete_pending_scheduled_sync_occurrence(
                            session,
                            config=config,
                            job=job,
                            occurrence=occurrence,
                        )
                        if outcome == "identity_conflict":
                            if _quarantine_pending_scheduled_sync_occurrence(
                                session,
                                occurrence_id=occurrence_id,
                                expected_attempt_count=attempt_count,
                                now=now,
                            ):
                                logger.error(
                                    "pending_scheduled_sync_occurrence_quarantined",
                                    extra={
                                        "occurrence_id": occurrence_id,
                                        "org_id": str(config.org_id),
                                        "error_code": "identity_conflict",
                                    },
                                )
                                quarantined += 1
                            else:
                                already_completed += 1
                        elif outcome == "completed":
                            completed += 1
                        else:
                            if _defer_pending_scheduled_sync_occurrence(
                                session,
                                occurrence_id=occurrence_id,
                                expected_attempt_count=attempt_count,
                                error_code="ineligible",
                                now=now,
                            ):
                                if (
                                    attempt_count + 1
                                    >= _PENDING_OCCURRENCE_RECONCILE_MAX_ATTEMPTS
                                ):
                                    quarantined += 1
                                else:
                                    retried += 1
                            else:
                                already_completed += 1
                except ScheduledSyncOccurrenceIneligibleError:
                    with session.begin_nested():
                        deferred = _defer_pending_scheduled_sync_occurrence(
                            session,
                            occurrence_id=occurrence_id,
                            expected_attempt_count=attempt_count,
                            error_code="ineligible",
                            now=now,
                        )
                    if deferred:
                        if (
                            attempt_count + 1
                            >= _PENDING_OCCURRENCE_RECONCILE_MAX_ATTEMPTS
                        ):
                            quarantined += 1
                        else:
                            retried += 1
                    else:
                        already_completed += 1
                except Exception:
                    logger.exception(
                        "pending_scheduled_sync_occurrence_reconciliation_failed",
                        extra={
                            "occurrence_id": occurrence_id,
                            "org_id": str(config.org_id),
                        },
                    )
                    with session.begin_nested():
                        deferred = _defer_pending_scheduled_sync_occurrence(
                            session,
                            occurrence_id=occurrence_id,
                            expected_attempt_count=attempt_count,
                            error_code="planner_error",
                            now=now,
                        )
                    if deferred:
                        if (
                            attempt_count + 1
                            >= _PENDING_OCCURRENCE_RECONCILE_MAX_ATTEMPTS
                        ):
                            quarantined += 1
                        else:
                            retried += 1
                    else:
                        already_completed += 1
                    errors += 1

                if completed >= claim_limit or scanned >= scan_limit:
                    break

    return {
        "scanned": scanned,
        "completed": completed,
        "retried": retried,
        "quarantined": quarantined,
        "already_completed": already_completed,
        "errors": errors,
    }


def _empty_pending_occurrence_reconcile_counts() -> dict[str, int]:
    return {
        "scanned": 0,
        "completed": 0,
        "retried": 0,
        "quarantined": 0,
        "already_completed": 0,
        "errors": 0,
    }


def _scheduled_occurrence_consumer_enabled() -> bool:
    """Read the rollout flag at task-call time, defaulting to disabled."""
    return env_flag("SYNC_SCHEDULED_OCCURRENCE_CONSUMER_ENABLED", default=False)


@celery_app.task(
    queue="scheduler",
    name="dev_health_ops.workers.tasks.consume_pending_scheduled_sync_occurrences",
)
def consume_pending_scheduled_sync_occurrences() -> dict[str, int]:
    """Materialize one bounded batch of Go-authored scheduled occurrences.

    This remains a default-off rollout seam. The same flag also controls the
    Beat registration, but the task checks it again so a queued message cannot
    open PostgreSQL after the consumer has been switched off.
    """
    if not _scheduled_occurrence_consumer_enabled():
        logger.info("sync_scheduler.pending_occurrence_consumer_disabled")
        return _empty_pending_occurrence_reconcile_counts()

    from dev_health_ops.db import get_postgres_session_sync

    try:
        with get_postgres_session_sync() as session:
            counts = reconcile_pending_scheduled_sync_occurrences(session)
            # The helper uses nested transactions to isolate a malformed
            # occurrence. Commit its successful slice as one outer task
            # transaction before acknowledging the bounded result.
            session.commit()
    except Exception:
        # get_postgres_session_sync rolls back the outer transaction before
        # control reaches here. Do not swallow this: Celery must record the
        # failure so the next Beat tick can retry the idempotent occurrences.
        logger.exception("sync_scheduler.pending_occurrence_consumer_failed")
        raise

    logger.info("sync_scheduler.pending_occurrence_consumer_completed", extra=counts)
    return counts


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
