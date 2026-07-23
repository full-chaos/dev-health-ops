from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    ScheduledSyncOccurrence,
    SyncConfiguration,
)
from dev_health_ops.sync.canonical_incident_gate import (
    require_canonical_incident_feature_for_update_sync,
    require_canonical_incident_feature_sync,
    sync_targets_require_canonical_incident_feature,
)
from dev_health_ops.sync.error_sanitize import sanitize_error_text
from dev_health_ops.sync.planner import plan_sync_run
from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed

SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION = "sync_scheduler_occurrence_v1"


@dataclass(frozen=True)
class SyncExecutionTriggerResult:
    sync_run_id: str
    job_run_id: str
    total_units: int
    dispatch_required: bool = True
    terminal_reason: str = ""


class ScheduledSyncOccurrenceConflictError(RuntimeError):
    """A stable occurrence identity resolved to different scheduling inputs."""


class ScheduledSyncOccurrenceIneligibleError(RuntimeError):
    """The locked scheduled configuration cannot produce an authorized plan."""


def scheduled_sync_occurrence_identity(
    config_id: str | uuid.UUID,
    scheduled_for: datetime,
) -> str:
    """Return the byte-identical occurrence identity used by the Go scheduler."""
    scheduled_for = _as_aware_utc(scheduled_for)
    fields = (
        ("identity_version", SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION),
        ("config_id", str(config_id)),
        ("scheduled_for", scheduled_for.strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"),
    )
    digest = hashlib.sha256()
    for name, value in fields:
        name_bytes = name.encode()
        value_bytes = value.encode()
        digest.update(str(len(name_bytes)).encode())
        digest.update(b":")
        digest.update(name_bytes)
        digest.update(str(len(value_bytes)).encode())
        digest.update(b":")
        digest.update(value_bytes)
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


def create_scheduled_sync_execution_trigger(
    session: Session,
    config: SyncConfiguration,
    job: ScheduledJob,
    org_id: str,
    *,
    scheduled_for: datetime,
    triggered_by: str = "schedule",
    mode: str = "incremental",
) -> SyncExecutionTriggerResult:
    """Idempotently materialize one scheduled occurrence in the caller transaction."""
    scheduled_for = _as_aware_utc(scheduled_for)
    locked_config = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.id == uuid.UUID(str(config.id)),
            SyncConfiguration.org_id == org_id,
        )
        .populate_existing()
        .with_for_update()
        .one_or_none()
    )
    if locked_config is None:
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration does not exist for organization"
        )
    _require_locked_schedule_contract(locked_config, job, org_id)
    occurrence_id = scheduled_sync_occurrence_identity(locked_config.id, scheduled_for)
    _require_locked_scheduled_eligibility(session, locked_config, org_id)

    occurrence = (
        session.query(ScheduledSyncOccurrence)
        .filter(ScheduledSyncOccurrence.occurrence_id == occurrence_id)
        .with_for_update()
        .one_or_none()
    )
    if occurrence is None:
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=occurrence_id,
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=org_id,
            sync_config_id=uuid.UUID(str(locked_config.id)),
            scheduled_job_id=uuid.UUID(str(job.id)),
            scheduled_for=scheduled_for,
        )
        session.add(occurrence)
        session.flush()
    else:
        _verify_scheduled_occurrence(
            occurrence, locked_config, job, org_id, scheduled_for
        )
        if occurrence.job_run_id is not None and occurrence.sync_run_id is not None:
            return _existing_scheduled_trigger_result(session, occurrence)

    trigger = create_sync_execution_trigger(
        session,
        locked_config,
        org_id,
        triggered_by=triggered_by,
        mode=mode,
    )
    if trigger is None:
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration has no planner route"
        )
    occurrence.job_run_id = uuid.UUID(trigger.job_run_id)
    occurrence.sync_run_id = uuid.UUID(trigger.sync_run_id)
    session.flush()
    return trigger


def ensure_pending_sync_job_run(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
    triggered_by: str,
    result: dict[str, Any] | None = None,
) -> str:
    config_uuid = uuid.UUID(str(config.id))
    job = (
        session.query(ScheduledJob)
        .filter(
            ScheduledJob.org_id == org_id,
            ScheduledJob.sync_config_id == config_uuid,
            ScheduledJob.job_type == "sync",
        )
        .one_or_none()
    )
    if job is None:
        sync_options = dict(config.sync_options or {})
        provider = str(config.provider or "")
        explicit_cron = sync_options.get("schedule_cron")
        job = ScheduledJob(
            name=f"sync-config-{config_uuid}",
            job_type="sync",
            schedule_cron=str(explicit_cron or "0 * * * *"),
            org_id=org_id,
            provider=provider,
            job_config={
                "provider": provider,
                "sync_config_id": str(config_uuid),
            },
            sync_config_id=config_uuid,
            tz=str(sync_options.get("timezone") or "UTC"),
            status=(
                JobStatus.ACTIVE.value
                if bool(config.is_active) and explicit_cron
                else JobStatus.PAUSED.value
            ),
        )
        session.add(job)
        session.flush()

    run = JobRun(
        job_id=uuid.UUID(str(job.id)),
        triggered_by=triggered_by,
        status=JobRunStatus.PENDING.value,
    )
    run.result = result
    session.add(run)
    session.flush()
    return str(run.id)


def merge_job_run_result(
    session: Session, run_id: str, result: dict[str, Any] | None = None
) -> None:
    if result is None:
        return
    run = (
        session.query(JobRun).filter(JobRun.id == uuid.UUID(str(run_id))).one_or_none()
    )
    if run is None:
        return
    current = run.result if isinstance(run.result, dict) else {}
    run.result = {**current, **result}
    session.flush()


def mark_job_run_failed(
    session: Session, run_id: str, error: BaseException | str
) -> None:
    """Terminalize a ``JobRun`` as failed.

    ``error`` is sanitized here, at the sink, rather than trusting every
    caller to have already redacted it (CHAOS-2766 codex review finding):
    a Celery/broker enqueue-failure exception can embed the configured
    broker/result-backend URL, including its credentials, and this column
    surfaces verbatim through admin job-history responses. Accepting
    ``BaseException | str`` (not just ``str``) means a caller that still
    pre-formats a message (e.g. ``f"dispatch enqueue failed: {exc}"``) stays
    covered too -- ``sanitize_error_text`` redacts credential-shaped
    substrings in plain text the same way it does in an exception's message.
    """
    completed_at = datetime.now(timezone.utc)
    run = (
        session.query(JobRun).filter(JobRun.id == uuid.UUID(str(run_id))).one_or_none()
    )
    if run is None:
        return
    run.status = JobRunStatus.FAILED.value
    run.completed_at = completed_at
    run.error = sanitize_error_text(error)
    started_at = getattr(run, "started_at", None)
    if started_at is not None:
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=completed_at.tzinfo)
        run.duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    session.flush()


def create_sync_execution_trigger(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
    *,
    triggered_by: str,
    mode: str,
    since: datetime | None = None,
    before: datetime | None = None,
    initial_job_result: dict[str, Any] | None = None,
) -> SyncExecutionTriggerResult | None:
    sync_targets = [str(target) for target in (config.sync_targets or [])]
    if sync_targets_require_canonical_incident_feature(sync_targets):
        require_canonical_incident_feature_sync(session, org_id)
    request = planner_request_for_config_if_routed(
        session, config, triggered_by=triggered_by, mode=mode
    )
    if request is None:
        return None
    if since is not None or before is not None:
        request = replace(request, since=since, before=before)

    job_run_id = ensure_pending_sync_job_run(
        session,
        config,
        org_id,
        triggered_by,
        initial_job_result,
    )
    plan = plan_sync_run(session, request)
    if not plan.dispatch_required:
        merge_job_run_result(
            session,
            job_run_id,
            {
                "sync_run_id": plan.sync_run_id,
                "terminal_status": "pagerduty_sync_disabled",
                "reason": plan.terminal_reason,
                "total_units": plan.total_units,
            },
        )
        mark_job_run_failed(session, job_run_id, plan.terminal_reason)
    else:
        merge_job_run_result(session, job_run_id, {"sync_run_id": plan.sync_run_id})
    return SyncExecutionTriggerResult(
        sync_run_id=plan.sync_run_id,
        job_run_id=job_run_id,
        total_units=plan.total_units,
        dispatch_required=plan.dispatch_required,
        terminal_reason=plan.terminal_reason,
    )


def _require_locked_scheduled_eligibility(
    session: Session,
    config: SyncConfiguration,
    org_id: str,
) -> None:
    from dev_health_ops.models.users import Organization
    from dev_health_ops.workers.org_guard import organization_exists_sync

    if not organization_exists_sync(session, org_id):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync organization does not exist"
        )
    try:
        org_uuid = uuid.UUID(str(org_id))
    except ValueError:
        org_uuid = None
    if org_uuid is not None and org_id != "default":
        organization = (
            session.query(Organization.id)
            .filter(Organization.id == org_uuid)
            .with_for_update(key_share=True)
            .one_or_none()
        )
        if organization is None:
            raise ScheduledSyncOccurrenceIneligibleError(
                "scheduled sync organization does not exist"
            )

    sync_targets = [str(target) for target in (config.sync_targets or [])]
    if sync_targets_require_canonical_incident_feature(sync_targets):
        if org_uuid is None:
            # Preserve the legacy non-UUID/default compatibility path. The
            # unlocked gate remains authoritative there because no UUID-scoped
            # feature row exists to lock.
            require_canonical_incident_feature_sync(session, org_id)
        else:
            require_canonical_incident_feature_for_update_sync(session, org_id)


def _require_locked_schedule_contract(
    config: SyncConfiguration,
    job: ScheduledJob,
    org_id: str,
) -> None:
    if not bool(config.is_active):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration is inactive"
        )
    if not str((config.sync_options or {}).get("schedule_cron") or ""):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync configuration is manual-only"
        )
    if (
        str(config.org_id) != org_id
        or str(job.org_id) != org_id
        or job.sync_config_id != uuid.UUID(str(config.id))
        or str(job.job_type) != "sync"
        or int(job.status) != JobStatus.ACTIVE.value
    ):
        raise ScheduledSyncOccurrenceIneligibleError(
            "scheduled sync marker does not match the locked configuration"
        )


def _verify_scheduled_occurrence(
    occurrence: ScheduledSyncOccurrence,
    config: SyncConfiguration,
    job: ScheduledJob,
    org_id: str,
    scheduled_for: datetime,
) -> None:
    persisted_for = _as_aware_utc(occurrence.scheduled_for)
    if (
        occurrence.identity_version != SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION
        or occurrence.org_id != org_id
        or occurrence.sync_config_id != uuid.UUID(str(config.id))
        or occurrence.scheduled_job_id != uuid.UUID(str(job.id))
        or persisted_for != scheduled_for
    ):
        raise ScheduledSyncOccurrenceConflictError(
            "scheduled sync occurrence identity conflicts with persisted inputs"
        )
    if (occurrence.job_run_id is None) != (occurrence.sync_run_id is None):
        raise ScheduledSyncOccurrenceConflictError(
            "scheduled sync occurrence has incomplete plan links"
        )


def _existing_scheduled_trigger_result(
    session: Session,
    occurrence: ScheduledSyncOccurrence,
) -> SyncExecutionTriggerResult:
    from dev_health_ops.models import SyncRun, SyncRunStatus

    job_run = (
        session.query(JobRun).filter(JobRun.id == occurrence.job_run_id).one_or_none()
    )
    sync_run = (
        session.query(SyncRun)
        .filter(SyncRun.id == occurrence.sync_run_id)
        .one_or_none()
    )
    if job_run is None or sync_run is None:
        raise ScheduledSyncOccurrenceConflictError(
            "scheduled sync occurrence plan links do not resolve"
        )
    result = sync_run.result if isinstance(sync_run.result, dict) else {}
    terminal = (
        sync_run.status == SyncRunStatus.FAILED.value
        and result.get("error_category") == "pagerduty_sync_disabled"
    )
    return SyncExecutionTriggerResult(
        sync_run_id=str(sync_run.id),
        job_run_id=str(job_run.id),
        total_units=int(sync_run.total_units or 0),
        dispatch_required=not terminal,
        terminal_reason=str(sync_run.error or "") if terminal else "",
    )


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
