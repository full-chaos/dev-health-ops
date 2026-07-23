from __future__ import annotations

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
    SyncConfiguration,
)
from dev_health_ops.sync.canonical_incident_gate import (
    require_canonical_incident_feature_sync,
    sync_targets_require_canonical_incident_feature,
)
from dev_health_ops.sync.error_sanitize import sanitize_error_text
from dev_health_ops.sync.planner import plan_sync_run
from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed


@dataclass(frozen=True)
class SyncExecutionTriggerResult:
    sync_run_id: str
    job_run_id: str
    total_units: int
    dispatch_required: bool = True
    terminal_reason: str = ""


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
