from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from dev_health_ops.models.reports import ReportRun, ReportRunStatus, SavedReport


def persist_report_run(
    session: Session,
    run_id: str,
    report_id: str,
    rendered_markdown: str,
    provenance: list[dict] | None = None,
) -> bool:
    """Persist one immutable rendered artifact, returning whether this call won.

    ``ReportRun`` is authoritative across Celery retries and the dormant Go
    handoff. A retry may observe an already-completed identical artifact, but
    it may never replace an artifact or revive a canceled run.
    """

    run = session.execute(
        select(ReportRun).where(ReportRun.id == run_id).with_for_update()
    ).scalar_one()
    fingerprint = "sha256:" + hashlib.sha256(rendered_markdown.encode()).hexdigest()
    if run.status == ReportRunStatus.CANCELED.value:
        return False
    if run.status == ReportRunStatus.SUCCESS.value:
        if run.artifact_fingerprint != fingerprint:
            raise RuntimeError("report artifact conflicts with completed run")
        return False

    now = datetime.now(timezone.utc)
    run.status = ReportRunStatus.SUCCESS.value
    run.completed_at = now
    if run.started_at:
        run.duration_seconds = (now - run.started_at).total_seconds()
    run.rendered_markdown = rendered_markdown
    run.provenance_records = provenance or []
    run.artifact_fingerprint = fingerprint
    run.notification_key = f"report.ready:{run.id}"
    run.notification_status = "pending"
    run.notification_claim_token = None
    run.notification_lease_expires_at = None

    report = session.execute(
        select(SavedReport).where(SavedReport.id == report_id)
    ).scalar_one()
    report.last_run_at = now
    report.last_run_status = ReportRunStatus.SUCCESS.value
    report.updated_at = now
    return True


def start_report_run(session: Session, run_id: str) -> bool:
    """CAS a pending run into running; duplicates observe the authoritative state."""

    run = session.execute(
        select(ReportRun).where(ReportRun.id == run_id).with_for_update()
    ).scalar_one_or_none()
    if run is None or run.status != ReportRunStatus.PENDING.value:
        return False
    run.status = ReportRunStatus.RUNNING.value
    run.started_at = datetime.now(timezone.utc)
    run.attempt_count += 1
    return True
