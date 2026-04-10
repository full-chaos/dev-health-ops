from __future__ import annotations

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
) -> None:
    run = session.execute(select(ReportRun).where(ReportRun.id == run_id)).scalar_one()

    now = datetime.now(timezone.utc)
    run.status = ReportRunStatus.SUCCESS.value
    run.completed_at = now
    if run.started_at:
        run.duration_seconds = (now - run.started_at).total_seconds()
    run.rendered_markdown = rendered_markdown
    run.provenance_records = provenance or []

    report = session.execute(
        select(SavedReport).where(SavedReport.id == report_id)
    ).scalar_one()
    report.last_run_at = now
    report.last_run_status = ReportRunStatus.SUCCESS.value
    report.updated_at = now
