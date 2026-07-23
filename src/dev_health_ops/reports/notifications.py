"""Idempotent report-ready notification state transitions."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dev_health_ops.models.reports import ReportRun, ReportRunStatus

ReportNotifier = Callable[[str, str], None]


def claim_report_notification(session: Session, run_id: str) -> tuple[str, str] | None:
    """Atomically reserve a completed report's notification side effect."""

    result = session.execute(
        update(ReportRun)
        .where(
            ReportRun.id == run_id,
            ReportRun.status == ReportRunStatus.SUCCESS.value,
            ReportRun.notification_status == "pending",
            ReportRun.notification_key.is_not(None),
        )
        .values(notification_status="delivering")
    )
    if not getattr(result, "rowcount", 0):
        return None
    run = session.execute(select(ReportRun).where(ReportRun.id == run_id)).scalar_one()
    return str(run.report_id), str(run.notification_key)


def complete_report_notification(session: Session, run_id: str) -> bool:
    """Mark a notification delivered only from its claimed state."""

    result = session.execute(
        update(ReportRun)
        .where(ReportRun.id == run_id, ReportRun.notification_status == "delivering")
        .values(notification_status="delivered", notification_sent_at=datetime.now(UTC))
    )
    return bool(getattr(result, "rowcount", 0))


def release_report_notification(session: Session, run_id: str) -> bool:
    """Return an unsuccessful delivery to pending so a later retry can deliver it."""

    result = session.execute(
        update(ReportRun)
        .where(ReportRun.id == run_id, ReportRun.notification_status == "delivering")
        .values(notification_status="pending")
    )
    return bool(getattr(result, "rowcount", 0))


def deliver_report_notification_once(
    session: Session, run_id: str, notifier: ReportNotifier
) -> bool:
    """Run a notifier at most once for the durable report-ready key."""

    claimed = claim_report_notification(session, run_id)
    if claimed is None:
        return False
    report_id, notification_key = claimed
    try:
        notifier(report_id, notification_key)
    except Exception:
        release_report_notification(session, run_id)
        raise
    return complete_report_notification(session, run_id)
