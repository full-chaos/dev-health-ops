"""Idempotent report-ready notification state transitions."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dev_health_ops.models.reports import ReportRun, ReportRunStatus

ReportNotifier = Callable[[str, str], None]
_NOTIFICATION_LEASE = timedelta(minutes=5)


def claim_report_notification(
    session: Session, run_id: str
) -> tuple[str, str, UUID] | None:
    """Atomically reserve a report-ready delivery with a recoverable lease."""

    now = datetime.now(UTC)
    token = uuid4()
    result = session.execute(
        update(ReportRun)
        .where(
            ReportRun.id == run_id,
            ReportRun.status == ReportRunStatus.SUCCESS.value,
            ReportRun.notification_key.is_not(None),
            (ReportRun.notification_status == "pending")
            | (
                (ReportRun.notification_status == "delivering")
                & (ReportRun.notification_lease_expires_at.is_not(None))
                & (ReportRun.notification_lease_expires_at <= now)
            ),
        )
        .values(
            notification_status="delivering",
            notification_claim_token=token,
            notification_lease_expires_at=now + _NOTIFICATION_LEASE,
        )
    )
    if not getattr(result, "rowcount", 0):
        return None
    run = session.execute(select(ReportRun).where(ReportRun.id == run_id)).scalar_one()
    return str(run.report_id), str(run.notification_key), token


def complete_report_notification(session: Session, run_id: str, token: UUID) -> bool:
    """Mark delivered only for the claimant that still owns the lease."""

    result = session.execute(
        update(ReportRun)
        .where(
            ReportRun.id == run_id,
            ReportRun.notification_status == "delivering",
            ReportRun.notification_claim_token == token,
        )
        .values(
            notification_status="delivered",
            notification_sent_at=datetime.now(UTC),
            notification_claim_token=None,
            notification_lease_expires_at=None,
        )
    )
    return bool(getattr(result, "rowcount", 0))


def release_report_notification(session: Session, run_id: str, token: UUID) -> bool:
    """Release only this claimant's failed delivery for a later retry."""

    result = session.execute(
        update(ReportRun)
        .where(
            ReportRun.id == run_id,
            ReportRun.notification_status == "delivering",
            ReportRun.notification_claim_token == token,
        )
        .values(
            notification_status="pending",
            notification_claim_token=None,
            notification_lease_expires_at=None,
        )
    )
    return bool(getattr(result, "rowcount", 0))


def deliver_report_notification_once(
    session: Session, run_id: str, notifier: ReportNotifier
) -> bool:
    """Run a notifier at most once for the durable report-ready key."""

    claimed = claim_report_notification(session, run_id)
    if claimed is None:
        return False
    report_id, notification_key, token = claimed
    try:
        notifier(report_id, notification_key)
    except Exception:
        release_report_notification(session, run_id, token)
        raise
    return complete_report_notification(session, run_id, token)
