"""Backfill the durable recurring-report paging marker.

Revision ID: 0097
Revises: 0096
Create Date: 2026-08-13 00:00:00

The native report sweep reads one bounded page ordered by
``scheduled_jobs.next_run_at``. Older report schedules may have a NULL or stale
marker because the legacy dispatcher populated it only after materialization.
This data-only migration derives the marker from the same cron, timezone, and
report base that both runtimes use. It does not change the schema.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import sqlalchemy as sa
from alembic import op
from croniter import croniter
from sqlalchemy.engine import Connection

revision: str = "0097"
down_revision: str | None = "0096"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

logger = logging.getLogger(__name__)

_MARKER_BATCH_SIZE = 500

_REPORT_SCHEDULE_BATCH_SQL = sa.text(
    """
    SELECT job.id::text AS job_id,
           job.schedule_cron,
           job.timezone,
           COALESCE(report.last_run_at, report.created_at) AS marker_base
    FROM public.scheduled_jobs AS job
    JOIN public.saved_reports AS report
      ON report.schedule_id = job.id
    WHERE job.job_type = 'report'
      AND (
          CAST(:after_job_id AS uuid) IS NULL
          OR job.id > CAST(:after_job_id AS uuid)
      )
    ORDER BY job.id
    LIMIT :batch_size
    """
)

_UPDATE_MARKER_SQL = sa.text(
    """
    UPDATE public.scheduled_jobs
    SET next_run_at = :next_run_at
    WHERE id = CAST(:job_id AS uuid)
    """
)


def _next_run_at(
    cron_expression: str,
    base: datetime,
    timezone_name: str | None,
) -> datetime:
    """Resolve one marker with the runtime's pinned croniter semantics."""

    if len(cron_expression.split()) != 5:
        raise ValueError("cron must contain exactly five fields")
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)

    schedule_timezone: tzinfo = timezone.utc
    if timezone_name:
        try:
            schedule_timezone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning(
                "Unknown schedule timezone %r during 0097; evaluating cron in UTC",
                timezone_name,
            )

    local_base = base.astimezone(schedule_timezone).replace(tzinfo=None)
    next_local = croniter(cron_expression, local_base).get_next(datetime)
    if not isinstance(next_local, datetime):
        raise TypeError("croniter.get_next did not return a datetime")
    return next_local.replace(tzinfo=schedule_timezone).astimezone(timezone.utc)


def _backfill_next_run_markers(
    bind: Connection,
    *,
    batch_size: int = _MARKER_BATCH_SIZE,
) -> int:
    """Backfill report markers in bounded keyset batches; return rows changed."""

    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    processed = 0
    after_job_id: str | None = None
    while True:
        rows = (
            bind.execute(
                _REPORT_SCHEDULE_BATCH_SQL,
                {"after_job_id": after_job_id, "batch_size": batch_size},
            )
            .mappings()
            .all()
        )
        if not rows:
            return processed

        for row in rows:
            job_id = str(row["job_id"])
            try:
                next_run_at = _next_run_at(
                    str(row["schedule_cron"]),
                    row["marker_base"],
                    str(row["timezone"]) if row["timezone"] else None,
                )
            except Exception as error:
                detail = (
                    str(error)
                    if isinstance(error, ValueError)
                    and str(error) == "cron must contain exactly five fields"
                    else "cron cannot be evaluated"
                )
                raise RuntimeError(
                    f"cannot backfill next_run_at for scheduled job {job_id}: {detail}"
                ) from error

            result = bind.execute(
                _UPDATE_MARKER_SQL,
                {"job_id": job_id, "next_run_at": next_run_at},
            )
            if result.rowcount != 1:
                raise RuntimeError(
                    f"cannot backfill next_run_at for scheduled job {job_id}: "
                    "row disappeared during migration"
                )
            processed += 1

        after_job_id = str(rows[-1]["job_id"])


def upgrade() -> None:
    _backfill_next_run_markers(op.get_bind())


def downgrade() -> None:
    # Data-only and intentionally irreversible: the pre-0097 marker might have
    # been NULL, stale, or already correct, and there is no truthful way to
    # reconstruct which. The column and its valid derived value remain usable by
    # the legacy dispatcher after a code rollback.
    pass
