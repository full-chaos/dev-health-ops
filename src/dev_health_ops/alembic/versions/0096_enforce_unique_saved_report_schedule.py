"""Enforce one saved report per recurring schedule.

Revision ID: 0096
Revises: 0095
Create Date: 2026-08-13 00:00:00

The scheduler must not guess which report owns a scheduled job. Before adding
the storage invariant, this migration locks writers and reports every sampled
duplicate group with its report IDs. It never deletes or rewrites customer
report definitions.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import Connection

revision: str = "0096"
down_revision: str | None = "0095"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "saved_reports"
_CONSTRAINT = "uq_saved_reports_schedule_id"
_AUDIT_SAMPLE_LIMIT = 25

DuplicateSchedule = tuple[str, tuple[str, ...], int]

_DUPLICATE_SCHEDULES_SQL = sa.text(
    """
    SELECT schedule_id::text AS schedule_id,
           array_agg(id::text ORDER BY id::text) AS report_ids,
           count(*) OVER () AS duplicate_schedule_count
    FROM public.saved_reports
    WHERE schedule_id IS NOT NULL
    GROUP BY schedule_id
    HAVING count(*) > 1
    ORDER BY schedule_id
    LIMIT :sample_limit
    """
)


def _lock_saved_reports(bind: Connection) -> None:
    # The audit and ALTER must observe one stable write set. Without this lock,
    # a writer can insert a duplicate after the query returns but before the
    # unique constraint scans the table, turning an actionable audit failure
    # into a raw mid-migration UniqueViolation.
    bind.execute(sa.text("LOCK TABLE public.saved_reports IN SHARE ROW EXCLUSIVE MODE"))


def _find_duplicate_schedules(bind: Connection) -> tuple[DuplicateSchedule, ...]:
    rows = (
        bind.execute(
            _DUPLICATE_SCHEDULES_SQL,
            {"sample_limit": _AUDIT_SAMPLE_LIMIT},
        )
        .mappings()
        .all()
    )
    return tuple(
        (
            str(row["schedule_id"]),
            tuple(str(report_id) for report_id in row["report_ids"]),
            int(row["duplicate_schedule_count"]),
        )
        for row in rows
    )


def _format_duplicate_error(duplicates: Sequence[DuplicateSchedule]) -> str:
    total = duplicates[0][2]
    sample_note = (
        f" showing the first {len(duplicates)} of {total} duplicate schedules"
        if total > len(duplicates)
        else ""
    )
    offending_rows = "; ".join(
        f"schedule_id={schedule_id} report_ids=[{', '.join(report_ids)}]"
        for schedule_id, report_ids, _ in duplicates
    )
    return (
        f"cannot add {_CONSTRAINT}: found {total} schedule_id value(s) used by "
        f"multiple saved_reports rows; offending rows{sample_note}: "
        f"{offending_rows}. Detach or remove duplicate report definitions so each "
        "non-null schedule_id belongs to at most one saved report, then rerun the "
        "migration"
    )


def upgrade() -> None:
    bind = op.get_bind()
    _lock_saved_reports(bind)
    duplicate_schedules = _find_duplicate_schedules(bind)
    if duplicate_schedules:
        raise RuntimeError(_format_duplicate_error(duplicate_schedules))

    op.create_unique_constraint(_CONSTRAINT, _TABLE, ["schedule_id"])


def downgrade() -> None:
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="unique")
