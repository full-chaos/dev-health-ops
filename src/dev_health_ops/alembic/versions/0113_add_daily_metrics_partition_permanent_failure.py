"""Add a durable failed_permanent terminal state to daily_metrics_partitions.

Revision ID: 0113
Revises: 0112
Create Date: 2026-08-26 00:00:00

CHAOS-4319: an ambiguous_refused compatibility-bridge claim whose ledger row
is stuck at "ambiguous" (worker_metrics.py's metric_compatibility_executions)
can never move again without a human /repair call. Before this migration,
Go's daily.go marked every ambiguous_refused response Retryable regardless,
so River kept re-claiming a partition that could only ever reproduce the
same 409 -- burning the job's whole attempt budget before silently
discarding it with no durable record of why. daily_metrics_partitions.status
only ever landed at 'failed' (via ReleasePartition), which
DispatchablePartitions treats as re-dispatchable, so the row would spin back
into the same stuck ledger entry on the next dispatch pass too.

This adds a distinct terminal status, 'failed_permanent', paired with a
bounded failure_reason, so an unrecoverable partition is durably recorded and
excluded from DispatchablePartitions's reclaim set (`status IN ('pending',
'failed')`) instead of retried forever or silently lost.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0113"
down_revision: str | None = "0112"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "daily_metrics_partitions"
_STATUS_CHECK = "ck_daily_metrics_partition_status"
_REASON_CHECK = "ck_daily_metrics_partition_failure_reason"
_INDEX = "ix_daily_metrics_partition_failed_permanent"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("failure_reason", sa.String(length=64), nullable=True),
    )
    op.drop_constraint(_STATUS_CHECK, _TABLE, type_="check")
    op.create_check_constraint(
        _STATUS_CHECK,
        _TABLE,
        "status IN ('pending', 'running', 'succeeded', 'failed', 'failed_permanent')",
    )
    op.create_check_constraint(
        _REASON_CHECK,
        _TABLE,
        "(status = 'failed_permanent') = (failure_reason IS NOT NULL)",
    )
    op.create_index(
        _INDEX,
        _TABLE,
        ["run_id"],
        postgresql_where=sa.text("status = 'failed_permanent'"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_constraint(_REASON_CHECK, _TABLE, type_="check")
    op.drop_constraint(_STATUS_CHECK, _TABLE, type_="check")
    # A downgrade target's own status vocabulary never included
    # 'failed_permanent' -- fold any such row back to plain 'failed' so the
    # recreated (narrower) check constraint below can actually apply. These
    # partitions become re-dispatchable again, exactly as they were before
    # this revision existed.
    op.execute(
        f"UPDATE {_TABLE} SET status = 'failed' WHERE status = 'failed_permanent'"
    )
    op.create_check_constraint(
        _STATUS_CHECK,
        _TABLE,
        "status IN ('pending', 'running', 'succeeded', 'failed')",
    )
    op.drop_column(_TABLE, "failure_reason")
