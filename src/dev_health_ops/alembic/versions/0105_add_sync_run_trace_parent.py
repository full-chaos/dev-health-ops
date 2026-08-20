"""Add the optional root trace_parent captured once when a sync run starts.

Revision ID: 0105
Revises: 0104

The Go sync-dispatch coordinator (dispatch_sync_run / finalize_sync_run /
post_sync / reference_discovery) resolves its authoritative domain reference
for every dispatch from a database lookup, not from decoded River arguments
(see internal/syncdispatchruntime.TransportArgs). This column is the durable
place that reference is joined through to recover the W3C traceparent the
Python planner captured once at plan time, so every dispatch across the whole
run's lifecycle parents its span from the same trace (CHAOS-3996).

Nullable, no backfill: a run planned before this migration, or planned while
tracing is disabled, simply has no traceparent to recover, exactly like a
worker_job_outbox row with no trace_parent (CHAOS-3993).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0105"
down_revision: str | None = "0104"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_runs"
_COLUMN = "trace_parent"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column(_TABLE, _COLUMN)
