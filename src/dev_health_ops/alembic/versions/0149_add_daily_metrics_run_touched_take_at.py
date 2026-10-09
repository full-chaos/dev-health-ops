"""Add ``daily_metrics_runs.touched_take_at`` -- the take time of the touched days a run was started for.

Revision ID: 0149
Revises: 0148

CHAOS-8881. A post-sync fan-out and the drain of the pending touched days start daily runs for days
that stored raw rows touched, and then mark those keys as dispatched in ClickHouse. The mark stamps
its keys one millisecond before the ClickHouse time the run read the pending days at (the take time).
Whether a mark reached the table could only be inferred ("a run has a result and every key it lists
is pending"), which is also the picture of keys touched again while the run ran. The take time,
written with the run in the transaction that creates it, makes the answer exact: a listed key that is
still pending and was last touched strictly before the take time is a key whose mark did not land.

The column is run bookkeeping (a timestamp of one read), not team attribution. It is nullable and has
no default and no backfill: a run created before this revision, or by a build that does not write it,
has no take time, and the drain does not stop on such a run (it counts it).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0149"
down_revision: str | None = "0148"
branch_labels = None
depends_on = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "daily_metrics_runs"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("touched_take_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "touched_take_at")
