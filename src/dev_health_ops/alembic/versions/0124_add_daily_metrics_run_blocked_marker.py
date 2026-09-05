"""Add a blocked marker to daily_metrics_runs.

Revision ID: 0124
Revises: 0123
Create Date: 2026-09-04 00:00:00

CHAOS-5040/CHAOS-4970: a daily_metrics_run holding at least one
'failed_permanent' partition (CHAOS-4319's terminal state, added in 0113) can
never finish. CompleteFinalize is the only writer of the run's completion
fence, and it is only reachable once CompletePartition observes ZERO
non-succeeded partitions -- so one failed_permanent partition means no
finalize, no fence, and every handoff gated on that fence waits forever.
Everything downstream (workgraph.build, then investment.materialize) sits at
'pending', which is indistinguishable from legitimately in-flight work. In
prod this went unnoticed from 2026-09-01: 112 runs stuck 'running', 79 of
them wedged this way, with no state anywhere recording the fact.

This adds a MARKER, deliberately not a new run status:

    blocked_at      timestamptz NULL
    blocked_reason  varchar(64) NULL

Why a marker and not a status value. `daily_metrics_runs.status = 'running'`
is load-bearing in roughly twenty SQL predicates across daily/postgres.go,
daily/redrive.go, daily/partition_recompute.go and
joboutbox/terminal_delivery_repair.go. Three of them are the operator
recovery path itself -- RedriveStrandedPartitions gates BOTH its
failed_permanent reset and its dispatchable select on status = 'running', and
RunningRunIDs (which supplies the run ids for the Python compatibility-ledger
repair) does too. A new terminal status would therefore drop a blocked run
out of `metrics daily-redrive` entirely, turning a freeze that IS fixable
today into one that is not. The marker changes no existing predicate, so
every reclaim, redrive and finalize path behaves exactly as before; only
visibility is added.

The columns are paired: a blocked run always carries both, an unblocked run
carries neither. The partial index serves the observer gauge's count and the
workerctl readback, both of which only ever ask for blocked rows.

No backfill. The marker is derived from live state by a single Go predicate
(one failed_permanent partition and nothing dispatchable left), evaluated
periodically, so the existing wedged runs are marked by that same predicate
on its next pass rather than by a second definition frozen into this
migration. Encoding the predicate in SQL here would create two definitions of
one invariant that drift apart the first time it changes.

Downgrade is a plain drop: because no status value or existing predicate was
touched, there is no data to fold back -- unlike 0113's downgrade, which has
to rewrite 'failed_permanent' rows to 'failed' before its narrower CHECK can
reapply.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0124"
down_revision: str | None = "0123"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "daily_metrics_runs"
_PAIRED_CHECK = "ck_daily_metrics_run_blocked_marker_paired"
_INDEX = "ix_daily_metrics_run_blocked"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("blocked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        _TABLE,
        sa.Column("blocked_reason", sa.String(length=64), nullable=True),
    )
    # Both or neither. A blocked_at with no reason would surface a run in the
    # gauge that the readback could not explain, which is the failure this
    # marker exists to remove rather than reproduce.
    op.create_check_constraint(
        _PAIRED_CHECK,
        _TABLE,
        "(blocked_at IS NULL) = (blocked_reason IS NULL)",
    )
    op.create_index(
        _INDEX,
        _TABLE,
        ["org_id"],
        postgresql_where=sa.text("blocked_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_constraint(_PAIRED_CHECK, _TABLE, type_="check")
    op.drop_column(_TABLE, "blocked_reason")
    op.drop_column(_TABLE, "blocked_at")
