"""Add sync_manual_triggers (CHAOS-4602).

Revision ID: 0119
Revises: 0118
Create Date: 2026-08-30 00:00:00

Creates/Modifies tables for:
- sync_manual_triggers: the backfill-selector payload for a manual "Sync
  Now" / backfill scheduled_sync_occurrences row.

Manual Sync Now and Backfill triggers are moving from Python's
plan_sync_run (src/dev_health_ops/sync/planner.py:264) to the Go scheduler
planner/materializer (CHAOS-4602 design page). scheduled_sync_occurrences
(migration 0050) is reused verbatim as the trigger row -- its pickup query
(internal/scheduler/sync/occurrence_reconciler.go's
dueOccurrenceKeysSQL/lockPendingOccurrenceSQL) is already producer-agnostic
and needs no change. What it CANNOT express is a one-off manual/backfill
request's own mode override and BackfillSelector (since/before/source_ids/
dataset_keys) -- sync_configurations.sync_options only ever describes a
config's SCHEDULED default (incremental hourly, say), never a specific
trigger's ask. This table is that missing payload, 1:1 with one occurrence.

Kept as a separate table rather than nullable columns bolted onto
scheduled_sync_occurrences: every one of the (large majority) scheduled
occurrences would otherwise carry columns that are always NULL for it.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0119"
down_revision: str | None = "0118"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_manual_triggers"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        # 1:1 with scheduled_sync_occurrences: this occurrence's own
        # deterministic identity IS this row's primary key, no surrogate id.
        sa.Column("occurrence_id", sa.Text(), nullable=False),
        # 'incremental' | 'full_resync' | 'backfill' -- overrides what
        # loadMaterializationPlan would otherwise derive from
        # sync_configurations.sync_options for this one occurrence.
        sa.Column("mode", sa.Text(), nullable=False),
        # BackfillSelector fields (src/dev_health_ops/api/admin/schemas/
        # integrations.py:129-136), backfill mode only -- NULL for
        # incremental/full_resync triggers.
        sa.Column("since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("before", sa.DateTime(timezone=True), nullable=True),
        # Explicit subset, matching plan_sync_run's own SyncPlanRequest.
        # source_ids/dataset_keys semantics: NULL means "all enabled", a
        # non-NULL (possibly empty) array is the explicit chosen subset.
        sa.Column("source_ids", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("dataset_keys", postgresql.ARRAY(sa.Text()), nullable=True),
        # 'manual' | 'backfill' -- carried through verbatim to the
        # resulting sync_runs.triggered_by, matching today's
        # create_sync_execution_trigger call sites
        # (api/admin/routers/sync.py:2773,2878).
        sa.Column("triggered_by", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["occurrence_id"],
            ["scheduled_sync_occurrences.occurrence_id"],
            name="fk_sync_manual_triggers_occurrence_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("occurrence_id", name="pk_sync_manual_triggers"),
        sa.CheckConstraint(
            "mode IN ('incremental', 'full_resync', 'backfill')",
            name="ck_sync_manual_triggers_mode",
        ),
        sa.CheckConstraint(
            "triggered_by IN ('manual', 'backfill')",
            name="ck_sync_manual_triggers_triggered_by",
        ),
        sa.CheckConstraint(
            "(mode = 'backfill') = (since IS NOT NULL AND before IS NOT NULL)",
            name="ck_sync_manual_triggers_backfill_selector",
        ),
    )


def downgrade() -> None:
    op.drop_table(_TABLE)
