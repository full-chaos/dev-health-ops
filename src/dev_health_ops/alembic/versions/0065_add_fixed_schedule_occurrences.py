"""Add the durable fixed-schedule occurrence ledger.

Revision ID: 0065
Revises: 0064

The Go scheduler replaces Celery Beat's fixed maintenance entries. Beat kept no
durable record of what it had fired, which is safe only because exactly one
Beat process may run. The Go scheduler is replica-safe instead: every schedule
derives a deterministic occurrence key from its identity and canonical due
time, and this table is the durable identity of record for that key.

The ledger is deliberately not River's job table. River prunes completed jobs
on its own retention schedule, so relying on job uniqueness alone would let a
schedule re-fire a due time once its job aged out. The primary key here is what
makes two racing replicas converge on one occurrence, and the unique constraint
on (schedule_id, scheduled_for) is what makes a changed identity derivation
fail loudly instead of silently duplicating a due time under a new key.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0065"
down_revision: str | None = "0064"
branch_labels = None
depends_on = None

_TABLE = "fixed_schedule_occurrences"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("occurrence_key", sa.Text(), nullable=False),
        sa.Column("identity_version", sa.Text(), nullable=False),
        sa.Column("schedule_id", sa.Text(), nullable=False),
        sa.Column("target_kind", sa.Text(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status", sa.String(length=16), nullable=False, server_default="claimed"
        ),
        sa.Column("handoff_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skip_reason", sa.String(length=64), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("occurrence_key", name="pk_fixed_schedule_occurrences"),
        sa.UniqueConstraint(
            "schedule_id",
            "scheduled_for",
            name="uq_fixed_schedule_occurrence_schedule_time",
        ),
        sa.CheckConstraint(
            "status IN ('claimed', 'materialized', 'skipped')",
            name="ck_fixed_schedule_occurrence_status",
        ),
        sa.CheckConstraint(
            "handoff_count >= 0",
            name="ck_fixed_schedule_occurrence_handoff_count",
        ),
        # A materialized occurrence must have produced work and a skipped one
        # must record why it did not. Without this an empty producer and a
        # working producer leave identical rows.
        sa.CheckConstraint(
            "(status = 'claimed' AND completed_at IS NULL AND handoff_count = 0"
            " AND skip_reason IS NULL)"
            " OR (status = 'materialized' AND completed_at IS NOT NULL"
            " AND handoff_count > 0 AND skip_reason IS NULL)"
            " OR (status = 'skipped' AND completed_at IS NOT NULL"
            " AND handoff_count = 0 AND skip_reason IS NOT NULL)",
            name="ck_fixed_schedule_occurrence_terminal_state",
        ),
    )
    op.create_index(
        "ix_fixed_schedule_occurrence_schedule_time",
        _TABLE,
        ["schedule_id", sa.text("scheduled_for DESC")],
        unique=False,
    )
    # Supports the "did any occurrence stall in claimed state" sweep without
    # scanning completed history.
    op.create_index(
        "ix_fixed_schedule_occurrence_incomplete",
        _TABLE,
        ["schedule_id", "scheduled_for"],
        unique=False,
        postgresql_where=sa.text("status = 'claimed'"),
    )


def downgrade() -> None:
    op.drop_index("ix_fixed_schedule_occurrence_incomplete", table_name=_TABLE)
    op.drop_index("ix_fixed_schedule_occurrence_schedule_time", table_name=_TABLE)
    op.drop_table(_TABLE)
