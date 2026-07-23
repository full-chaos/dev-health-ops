"""Add durable retry and quarantine state to scheduled sync occurrences.

Revision ID: 0051
Revises: 0050
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scheduled_sync_occurrences",
        sa.Column(
            "reconcile_attempt_count", sa.Integer(), nullable=False, server_default="0"
        ),
    )
    op.add_column(
        "scheduled_sync_occurrences",
        sa.Column(
            "reconcile_next_attempt_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.add_column(
        "scheduled_sync_occurrences",
        sa.Column("reconcile_error_code", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "scheduled_sync_occurrences",
        sa.Column("reconcile_error_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "scheduled_sync_occurrences",
        sa.Column(
            "reconcile_status",
            sa.String(length=16),
            nullable=False,
            server_default="pending",
        ),
    )
    # Existing Python-planned occurrences predate this lifecycle. Their paired
    # links are authoritative, so preserve them before installing the
    # bidirectional completed/link invariant below.
    op.execute(
        "UPDATE scheduled_sync_occurrences SET reconcile_status = 'completed' "
        "WHERE job_run_id IS NOT NULL AND sync_run_id IS NOT NULL"
    )
    with op.batch_alter_table("scheduled_sync_occurrences") as batch:
        batch.create_check_constraint(
            "ck_scheduled_sync_occurrence_reconcile_attempt_count",
            "reconcile_attempt_count >= 0",
        )
        batch.create_check_constraint(
            "ck_scheduled_sync_occurrence_reconcile_status",
            "reconcile_status IN ('pending', 'retry', 'completed', 'quarantined')",
        )
        batch.create_check_constraint(
            "ck_scheduled_sync_occurrence_reconcile_error_code",
            "reconcile_error_code IN ('identity_conflict', 'ineligible', "
            "'planner_error', 'retry_exhausted') OR reconcile_error_code IS NULL",
        )
        batch.create_check_constraint(
            "ck_scheduled_sync_occurrence_reconcile_error_state",
            "(reconcile_error_code IS NULL AND reconcile_error_at IS NULL) OR "
            "(reconcile_error_code IS NOT NULL AND reconcile_error_at IS NOT NULL)",
        )
        batch.create_check_constraint(
            "ck_scheduled_sync_occurrence_reconcile_completed_state",
            "(reconcile_status = 'completed' AND job_run_id IS NOT NULL AND "
            "sync_run_id IS NOT NULL) OR (reconcile_status <> 'completed' AND "
            "job_run_id IS NULL AND sync_run_id IS NULL)",
        )
        batch.create_check_constraint(
            "ck_scheduled_sync_occurrence_reconcile_quarantined_state",
            "reconcile_status <> 'quarantined' OR "
            "(job_run_id IS NULL AND sync_run_id IS NULL AND "
            "reconcile_error_code IS NOT NULL)",
        )
        batch.create_index(
            "ix_scheduled_sync_occurrence_reconcile_due",
            [
                "reconcile_status",
                "reconcile_next_attempt_at",
                "sync_config_id",
                "scheduled_job_id",
                "scheduled_for",
                "org_id",
            ],
            postgresql_where=sa.text("reconcile_status IN ('pending', 'retry')"),
        )


def downgrade() -> None:
    with op.batch_alter_table("scheduled_sync_occurrences") as batch:
        batch.drop_index("ix_scheduled_sync_occurrence_reconcile_due")
        batch.drop_constraint(
            "ck_scheduled_sync_occurrence_reconcile_quarantined_state", type_="check"
        )
        batch.drop_constraint(
            "ck_scheduled_sync_occurrence_reconcile_completed_state", type_="check"
        )
        batch.drop_constraint(
            "ck_scheduled_sync_occurrence_reconcile_error_state", type_="check"
        )
        batch.drop_constraint(
            "ck_scheduled_sync_occurrence_reconcile_error_code", type_="check"
        )
        batch.drop_constraint(
            "ck_scheduled_sync_occurrence_reconcile_status", type_="check"
        )
        batch.drop_constraint(
            "ck_scheduled_sync_occurrence_reconcile_attempt_count", type_="check"
        )
        batch.drop_column("reconcile_status")
        batch.drop_column("reconcile_error_at")
        batch.drop_column("reconcile_error_code")
        batch.drop_column("reconcile_next_attempt_at")
        batch.drop_column("reconcile_attempt_count")
