"""Add durable state for remaining metric family partitions.

Revision ID: 0058_add_remaining_metric_river_state
Revises: 0057_add_daily_metrics_river_state
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0058"
down_revision: str | None = "0057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "remaining_metric_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("family", sa.Text(), nullable=False),
        sa.Column("generation", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("generation_seed", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.statement_timestamp(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.statement_timestamp(),
        ),
        sa.CheckConstraint(
            "family IN ('capacity', 'complexity', 'dora', 'extra_metrics', "
            "'membership_backfill', 'recommendations', 'release_impact', "
            "'team_metrics')",
            name="ck_remaining_metric_run_family",
        ),
        sa.CheckConstraint(
            "length(generation) BETWEEN 1 AND 128",
            name="ck_remaining_metric_run_generation",
        ),
        sa.CheckConstraint(
            "length(scope_key) BETWEEN 1 AND 512",
            name="ck_remaining_metric_run_scope_key",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')",
            name="ck_remaining_metric_run_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "family",
            "generation",
            "scope_key",
            name="uq_remaining_metric_run_scope",
        ),
    )
    op.create_table(
        "remaining_metric_partitions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("scope", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("claim_token", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("output_evidence", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.statement_timestamp(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.statement_timestamp(),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["remaining_metric_runs.id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint(
            "ordinal >= 1", name="ck_remaining_metric_partition_ordinal"
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')",
            name="ck_remaining_metric_partition_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0", name="ck_remaining_metric_partition_attempt_count"
        ),
        sa.CheckConstraint(
            "output_evidence IS NULL OR length(output_evidence) BETWEEN 1 AND 4096",
            name="ck_remaining_metric_partition_evidence",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND claim_token IS NOT NULL AND "
            "lease_expires_at IS NOT NULL) OR (status <> 'running' AND "
            "claim_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_remaining_metric_partition_lease",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "ordinal", name="uq_remaining_metric_partition_ordinal"
        ),
    )
    op.create_index(
        "ix_remaining_metric_partitions_claim",
        "remaining_metric_partitions",
        ["status", "lease_expires_at", "run_id", "ordinal"],
    )


def downgrade() -> None:
    op.drop_table("remaining_metric_partitions", if_exists=True)
    op.drop_table("remaining_metric_runs", if_exists=True)
