"""Add durable daily-metrics run, partition, and finalize state.

Revision ID: 0057
Revises: 0056
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from dev_health_ops.models.git import GUID

revision: str = "0057"
down_revision: str | None = "0056"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "daily_metrics_runs",
        sa.Column("id", GUID(), nullable=False),
        sa.Column("org_id", GUID(), nullable=False),
        sa.Column("target_day", sa.Date(), nullable=False),
        sa.Column("generation", sa.String(length=64), nullable=False),
        sa.Column(
            "status", sa.String(length=16), nullable=False, server_default="pending"
        ),
        sa.Column(
            "finalization_status",
            sa.String(length=16),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("finalization_claim_token", GUID(), nullable=True),
        sa.Column(
            "finalization_lease_expires_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')",
            name="ck_daily_metrics_run_status",
        ),
        sa.CheckConstraint(
            "finalization_status IN ('pending', 'running', 'succeeded', 'failed')",
            name="ck_daily_metrics_finalize_status",
        ),
        sa.CheckConstraint(
            "(finalization_status = 'running' AND finalization_claim_token IS NOT NULL AND finalization_lease_expires_at IS NOT NULL) OR (finalization_status <> 'running' AND finalization_claim_token IS NULL AND finalization_lease_expires_at IS NULL)",
            name="ck_daily_metrics_finalize_lease",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id", "target_day", "generation", name="uq_daily_metrics_run_generation"
        ),
    )
    op.create_table(
        "daily_metrics_partitions",
        sa.Column("id", GUID(), nullable=False),
        sa.Column("run_id", GUID(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("repo_ids", sa.JSON(), nullable=False),
        sa.Column(
            "status", sa.String(length=16), nullable=False, server_default="pending"
        ),
        sa.Column("claim_token", GUID(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"], ["daily_metrics_runs.id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint("ordinal >= 0", name="ck_daily_metrics_partition_ordinal"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed')",
            name="ck_daily_metrics_partition_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0", name="ck_daily_metrics_partition_attempts"
        ),
        sa.CheckConstraint(
            "(status = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL) OR (status <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_daily_metrics_partition_lease",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "ordinal", name="uq_daily_metrics_partition_ordinal"
        ),
    )
    op.create_index(
        "ix_daily_metrics_partition_reclaim",
        "daily_metrics_partitions",
        ["status", "lease_expires_at"],
    )
    op.create_index(
        "ix_daily_metrics_partition_run_status",
        "daily_metrics_partitions",
        ["run_id", "status"],
    )


def downgrade() -> None:
    op.drop_table("daily_metrics_partitions")
    op.drop_table("daily_metrics_runs")
