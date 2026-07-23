"""Add durable scheduled sync occurrence identities.

Revision ID: 0050
Revises: 0049
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from dev_health_ops.models.git import GUID

revision = "0050"
down_revision = "0049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scheduled_sync_occurrences",
        sa.Column("occurrence_id", sa.Text(), nullable=False),
        sa.Column("identity_version", sa.Text(), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_config_id", GUID(), nullable=False),
        sa.Column("scheduled_job_id", GUID(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("job_run_id", GUID(), nullable=True),
        sa.Column("sync_run_id", GUID(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "(job_run_id IS NULL AND sync_run_id IS NULL) OR "
            "(job_run_id IS NOT NULL AND sync_run_id IS NOT NULL)",
            name="ck_scheduled_sync_occurrence_plan_links",
        ),
        sa.ForeignKeyConstraint(
            ["job_run_id"],
            ["job_runs.id"],
            name="fk_scheduled_sync_occurrences_job_run_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["scheduled_job_id"],
            ["scheduled_jobs.id"],
            name="fk_scheduled_sync_occurrences_scheduled_job_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["sync_config_id"],
            ["sync_configurations.id"],
            name="fk_scheduled_sync_occurrences_sync_config_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["sync_run_id"],
            ["sync_runs.id"],
            name="fk_scheduled_sync_occurrences_sync_run_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("occurrence_id", name="pk_scheduled_sync_occurrences"),
        sa.UniqueConstraint(
            "sync_config_id",
            "scheduled_for",
            name="uq_scheduled_sync_occurrence_config_time",
        ),
    )
    op.create_index(
        "ix_scheduled_sync_occurrences_org_id",
        "scheduled_sync_occurrences",
        ["org_id"],
    )
    op.create_index(
        "ix_scheduled_sync_occurrence_org_config_time",
        "scheduled_sync_occurrences",
        ["org_id", "sync_config_id", "scheduled_for"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_scheduled_sync_occurrence_org_config_time",
        table_name="scheduled_sync_occurrences",
    )
    op.drop_index(
        "ix_scheduled_sync_occurrences_org_id",
        table_name="scheduled_sync_occurrences",
    )
    op.drop_table("scheduled_sync_occurrences")
