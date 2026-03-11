"""Add backfill_jobs table.

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-11 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backfill_jobs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.String(), nullable=False),
        sa.Column("sync_config_id", UUID(as_uuid=True), nullable=False),
        sa.Column("celery_task_id", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("since_date", sa.Date(), nullable=False),
        sa.Column("before_date", sa.Date(), nullable=False),
        sa.Column("total_chunks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_chunks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_chunks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["sync_config_id"],
            ["sync_configurations.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_backfill_jobs_org_id",
        "backfill_jobs",
        ["org_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_backfill_jobs_org_id", table_name="backfill_jobs")
    op.drop_table("backfill_jobs")
