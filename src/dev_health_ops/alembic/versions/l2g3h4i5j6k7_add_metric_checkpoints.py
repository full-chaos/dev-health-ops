"""Add metric_checkpoints table.

Revision ID: l2g3h4i5j6k7
Revises: k1f2g3h4i5j6
Create Date: 2026-02-12 10:00:00

Adds the metric_checkpoints table for tracking metrics computation
watermarks.  Enables resume-on-failure and distributed coordination
for the partitioned daily-metrics pipeline (gh-422, gh-423).
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "l2g3h4i5j6k7"
down_revision: Union[str, None] = "k1f2g3h4i5j6"
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None


def upgrade() -> None:
    op.create_table(
        "metric_checkpoints",
        sa.Column("id", sa.dialects.postgresql.UUID(), primary_key=True),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("repo_id", sa.dialects.postgresql.UUID(), nullable=True),
        sa.Column(
            "metric_type",
            sa.Text(),
            nullable=False,
            comment="Computation scope: daily_batch, daily_finalize, rebuild",
        ),
        sa.Column(
            "day",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Target date",
        ),
        sa.Column(
            "status",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="0=pending, 1=running, 2=completed, 3=failed",
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "worker_id",
            sa.Text(),
            nullable=True,
            comment="Celery task ID for distributed locking",
        ),
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
        sa.UniqueConstraint(
            "org_id",
            "repo_id",
            "metric_type",
            "day",
            name="uq_checkpoint_scope",
        ),
    )
    op.create_index(
        "ix_checkpoint_status_day",
        "metric_checkpoints",
        ["status", "day"],
    )
    op.create_index(
        "ix_checkpoint_org_type_day",
        "metric_checkpoints",
        ["org_id", "metric_type", "day"],
    )


def downgrade() -> None:
    op.drop_index("ix_checkpoint_org_type_day", table_name="metric_checkpoints")
    op.drop_index("ix_checkpoint_status_day", table_name="metric_checkpoints")
    op.drop_table("metric_checkpoints")
