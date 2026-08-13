"""Add the durable report execution lease and reclaim counter.

Revision ID: 0098
Revises: 0097
Create Date: 2026-08-13 00:00:00

Existing ``running`` rows predate execution fencing. They intentionally keep a
NULL token and lease after this migration. Both report runtimes treat that
state as expired, so the first replacement worker can reclaim the same run ID
instead of leaving it permanently stranded.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from dev_health_ops.models.git import GUID

revision: str = "0098"
down_revision: str | None = "0097"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.add_column(
        "report_runs", sa.Column("execution_claim_token", GUID(), nullable=True)
    )
    op.add_column(
        "report_runs",
        sa.Column(
            "execution_lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "report_runs",
        sa.Column(
            "execution_reclaim_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.create_index(
        "ix_report_runs_execution_reclaim",
        "report_runs",
        ["status", "execution_lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_report_runs_execution_reclaim", table_name="report_runs")
    op.drop_column("report_runs", "execution_reclaim_count")
    op.drop_column("report_runs", "execution_lease_expires_at")
    op.drop_column("report_runs", "execution_claim_token")
