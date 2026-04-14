"""Add security_alerts table.

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-14 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "security_alerts",
        sa.Column(
            "repo_id",
            UUID(as_uuid=True),
            sa.ForeignKey("repos.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("alert_id", sa.Text(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("package_name", sa.Text(), nullable=True),
        sa.Column("cve_id", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fixed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dismissed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_synced", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_security_alerts_source",
        "security_alerts",
        ["source"],
    )
    op.create_index(
        "ix_security_alerts_severity",
        "security_alerts",
        ["severity"],
    )


def downgrade() -> None:
    op.drop_index("ix_security_alerts_severity", table_name="security_alerts")
    op.drop_index("ix_security_alerts_source", table_name="security_alerts")
    op.drop_table("security_alerts")
