"""Add saved_reports and report_runs tables.

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-10 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# Alembic reads these module-level variables at runtime for migration ordering.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "saved_reports",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="", index=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("report_plan", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "template_source_id",
            UUID(as_uuid=True),
            sa.ForeignKey("saved_reports.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("parameters", sa.JSON(), nullable=True),
        sa.Column(
            "schedule_id",
            UUID(as_uuid=True),
            sa.ForeignKey("scheduled_jobs.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_status", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("created_by", sa.Text(), nullable=True),
    )
    op.create_index("ix_saved_reports_org_name", "saved_reports", ["org_id", "name"])
    op.create_index(
        "ix_saved_reports_org_template", "saved_reports", ["org_id", "is_template"]
    )

    op.create_table(
        "report_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "report_id",
            UUID(as_uuid=True),
            sa.ForeignKey("saved_reports.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("rendered_markdown", sa.Text(), nullable=True),
        sa.Column("artifact_url", sa.Text(), nullable=True),
        sa.Column("provenance_records", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_traceback", sa.Text(), nullable=True),
        sa.Column("triggered_by", sa.Text(), nullable=False, server_default="manual"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_report_runs_report_created", "report_runs", ["report_id", "created_at"]
    )
    op.create_index("ix_report_runs_status", "report_runs", ["status"])


def downgrade() -> None:
    op.drop_index("ix_report_runs_status", table_name="report_runs")
    op.drop_index("ix_report_runs_report_created", table_name="report_runs")
    op.drop_table("report_runs")

    op.drop_index("ix_saved_reports_org_template", table_name="saved_reports")
    op.drop_index("ix_saved_reports_org_name", table_name="saved_reports")
    op.drop_table("saved_reports")
