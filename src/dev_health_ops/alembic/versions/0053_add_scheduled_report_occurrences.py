"""Add durable report occurrence and execution idempotency metadata.

Revision ID: 0053
Revises: 0052
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from dev_health_ops.models.git import GUID

revision: str = "0053"
down_revision: str | None = "0052"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "scheduled_report_occurrences",
        sa.Column("occurrence_id", sa.Text(), nullable=False),
        sa.Column("identity_version", sa.Text(), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("report_id", GUID(), nullable=False),
        sa.Column("scheduled_job_id", GUID(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("report_run_id", GUID(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["report_id"], ["saved_reports.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["scheduled_job_id"], ["scheduled_jobs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["report_run_id"], ["report_runs.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("occurrence_id"),
        sa.UniqueConstraint(
            "report_id",
            "scheduled_for",
            name="uq_scheduled_report_occurrence_report_time",
        ),
        sa.UniqueConstraint("report_run_id"),
    )
    op.create_index(
        "ix_scheduled_report_occurrences_org_id",
        "scheduled_report_occurrences",
        ["org_id"],
    )
    op.create_index(
        "ix_scheduled_report_occurrence_org_report_time",
        "scheduled_report_occurrences",
        ["org_id", "report_id", "scheduled_for"],
    )
    op.add_column(
        "report_runs", sa.Column("scheduled_occurrence_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "report_runs",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "report_runs", sa.Column("artifact_fingerprint", sa.Text(), nullable=True)
    )
    op.add_column(
        "report_runs", sa.Column("notification_key", sa.Text(), nullable=True)
    )
    op.add_column(
        "report_runs",
        sa.Column(
            "notification_status", sa.Text(), nullable=False, server_default="pending"
        ),
    )
    op.add_column(
        "report_runs",
        sa.Column("notification_sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_report_runs_scheduled_occurrence_id",
        "report_runs",
        "scheduled_report_occurrences",
        ["scheduled_occurrence_id"],
        ["occurrence_id"],
        ondelete="SET NULL",
    )
    op.create_unique_constraint(
        "uq_report_runs_scheduled_occurrence_id",
        "report_runs",
        ["scheduled_occurrence_id"],
    )
    op.create_unique_constraint(
        "uq_report_runs_notification_key", "report_runs", ["notification_key"]
    )
    op.create_index(
        "ix_report_runs_notification_key", "report_runs", ["notification_key"]
    )


def downgrade() -> None:
    op.drop_index("ix_report_runs_notification_key", table_name="report_runs")
    op.drop_constraint("uq_report_runs_notification_key", "report_runs", type_="unique")
    op.drop_constraint(
        "uq_report_runs_scheduled_occurrence_id", "report_runs", type_="unique"
    )
    op.drop_constraint(
        "fk_report_runs_scheduled_occurrence_id", "report_runs", type_="foreignkey"
    )
    op.drop_column("report_runs", "notification_sent_at")
    op.drop_column("report_runs", "notification_key")
    op.drop_column("report_runs", "notification_status")
    op.drop_column("report_runs", "artifact_fingerprint")
    op.drop_column("report_runs", "attempt_count")
    op.drop_column("report_runs", "scheduled_occurrence_id")
    op.drop_index(
        "ix_scheduled_report_occurrence_org_report_time",
        table_name="scheduled_report_occurrences",
    )
    op.drop_index(
        "ix_scheduled_report_occurrences_org_id",
        table_name="scheduled_report_occurrences",
    )
    op.drop_table("scheduled_report_occurrences")
