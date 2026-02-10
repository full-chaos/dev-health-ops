"""Convert job status columns from text to integer.

Revision ID: j0e1f2g3h4i5
Revises: i9d0e1f2g3h4
Create Date: 2026-02-09 23:55:00

Migrates scheduled_jobs.status, scheduled_jobs.last_run_status, and
job_runs.status from text labels to integer codes. The API layer maps
integers back to labels for external consumers.
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "j0e1f2g3h4i5"
down_revision: Union[str, None] = "i9d0e1f2g3h4"
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None

JOB_STATUS_MAP = {"active": 0, "paused": 1, "disabled": 2}
RUN_STATUS_MAP = {"pending": 0, "running": 1, "success": 2, "failed": 3, "cancelled": 4}

JOB_STATUS_REVERSE = {v: k for k, v in JOB_STATUS_MAP.items()}
RUN_STATUS_REVERSE = {v: k for k, v in RUN_STATUS_MAP.items()}


def upgrade() -> None:
    op.add_column("scheduled_jobs", sa.Column("status_int", sa.Integer, nullable=True))
    op.add_column(
        "scheduled_jobs", sa.Column("last_run_status_int", sa.Integer, nullable=True)
    )
    op.add_column("job_runs", sa.Column("status_int", sa.Integer, nullable=True))

    for text_val, int_val in JOB_STATUS_MAP.items():
        op.execute(
            f"UPDATE scheduled_jobs SET status_int = {int_val} WHERE status = '{text_val}'"
        )

    for text_val, int_val in RUN_STATUS_MAP.items():
        op.execute(
            f"UPDATE scheduled_jobs SET last_run_status_int = {int_val} WHERE last_run_status = '{text_val}'"
        )
        op.execute(
            f"UPDATE job_runs SET status_int = {int_val} WHERE status = '{text_val}'"
        )

    op.execute("UPDATE scheduled_jobs SET status_int = 0 WHERE status_int IS NULL")
    op.execute("UPDATE job_runs SET status_int = 0 WHERE status_int IS NULL")

    op.drop_column("scheduled_jobs", "status")
    op.drop_column("scheduled_jobs", "last_run_status")
    op.drop_column("job_runs", "status")

    op.alter_column(
        "scheduled_jobs", "status_int", new_column_name="status", nullable=False
    )
    op.alter_column(
        "scheduled_jobs", "last_run_status_int", new_column_name="last_run_status"
    )
    op.alter_column("job_runs", "status_int", new_column_name="status", nullable=False)

    op.create_index("ix_job_runs_status", "job_runs", ["status"])


def downgrade() -> None:
    op.drop_index("ix_job_runs_status", table_name="job_runs")

    op.add_column("scheduled_jobs", sa.Column("status_text", sa.Text, nullable=True))
    op.add_column(
        "scheduled_jobs", sa.Column("last_run_status_text", sa.Text, nullable=True)
    )
    op.add_column("job_runs", sa.Column("status_text", sa.Text, nullable=True))

    for text_val, int_val in JOB_STATUS_MAP.items():
        op.execute(
            f"UPDATE scheduled_jobs SET status_text = '{text_val}' WHERE status = {int_val}"
        )

    for text_val, int_val in RUN_STATUS_MAP.items():
        op.execute(
            f"UPDATE scheduled_jobs SET last_run_status_text = '{text_val}' WHERE last_run_status = {int_val}"
        )
        op.execute(
            f"UPDATE job_runs SET status_text = '{text_val}' WHERE status = {int_val}"
        )

    op.execute(
        "UPDATE scheduled_jobs SET status_text = 'active' WHERE status_text IS NULL"
    )
    op.execute("UPDATE job_runs SET status_text = 'pending' WHERE status_text IS NULL")

    op.drop_column("scheduled_jobs", "status")
    op.drop_column("scheduled_jobs", "last_run_status")
    op.drop_column("job_runs", "status")

    op.alter_column(
        "scheduled_jobs", "status_text", new_column_name="status", nullable=False
    )
    op.alter_column(
        "scheduled_jobs", "last_run_status_text", new_column_name="last_run_status"
    )
    op.alter_column("job_runs", "status_text", new_column_name="status", nullable=False)

    op.create_index("ix_job_runs_status", "job_runs", ["status"])
