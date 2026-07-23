"""Add durable execution idempotency state for Go worker jobs.

Revision ID: 0052
Revises: 0051
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0052"
down_revision = "0051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "worker_job_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_kind", sa.String(length=96), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=True),
        sa.Column("domain_type", sa.String(length=64), nullable=False),
        sa.Column("domain_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("claim_token", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", sa.String(length=16), nullable=True),
        sa.Column("error_category", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('running', 'retryable', 'succeeded', 'terminal')",
            name="ck_worker_job_run_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 1", name="ck_worker_job_run_attempt_count"
        ),
        sa.CheckConstraint(
            "(status = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL AND finished_at IS NULL) OR (status <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL AND finished_at IS NOT NULL)",
            name="ck_worker_job_run_claim_state",
        ),
        sa.CheckConstraint(
            "(result IS NULL AND error_category IS NULL) OR (result IS NOT NULL AND error_category IS NOT NULL)",
            name="ck_worker_job_run_result_state",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "job_kind", "idempotency_key", name="uq_worker_job_run_key"
        ),
    )
    op.create_index(
        "ix_worker_job_run_reclaim", "worker_job_runs", ["status", "lease_expires_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_worker_job_run_reclaim", table_name="worker_job_runs")
    op.drop_table("worker_job_runs")
