"""Add durable payload-free worker operator audit records.

Revision ID: 0043
Revises: 0042
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0043"
down_revision: str | None = "0042"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "worker_operator_audits",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("credential_id", UUID(as_uuid=True), nullable=True),
        sa.Column("principal_type", sa.String(length=32), nullable=False),
        sa.Column("principal_id", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("resource_type", sa.String(length=32), nullable=False),
        sa.Column("resource_id", sa.String(length=256), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("correlation_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "principal_type = 'service_credential'",
            name="ck_worker_operator_audits_principal_type",
        ),
        sa.CheckConstraint(
            "action IN ('jobs.cancel', 'jobs.retry', 'queues.pause', "
            "'queues.resume', 'workers.drain')",
            name="ck_worker_operator_audits_action",
        ),
        sa.CheckConstraint(
            "status IN ('started', 'succeeded', 'failed', 'outcome_unknown')",
            name="ck_worker_operator_audits_status",
        ),
        sa.CheckConstraint(
            "(status = 'started' AND completed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed', 'outcome_unknown') "
            "AND completed_at IS NOT NULL)",
            name="ck_worker_operator_audits_completion",
        ),
        sa.ForeignKeyConstraint(
            ["credential_id"],
            ["internal_service_credentials.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_worker_operator_audits_credential_created",
        "worker_operator_audits",
        ["credential_id", "created_at"],
    )
    op.create_index(
        "ix_worker_operator_audits_correlation",
        "worker_operator_audits",
        ["correlation_id"],
    )


def downgrade() -> None:
    op.drop_table("worker_operator_audits")
