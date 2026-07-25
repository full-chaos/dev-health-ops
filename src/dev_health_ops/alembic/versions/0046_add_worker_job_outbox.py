"""Add the language-neutral worker job outbox.

Revision ID: 0046
Revises: 0045
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0046"
down_revision: str | None = "0045"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "worker_job_outbox",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("dedupe_key", sa.String(length=256), nullable=False),
        sa.Column("job_kind", sa.String(length=96), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("args", sa.JSON(), nullable=False),
        sa.Column("payload_hash", sa.String(length=71), nullable=False),
        sa.Column("queue", sa.String(length=96), nullable=False),
        sa.Column("priority", sa.SmallInteger(), nullable=False),
        sa.Column("max_attempts", sa.SmallInteger(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("claim_token", UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.Column("last_error_detail", sa.String(length=256), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("river_job_id", sa.BigInteger(), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'claimed', 'delivered', 'dead')",
            name="ck_worker_job_outbox_status",
        ),
        sa.CheckConstraint(
            "contract_version > 0",
            name="ck_worker_job_outbox_contract_version",
        ),
        sa.CheckConstraint(
            "priority BETWEEN 1 AND 4",
            name="ck_worker_job_outbox_priority",
        ),
        sa.CheckConstraint(
            "max_attempts BETWEEN 1 AND 25",
            name="ck_worker_job_outbox_max_attempts",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_worker_job_outbox_attempt_count",
        ),
        sa.CheckConstraint(
            "length(payload_hash) = 71 AND payload_hash LIKE 'sha256:%'",
            name="ck_worker_job_outbox_payload_hash",
        ),
        sa.CheckConstraint(
            "length(CAST(args AS TEXT)) <= 16384",
            name="ck_worker_job_outbox_args_size",
        ),
        sa.CheckConstraint(
            "(status = 'claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL "
            "AND claim_expires_at IS NOT NULL) OR "
            "(status <> 'claimed' AND claim_token IS NULL AND claimed_at IS NULL "
            "AND claim_expires_at IS NULL)",
            name="ck_worker_job_outbox_claim_state",
        ),
        sa.CheckConstraint(
            "(status = 'delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL) "
            "OR (status <> 'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)",
            name="ck_worker_job_outbox_delivery_state",
        ),
        sa.CheckConstraint(
            "(last_error_code IS NULL AND last_error_detail IS NULL AND last_error_at IS NULL) "
            "OR (last_error_code IS NOT NULL AND last_error_detail IS NOT NULL "
            "AND last_error_at IS NOT NULL)",
            name="ck_worker_job_outbox_error_state",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedupe_key", name="uq_worker_job_outbox_dedupe_key"),
        sa.UniqueConstraint("river_job_id", name="uq_worker_job_outbox_river_job_id"),
    )
    op.create_index(
        "ix_worker_job_outbox_due",
        "worker_job_outbox",
        ["status", "next_attempt_at", "scheduled_at", "created_at"],
        postgresql_where=sa.text("status IN ('pending', 'claimed')"),
    )
    op.create_index(
        "ix_worker_job_outbox_claim_expiry",
        "worker_job_outbox",
        ["claim_expires_at"],
        postgresql_where=sa.text("status = 'claimed'"),
    )
    op.create_index(
        "ix_worker_job_outbox_terminal",
        "worker_job_outbox",
        ["status", "delivered_at", "updated_at"],
        postgresql_where=sa.text("status IN ('delivered', 'dead')"),
    )


def downgrade() -> None:
    op.drop_table("worker_job_outbox")
