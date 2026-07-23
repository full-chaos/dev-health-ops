"""Persist generic webhook and billing delivery references.

Revision ID: 0054
Revises: 0053
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=16), nullable=False),
        sa.Column("delivery_key", sa.String(length=256), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("raw_event_type", sa.String(length=256), nullable=False),
        sa.Column("org_ref", sa.String(length=256), nullable=True),
        sa.Column("repo_name", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "provider IN ('github', 'gitlab', 'jira')",
            name="ck_webhook_delivery_provider",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "delivery_key", name="uq_webhook_delivery_key"),
    )
    op.create_index("ix_webhook_delivery_created", "webhook_deliveries", ["created_at"])
    op.create_table(
        "billing_notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("notification_type", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("attributes", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_billing_notification_key"),
    )
    op.create_index(
        "ix_billing_notification_org_created",
        "billing_notifications",
        ["org_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_billing_notification_org_created", table_name="billing_notifications"
    )
    op.drop_table("billing_notifications")
    op.drop_index("ix_webhook_delivery_created", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
