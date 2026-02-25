"""Add refunds table for Stripe refund tracking.

Revision ID: q7l8m9n0o1p2
Revises: p6k7l8m9n0o1
Create Date: 2026-02-24 15:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "q7l8m9n0o1p2"
down_revision = "p6k7l8m9n0o1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "refunds",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("invoice_id", UUID(as_uuid=True), nullable=True),
        sa.Column("subscription_id", UUID(as_uuid=True), nullable=True),
        sa.Column("stripe_refund_id", sa.Text(), nullable=False),
        sa.Column("stripe_charge_id", sa.Text(), nullable=False),
        sa.Column("stripe_payment_intent_id", sa.Text(), nullable=True),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False, server_default="usd"),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("initiated_by", UUID(as_uuid=True), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["invoice_id"], ["invoices.id"]),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"]),
        sa.ForeignKeyConstraint(["initiated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_refund_id", name="uq_refunds_stripe_refund_id"),
    )
    op.create_index("ix_refunds_org_id", "refunds", ["org_id"])
    op.create_index("ix_refunds_invoice_id", "refunds", ["invoice_id"])
    op.create_index("ix_refunds_subscription_id", "refunds", ["subscription_id"])
    op.create_index("ix_refunds_stripe_refund_id", "refunds", ["stripe_refund_id"])
    op.create_index("ix_refunds_stripe_charge_id", "refunds", ["stripe_charge_id"])


def downgrade() -> None:
    op.drop_index("ix_refunds_stripe_charge_id", table_name="refunds")
    op.drop_index("ix_refunds_stripe_refund_id", table_name="refunds")
    op.drop_index("ix_refunds_subscription_id", table_name="refunds")
    op.drop_index("ix_refunds_invoice_id", table_name="refunds")
    op.drop_index("ix_refunds_org_id", table_name="refunds")
    op.drop_table("refunds")
