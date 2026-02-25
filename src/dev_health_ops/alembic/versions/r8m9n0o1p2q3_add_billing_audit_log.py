"""Add billing_audit_log table.

Revision ID: r8m9n0o1p2q3
Revises: q7l8m9n0o1p2
Create Date: 2026-02-24
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "r8m9n0o1p2q3"
down_revision: Union[str, None] = "q7l8m9n0o1p2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "billing_audit_log",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id"),
            nullable=False,
        ),
        sa.Column(
            "actor_id", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True
        ),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("resource_id", UUID(as_uuid=True), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("stripe_event_id", sa.Text(), nullable=True),
        sa.Column("local_state", sa.JSON(), nullable=True),
        sa.Column("stripe_state", sa.JSON(), nullable=True),
        sa.Column("reconciliation_status", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
    )

    op.create_index("ix_billing_audit_log_org_id", "billing_audit_log", ["org_id"])
    op.create_index("ix_billing_audit_log_action", "billing_audit_log", ["action"])
    op.create_index(
        "ix_billing_audit_log_stripe_event_id", "billing_audit_log", ["stripe_event_id"]
    )
    op.create_index(
        "ix_billing_audit_log_org_created",
        "billing_audit_log",
        ["org_id", "created_at"],
    )
    op.create_index(
        "ix_billing_audit_log_resource",
        "billing_audit_log",
        ["resource_type", "resource_id"],
    )
    op.create_index(
        "ix_billing_audit_log_reconciliation_status",
        "billing_audit_log",
        ["reconciliation_status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_billing_audit_log_reconciliation_status", table_name="billing_audit_log"
    )
    op.drop_index("ix_billing_audit_log_resource", table_name="billing_audit_log")
    op.drop_index("ix_billing_audit_log_org_created", table_name="billing_audit_log")
    op.drop_index(
        "ix_billing_audit_log_stripe_event_id", table_name="billing_audit_log"
    )
    op.drop_index("ix_billing_audit_log_action", table_name="billing_audit_log")
    op.drop_index("ix_billing_audit_log_org_id", table_name="billing_audit_log")
    op.drop_table("billing_audit_log")
