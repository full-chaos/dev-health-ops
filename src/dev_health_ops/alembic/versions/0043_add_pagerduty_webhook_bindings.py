"""Add PagerDuty webhook binding lifecycle table.

Revision ID: 0043
Revises: 0042
"""

from __future__ import annotations

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
        "pagerduty_webhook_bindings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("integration_source_id", UUID(as_uuid=True), nullable=False),
        sa.Column("credential_id", UUID(as_uuid=True), nullable=True),
        sa.Column("provider_subscription_id", sa.Text(), nullable=False),
        sa.Column("signing_secret_encrypted", sa.Text(), nullable=False),
        sa.Column("signing_secret_key_version", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="candidate"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["integration_source_id"], ["integration_sources.id"]),
        sa.ForeignKeyConstraint(
            ["credential_id"], ["integration_credentials.id"], ondelete="SET NULL"
        ),
        sa.CheckConstraint(
            "status IN ('candidate', 'ready', 'active', 'inactive')",
            name="ck_pagerduty_webhook_bindings_status",
        ),
        sa.CheckConstraint(
            "status != 'active' OR length(trim(provider_subscription_id)) > 0",
            name="ck_pagerduty_webhook_bindings_active_subscription_required",
        ),
        sa.CheckConstraint(
            "status != 'active' OR credential_id IS NOT NULL",
            name="ck_pagerduty_webhook_bindings_active_credential_required",
        ),
    )
    op.create_index(
        "ix_pagerduty_webhook_bindings_org_id",
        "pagerduty_webhook_bindings",
        ["org_id"],
    )
    op.create_index(
        "ix_pagerduty_webhook_bindings_integration_source_id",
        "pagerduty_webhook_bindings",
        ["integration_source_id"],
    )
    op.create_index(
        "ix_pagerduty_webhook_bindings_credential_id",
        "pagerduty_webhook_bindings",
        ["credential_id"],
    )
    op.create_index(
        "uq_pagerduty_webhook_bindings_active_source",
        "pagerduty_webhook_bindings",
        ["org_id", "integration_source_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_index(
        "uq_pagerduty_webhook_bindings_active_subscription",
        "pagerduty_webhook_bindings",
        ["org_id", "provider_subscription_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_pagerduty_webhook_bindings_active_subscription",
        table_name="pagerduty_webhook_bindings",
    )
    op.drop_index(
        "uq_pagerduty_webhook_bindings_active_source",
        table_name="pagerduty_webhook_bindings",
    )
    op.drop_index(
        "ix_pagerduty_webhook_bindings_credential_id",
        table_name="pagerduty_webhook_bindings",
    )
    op.drop_index(
        "ix_pagerduty_webhook_bindings_integration_source_id",
        table_name="pagerduty_webhook_bindings",
    )
    op.drop_index(
        "ix_pagerduty_webhook_bindings_org_id",
        table_name="pagerduty_webhook_bindings",
    )
    op.drop_table("pagerduty_webhook_bindings")
