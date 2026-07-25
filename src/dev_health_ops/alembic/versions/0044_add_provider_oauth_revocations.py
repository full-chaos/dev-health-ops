"""Add durable encrypted OAuth revocation retry records.

Revision ID: 0044
Revises: 0043
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0044"
down_revision: str | None = "0043"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "provider_oauth_revocations",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("credential_name", sa.Text(), nullable=False),
        sa.Column("purpose", sa.Text(), nullable=False),
        sa.Column("token_encrypted", sa.Text(), nullable=False),
        sa.Column("token_key_version", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "purpose IN ('replacement', 'disconnect')",
            name="ck_provider_oauth_revocations_purpose",
        ),
        sa.CheckConstraint(
            "status = 'pending'",
            name="ck_provider_oauth_revocations_status",
        ),
    )
    op.create_index(
        "ix_provider_oauth_revocations_org_id",
        "provider_oauth_revocations",
        ["org_id"],
    )
    op.create_index(
        "ix_provider_oauth_revocations_pending",
        "provider_oauth_revocations",
        ["org_id", "provider", "credential_name", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_provider_oauth_revocations_pending",
        table_name="provider_oauth_revocations",
    )
    op.drop_index(
        "ix_provider_oauth_revocations_org_id",
        table_name="provider_oauth_revocations",
    )
    op.drop_table("provider_oauth_revocations")
