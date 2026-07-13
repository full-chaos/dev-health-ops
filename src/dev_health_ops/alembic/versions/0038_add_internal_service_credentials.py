"""Add dedicated internal service credentials for ACR entitlement lookups.

Revision ID: 0038
Revises: 0037
Create Date: 2026-07-13 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0038"
down_revision: str | None = "0037"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "internal_service_credentials",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("service_name", sa.Text(), nullable=False),
        sa.Column("token_hash", sa.Text(), nullable=False, unique=True),
        sa.Column("token_prefix", sa.Text(), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("created_by_user_id", UUID(as_uuid=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"], ondelete="SET NULL"
        ),
    )
    op.create_index(
        "ix_internal_service_credentials_service_active",
        "internal_service_credentials",
        ["service_name", "revoked_at"],
    )
    op.create_index(
        "ix_internal_service_credentials_token_hash",
        "internal_service_credentials",
        ["token_hash"],
        unique=True,
    )
    op.create_table(
        "internal_service_credential_audits",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("credential_id", UUID(as_uuid=True), nullable=True),
        sa.Column("requested_org_id", sa.Text(), nullable=True),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["credential_id"], ["internal_service_credentials.id"], ondelete="SET NULL"
        ),
    )
    op.create_index(
        "ix_internal_service_credential_audits_credential_id",
        "internal_service_credential_audits",
        ["credential_id"],
    )
    op.create_index(
        "ix_internal_service_credential_audits_requested_org_id",
        "internal_service_credential_audits",
        ["requested_org_id"],
    )


def downgrade() -> None:
    op.drop_table("internal_service_credential_audits")
    op.drop_table("internal_service_credentials")
