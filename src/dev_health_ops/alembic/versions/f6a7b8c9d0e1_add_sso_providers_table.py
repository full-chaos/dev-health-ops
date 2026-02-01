"""Add sso_providers table for Enterprise SSO.

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-01-30

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, None] = "e5f6a7b8c9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sso_providers",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("protocol", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending_setup"),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "allow_idp_initiated", sa.Boolean(), nullable=False, server_default="false"
        ),
        sa.Column(
            "auto_provision_users", sa.Boolean(), nullable=False, server_default="true"
        ),
        sa.Column("default_role", sa.Text(), nullable=False, server_default="member"),
        sa.Column("config", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("encrypted_secrets", sa.JSON(), nullable=True),
        sa.Column("allowed_domains", sa.JSON(), nullable=True),
        sa.Column("last_metadata_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_unique_constraint(
        "uq_sso_provider_org_name", "sso_providers", ["org_id", "name"]
    )

    op.create_index("ix_sso_providers_org_id", "sso_providers", ["org_id"])
    op.create_index("ix_sso_providers_protocol", "sso_providers", ["protocol"])
    op.create_index(
        "ix_sso_providers_org_protocol", "sso_providers", ["org_id", "protocol"]
    )
    op.create_index(
        "ix_sso_providers_org_status", "sso_providers", ["org_id", "status"]
    )


def downgrade() -> None:
    op.drop_index("ix_sso_providers_org_status", table_name="sso_providers")
    op.drop_index("ix_sso_providers_org_protocol", table_name="sso_providers")
    op.drop_index("ix_sso_providers_protocol", table_name="sso_providers")
    op.drop_index("ix_sso_providers_org_id", table_name="sso_providers")
    op.drop_constraint("uq_sso_provider_org_name", "sso_providers", type_="unique")
    op.drop_table("sso_providers")
