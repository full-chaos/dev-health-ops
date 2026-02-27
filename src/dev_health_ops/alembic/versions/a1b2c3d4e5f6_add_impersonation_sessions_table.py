"""Add impersonation_sessions table.

Revision ID: a1b2c3d4e5f6
Revises: u1b2c3d4e5f6
Create Date: 2026-02-27
"""

from typing import Union, Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "u1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "impersonation_sessions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "admin_user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "target_user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "target_org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("target_role", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ip_address", sa.String(length=45), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "admin_user_id != target_user_id",
            name="ck_impersonation_not_self",
        ),
    )

    op.create_index(
        "ix_impersonation_sessions_admin_user_id",
        "impersonation_sessions",
        ["admin_user_id"],
        unique=False,
    )
    op.create_index(
        "ix_impersonation_sessions_target_user_id",
        "impersonation_sessions",
        ["target_user_id"],
        unique=False,
    )
    op.create_index(
        "ix_impersonation_sessions_target_org_id",
        "impersonation_sessions",
        ["target_org_id"],
        unique=False,
    )

    # Partial index for fast lookup of active sessions by admin_id
    op.create_index(
        "ix_impersonation_sessions_admin_active",
        "impersonation_sessions",
        ["admin_user_id"],
        unique=False,
        postgresql_where=sa.text("ended_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_impersonation_sessions_admin_active",
        table_name="impersonation_sessions",
    )
    op.drop_index(
        "ix_impersonation_sessions_target_org_id",
        table_name="impersonation_sessions",
    )
    op.drop_index(
        "ix_impersonation_sessions_target_user_id",
        table_name="impersonation_sessions",
    )
    op.drop_index(
        "ix_impersonation_sessions_admin_user_id",
        table_name="impersonation_sessions",
    )
    op.drop_table("impersonation_sessions")
