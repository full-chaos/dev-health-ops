"""Add org_invites table.

Revision ID: v2c3d4e5f6g7
Revises: u1b2c3d4e5f6
Create Date: 2026-02-26
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "v2c3d4e5f6g7"
down_revision: Union[str, None] = "u1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "org_invites",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False, server_default="member"),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column(
            "invited_by_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.UniqueConstraint("token_hash", name="uq_org_invites_token_hash"),
    )

    op.create_index("ix_org_invites_org_id", "org_invites", ["org_id"])
    op.create_index("ix_org_invites_email", "org_invites", ["email"])
    op.create_index("ix_org_invites_token_hash", "org_invites", ["token_hash"])
    op.create_index("ix_org_invites_invited_by_id", "org_invites", ["invited_by_id"])
    op.create_index("ix_org_invites_status", "org_invites", ["status"])
    op.create_index(
        "ix_org_invites_org_email_status",
        "org_invites",
        ["org_id", "email", "status"],
    )
    op.create_index(
        "ix_org_invites_org_expires",
        "org_invites",
        ["org_id", "expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_org_invites_org_expires", table_name="org_invites")
    op.drop_index("ix_org_invites_org_email_status", table_name="org_invites")
    op.drop_index("ix_org_invites_status", table_name="org_invites")
    op.drop_index("ix_org_invites_invited_by_id", table_name="org_invites")
    op.drop_index("ix_org_invites_token_hash", table_name="org_invites")
    op.drop_index("ix_org_invites_email", table_name="org_invites")
    op.drop_index("ix_org_invites_org_id", table_name="org_invites")
    op.drop_table("org_invites")
