"""Add org_ip_allowlist table for Enterprise IP restriction.

Revision ID: g7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-01-30

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "g7b8c9d0e1f2"
down_revision: Union[str, None] = "f6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "org_ip_allowlist",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("ip_range", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_by_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
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
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_unique_constraint(
        "uq_org_ip_allowlist_org_range", "org_ip_allowlist", ["org_id", "ip_range"]
    )

    op.create_index("ix_org_ip_allowlist_org_id", "org_ip_allowlist", ["org_id"])
    op.create_index(
        "ix_org_ip_allowlist_org_active", "org_ip_allowlist", ["org_id", "is_active"]
    )


def downgrade() -> None:
    op.drop_index("ix_org_ip_allowlist_org_active", table_name="org_ip_allowlist")
    op.drop_index("ix_org_ip_allowlist_org_id", table_name="org_ip_allowlist")
    op.drop_constraint(
        "uq_org_ip_allowlist_org_range", "org_ip_allowlist", type_="unique"
    )
    op.drop_table("org_ip_allowlist")
