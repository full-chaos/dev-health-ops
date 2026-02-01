"""Add org_retention_policies table

Revision ID: h8c9d0e1f2g3
Revises: g7b8c9d0e1f2
Create Date: 2026-01-30 18:51:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "h8c9d0e1f2g3"
down_revision: Union[str, None] = "g7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "org_retention_policies",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("resource_type", sa.String(50), nullable=False),
        sa.Column("retention_days", sa.Integer(), nullable=False, default=90),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_deleted_count", sa.Integer(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_id", sa.UUID(), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint(
            "org_id", "resource_type", name="uq_org_retention_resource"
        ),
    )

    op.create_index(
        "ix_retention_policies_org_id",
        "org_retention_policies",
        ["org_id"],
    )
    op.create_index(
        "ix_retention_policies_org_active",
        "org_retention_policies",
        ["org_id", "is_active"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_retention_policies_org_active", table_name="org_retention_policies"
    )
    op.drop_index("ix_retention_policies_org_id", table_name="org_retention_policies")
    op.drop_table("org_retention_policies")
