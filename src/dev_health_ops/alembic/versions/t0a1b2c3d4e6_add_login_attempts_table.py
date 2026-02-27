"""Add login_attempts table for account lockout tracking.

Revision ID: t0a1b2c3d4e6
Revises: t0a1b2c3d4e5
Create Date: 2026-02-26
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "t0a1b2c3d4e6"
down_revision: Union[str, None] = "t0a1b2c3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "login_attempts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
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
        sa.UniqueConstraint("email", name="uq_login_attempts_email"),
    )

    op.create_index("ix_login_attempts_email", "login_attempts", ["email"])
    op.create_index(
        "ix_login_attempts_locked_until",
        "login_attempts",
        ["locked_until"],
    )


def downgrade() -> None:
    op.drop_index("ix_login_attempts_locked_until", table_name="login_attempts")
    op.drop_index("ix_login_attempts_email", table_name="login_attempts")
    op.drop_table("login_attempts")
