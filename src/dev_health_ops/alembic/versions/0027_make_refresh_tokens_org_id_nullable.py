"""Allow orgless refresh-token records.

Revision ID: 0027
Revises: 0026
Create Date: 2026-06-26 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027"
down_revision: str | None = "0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.alter_column(
        "refresh_tokens",
        "org_id",
        existing_type=sa.UUID(),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "refresh_tokens",
        "org_id",
        existing_type=sa.UUID(),
        nullable=False,
    )
