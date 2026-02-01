"""add pr body column

Revision ID: e1f2a3b4c5d6
Revises: d4b8e3f5a1c2
Create Date: 2025-01-01 00:00:00.000000

"""

from alembic import op
from sqlalchemy import Column, Text


# revision identifiers, used by Alembic.
revision = "e1f2a3b4c5d6"
down_revision = "d4b8e3f5a1c2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Analytics tables now in ClickHouse only - no-op for migration chain
    pass


def downgrade() -> None:
    pass
