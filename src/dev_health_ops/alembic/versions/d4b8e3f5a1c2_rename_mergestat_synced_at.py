"""rename mergestat_synced_at

Revision ID: d4b8e3f5a1c2
Revises: c3a7b2d4e5f6
Create Date: 2025-12-30 00:00:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "d4b8e3f5a1c2"
down_revision = "c3a7b2d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Analytics tables now in ClickHouse only - no-op for migration chain
    pass


def downgrade() -> None:
    pass
