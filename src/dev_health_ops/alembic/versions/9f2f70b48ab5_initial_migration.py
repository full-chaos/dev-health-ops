"""Initial migration

Revision ID: 9f2f70b48ab5
Revises:
Create Date: 2025-04-25 15:35:03.861966

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9f2f70b48ab5"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema.

    NOTE: Analytics tables (git_*, repos) are now in ClickHouse only.
    This migration is kept as a no-op to preserve the migration chain.
    """
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
