"""Add CI/CD, deployment, and incident tables

Revision ID: c3a7b2d4e5f6
Revises: 9f2f70b48ab5
Create Date: 2025-09-17 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c3a7b2d4e5f6"
down_revision: Union[str, None] = "9f2f70b48ab5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Analytics tables now in ClickHouse only - no-op for migration chain
    pass


def downgrade() -> None:
    pass
