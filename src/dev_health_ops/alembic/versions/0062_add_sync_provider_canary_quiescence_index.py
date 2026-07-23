"""Index the bounded Celery canary-quiescence probe.

Revision ID: 0062
Revises: 0061
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0062"
down_revision: str | None = "0061"
branch_labels = None
depends_on = None

_INDEX = "ix_sync_run_units_canary_quiescence"
_ACTIVE = "status IN ('dispatching', 'running')"


def upgrade() -> None:
    op.create_index(
        _INDEX,
        "sync_run_units",
        ["status", "provider", "dataset_key"],
        unique=False,
        postgresql_where=sa.text(_ACTIVE),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="sync_run_units")
