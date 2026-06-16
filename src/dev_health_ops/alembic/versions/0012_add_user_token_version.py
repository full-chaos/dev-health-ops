"""Add token_version to users for JWT credential invalidation.

Revision ID: 0012
Revises: 0011
Create Date: 2026-06-16 00:00:00

Access JWTs embed the user's current integer token_version. Credential changes
increment the value, immediately invalidating already-issued access tokens whose
claim no longer matches the persisted anchor.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "users"
_COLUMN = "token_version"


def _has_column(conn: sa.Connection, table: str, column: str) -> bool:
    inspector = sa.inspect(conn)
    if table not in inspector.get_table_names():
        return True
    return any(col["name"] == column for col in inspector.get_columns(table))


def upgrade() -> None:
    conn = op.get_bind()
    if _has_column(conn, _TABLE, _COLUMN):
        return
    op.add_column(
        _TABLE,
        sa.Column(
            _COLUMN,
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if _TABLE not in inspector.get_table_names():
        return
    if any(col["name"] == _COLUMN for col in inspector.get_columns(_TABLE)):
        op.drop_column(_TABLE, _COLUMN)
