"""Add sync unit expired-lease retry columns.

Revision ID: 0028
Revises: 0027
Create Date: 2026-06-28 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028"
down_revision: str | None = "0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    _add_column_if_missing(
        "sync_run_units",
        sa.Column(
            "expired_lease_retry_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column("last_retry_reason", sa.Text(), nullable=True),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column("retry_exhausted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    _drop_column_if_present("sync_run_units", "retry_exhausted_at")
    _drop_column_if_present("sync_run_units", "last_retry_reason")
    _drop_column_if_present("sync_run_units", "expired_lease_retry_count")


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    existing_columns = _column_names(table_name)
    if column.name not in existing_columns:
        op.add_column(table_name, column)


def _drop_column_if_present(table_name: str, column_name: str) -> None:
    existing_columns = _column_names(table_name)
    if column_name in existing_columns:
        op.drop_column(table_name, column_name)


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}
