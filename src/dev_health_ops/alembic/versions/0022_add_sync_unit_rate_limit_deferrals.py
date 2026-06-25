"""Add sync run unit rate-limit deferral columns.

Retry safety: the deferral columns are guarded individually so a rerun after a
partial failure between column creation and concurrent index creation can resume
instead of failing on duplicate columns.

Concurrent-index retry safety: a ``CREATE INDEX CONCURRENTLY`` that fails midway
leaves an INVALID index of the same name, and a plain
``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` would then silently skip it. The
upgrade drops any leftover index of that name with
``DROP INDEX CONCURRENTLY IF EXISTS`` before creating, guaranteeing a retry
always yields a VALID index. The downgrade mirrors the column handling by
dropping columns only when present.

Revision ID: 0022
Revises: 0021
Create Date: 2026-06-24 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0022"
down_revision: str | None = "0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    _add_column_if_missing(
        "sync_run_units",
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column(
            "rate_limit_deferrals",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column(
            "rate_limit_first_seen_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS ix_sync_run_units_status_available"
        )
        op.create_index(
            "ix_sync_run_units_status_available",
            "sync_run_units",
            ["sync_run_id", "status", "available_at"],
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_sync_run_units_status_available",
            table_name="sync_run_units",
            postgresql_concurrently=True,
            if_exists=True,
        )
    _drop_column_if_present("sync_run_units", "rate_limit_first_seen_at")
    _drop_column_if_present("sync_run_units", "rate_limit_deferrals")
    _drop_column_if_present("sync_run_units", "available_at")


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
