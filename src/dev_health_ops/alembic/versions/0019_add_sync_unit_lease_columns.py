"""Add sync run unit lease columns.

Retry safety: the lease columns are guarded individually so a rerun after a
partial failure between column creation and concurrent index creation can resume
instead of failing on duplicate columns. The downgrade mirrors this by dropping
columns only when present.

Revision ID: 0019
Revises: 0018
Create Date: 2026-06-20 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    _add_column_if_missing(
        "sync_run_units", sa.Column("lease_owner", sa.Text(), nullable=True)
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_sync_run_units_bucket_status_lease",
            "sync_run_units",
            ["org_id", "provider", "cost_class", "status", "lease_expires_at"],
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_sync_run_units_bucket_status_lease",
            table_name="sync_run_units",
            postgresql_concurrently=True,
            if_exists=True,
        )
    _drop_column_if_present("sync_run_units", "last_heartbeat_at")
    _drop_column_if_present("sync_run_units", "lease_expires_at")
    _drop_column_if_present("sync_run_units", "lease_owner")


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
