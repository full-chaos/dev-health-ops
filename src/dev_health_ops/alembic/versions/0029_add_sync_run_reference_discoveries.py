"""Add sync run reference discovery ledger.

Revision ID: 0029
Revises: 0028
Create Date: 2026-06-29 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0029"
down_revision: str | None = "0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    if not _table_exists("sync_run_reference_discoveries"):
        op.create_table(
            "sync_run_reference_discoveries",
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("sync_run_id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("lease_owner", sa.Text(), nullable=True),
            sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("result", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["sync_run_id"], ["sync_runs.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "sync_run_id", name="uq_sync_run_reference_discoveries_run"
            ),
        )
    _create_index_if_missing(
        "ix_sync_run_reference_discoveries_status_available",
        "sync_run_reference_discoveries",
        ["status", "available_at"],
    )
    _create_index_if_missing(
        "ix_sync_run_reference_discoveries_org",
        "sync_run_reference_discoveries",
        ["org_id"],
    )


def downgrade() -> None:
    if _table_exists("sync_run_reference_discoveries"):
        op.drop_table("sync_run_reference_discoveries")


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _create_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing_indexes = {
        index["name"] for index in sa.inspect(bind).get_indexes(table_name)
    }
    if index_name not in existing_indexes:
        op.create_index(index_name, table_name, columns)
