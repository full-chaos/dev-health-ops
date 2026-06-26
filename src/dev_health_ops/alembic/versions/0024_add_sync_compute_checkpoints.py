"""Add sync compute checkpoints.

Revision ID: 0024
Revises: 0023
Create Date: 2026-06-25 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    if not _table_exists("sync_compute_checkpoints"):
        op.create_table(
            "sync_compute_checkpoints",
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("sync_run_id", UUID(as_uuid=True), nullable=False),
            sa.Column("sync_run_unit_id", UUID(as_uuid=True), nullable=False),
            sa.Column("source_id", UUID(as_uuid=True), nullable=True),
            sa.Column("provider", sa.Text(), nullable=False),
            sa.Column("dataset_key", sa.String(), nullable=False),
            sa.Column("compute_type", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("window_start", sa.DateTime(timezone=True), nullable=True),
            sa.Column("window_end", sa.DateTime(timezone=True), nullable=True),
            sa.Column("checkpointed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("metadata", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ["sync_run_id"], ["sync_runs.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(
                ["sync_run_unit_id"], ["sync_run_units.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(
                ["source_id"], ["integration_sources.id"], ondelete="SET NULL"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "sync_run_id",
                "sync_run_unit_id",
                "compute_type",
                name="uq_sync_compute_checkpoint_unit_type",
            ),
        )
    _create_index_if_missing(
        "ix_sync_compute_checkpoints_org_status",
        "sync_compute_checkpoints",
        ["org_id", "status"],
    )
    _create_index_if_missing(
        "ix_sync_compute_checkpoints_run",
        "sync_compute_checkpoints",
        ["sync_run_id", "compute_type"],
    )


def downgrade() -> None:
    if _table_exists("sync_compute_checkpoints"):
        op.drop_table("sync_compute_checkpoints")


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
