"""Add sync run unit lease columns.

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
    op.add_column("sync_run_units", sa.Column("lease_owner", sa.Text(), nullable=True))
    op.add_column(
        "sync_run_units",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "sync_run_units",
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_sync_run_units_bucket_status_lease",
            "sync_run_units",
            ["org_id", "provider", "cost_class", "status", "lease_expires_at"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_sync_run_units_bucket_status_lease",
            table_name="sync_run_units",
            postgresql_concurrently=True,
        )
    op.drop_column("sync_run_units", "last_heartbeat_at")
    op.drop_column("sync_run_units", "lease_expires_at")
    op.drop_column("sync_run_units", "lease_owner")
