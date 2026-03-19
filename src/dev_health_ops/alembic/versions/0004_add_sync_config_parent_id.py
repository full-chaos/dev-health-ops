"""Add parent_id self-referencing FK to sync_configurations.

Revision ID: 0004
Revises: 0003
Create Date: 2026-03-18 00:00:00

Enables parent/child sync config hierarchy: a parent holds shared settings
(provider, credential, targets, schedule) while each child represents one
repo and runs as an independent job.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from dev_health_ops.models.types import GUID

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "sync_configurations",
        sa.Column("parent_id", GUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_sync_config_parent",
        "sync_configurations",
        "sync_configurations",
        ["parent_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_sync_config_parent_id",
        "sync_configurations",
        ["parent_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_sync_config_parent_id", table_name="sync_configurations")
    op.drop_constraint(
        "fk_sync_config_parent", "sync_configurations", type_="foreignkey"
    )
    op.drop_column("sync_configurations", "parent_id")
