"""Add sync configuration migration compatibility links.

Revision ID: 0016
Revises: 0015
Create Date: 2026-06-17 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.add_column(
        "sync_configurations",
        sa.Column("migrated_integration_id", UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "sync_configurations",
        sa.Column("migrated_source_id", UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_sync_config_migrated_integration_id_integrations",
        "sync_configurations",
        "integrations",
        ["migrated_integration_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_sync_config_migrated_source_id_integration_sources",
        "sync_configurations",
        "integration_sources",
        ["migrated_source_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_sync_configurations_migrated_integration_id",
        "sync_configurations",
        ["migrated_integration_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_configurations_migrated_source_id",
        "sync_configurations",
        ["migrated_source_id"],
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_sync_configurations_migrated_source_id",
        table_name="sync_configurations",
        if_exists=True,
    )
    op.drop_index(
        "ix_sync_configurations_migrated_integration_id",
        table_name="sync_configurations",
        if_exists=True,
    )
    op.drop_constraint(
        "fk_sync_config_migrated_source_id_integration_sources",
        "sync_configurations",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_sync_config_migrated_integration_id_integrations",
        "sync_configurations",
        type_="foreignkey",
    )
    op.drop_column("sync_configurations", "migrated_source_id")
    op.drop_column("sync_configurations", "migrated_integration_id")
