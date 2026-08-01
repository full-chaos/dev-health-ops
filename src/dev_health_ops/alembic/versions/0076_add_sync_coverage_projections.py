"""Add compact exact-history sync coverage projections.

Revision ID: 0076
Revises: 0075
Create Date: 2026-07-31 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0076"
down_revision: str | None = "0075"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sync_coverage_projections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.String(), nullable=False),
        sa.Column("sync_config_id", sa.Uuid(), nullable=False),
        sa.Column("history_lookback_days", sa.Integer(), nullable=False),
        sa.Column("projection_version", sa.Integer(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("backfill_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("invalidated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["sync_config_id"], ["sync_configurations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "sync_config_id",
            "history_lookback_days",
            name="uq_sync_coverage_projection_org_config_window",
        ),
    )
    op.create_index(
        "ix_sync_coverage_projection_org_config",
        "sync_coverage_projections",
        ["org_id", "sync_config_id"],
    )
    op.create_index(
        "ix_sync_coverage_projection_refresh_order",
        "sync_coverage_projections",
        ["invalidated_at", "updated_at"],
    )
    op.create_index(
        "ix_sync_run_units_coverage_scan",
        "sync_run_units",
        ["org_id", "integration_id", "source_id", "dataset_key", "before_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_sync_run_units_coverage_scan", table_name="sync_run_units")
    op.drop_index(
        "ix_sync_coverage_projection_refresh_order",
        table_name="sync_coverage_projections",
    )
    op.drop_index(
        "ix_sync_coverage_projection_org_config",
        table_name="sync_coverage_projections",
    )
    op.drop_table("sync_coverage_projections")
