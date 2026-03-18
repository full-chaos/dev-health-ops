"""Add tier_limits table with seed data.

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-18 00:00:00

Moves hardcoded TIER_LIMITS into a database table so limits can be changed
at runtime without a code deploy.  TierLimitService reads from this table
first, falling back to the hardcoded defaults only when no row exists.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Seed data — must stay in sync with TIER_LIMITS_DEFAULTS in models/licensing.py
_SEED_ROWS: list[dict[str, str | None]] = [
    # Community
    {
        "tier": "community",
        "limit_key": "max_users",
        "limit_value": "5",
        "description": "Maximum users per organization",
    },
    {
        "tier": "community",
        "limit_key": "max_repos",
        "limit_value": "3",
        "description": "Maximum synced repositories",
    },
    {
        "tier": "community",
        "limit_key": "max_work_items",
        "limit_value": "1000",
        "description": "Maximum tracked work items",
    },
    {
        "tier": "community",
        "limit_key": "retention_days",
        "limit_value": "30",
        "description": "Data retention in days",
    },
    {
        "tier": "community",
        "limit_key": "backfill_days",
        "limit_value": "30",
        "description": "Maximum backfill depth in days",
    },
    {
        "tier": "community",
        "limit_key": "api_rate_limit_per_min",
        "limit_value": "100",
        "description": "API requests per minute",
    },
    {
        "tier": "community",
        "limit_key": "min_sync_interval_hours",
        "limit_value": "24",
        "description": "Minimum hours between scheduled syncs",
    },
    # Team
    {
        "tier": "team",
        "limit_key": "max_users",
        "limit_value": "20",
        "description": "Maximum users per organization",
    },
    {
        "tier": "team",
        "limit_key": "max_repos",
        "limit_value": "10",
        "description": "Maximum synced repositories",
    },
    {
        "tier": "team",
        "limit_key": "max_work_items",
        "limit_value": "10000",
        "description": "Maximum tracked work items",
    },
    {
        "tier": "team",
        "limit_key": "retention_days",
        "limit_value": "90",
        "description": "Data retention in days",
    },
    {
        "tier": "team",
        "limit_key": "backfill_days",
        "limit_value": "90",
        "description": "Maximum backfill depth in days",
    },
    {
        "tier": "team",
        "limit_key": "api_rate_limit_per_min",
        "limit_value": "500",
        "description": "API requests per minute",
    },
    {
        "tier": "team",
        "limit_key": "min_sync_interval_hours",
        "limit_value": "6",
        "description": "Minimum hours between scheduled syncs",
    },
    # Enterprise (null = unlimited)
    {
        "tier": "enterprise",
        "limit_key": "max_users",
        "limit_value": None,
        "description": "Maximum users per organization",
    },
    {
        "tier": "enterprise",
        "limit_key": "max_repos",
        "limit_value": None,
        "description": "Maximum synced repositories",
    },
    {
        "tier": "enterprise",
        "limit_key": "max_work_items",
        "limit_value": None,
        "description": "Maximum tracked work items",
    },
    {
        "tier": "enterprise",
        "limit_key": "retention_days",
        "limit_value": None,
        "description": "Data retention in days",
    },
    {
        "tier": "enterprise",
        "limit_key": "backfill_days",
        "limit_value": None,
        "description": "Maximum backfill depth in days",
    },
    {
        "tier": "enterprise",
        "limit_key": "api_rate_limit_per_min",
        "limit_value": None,
        "description": "API requests per minute",
    },
    {
        "tier": "enterprise",
        "limit_key": "min_sync_interval_hours",
        "limit_value": "0.25",
        "description": "Minimum hours between scheduled syncs",
    },
]


def upgrade() -> None:
    tier_limits = op.create_table(
        "tier_limits",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("limit_key", sa.Text(), nullable=False),
        sa.Column("limit_value", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tier", "limit_key", name="uq_tier_limit_key"),
    )
    op.create_index("ix_tier_limits_tier", "tier_limits", ["tier"])

    # Seed default rows
    import uuid as _uuid

    op.bulk_insert(
        tier_limits,
        [{"id": str(_uuid.uuid4()), **row} for row in _SEED_ROWS],
    )


def downgrade() -> None:
    op.drop_index("ix_tier_limits_tier", table_name="tier_limits")
    op.drop_table("tier_limits")
