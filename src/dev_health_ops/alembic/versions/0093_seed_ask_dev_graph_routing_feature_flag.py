"""Seed the default-disabled Ask Dev graph-routing entitlement.

Revision ID: 0093
Revises: 0092
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0093"
down_revision: str | None = "0092"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FEATURE_KEY = "ask_dev_graph_routing"


def upgrade() -> None:
    conn = op.get_bind()
    now = datetime.now(timezone.utc)
    conn.execute(
        sa.text(
            """
            INSERT INTO feature_flags
                (id, key, name, category, min_tier, is_enabled, is_beta,
                 is_deprecated, created_at, updated_at)
            SELECT :id, :key, :name, :category, :min_tier,
                   TRUE, TRUE, FALSE, :created_at, :updated_at
            WHERE NOT EXISTS (
                SELECT 1 FROM feature_flags WHERE key = :key
            )
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "key": _FEATURE_KEY,
            "name": "Ask Dev Graph-Assisted Routing",
            "category": "analytics",
            "min_tier": "community",
            "created_at": now,
            "updated_at": now,
        },
    )


def downgrade() -> None:
    # Preserve the additive registration because organization and license
    # overrides may reference it, matching the earlier Ask Dev flags.
    pass
