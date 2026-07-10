"""Seed the hosted Agent Context Runtime entitlement flag.

Revision ID: 0037
Revises: 0036
Create Date: 2026-07-10 00:00:00
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0037"
down_revision: str | None = "0036"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_FEATURE_KEY = "agent_context_runtime"
_FEATURE_NAME = "Agent Context Runtime"
_FEATURE_CATEGORY = "integrations"
_FEATURE_MIN_TIER = "community"


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
                   TRUE, FALSE, FALSE, :created_at, :updated_at
            WHERE NOT EXISTS (
                SELECT 1 FROM feature_flags WHERE key = :key
            )
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "key": _FEATURE_KEY,
            "name": _FEATURE_NAME,
            "category": _FEATURE_CATEGORY,
            "min_tier": _FEATURE_MIN_TIER,
            "created_at": now,
            "updated_at": now,
        },
    )


def downgrade() -> None:
    pass
