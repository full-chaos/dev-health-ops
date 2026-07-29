"""Seed the default-disabled Ask Dev contextual-entrypoints entitlement.

Revision ID: 0070
Revises: 0069
Create Date: 2026-07-29 00:00:00

The row is globally available so the canonical feature decision can act as an
operator kill switch. ``ask_dev_contextual_entrypoints`` is registered as an
explicit-enable feature, so every tier remains denied until an organization or
license override grants it. The migration intentionally does not alter the
base ``ask_dev`` entitlement.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0070"
down_revision: str | None = "0069"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_FEATURE_KEY = "ask_dev_contextual_entrypoints"
_FEATURE_NAME = "Ask Dev Contextual Entrypoints"
_FEATURE_CATEGORY = "analytics"
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
    # Organization and license overrides may reference this row. Preserve the
    # additive registration on rollback, matching the base Ask Dev entitlement.
    pass
