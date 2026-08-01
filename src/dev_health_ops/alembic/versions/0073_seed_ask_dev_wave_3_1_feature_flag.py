"""Seed the default-disabled Ask Dev Wave 3.1 entitlement.

Revision ID: 0073
Revises: 0072
Create Date: 2026-07-31 00:00:00

``ask_dev_wave_3_1`` is the Amendment TRD v2 §15 Phase A rollout gate for
server-owned question interpretation and the named-subject preflight
(CHAOS-3292). The row is globally available so the canonical feature decision
can act as an operator kill switch, and the feature is registered as an
explicit-enable one, so every tier stays denied until an organization or
license override grants it. With the flag off a run behaves exactly as it does
today.

Follows ``0067``/``0070``: additive, idempotent, and preserved on rollback
because organization and license overrides may reference the row.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0073"
down_revision: str | None = "0072"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_FEATURE_KEY = "ask_dev_wave_3_1"
_FEATURE_NAME = "Ask Dev Wave 3.1"
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
                   TRUE, TRUE, FALSE, :created_at, :updated_at
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
