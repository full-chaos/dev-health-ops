from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0042"
down_revision: str | None = "0041"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_FEATURE_KEY = "canonical_incident_ingestion"
_FEATURE_NAME = "Canonical Incident Ingestion"
_FEATURE_CATEGORY = "integrations"
_FEATURE_MIN_TIER = "community"


def upgrade() -> None:
    connection = op.get_bind()
    now = datetime.now(UTC)
    connection.execute(
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
    connection = op.get_bind()
    connection.execute(
        sa.text(
            """
            DELETE FROM org_feature_overrides
            WHERE feature_id IN (
                SELECT id FROM feature_flags WHERE key = :key
            )
            """
        ),
        {"key": _FEATURE_KEY},
    )
    connection.execute(
        sa.text("DELETE FROM feature_flags WHERE key = :key"),
        {"key": _FEATURE_KEY},
    )
