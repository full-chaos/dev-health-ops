"""Enable canonical incident ingestion by default.

Revision ID: 0048
Revises: 0047
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0048"
down_revision: str | None = "0047"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FEATURE_KEY = "canonical_incident_ingestion"


def upgrade() -> None:
    """Repair a missed seed and make the registered feature globally available."""
    now = datetime.now(UTC)
    op.get_bind().execute(
        sa.text(
            """
            INSERT INTO feature_flags
                (id, key, name, description, category, min_tier, is_enabled,
                 is_beta, is_deprecated, created_at, updated_at)
            VALUES
                (:id, :key, :name, :description, :category, :min_tier, TRUE,
                 FALSE, FALSE, :created_at, :updated_at)
            ON CONFLICT (key) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                category = excluded.category,
                min_tier = excluded.min_tier,
                is_enabled = TRUE,
                is_deprecated = FALSE,
                updated_at = excluded.updated_at
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "key": _FEATURE_KEY,
            "name": "Canonical Incident Ingestion",
            "description": "Canonical operational incident ingestion and consumption",
            "category": "integrations",
            "min_tier": "community",
            "created_at": now,
            "updated_at": now,
        },
    )


def downgrade() -> None:
    """Keep the availability row intact; rollout policy is versioned in code."""
