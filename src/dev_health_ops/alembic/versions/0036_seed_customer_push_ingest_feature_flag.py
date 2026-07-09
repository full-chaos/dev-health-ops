"""Seed the customer_push_ingest feature flag.

Revision ID: 0036
Revises: 0035
Create Date: 2026-07-09 00:00:00

Adds the ``customer_push_ingest`` feature flag (Customer Push Ingest,
integrations, min tier team) to the feature_flags table so customer-owned
external ingestion runners can be gated. Idempotent: INSERT ... WHERE NOT
EXISTS, safe to re-run. Does NOT edit the historical 0007 seed migration.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0036"
down_revision: str | None = "0035"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_FEATURE_KEY = "customer_push_ingest"
_FEATURE_NAME = "Customer Push Ingest"
_FEATURE_CATEGORY = "integrations"
_FEATURE_MIN_TIER = "team"


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
