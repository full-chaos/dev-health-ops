"""Seed the byo_llm feature flag.

Revision ID: 0017
Revises: 0016
Create Date: 2026-06-19 00:00:00

Adds the ``byo_llm`` feature flag (BYO LLM, analytics, min tier team) to the
feature_flags table so org BYO-LLM configuration and runtime use of org
credentials can be gated. Idempotent: INSERT ... WHERE NOT EXISTS, safe to
re-run. Does NOT edit the historical 0007 seed migration.

Downgrade deletes only the byo_llm row, leaving other flags untouched.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Alembic reads the four names above via module introspection; declare them
# as public to quiet `py/unused-global-variable`.
__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

# (key, name, category, min_tier) — must stay in sync with STANDARD_FEATURES
# in licensing/registry.py. Inlined to avoid importing app code at migration
# runtime.
_FEATURE_KEY = "byo_llm"
_FEATURE_NAME = "BYO LLM"
_FEATURE_CATEGORY = "analytics"
_FEATURE_MIN_TIER = "team"


def upgrade() -> None:
    conn = op.get_bind()
    now = datetime.now(timezone.utc)

    # Idempotent: only insert if the key doesn't already exist.
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
    # Intentionally non-destructive (CHAOS-2551 review): this migration only
    # idempotently seeds a single data row and creates no schema. Deleting the
    # byo_llm feature_flags row on downgrade would be unsafe because:
    #   1. the row may have pre-existed this migration (the upgrade skips insert
    #      when it already exists), so a delete would remove state we did not
    #      create; and
    #   2. org_feature_overrides reference feature_flags.id with ON DELETE
    #      CASCADE, so deleting the flag would erase admin-configured per-org
    #      enable/disable state.
    # Leaving the inert data row in place is the safe rollback; the reverted
    # application code no longer gates on it.
    pass
