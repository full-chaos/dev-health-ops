"""Seed feature_flags table from STANDARD_FEATURES registry.

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-15 00:00:00

Seeds all 25 STANDARD_FEATURES rows into the feature_flags table so the DB
reflects the canonical feature registry.  Idempotent: uses INSERT … WHERE NOT
EXISTS so it is safe to re-run.

Downgrade deletes only the rows inserted by this migration (matched by key),
leaving any manually added flags untouched.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Alembic reads the four names above via module introspection; declare them
# as public to quiet `py/unused-global-variable`.
__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

# Keys seeded by this migration — used in downgrade() to delete only these rows.
# Must stay in sync with STANDARD_FEATURES in models/licensing.py.
# Inlined here to avoid importing application code at migration runtime.
_SEED_KEYS: list[tuple[str, str, str, str]] = [
    # (key, name, category, min_tier)
    ("git_sync", "Git Sync", "core", "community"),
    ("work_items_sync", "Work Items Sync", "core", "community"),
    ("basic_analytics", "Basic Analytics", "analytics", "community"),
    ("team_management", "Team Management", "core", "community"),
    ("github_integration", "GitHub Integration", "integrations", "team"),
    ("gitlab_integration", "GitLab Integration", "integrations", "team"),
    ("jira_integration", "Jira Integration", "integrations", "team"),
    ("investment_view", "Investment View", "analytics", "team"),
    ("api_access", "API Access", "core", "team"),
    ("capacity_forecast", "Capacity Forecast", "analytics", "team"),
    ("work_graph", "Work Graph", "analytics", "team"),
    ("quadrant_analysis", "Quadrant Analysis", "analytics", "team"),
    ("linear_integration", "Linear Integration", "integrations", "team"),
    ("llm_categorization", "LLM Categorization", "analytics", "team"),
    ("webhooks", "Webhooks", "integrations", "team"),
    ("scheduled_jobs", "Scheduled Jobs", "core", "team"),
    ("sso_saml", "SAML SSO", "security", "enterprise"),
    ("sso_oidc", "OIDC SSO", "security", "enterprise"),
    ("audit_log", "Audit Log", "compliance", "enterprise"),
    ("custom_retention", "Custom Retention", "compliance", "enterprise"),
    ("ip_allowlist", "IP Allowlist", "security", "enterprise"),
    ("data_export", "Data Export", "compliance", "enterprise"),
    ("multi_org", "Multi-Organization", "admin", "enterprise"),
    ("custom_branding", "Custom Branding", "admin", "enterprise"),
    ("priority_support", "Priority Support", "admin", "enterprise"),
]


def upgrade() -> None:
    conn = op.get_bind()
    now = datetime.now(timezone.utc)

    for key, name, category, min_tier in _SEED_KEYS:
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
                "key": key,
                "name": name,
                "category": category,
                "min_tier": min_tier,
                "created_at": now,
                "updated_at": now,
            },
        )


def downgrade() -> None:
    conn = op.get_bind()
    keys = [key for key, _name, _cat, _tier in _SEED_KEYS]
    # Delete only the rows that were seeded by this migration.
    conn.execute(
        sa.text("DELETE FROM feature_flags WHERE key = ANY(:keys)"),
        {"keys": keys},
    )
