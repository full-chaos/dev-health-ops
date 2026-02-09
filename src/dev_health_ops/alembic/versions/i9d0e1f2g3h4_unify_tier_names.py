"""Unify tier names: free->community, starter->team, remove pro.

Revision ID: i9d0e1f2g3h4
Revises: h8c9d0e1f2g3
Create Date: 2026-02-09 16:30:00

Consolidates the dual-enum tier system (Tier + LicenseTier) into a single
set of canonical tier names: community, team, enterprise.

Data migration only — no schema changes.
"""

from typing import Union

from alembic import op


revision: str = "i9d0e1f2g3h4"
down_revision: Union[str, None] = "h8c9d0e1f2g3"
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None


def upgrade() -> None:
    op.execute("UPDATE organizations SET tier = 'community' WHERE tier = 'free'")
    op.execute("UPDATE organizations SET tier = 'team' WHERE tier = 'starter'")
    op.execute("UPDATE organizations SET tier = 'team' WHERE tier = 'pro'")

    op.execute("UPDATE org_licenses SET tier = 'community' WHERE tier = 'free'")
    op.execute("UPDATE org_licenses SET tier = 'team' WHERE tier = 'starter'")
    op.execute("UPDATE org_licenses SET tier = 'team' WHERE tier = 'pro'")

    op.execute(
        "UPDATE feature_flags SET min_tier = 'community' WHERE min_tier = 'free'"
    )
    op.execute("UPDATE feature_flags SET min_tier = 'team' WHERE min_tier = 'starter'")
    op.execute("UPDATE feature_flags SET min_tier = 'team' WHERE min_tier = 'pro'")


def downgrade() -> None:
    op.execute("UPDATE organizations SET tier = 'free' WHERE tier = 'community'")
    op.execute("UPDATE organizations SET tier = 'starter' WHERE tier = 'team'")

    op.execute("UPDATE org_licenses SET tier = 'free' WHERE tier = 'community'")
    op.execute("UPDATE org_licenses SET tier = 'starter' WHERE tier = 'team'")

    op.execute(
        "UPDATE feature_flags SET min_tier = 'free' WHERE min_tier = 'community'"
    )
    op.execute("UPDATE feature_flags SET min_tier = 'starter' WHERE min_tier = 'team'")
