"""Add managed_by column to org_licenses and organizations for tier-source guard.

Revision ID: 0011
Revises: 0010
Create Date: 2026-06-14 00:00:00

Background (CHAOS-2301)
-----------------------
Stripe webhooks (customer.subscription.deleted) and the reconciliation
resolve_mismatch path unconditionally clobber Organization.tier and
OrgLicense.tier back to 'community' when a subscription is cancelled.
This breaks manually-granted tiers (e.g. enterprise granted by an admin
for a self-hosted customer) because Stripe has no subscription for them.

Fix: add a ``managed_by`` column (TEXT, default 'stripe') to both
``org_licenses`` and ``organizations``.  The webhook revoke path and
reconciliation only mutate tiers where ``managed_by = 'stripe'``.
Manual grants set ``managed_by = 'manual'`` and are never touched by
Stripe events.

Allowed values: 'stripe' | 'manual'

Migration is idempotent: uses _has_column guard so it is a no-op on
fresh deployments where create_all already materialised the column.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_COLUMN = "managed_by"


def _has_column(conn: sa.Connection, table: str, column: str) -> bool:
    inspector = sa.inspect(conn)
    if table not in inspector.get_table_names():
        return True  # table not yet bootstrapped; create_all will add it
    return any(col["name"] == column for col in inspector.get_columns(table))


def upgrade() -> None:
    conn = op.get_bind()

    if not _has_column(conn, "org_licenses", _COLUMN):
        op.add_column(
            "org_licenses",
            sa.Column(
                _COLUMN,
                sa.Text(),
                nullable=False,
                server_default="stripe",
                comment=(
                    "Who manages this tier: 'stripe' (webhook-controlled) "
                    "or 'manual' (admin-granted, immune to Stripe events)"
                ),
            ),
        )
    op.execute(
        """
        UPDATE org_licenses
        SET managed_by = 'manual'
        WHERE managed_by = 'stripe'
          AND coalesce(tier, 'community') <> 'community'
          AND coalesce(is_valid, false) = true
          AND NOT EXISTS (
            SELECT 1
            FROM subscriptions
            WHERE subscriptions.org_id = org_licenses.org_id
              AND subscriptions.status IN ('active', 'trialing', 'past_due')
          )
        """
    )

    if not _has_column(conn, "organizations", _COLUMN):
        op.add_column(
            "organizations",
            sa.Column(
                _COLUMN,
                sa.Text(),
                nullable=False,
                server_default="stripe",
                comment=(
                    "Who manages this org's tier: 'stripe' or 'manual'. "
                    "Mirrors org_licenses.managed_by for fast webhook checks."
                ),
            ),
        )
    op.execute(
        """
        UPDATE organizations
        SET managed_by = 'manual'
        WHERE managed_by = 'stripe'
          AND coalesce(tier, 'community') <> 'community'
          AND NOT EXISTS (
            SELECT 1
            FROM subscriptions
            WHERE subscriptions.org_id = organizations.id
              AND subscriptions.status IN ('active', 'trialing', 'past_due')
          )
        """
    )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "org_licenses" in inspector.get_table_names():
        if any(col["name"] == _COLUMN for col in inspector.get_columns("org_licenses")):
            op.drop_column("org_licenses", _COLUMN)

    if "organizations" in inspector.get_table_names():
        if any(
            col["name"] == _COLUMN for col in inspector.get_columns("organizations")
        ):
            op.drop_column("organizations", _COLUMN)
