"""Add updated_by to org_feature_overrides and ON DELETE CASCADE to billing_prices.plan_id.

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-15 00:00:00

G6 (CHAOS-1209): Adds updated_by_id (nullable FK → users.id) to org_feature_overrides
so every mutation can record the acting user.

G7 (CHAOS-1210): Drops and recreates the billing_prices.plan_id FK with
ON DELETE CASCADE so that deleting a BillingPlan removes its prices automatically.
billing_subscriptions is intentionally NOT cascaded — subscription history survives
plan deletion.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # G6: add updated_by column to org_feature_overrides
    op.add_column(
        "org_feature_overrides",
        sa.Column("updated_by", UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_org_feature_overrides_updated_by_users",
        "org_feature_overrides",
        "users",
        ["updated_by"],
        ["id"],
        ondelete="SET NULL",
    )

    # G7: recreate billing_prices.plan_id FK with ON DELETE CASCADE
    op.drop_constraint(
        "billing_prices_plan_id_fkey",
        "billing_prices",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "billing_prices_plan_id_fkey",
        "billing_prices",
        "billing_plans",
        ["plan_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # G7: revert billing_prices.plan_id FK to no ondelete
    op.drop_constraint(
        "billing_prices_plan_id_fkey",
        "billing_prices",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "billing_prices_plan_id_fkey",
        "billing_prices",
        "billing_plans",
        ["plan_id"],
        ["id"],
    )

    # G6: remove updated_by column from org_feature_overrides
    op.drop_constraint(
        "fk_org_feature_overrides_updated_by_users",
        "org_feature_overrides",
        type_="foreignkey",
    )
    op.drop_column("org_feature_overrides", "updated_by")
