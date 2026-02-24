from typing import Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "n4i5j6k7l8m9"
down_revision: Union[str, None] = "m3h4i5j6k7l8"
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None


def upgrade() -> None:
    op.create_table(
        "billing_plans",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stripe_product_id", sa.Text(), nullable=True),
        sa.Column(
            "metadata", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("key", name="uq_billing_plans_key"),
        sa.UniqueConstraint(
            "stripe_product_id", name="uq_billing_plans_stripe_product_id"
        ),
    )
    op.create_index("ix_billing_plans_key", "billing_plans", ["key"])

    op.create_table(
        "billing_prices",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column("plan_id", postgresql.UUID(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False, server_default="usd"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("stripe_price_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["plan_id"], ["billing_plans.id"]),
        sa.UniqueConstraint(
            "stripe_price_id", name="uq_billing_prices_stripe_price_id"
        ),
    )
    op.create_index("ix_billing_prices_plan_id", "billing_prices", ["plan_id"])

    op.create_table(
        "feature_bundles",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("key", name="uq_feature_bundles_key"),
    )
    op.create_index("ix_feature_bundles_key", "feature_bundles", ["key"])

    op.create_table(
        "plan_feature_bundles",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column("plan_id", postgresql.UUID(), nullable=False),
        sa.Column("bundle_id", postgresql.UUID(), nullable=False),
        sa.ForeignKeyConstraint(["plan_id"], ["billing_plans.id"]),
        sa.ForeignKeyConstraint(["bundle_id"], ["feature_bundles.id"]),
        sa.UniqueConstraint("plan_id", "bundle_id", name="uq_plan_feature_bundle"),
    )
    op.create_index(
        "ix_plan_feature_bundles_plan_id", "plan_feature_bundles", ["plan_id"]
    )
    op.create_index(
        "ix_plan_feature_bundles_bundle_id", "plan_feature_bundles", ["bundle_id"]
    )


def downgrade() -> None:
    op.drop_index(
        "ix_plan_feature_bundles_bundle_id", table_name="plan_feature_bundles"
    )
    op.drop_index("ix_plan_feature_bundles_plan_id", table_name="plan_feature_bundles")
    op.drop_table("plan_feature_bundles")

    op.drop_index("ix_feature_bundles_key", table_name="feature_bundles")
    op.drop_table("feature_bundles")

    op.drop_index("ix_billing_prices_plan_id", table_name="billing_prices")
    op.drop_table("billing_prices")

    op.drop_index("ix_billing_plans_key", table_name="billing_plans")
    op.drop_table("billing_plans")
