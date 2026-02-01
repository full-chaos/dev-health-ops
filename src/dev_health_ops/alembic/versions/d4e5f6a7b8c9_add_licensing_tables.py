"""Add licensing and feature flag tables for Enterprise Edition.

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-01-30 18:30:00

Creates tables for:
- feature_flags: Global feature definitions with tier gating
- org_feature_overrides: Per-org feature enable/disable toggles
- org_licenses: License key storage and validation metadata
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "feature_flags",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=False, server_default="core"),
        sa.Column("min_tier", sa.Text(), nullable=False, server_default="free"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("is_beta", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "is_deprecated", sa.Boolean(), nullable=False, server_default="false"
        ),
        sa.Column("config_schema", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key", name="uq_feature_flags_key"),
    )
    op.create_index("ix_feature_flags_key", "feature_flags", ["key"])
    op.create_index("ix_feature_flags_category", "feature_flags", ["category"])

    op.create_table(
        "org_feature_overrides",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("feature_id", UUID(as_uuid=True), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("config", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_by", UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["feature_id"], ["feature_flags.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("org_id", "feature_id", name="uq_org_feature_override"),
    )
    op.create_index("ix_org_feature_overrides_org", "org_feature_overrides", ["org_id"])
    op.create_index(
        "ix_org_feature_overrides_feature", "org_feature_overrides", ["feature_id"]
    )

    op.create_table(
        "org_licenses",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("license_key", sa.Text(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=False, server_default="free"),
        sa.Column("licensed_users", sa.Integer(), nullable=True),
        sa.Column("licensed_repos", sa.Integer(), nullable=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_valid", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("license_type", sa.Text(), nullable=False, server_default="saas"),
        sa.Column("customer_id", sa.Text(), nullable=True),
        sa.Column("features_override", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("limits_override", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("org_id", name="uq_org_licenses_org"),
    )
    op.create_index("ix_org_licenses_org", "org_licenses", ["org_id"])


def downgrade() -> None:
    op.drop_table("org_licenses")
    op.drop_table("org_feature_overrides")
    op.drop_table("feature_flags")
