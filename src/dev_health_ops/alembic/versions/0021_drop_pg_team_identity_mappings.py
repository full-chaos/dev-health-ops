"""Drop the dead Postgres team/identity mapping tables.

ClickHouse became the system of record for the team catalog and identity
records in CHAOS-2600 CS5 (CH ``teams`` / ``identities`` tables). The Postgres
``team_mappings`` and ``identity_mappings`` tables were left behind as dead
remnants with no live writers or readers. CS6 removes the models, services, and
admin surface that used them; this migration drops the tables themselves.

The upgrade drops each table's indexes (``if_exists`` guarded for rerun safety)
then the tables. The downgrade recreates BOTH tables and their indexes EXACTLY
as ``0001_initial_schema`` created them, so the migration chain stays reversible
and consistent.

Revision ID: 0021
Revises: 0020
Create Date: 2026-06-22 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0021"
down_revision: str | None = "0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    # identity_mappings
    op.drop_index(
        "ix_identity_org_email", table_name="identity_mappings", if_exists=True
    )
    op.drop_index("ix_identity_email", table_name="identity_mappings", if_exists=True)
    op.drop_index(
        "ix_identity_canonical_id", table_name="identity_mappings", if_exists=True
    )
    op.drop_index("ix_identity_org_id", table_name="identity_mappings", if_exists=True)
    op.drop_table("identity_mappings", if_exists=True)

    # team_mappings
    op.drop_index("ix_team_mapping_team_id", table_name="team_mappings", if_exists=True)
    op.drop_index("ix_team_mapping_org_id", table_name="team_mappings", if_exists=True)
    op.drop_table("team_mappings", if_exists=True)


def downgrade() -> None:
    # Recreate both tables + indexes verbatim from 0001_initial_schema so a
    # downgrade leaves the schema consistent with the rest of the chain.
    op.create_table(
        "identity_mappings",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("canonical_id", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("email", sa.Text(), nullable=True),
        sa.Column(
            "provider_identities", sa.JSON(), nullable=False, server_default="{}"
        ),
        sa.Column("team_ids", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "canonical_id", name="uq_identity_org_canonical"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_org_id",
        "identity_mappings",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_canonical_id",
        "identity_mappings",
        ["canonical_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_email",
        "identity_mappings",
        ["email"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_org_email",
        "identity_mappings",
        ["org_id", "email"],
        if_not_exists=True,
    )

    op.create_table(
        "team_mappings",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("team_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("repo_patterns", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("project_keys", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("extra_data", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "managed_fields",
            sa.JSON(),
            nullable=False,
            server_default="[]",
            comment="Fields the provider owns (e.g. name, repo_patterns)",
        ),
        sa.Column(
            "sync_policy",
            sa.Integer(),
            nullable=False,
            server_default="1",
            comment="0=merge (auto-apply), 1=flag (review), 2=manual_only",
        ),
        sa.Column(
            "flagged_changes",
            sa.JSON(),
            nullable=True,
            comment="Pending provider-suggested changes for admin review",
        ),
        sa.Column(
            "last_drift_sync_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Last time this team was checked for drift",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "team_id", name="uq_team_mapping_org_team"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_team_mapping_org_id",
        "team_mappings",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_team_mapping_team_id",
        "team_mappings",
        ["team_id"],
        if_not_exists=True,
    )
