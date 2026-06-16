"""Add github_app_installations table for frictionless GitHub App install.

Revision ID: 0014
Revises: 0013
Create Date: 2026-06-16 00:00:00

Maps a GitHub App ``installation_id`` to a Dev Health organization so the
one-click install flow (signed-state callback + ``installation`` webhook) can
persist the installation lifecycle. The App-mode credential the sync pipeline
consumes is still written to ``integration_credentials``; this table only tracks
the installation -> org link and its suspended/active state.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "github_app_installations"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("installation_id", sa.BigInteger(), nullable=False),
        sa.Column("account_login", sa.Text(), nullable=True),
        sa.Column("account_type", sa.Text(), nullable=True),
        sa.Column("org_id", sa.Text(), nullable=True),
        sa.Column("suspended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_github_app_installations_installation_id",
        _TABLE,
        ["installation_id"],
        unique=True,
        if_not_exists=True,
    )
    op.create_index(
        "ix_github_app_installations_org_id",
        _TABLE,
        ["org_id"],
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_github_app_installations_org_id", table_name=_TABLE, if_exists=True
    )
    op.drop_index(
        "ix_github_app_installations_installation_id",
        table_name=_TABLE,
        if_exists=True,
    )
    op.drop_table(_TABLE, if_exists=True)
