"""Retain only server-generated PagerDuty OAuth authorization state.

Revision ID: 0045
Revises: 0044
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0045"
down_revision: str | None = "0044"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("pagerduty_oauth_authorization_requests", "initiated_by")
    op.drop_column("pagerduty_oauth_authorization_requests", "subdomain")
    op.drop_column("pagerduty_oauth_authorization_requests", "region")
    op.drop_column("pagerduty_oauth_authorization_requests", "enabled_datasets")
    op.drop_column("pagerduty_oauth_authorization_requests", "credential_name")


def downgrade() -> None:
    op.add_column(
        "pagerduty_oauth_authorization_requests",
        sa.Column("credential_name", sa.Text(), nullable=True),
    )
    op.add_column(
        "pagerduty_oauth_authorization_requests",
        sa.Column("enabled_datasets", sa.JSON(), nullable=True),
    )
    op.add_column(
        "pagerduty_oauth_authorization_requests",
        sa.Column("region", sa.Text(), nullable=True),
    )
    op.add_column(
        "pagerduty_oauth_authorization_requests",
        sa.Column("subdomain", sa.Text(), nullable=True),
    )
    op.add_column(
        "pagerduty_oauth_authorization_requests",
        sa.Column("initiated_by", sa.Text(), nullable=True),
    )
