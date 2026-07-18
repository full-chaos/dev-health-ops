"""Add PagerDuty OAuth setup: authorization-request state + token metadata.

Revision ID: 0041
Revises: 0040
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0041"
down_revision: str | None = "0040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pagerduty_oauth_authorization_requests",
        sa.Column("state_hash", sa.Text(), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("credential_name", sa.Text(), nullable=False),
        sa.Column("code_verifier_encrypted", sa.Text(), nullable=False),
        sa.Column("enabled_datasets", sa.JSON(), nullable=False),
        sa.Column("region", sa.Text(), nullable=False),
        sa.Column("subdomain", sa.Text(), nullable=True),
        sa.Column("initiated_by", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("state_hash"),
    )
    op.create_index(
        "ix_pagerduty_oauth_authorization_requests_org_id",
        "pagerduty_oauth_authorization_requests",
        ["org_id"],
    )
    op.create_index(
        "ix_pagerduty_oauth_authorization_requests_expires_at",
        "pagerduty_oauth_authorization_requests",
        ["expires_at"],
    )

    op.add_column(
        "provider_oauth_credentials",
        sa.Column("binding_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "provider_oauth_credentials",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "provider_oauth_credentials",
        sa.Column("granted_scopes", sa.JSON(), nullable=True),
    )
    op.add_column(
        "provider_oauth_credentials",
        sa.Column(
            "has_refresh_token",
            sa.Boolean(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "provider_oauth_credentials",
        sa.Column("account_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "provider_oauth_credentials",
        sa.Column("account_display", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("provider_oauth_credentials", "account_display")
    op.drop_column("provider_oauth_credentials", "account_id")
    op.drop_column("provider_oauth_credentials", "has_refresh_token")
    op.drop_column("provider_oauth_credentials", "granted_scopes")
    op.drop_column("provider_oauth_credentials", "expires_at")
    op.drop_column("provider_oauth_credentials", "binding_id")
    op.drop_index(
        "ix_pagerduty_oauth_authorization_requests_expires_at",
        table_name="pagerduty_oauth_authorization_requests",
    )
    op.drop_index(
        "ix_pagerduty_oauth_authorization_requests_org_id",
        table_name="pagerduty_oauth_authorization_requests",
    )
    op.drop_table("pagerduty_oauth_authorization_requests")
