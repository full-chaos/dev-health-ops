"""Add drift sync fields to team_mappings.

Revision ID: k1f2g3h4i5j6
Revises: j0e1f2g3h4i5
Create Date: 2026-02-10 10:00:00

Adds managed_fields, sync_policy, and flagged_changes columns
to support daily drift sync with safe diff semantics (Phase 2).
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "k1f2g3h4i5j6"
down_revision: Union[str, None] = "j0e1f2g3h4i5"
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None


def upgrade() -> None:
    op.add_column(
        "team_mappings",
        sa.Column(
            "managed_fields",
            sa.JSON,
            nullable=False,
            server_default="[]",
            comment="Fields the provider owns (e.g. name, repo_patterns)",
        ),
    )
    op.add_column(
        "team_mappings",
        sa.Column(
            "sync_policy",
            sa.Integer,
            nullable=False,
            server_default="1",
            comment="0=merge (auto-apply), 1=flag (review), 2=manual_only",
        ),
    )
    op.add_column(
        "team_mappings",
        sa.Column(
            "flagged_changes",
            sa.JSON,
            nullable=True,
            comment="Pending provider-suggested changes for admin review",
        ),
    )
    op.add_column(
        "team_mappings",
        sa.Column(
            "last_drift_sync_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Last time this team was checked for drift",
        ),
    )


def downgrade() -> None:
    op.drop_column("team_mappings", "last_drift_sync_at")
    op.drop_column("team_mappings", "flagged_changes")
    op.drop_column("team_mappings", "sync_policy")
    op.drop_column("team_mappings", "managed_fields")
