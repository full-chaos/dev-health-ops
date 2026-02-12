"""Add sync_watermarks table.

Revision ID: m3h4i5j6k7l8
Revises: l2g3h4i5j6k7
Create Date: 2026-02-12 12:00:00

Adds the sync_watermarks table for tracking per-repo incremental sync
watermarks.  Enables incremental data fetching by recording the last
successful sync timestamp per (org, repo, target) combination (gh-427).
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "m3h4i5j6k7l8"
down_revision: Union[str, None] = "l2g3h4i5j6k7"
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None


def upgrade() -> None:
    op.create_table(
        "sync_watermarks",
        sa.Column("id", sa.dialects.postgresql.UUID(), primary_key=True),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column(
            "repo_id",
            sa.Text(),
            nullable=False,
            comment="owner/repo for GitHub, project_id for GitLab",
        ),
        sa.Column(
            "target",
            sa.Text(),
            nullable=False,
            comment="Sync target: git, prs, cicd, deployments, incidents, work-items",
        ),
        sa.Column(
            "last_synced_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Timestamp of last successful sync for this target",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "org_id",
            "repo_id",
            "target",
            name="uq_sync_watermark_org_repo_target",
        ),
    )
    op.create_index(
        "ix_sync_watermark_org_repo",
        "sync_watermarks",
        ["org_id", "repo_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_sync_watermark_org_repo", table_name="sync_watermarks")
    op.drop_table("sync_watermarks")
