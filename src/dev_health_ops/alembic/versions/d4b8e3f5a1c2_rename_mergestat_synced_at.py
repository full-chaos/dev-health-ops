"""rename mergestat_synced_at

Revision ID: d4b8e3f5a1c2
Revises: c3a7b2d4e5f6
Create Date: 2025-12-30 00:00:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "d4b8e3f5a1c2"
down_revision = "c3a7b2d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename _mergestat_synced_at to last_synced
    tables = [
        "git_refs",
        "git_files",
        "git_commits",
        "git_commit_stats",
        "git_blame",
        "git_pull_requests",
        "git_pull_request_reviews",
        "ci_pipeline_runs",
        "deployments",
        "incidents",
    ]

    for table in tables:
        op.alter_column(table, "_mergestat_synced_at", new_column_name="last_synced")


def downgrade() -> None:
    # Rename last_synced back to _mergestat_synced_at
    tables = [
        "git_refs",
        "git_files",
        "git_commits",
        "git_commit_stats",
        "git_blame",
        "git_pull_requests",
        "git_pull_request_reviews",
        "ci_pipeline_runs",
        "deployments",
        "incidents",
    ]

    for table in tables:
        op.alter_column(table, "last_synced", new_column_name="_mergestat_synced_at")
