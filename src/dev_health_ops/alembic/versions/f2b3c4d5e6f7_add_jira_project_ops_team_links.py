"""Add jira project ops team link table.

Revision ID: f2b3c4d5e6f7
Revises: e1f2a3b4c5d6
Create Date: 2026-01-27 05:23:52
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f2b3c4d5e6f7"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jira_project_ops_team_links",
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("ops_team_id", sa.Text(), nullable=False),
        sa.Column("project_name", sa.Text(), nullable=False),
        sa.Column("ops_team_name", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("project_key", "ops_team_id"),
    )


def downgrade() -> None:
    op.drop_table("jira_project_ops_team_links")
