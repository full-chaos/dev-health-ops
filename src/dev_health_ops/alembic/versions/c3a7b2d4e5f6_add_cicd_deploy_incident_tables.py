"""Add CI/CD, deployment, and incident tables

Revision ID: c3a7b2d4e5f6
Revises: 9f2f70b48ab5
Create Date: 2025-09-17 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c3a7b2d4e5f6"
down_revision: Union[str, None] = "9f2f70b48ab5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ci_pipeline_runs",
        sa.Column("repo_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "_mergestat_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="timestamp when record was synced into the MergeStat database",
        ),
        sa.ForeignKeyConstraint(["repo_id"], ["repos.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("repo_id", "run_id"),
    )

    op.create_table(
        "deployments",
        sa.Column("repo_id", sa.UUID(), nullable=False),
        sa.Column("deployment_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("environment", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deployed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("merged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pull_request_number", sa.Integer(), nullable=True),
        sa.Column(
            "_mergestat_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="timestamp when record was synced into the MergeStat database",
        ),
        sa.ForeignKeyConstraint(["repo_id"], ["repos.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("repo_id", "deployment_id"),
    )

    op.create_table(
        "incidents",
        sa.Column("repo_id", sa.UUID(), nullable=False),
        sa.Column("incident_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "_mergestat_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="timestamp when record was synced into the MergeStat database",
        ),
        sa.ForeignKeyConstraint(["repo_id"], ["repos.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("repo_id", "incident_id"),
    )


def downgrade() -> None:
    op.drop_table("incidents")
    op.drop_table("deployments")
    op.drop_table("ci_pipeline_runs")
