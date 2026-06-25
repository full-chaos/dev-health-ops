"""Add investment provider batch control-plane tables.

Revision ID: 0024
Revises: 0023
Create Date: 2026-06-25 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.create_table(
        "investment_batch_jobs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("model", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("provider_job_id", sa.String(), nullable=True),
        sa.Column("local_correlation_id", sa.String(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("prompt_version", sa.String(), nullable=False),
        sa.Column("contract_version", sa.String(), nullable=False),
        sa.Column("total_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("provider_metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "local_correlation_id",
            name="uq_investment_batch_jobs_org_correlation",
        ),
    )
    op.create_index(
        "ix_investment_batch_jobs_org_id", "investment_batch_jobs", ["org_id"]
    )
    op.create_index(
        "ix_investment_batch_jobs_status", "investment_batch_jobs", ["status"]
    )
    op.create_index(
        "ix_investment_batch_jobs_provider_job_id",
        "investment_batch_jobs",
        ["provider_job_id"],
    )
    op.create_index(
        "ix_investment_batch_jobs_local_correlation_id",
        "investment_batch_jobs",
        ["local_correlation_id"],
    )
    op.create_index(
        "ix_investment_batch_jobs_run_id", "investment_batch_jobs", ["run_id"]
    )
    op.create_index(
        "ix_investment_batch_jobs_org_status",
        "investment_batch_jobs",
        ["org_id", "status"],
    )
    op.create_index(
        "ix_investment_batch_jobs_org_provider_job",
        "investment_batch_jobs",
        ["org_id", "provider_job_id"],
    )

    op.create_table(
        "investment_batch_items",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("job_id", UUID(as_uuid=True), nullable=False),
        sa.Column("work_unit_id", sa.Text(), nullable=False),
        sa.Column("component_index", sa.Integer(), nullable=False),
        sa.Column("custom_id", sa.String(), nullable=False),
        sa.Column("input_hash", sa.String(), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("model", sa.String(), nullable=False),
        sa.Column("prompt_version", sa.String(), nullable=False),
        sa.Column("contract_version", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("validation_status", sa.String(), nullable=True),
        sa.Column("repair_status", sa.String(), nullable=True),
        sa.Column("fallback_status", sa.String(), nullable=True),
        sa.Column("provider_response", sa.JSON(), nullable=True),
        sa.Column("provider_error", sa.JSON(), nullable=True),
        sa.Column("audit", sa.JSON(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"], ["investment_batch_jobs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "job_id",
            "custom_id",
            name="uq_investment_batch_items_job_custom",
        ),
    )
    op.create_index(
        "ix_investment_batch_items_org_id", "investment_batch_items", ["org_id"]
    )
    op.create_index(
        "ix_investment_batch_items_status", "investment_batch_items", ["status"]
    )
    op.create_index(
        "ix_investment_batch_items_org_job",
        "investment_batch_items",
        ["org_id", "job_id"],
    )
    op.create_index(
        "ix_investment_batch_items_org_custom",
        "investment_batch_items",
        ["org_id", "custom_id"],
    )
    op.create_index(
        "ix_investment_batch_items_org_status",
        "investment_batch_items",
        ["org_id", "status"],
    )
    op.create_index(
        "ix_investment_batch_items_idempotency_lookup",
        "investment_batch_items",
        [
            "org_id",
            "work_unit_id",
            "component_index",
            "input_hash",
            "provider",
            "model",
            "prompt_version",
            "contract_version",
        ],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_investment_batch_items_idempotency_lookup",
        table_name="investment_batch_items",
    )
    op.drop_index(
        "ix_investment_batch_items_org_status", table_name="investment_batch_items"
    )
    op.drop_index(
        "ix_investment_batch_items_org_custom", table_name="investment_batch_items"
    )
    op.drop_index(
        "ix_investment_batch_items_org_job", table_name="investment_batch_items"
    )
    op.drop_index(
        "ix_investment_batch_items_status", table_name="investment_batch_items"
    )
    op.drop_index(
        "ix_investment_batch_items_org_id", table_name="investment_batch_items"
    )
    op.drop_table("investment_batch_items")
    op.drop_index(
        "ix_investment_batch_jobs_org_provider_job", table_name="investment_batch_jobs"
    )
    op.drop_index(
        "ix_investment_batch_jobs_org_status", table_name="investment_batch_jobs"
    )
    op.drop_index("ix_investment_batch_jobs_run_id", table_name="investment_batch_jobs")
    op.drop_index(
        "ix_investment_batch_jobs_local_correlation_id",
        table_name="investment_batch_jobs",
    )
    op.drop_index(
        "ix_investment_batch_jobs_provider_job_id", table_name="investment_batch_jobs"
    )
    op.drop_index("ix_investment_batch_jobs_status", table_name="investment_batch_jobs")
    op.drop_index("ix_investment_batch_jobs_org_id", table_name="investment_batch_jobs")
    op.drop_table("investment_batch_jobs")
