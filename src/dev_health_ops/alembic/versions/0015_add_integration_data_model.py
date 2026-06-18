"""Add integration source dataset sync run data model.

Revision ID: 0015
Revises: 0014
Create Date: 2026-06-17 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.create_table(
        "integrations",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("credential_id", UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("schedule_cron", sa.Text(), nullable=True),
        sa.Column("timezone", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["credential_id"], ["integration_credentials.id"]),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_integrations_org_id", "integrations", ["org_id"], if_not_exists=True
    )
    op.create_index(
        "ix_integrations_provider", "integrations", ["provider"], if_not_exists=True
    )

    op.create_table(
        "integration_sources",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("integration_id", UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("full_name", sa.Text(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sync_success", sa.Boolean(), nullable=True),
        sa.Column("last_sync_error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["integration_id"], ["integrations.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "integration_id",
            "provider",
            "external_id",
            name="uq_integration_sources_org_integration_provider_external",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_integration_sources_org_id",
        "integration_sources",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_integration_sources_org_integration_enabled",
        "integration_sources",
        ["org_id", "integration_id", "is_enabled"],
        if_not_exists=True,
    )

    op.create_table(
        "integration_datasets",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("integration_id", UUID(as_uuid=True), nullable=False),
        sa.Column("dataset_key", sa.String(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False),
        sa.Column("options", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["integration_id"], ["integrations.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "integration_id",
            "dataset_key",
            name="uq_integration_datasets_org_integration_dataset",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_integration_datasets_org_id",
        "integration_datasets",
        ["org_id"],
        if_not_exists=True,
    )

    op.create_table(
        "sync_runs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("integration_id", UUID(as_uuid=True), nullable=False),
        sa.Column("triggered_by", sa.Text(), nullable=False),
        sa.Column("mode", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("total_units", sa.Integer(), nullable=False),
        sa.Column("completed_units", sa.Integer(), nullable=False),
        sa.Column("failed_units", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["integration_id"], ["integrations.id"]),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index("ix_sync_runs_org_id", "sync_runs", ["org_id"], if_not_exists=True)
    op.create_index(
        "ix_sync_runs_org_integration_status",
        "sync_runs",
        ["org_id", "integration_id", "status"],
        if_not_exists=True,
    )

    op.create_table(
        "sync_run_units",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("integration_id", UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("dataset_key", sa.String(), nullable=False),
        sa.Column("cost_class", sa.String(), nullable=False),
        sa.Column("mode", sa.String(), nullable=False),
        sa.Column("since_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("before_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("duration_seconds", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("processor_flags", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["integration_sources.id"]),
        sa.ForeignKeyConstraint(["sync_run_id"], ["sync_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_run_units_org_id", "sync_run_units", ["org_id"], if_not_exists=True
    )
    op.create_index(
        "ix_sync_run_units_run_status",
        "sync_run_units",
        ["sync_run_id", "status"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_run_units_source_id",
        "sync_run_units",
        ["source_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_run_units_dataset_key",
        "sync_run_units",
        ["dataset_key"],
        if_not_exists=True,
    )

    op.create_table(
        "sync_run_post_dispatches",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["sync_run_id"], ["sync_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "sync_run_id", "kind", name="uq_sync_run_post_dispatches_run_kind"
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_run_post_dispatches_org_id",
        "sync_run_post_dispatches",
        ["org_id"],
        if_not_exists=True,
    )

    op.add_column("sync_watermarks", sa.Column("source_id", sa.Text(), nullable=True))
    op.add_column("sync_watermarks", sa.Column("dataset_key", sa.Text(), nullable=True))
    op.execute("UPDATE sync_watermarks SET source_id = repo_id WHERE source_id IS NULL")
    op.execute(
        "UPDATE sync_watermarks SET dataset_key = target WHERE dataset_key IS NULL"
    )
    op.alter_column(
        "sync_watermarks",
        "source_id",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.alter_column(
        "sync_watermarks",
        "dataset_key",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.create_unique_constraint(
        "uq_sync_watermark_org_source_dataset",
        "sync_watermarks",
        ["org_id", "source_id", "dataset_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_sync_watermark_org_source_dataset", "sync_watermarks", type_="unique"
    )
    op.drop_column("sync_watermarks", "dataset_key")
    op.drop_column("sync_watermarks", "source_id")

    op.drop_index(
        "ix_sync_run_post_dispatches_org_id",
        table_name="sync_run_post_dispatches",
        if_exists=True,
    )
    op.drop_table("sync_run_post_dispatches", if_exists=True)

    op.drop_index(
        "ix_sync_run_units_dataset_key", table_name="sync_run_units", if_exists=True
    )
    op.drop_index(
        "ix_sync_run_units_source_id", table_name="sync_run_units", if_exists=True
    )
    op.drop_index(
        "ix_sync_run_units_run_status", table_name="sync_run_units", if_exists=True
    )
    op.drop_index(
        "ix_sync_run_units_org_id", table_name="sync_run_units", if_exists=True
    )
    op.drop_table("sync_run_units", if_exists=True)

    op.drop_index(
        "ix_sync_runs_org_integration_status", table_name="sync_runs", if_exists=True
    )
    op.drop_index("ix_sync_runs_org_id", table_name="sync_runs", if_exists=True)
    op.drop_table("sync_runs", if_exists=True)

    op.drop_index(
        "ix_integration_datasets_org_id",
        table_name="integration_datasets",
        if_exists=True,
    )
    op.drop_table("integration_datasets", if_exists=True)

    op.drop_index(
        "ix_integration_sources_org_integration_enabled",
        table_name="integration_sources",
        if_exists=True,
    )
    op.drop_index(
        "ix_integration_sources_org_id",
        table_name="integration_sources",
        if_exists=True,
    )
    op.drop_table("integration_sources", if_exists=True)

    op.drop_index("ix_integrations_provider", table_name="integrations", if_exists=True)
    op.drop_index("ix_integrations_org_id", table_name="integrations", if_exists=True)
    op.drop_table("integrations", if_exists=True)
