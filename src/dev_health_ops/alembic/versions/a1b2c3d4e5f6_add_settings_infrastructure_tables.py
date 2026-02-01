"""Add settings infrastructure tables for Enterprise Edition.

Revision ID: a1b2c3d4e5f6
Revises: 1a2b3c4d5e6f
Create Date: 2026-01-30 08:30:00

Creates tables for:
- settings: Key-value configuration store
- integration_credentials: Encrypted provider credentials
- sync_configurations: Data sync job configurations
- scheduled_jobs: Recurring job definitions
- job_runs: Job execution history
- identity_mappings: Cross-provider user identity correlation
- team_mappings: Team-to-scope mappings (replaces YAML config)
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "a1b2c3d4e5f6"
down_revision = "1a2b3c4d5e6f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "settings",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("is_encrypted", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id", "category", "key", name="uq_settings_org_category_key"
        ),
    )
    op.create_index("ix_settings_org_id", "settings", ["org_id"])
    op.create_index("ix_settings_category", "settings", ["category"])
    op.create_index("ix_settings_org_category", "settings", ["org_id", "category"])

    op.create_table(
        "integration_credentials",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("credentials_encrypted", sa.Text(), nullable=True),
        sa.Column("config", sa.JSON(), nullable=True),
        sa.Column("last_test_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_test_success", sa.Boolean(), nullable=True),
        sa.Column("last_test_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id", "provider", "name", name="uq_credentials_org_provider_name"
        ),
    )
    op.create_index("ix_credentials_org_id", "integration_credentials", ["org_id"])
    op.create_index("ix_credentials_provider", "integration_credentials", ["provider"])
    op.create_index(
        "ix_credentials_org_provider", "integration_credentials", ["org_id", "provider"]
    )

    op.create_table(
        "sync_configurations",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("credential_id", UUID(as_uuid=True), nullable=True),
        sa.Column("sync_targets", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("sync_options", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sync_success", sa.Boolean(), nullable=True),
        sa.Column("last_sync_error", sa.Text(), nullable=True),
        sa.Column("last_sync_stats", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["credential_id"], ["integration_credentials.id"], ondelete="SET NULL"
        ),
        sa.UniqueConstraint("org_id", "name", name="uq_sync_config_org_name"),
    )
    op.create_index("ix_sync_config_org_id", "sync_configurations", ["org_id"])
    op.create_index("ix_sync_config_provider", "sync_configurations", ["provider"])
    op.create_index(
        "ix_sync_config_org_provider", "sync_configurations", ["org_id", "provider"]
    )

    op.create_table(
        "scheduled_jobs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("schedule_cron", sa.Text(), nullable=False),
        sa.Column("timezone", sa.Text(), nullable=False, server_default="UTC"),
        sa.Column("job_config", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("sync_config_id", UUID(as_uuid=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="active"),
        sa.Column("is_running", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_status", sa.Text(), nullable=True),
        sa.Column("last_run_duration_seconds", sa.Integer(), nullable=True),
        sa.Column("last_run_error", sa.Text(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("run_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["sync_config_id"], ["sync_configurations.id"], ondelete="SET NULL"
        ),
        sa.UniqueConstraint("org_id", "name", name="uq_scheduled_job_org_name"),
    )
    op.create_index("ix_scheduled_job_org_id", "scheduled_jobs", ["org_id"])
    op.create_index("ix_scheduled_job_type", "scheduled_jobs", ["job_type"])
    op.create_index(
        "ix_scheduled_job_org_type", "scheduled_jobs", ["org_id", "job_type"]
    )
    op.create_index("ix_scheduled_job_next_run", "scheduled_jobs", ["next_run_at"])

    op.create_table(
        "job_runs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Integer(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_traceback", sa.Text(), nullable=True),
        sa.Column(
            "triggered_by", sa.Text(), nullable=False, server_default="scheduler"
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["job_id"], ["scheduled_jobs.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_job_runs_job_id", "job_runs", ["job_id"])
    op.create_index("ix_job_runs_status", "job_runs", ["status"])
    op.create_index("ix_job_runs_job_created", "job_runs", ["job_id", "created_at"])

    op.create_table(
        "identity_mappings",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("canonical_id", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("email", sa.Text(), nullable=True),
        sa.Column(
            "provider_identities", sa.JSON(), nullable=False, server_default="{}"
        ),
        sa.Column("team_ids", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "canonical_id", name="uq_identity_org_canonical"),
    )
    op.create_index("ix_identity_org_id", "identity_mappings", ["org_id"])
    op.create_index("ix_identity_canonical_id", "identity_mappings", ["canonical_id"])
    op.create_index("ix_identity_email", "identity_mappings", ["email"])
    op.create_index("ix_identity_org_email", "identity_mappings", ["org_id", "email"])

    op.create_table(
        "team_mappings",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("team_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("repo_patterns", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("project_keys", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("extra_data", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "team_id", name="uq_team_mapping_org_team"),
    )
    op.create_index("ix_team_mapping_org_id", "team_mappings", ["org_id"])
    op.create_index("ix_team_mapping_team_id", "team_mappings", ["team_id"])


def downgrade() -> None:
    op.drop_table("team_mappings")
    op.drop_table("identity_mappings")
    op.drop_table("job_runs")
    op.drop_table("scheduled_jobs")
    op.drop_table("sync_configurations")
    op.drop_table("integration_credentials")
    op.drop_table("settings")
