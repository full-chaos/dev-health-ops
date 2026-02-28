"""Initial consolidated schema migration.

Revision ID: 0001
Revises:
Create Date: 2026-02-27 00:00:00
"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


STANDARD_PERMISSIONS = [
    ("analytics:read", "analytics", "read", "View analytics dashboards and metrics"),
    ("analytics:export", "analytics", "export", "Export analytics data"),
    ("metrics:read", "metrics", "read", "View computed metrics"),
    ("metrics:compute", "metrics", "compute", "Trigger metric computation jobs"),
    ("work_items:read", "work_items", "read", "View work items and investments"),
    ("work_items:sync", "work_items", "sync", "Sync work items from providers"),
    ("git:read", "git", "read", "View git commits and PRs"),
    ("git:sync", "git", "sync", "Sync git data from repositories"),
    ("teams:read", "teams", "read", "View team configurations"),
    ("teams:write", "teams", "write", "Create and modify teams"),
    ("settings:read", "settings", "read", "View organization settings"),
    ("settings:write", "settings", "write", "Modify organization settings"),
    ("integrations:read", "integrations", "read", "View integration configurations"),
    (
        "integrations:write",
        "integrations",
        "write",
        "Configure integrations and credentials",
    ),
    ("members:read", "members", "read", "View organization members"),
    ("members:invite", "members", "invite", "Invite new members"),
    ("members:manage", "members", "manage", "Manage member roles and remove members"),
    ("org:read", "org", "read", "View organization details"),
    ("org:write", "org", "write", "Modify organization details"),
    ("org:delete", "org", "delete", "Delete the organization"),
    ("admin:users", "admin", "users", "Manage all users (superuser only)"),
    ("admin:orgs", "admin", "orgs", "Manage all organizations (superuser only)"),
]

ROLE_PERMISSIONS = {
    "viewer": [
        "analytics:read",
        "metrics:read",
        "work_items:read",
        "git:read",
        "teams:read",
        "settings:read",
        "members:read",
        "org:read",
    ],
    "member": ["analytics:export"],
    "admin": [
        "metrics:compute",
        "work_items:sync",
        "git:sync",
        "teams:write",
        "settings:write",
        "integrations:read",
        "integrations:write",
        "members:invite",
        "members:manage",
        "org:write",
    ],
    "owner": ["org:delete"],
}


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
        if_not_exists=True,
    )
    op.create_index("ix_settings_org_id", "settings", ["org_id"], if_not_exists=True)
    op.create_index(
        "ix_settings_category", "settings", ["category"], if_not_exists=True
    )
    op.create_index(
        "ix_settings_org_category",
        "settings",
        ["org_id", "category"],
        if_not_exists=True,
    )

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
            "org_id",
            "provider",
            "name",
            name="uq_credentials_org_provider_name",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_credentials_org_id",
        "integration_credentials",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_credentials_provider",
        "integration_credentials",
        ["provider"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_credentials_org_provider",
        "integration_credentials",
        ["org_id", "provider"],
        if_not_exists=True,
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
            ["credential_id"],
            ["integration_credentials.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("org_id", "name", name="uq_sync_config_org_name"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_config_org_id",
        "sync_configurations",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_config_provider",
        "sync_configurations",
        ["provider"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_config_org_provider",
        "sync_configurations",
        ["org_id", "provider"],
        if_not_exists=True,
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
        sa.Column("status", sa.Integer(), nullable=False),
        sa.Column("is_running", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_status", sa.Integer(), nullable=True),
        sa.Column("last_run_duration_seconds", sa.Integer(), nullable=True),
        sa.Column("last_run_error", sa.Text(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("run_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["sync_config_id"],
            ["sync_configurations.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("org_id", "name", name="uq_scheduled_job_org_name"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_scheduled_job_org_id",
        "scheduled_jobs",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_scheduled_job_type",
        "scheduled_jobs",
        ["job_type"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_scheduled_job_org_type",
        "scheduled_jobs",
        ["org_id", "job_type"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_scheduled_job_next_run",
        "scheduled_jobs",
        ["next_run_at"],
        if_not_exists=True,
    )

    op.create_table(
        "job_runs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.Integer(), nullable=False),
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
        if_not_exists=True,
    )
    op.create_index("ix_job_runs_job_id", "job_runs", ["job_id"], if_not_exists=True)
    op.create_index("ix_job_runs_status", "job_runs", ["status"], if_not_exists=True)
    op.create_index(
        "ix_job_runs_job_created",
        "job_runs",
        ["job_id", "created_at"],
        if_not_exists=True,
    )

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
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_org_id",
        "identity_mappings",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_canonical_id",
        "identity_mappings",
        ["canonical_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_email",
        "identity_mappings",
        ["email"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_identity_org_email",
        "identity_mappings",
        ["org_id", "email"],
        if_not_exists=True,
    )

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
        sa.Column(
            "managed_fields",
            sa.JSON(),
            nullable=False,
            server_default="[]",
            comment="Fields the provider owns (e.g. name, repo_patterns)",
        ),
        sa.Column(
            "sync_policy",
            sa.Integer(),
            nullable=False,
            server_default="1",
            comment="0=merge (auto-apply), 1=flag (review), 2=manual_only",
        ),
        sa.Column(
            "flagged_changes",
            sa.JSON(),
            nullable=True,
            comment="Pending provider-suggested changes for admin review",
        ),
        sa.Column(
            "last_drift_sync_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Last time this team was checked for drift",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "team_id", name="uq_team_mapping_org_team"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_team_mapping_org_id",
        "team_mappings",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_team_mapping_team_id",
        "team_mappings",
        ["team_id"],
        if_not_exists=True,
    )

    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("username", sa.Text(), nullable=True),
        sa.Column("password_hash", sa.Text(), nullable=True),
        sa.Column("full_name", sa.Text(), nullable=True),
        sa.Column("avatar_url", sa.Text(), nullable=True),
        sa.Column("auth_provider", sa.Text(), nullable=True, server_default="local"),
        sa.Column("auth_provider_id", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("is_verified", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_superuser", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_users_email",
        "users",
        ["email"],
        unique=True,
        if_not_exists=True,
    )
    op.create_index(
        "ix_users_username",
        "users",
        ["username"],
        unique=True,
        if_not_exists=True,
    )
    op.create_index(
        "ix_users_auth_provider",
        "users",
        ["auth_provider", "auth_provider_id"],
        if_not_exists=True,
    )

    op.create_table(
        "organizations",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("settings", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("tier", sa.Text(), nullable=False, server_default="'free'"),
        sa.Column("stripe_customer_id", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_organizations_slug",
        "organizations",
        ["slug"],
        unique=True,
        if_not_exists=True,
    )

    op.create_table(
        "memberships",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("role", sa.Text(), nullable=False, server_default="'member'"),
        sa.Column("invited_by_id", UUID(as_uuid=True), nullable=True),
        sa.Column("joined_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["invited_by_id"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("user_id", "org_id", name="uq_membership_user_org"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_memberships_user_id",
        "memberships",
        ["user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_memberships_org_id",
        "memberships",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_memberships_org_role",
        "memberships",
        ["org_id", "role"],
        if_not_exists=True,
    )

    op.create_table(
        "permissions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("resource", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint("resource", "action", name="uq_permission_resource_action"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_permissions_name",
        "permissions",
        ["name"],
        unique=True,
        if_not_exists=True,
    )
    op.create_index(
        "ix_permissions_resource",
        "permissions",
        ["resource"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_permissions_resource_action",
        "permissions",
        ["resource", "action"],
        if_not_exists=True,
    )

    op.create_table(
        "role_permissions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("permission_id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["permission_id"], ["permissions.id"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint("role", "permission_id", name="uq_role_permission"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_role_permissions_role",
        "role_permissions",
        ["role"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_role_permissions_permission_id",
        "role_permissions",
        ["permission_id"],
        if_not_exists=True,
    )

    op.create_table(
        "feature_flags",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=False, server_default="core"),
        sa.Column("min_tier", sa.Text(), nullable=False, server_default="community"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("is_beta", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "is_deprecated", sa.Boolean(), nullable=False, server_default="false"
        ),
        sa.Column("config_schema", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key", name="uq_feature_flags_key"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_feature_flags_key", "feature_flags", ["key"], if_not_exists=True
    )
    op.create_index(
        "ix_feature_flags_category",
        "feature_flags",
        ["category"],
        if_not_exists=True,
    )

    op.create_table(
        "org_feature_overrides",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("feature_id", UUID(as_uuid=True), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("config", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_by", UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["feature_id"], ["feature_flags.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("org_id", "feature_id", name="uq_org_feature_override"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_feature_overrides_org",
        "org_feature_overrides",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_feature_overrides_feature",
        "org_feature_overrides",
        ["feature_id"],
        if_not_exists=True,
    )

    op.create_table(
        "org_licenses",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("license_key", sa.Text(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=False, server_default="community"),
        sa.Column("licensed_users", sa.Integer(), nullable=True),
        sa.Column("licensed_repos", sa.Integer(), nullable=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_valid", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("license_type", sa.Text(), nullable=False, server_default="saas"),
        sa.Column("customer_id", sa.Text(), nullable=True),
        sa.Column("features_override", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("limits_override", sa.JSON(), nullable=True, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("org_id", name="uq_org_licenses_org"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_licenses_org",
        "org_licenses",
        ["org_id"],
        if_not_exists=True,
    )

    op.create_table(
        "audit_logs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("changes", sa.JSON(), nullable=True),
        sa.Column("request_metadata", sa.JSON(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="success"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_org_id",
        "audit_logs",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_user_id",
        "audit_logs",
        ["user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_action",
        "audit_logs",
        ["action"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_resource_type",
        "audit_logs",
        ["resource_type"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_resource_id",
        "audit_logs",
        ["resource_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_created_at",
        "audit_logs",
        ["created_at"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_org_created",
        "audit_logs",
        ["org_id", "created_at"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_user_created",
        "audit_logs",
        ["user_id", "created_at"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_resource",
        "audit_logs",
        ["resource_type", "resource_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_action_created",
        "audit_logs",
        ["action", "created_at"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_audit_logs_org_action_created",
        "audit_logs",
        ["org_id", "action", "created_at"],
        if_not_exists=True,
    )

    op.create_table(
        "sso_providers",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("protocol", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending_setup"),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "allow_idp_initiated",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column(
            "auto_provision_users",
            sa.Boolean(),
            nullable=False,
            server_default="true",
        ),
        sa.Column("default_role", sa.Text(), nullable=False, server_default="member"),
        sa.Column("config", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("encrypted_secrets", sa.JSON(), nullable=True),
        sa.Column("allowed_domains", sa.JSON(), nullable=True),
        sa.Column("last_metadata_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("org_id", "name", name="uq_sso_provider_org_name"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_sso_providers_org_id",
        "sso_providers",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sso_providers_protocol",
        "sso_providers",
        ["protocol"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sso_providers_org_protocol",
        "sso_providers",
        ["org_id", "protocol"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_sso_providers_org_status",
        "sso_providers",
        ["org_id", "status"],
        if_not_exists=True,
    )

    op.create_table(
        "org_ip_allowlist",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("ip_range", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_by_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("org_id", "ip_range", name="uq_org_ip_allowlist_org_range"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_ip_allowlist_org_id",
        "org_ip_allowlist",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_ip_allowlist_org_active",
        "org_ip_allowlist",
        ["org_id", "is_active"],
        if_not_exists=True,
    )

    op.create_table(
        "org_retention_policies",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("resource_type", sa.String(length=50), nullable=False),
        sa.Column("retention_days", sa.Integer(), nullable=False, default=90),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_deleted_count", sa.Integer(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_id", UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_id"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "org_id", "resource_type", name="uq_org_retention_resource"
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_retention_policies_org_id",
        "org_retention_policies",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_retention_policies_org_active",
        "org_retention_policies",
        ["org_id", "is_active"],
        if_not_exists=True,
    )

    op.create_table(
        "metric_checkpoints",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", sa.Text(), nullable=False, server_default="default"),
        sa.Column("repo_id", UUID(as_uuid=True), nullable=True),
        sa.Column(
            "metric_type",
            sa.Text(),
            nullable=False,
            comment="Computation scope: daily_batch, daily_finalize, rebuild",
        ),
        sa.Column(
            "day", sa.DateTime(timezone=True), nullable=False, comment="Target date"
        ),
        sa.Column(
            "status",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="0=pending, 1=running, 2=completed, 3=failed",
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "worker_id",
            sa.Text(),
            nullable=True,
            comment="Celery task ID for distributed locking",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
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
            "metric_type",
            "day",
            name="uq_checkpoint_scope",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_checkpoint_status_day",
        "metric_checkpoints",
        ["status", "day"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_checkpoint_org_type_day",
        "metric_checkpoints",
        ["org_id", "metric_type", "day"],
        if_not_exists=True,
    )

    op.create_table(
        "sync_watermarks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
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
        if_not_exists=True,
    )
    op.create_index(
        "ix_sync_watermark_org_repo",
        "sync_watermarks",
        ["org_id", "repo_id"],
        if_not_exists=True,
    )

    op.create_table(
        "billing_plans",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stripe_product_id", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("key", name="uq_billing_plans_key"),
        sa.UniqueConstraint(
            "stripe_product_id",
            name="uq_billing_plans_stripe_product_id",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_plans_key",
        "billing_plans",
        ["key"],
        if_not_exists=True,
    )

    op.create_table(
        "billing_prices",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("plan_id", UUID(as_uuid=True), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False, server_default="usd"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("stripe_price_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["plan_id"], ["billing_plans.id"]),
        sa.UniqueConstraint(
            "stripe_price_id",
            name="uq_billing_prices_stripe_price_id",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_prices_plan_id",
        "billing_prices",
        ["plan_id"],
        if_not_exists=True,
    )

    op.create_table(
        "feature_bundles",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("key", name="uq_feature_bundles_key"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_feature_bundles_key",
        "feature_bundles",
        ["key"],
        if_not_exists=True,
    )

    op.create_table(
        "plan_feature_bundles",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("plan_id", UUID(as_uuid=True), nullable=False),
        sa.Column("bundle_id", UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["plan_id"], ["billing_plans.id"]),
        sa.ForeignKeyConstraint(["bundle_id"], ["feature_bundles.id"]),
        sa.UniqueConstraint("plan_id", "bundle_id", name="uq_plan_feature_bundle"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_plan_feature_bundles_plan_id",
        "plan_feature_bundles",
        ["plan_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_plan_feature_bundles_bundle_id",
        "plan_feature_bundles",
        ["bundle_id"],
        if_not_exists=True,
    )

    op.create_table(
        "subscriptions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("billing_plan_id", UUID(as_uuid=True), nullable=False),
        sa.Column("billing_price_id", UUID(as_uuid=True), nullable=False),
        sa.Column("stripe_subscription_id", sa.Text(), nullable=False),
        sa.Column("stripe_customer_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cancel_at_period_end", sa.Boolean(), server_default="false"),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trial_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trial_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", sa.JSON(), server_default="{}", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["billing_plan_id"],
            ["billing_plans.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["billing_price_id"],
            ["billing_prices.id"],
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "stripe_subscription_id", name="uq_subscriptions_stripe_id"
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_subscriptions_org_id",
        "subscriptions",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_subscriptions_customer_id",
        "subscriptions",
        ["stripe_customer_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_subscriptions_stripe_subscription_id",
        "subscriptions",
        ["stripe_subscription_id"],
        if_not_exists=True,
    )

    op.create_table(
        "subscription_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("subscription_id", UUID(as_uuid=True), nullable=False),
        sa.Column("stripe_event_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("previous_status", sa.Text(), nullable=True),
        sa.Column("new_status", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), server_default="{}", nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["subscription_id"], ["subscriptions.id"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint(
            "stripe_event_id",
            name="uq_subscription_events_stripe_event",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_subscription_events_subscription_id",
        "subscription_events",
        ["subscription_id"],
        if_not_exists=True,
    )

    op.create_table(
        "invoices",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("subscription_id", UUID(as_uuid=True), nullable=True),
        sa.Column("stripe_invoice_id", sa.Text(), nullable=False),
        sa.Column("stripe_customer_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("amount_due", sa.Integer(), nullable=False),
        sa.Column("amount_paid", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("amount_remaining", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("currency", sa.Text(), nullable=False, server_default="usd"),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("hosted_invoice_url", sa.Text(), nullable=True),
        sa.Column("pdf_url", sa.Text(), nullable=True),
        sa.Column("payment_intent_id", sa.Text(), nullable=True),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("voided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_invoice_id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_invoices_org_id",
        "invoices",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_invoices_subscription_id",
        "invoices",
        ["subscription_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_invoices_stripe_invoice_id",
        "invoices",
        ["stripe_invoice_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_invoices_stripe_customer_id",
        "invoices",
        ["stripe_customer_id"],
        if_not_exists=True,
    )

    op.create_table(
        "invoice_line_items",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("invoice_id", UUID(as_uuid=True), nullable=False),
        sa.Column("stripe_line_item_id", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stripe_price_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["invoice_id"], ["invoices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_invoice_line_items_invoice_id",
        "invoice_line_items",
        ["invoice_id"],
        if_not_exists=True,
    )

    op.create_table(
        "refunds",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", UUID(as_uuid=True), nullable=False),
        sa.Column("invoice_id", UUID(as_uuid=True), nullable=True),
        sa.Column("subscription_id", UUID(as_uuid=True), nullable=True),
        sa.Column("stripe_refund_id", sa.Text(), nullable=False),
        sa.Column("stripe_charge_id", sa.Text(), nullable=False),
        sa.Column("stripe_payment_intent_id", sa.Text(), nullable=True),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False, server_default="usd"),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("initiated_by", UUID(as_uuid=True), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["invoice_id"], ["invoices.id"]),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"]),
        sa.ForeignKeyConstraint(["initiated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_refund_id", name="uq_refunds_stripe_refund_id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_refunds_org_id",
        "refunds",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refunds_invoice_id",
        "refunds",
        ["invoice_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refunds_subscription_id",
        "refunds",
        ["subscription_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refunds_stripe_refund_id",
        "refunds",
        ["stripe_refund_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refunds_stripe_charge_id",
        "refunds",
        ["stripe_charge_id"],
        if_not_exists=True,
    )

    op.create_table(
        "billing_audit_log",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id"),
            nullable=False,
        ),
        sa.Column(
            "actor_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=True,
        ),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("resource_id", UUID(as_uuid=True), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("stripe_event_id", sa.Text(), nullable=True),
        sa.Column("local_state", sa.JSON(), nullable=True),
        sa.Column("stripe_state", sa.JSON(), nullable=True),
        sa.Column("reconciliation_status", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("now()"),
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_audit_log_org_id",
        "billing_audit_log",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_audit_log_action",
        "billing_audit_log",
        ["action"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_audit_log_stripe_event_id",
        "billing_audit_log",
        ["stripe_event_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_audit_log_org_created",
        "billing_audit_log",
        ["org_id", "created_at"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_audit_log_resource",
        "billing_audit_log",
        ["resource_type", "resource_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_audit_log_reconciliation_status",
        "billing_audit_log",
        ["reconciliation_status"],
        if_not_exists=True,
    )

    op.create_table(
        "refresh_tokens",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column("family_id", UUID(as_uuid=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("replaced_by_hash", sa.Text(), nullable=True),
        sa.Column("ip_address", sa.Text(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("token_hash", name="uq_refresh_tokens_token_hash"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_refresh_tokens_user_id",
        "refresh_tokens",
        ["user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refresh_tokens_token_hash",
        "refresh_tokens",
        ["token_hash"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refresh_tokens_family_id",
        "refresh_tokens",
        ["family_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refresh_tokens_user_family",
        "refresh_tokens",
        ["user_id", "family_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_refresh_tokens_expires",
        "refresh_tokens",
        ["expires_at"],
        if_not_exists=True,
    )

    op.create_table(
        "email_verification_tokens",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "token_hash",
            name="uq_email_verification_tokens_token_hash",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_email_verification_tokens_user_id",
        "email_verification_tokens",
        ["user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_email_verification_tokens_token_hash",
        "email_verification_tokens",
        ["token_hash"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_email_verification_tokens_user_expires",
        "email_verification_tokens",
        ["user_id", "expires_at"],
        if_not_exists=True,
    )

    op.create_table(
        "login_attempts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("email", name="uq_login_attempts_email"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_login_attempts_email",
        "login_attempts",
        ["email"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_login_attempts_locked_until",
        "login_attempts",
        ["locked_until"],
        if_not_exists=True,
    )

    op.create_table(
        "password_reset_tokens",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "token_hash",
            name="uq_password_reset_tokens_token_hash",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_password_reset_tokens_user_id",
        "password_reset_tokens",
        ["user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_password_reset_tokens_token_hash",
        "password_reset_tokens",
        ["token_hash"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_password_reset_tokens_user_expires",
        "password_reset_tokens",
        ["user_id", "expires_at"],
        if_not_exists=True,
    )

    op.create_table(
        "org_invites",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False, server_default="member"),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column(
            "invited_by_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("token_hash", name="uq_org_invites_token_hash"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_org_id",
        "org_invites",
        ["org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_email",
        "org_invites",
        ["email"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_token_hash",
        "org_invites",
        ["token_hash"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_invited_by_id",
        "org_invites",
        ["invited_by_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_status",
        "org_invites",
        ["status"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_org_email_status",
        "org_invites",
        ["org_id", "email", "status"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_org_invites_org_expires",
        "org_invites",
        ["org_id", "expires_at"],
        if_not_exists=True,
    )

    op.create_table(
        "impersonation_sessions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "admin_user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "target_user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "target_org_id",
            UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("target_role", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ip_address", sa.String(length=45), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "admin_user_id != target_user_id", name="ck_impersonation_not_self"
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_impersonation_sessions_admin_user_id",
        "impersonation_sessions",
        ["admin_user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_impersonation_sessions_target_user_id",
        "impersonation_sessions",
        ["target_user_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_impersonation_sessions_target_org_id",
        "impersonation_sessions",
        ["target_org_id"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_impersonation_sessions_admin_active",
        "impersonation_sessions",
        ["admin_user_id"],
        postgresql_where=sa.text("ended_at IS NULL"),
        if_not_exists=True,
    )

    _seed_permissions()

    op.execute("UPDATE organizations SET tier = 'community' WHERE tier = 'free'")
    op.execute("UPDATE organizations SET tier = 'team' WHERE tier = 'starter'")
    op.execute("UPDATE organizations SET tier = 'team' WHERE tier = 'pro'")

    op.execute("UPDATE org_licenses SET tier = 'community' WHERE tier = 'free'")
    op.execute("UPDATE org_licenses SET tier = 'team' WHERE tier = 'starter'")
    op.execute("UPDATE org_licenses SET tier = 'team' WHERE tier = 'pro'")

    op.execute(
        "UPDATE feature_flags SET min_tier = 'community' WHERE min_tier = 'free'"
    )
    op.execute("UPDATE feature_flags SET min_tier = 'team' WHERE min_tier = 'starter'")
    op.execute("UPDATE feature_flags SET min_tier = 'team' WHERE min_tier = 'pro'")


def downgrade() -> None:
    op.drop_table("impersonation_sessions", if_exists=True)
    op.drop_table("org_invites", if_exists=True)
    op.drop_table("password_reset_tokens", if_exists=True)
    op.drop_table("login_attempts", if_exists=True)
    op.drop_table("email_verification_tokens", if_exists=True)
    op.drop_table("refresh_tokens", if_exists=True)
    op.drop_table("billing_audit_log", if_exists=True)
    op.drop_table("refunds", if_exists=True)
    op.drop_table("invoice_line_items", if_exists=True)
    op.drop_table("invoices", if_exists=True)
    op.drop_table("subscription_events", if_exists=True)
    op.drop_table("subscriptions", if_exists=True)
    op.drop_table("plan_feature_bundles", if_exists=True)
    op.drop_table("feature_bundles", if_exists=True)
    op.drop_table("billing_prices", if_exists=True)
    op.drop_table("billing_plans", if_exists=True)
    op.drop_table("sync_watermarks", if_exists=True)
    op.drop_table("metric_checkpoints", if_exists=True)
    op.drop_table("org_retention_policies", if_exists=True)
    op.drop_table("org_ip_allowlist", if_exists=True)
    op.drop_table("sso_providers", if_exists=True)
    op.drop_table("audit_logs", if_exists=True)
    op.drop_table("org_licenses", if_exists=True)
    op.drop_table("org_feature_overrides", if_exists=True)
    op.drop_table("feature_flags", if_exists=True)
    op.drop_table("role_permissions", if_exists=True)
    op.drop_table("permissions", if_exists=True)
    op.drop_table("memberships", if_exists=True)
    op.drop_table("organizations", if_exists=True)
    op.drop_table("users", if_exists=True)
    op.drop_table("team_mappings", if_exists=True)
    op.drop_table("identity_mappings", if_exists=True)
    op.drop_table("job_runs", if_exists=True)
    op.drop_table("scheduled_jobs", if_exists=True)
    op.drop_table("sync_configurations", if_exists=True)
    op.drop_table("integration_credentials", if_exists=True)
    op.drop_table("settings", if_exists=True)


def _seed_permissions() -> None:
    conn = op.get_bind()
    permission_ids: dict[str, uuid.UUID] = {}

    for name, resource, action, description in STANDARD_PERMISSIONS:
        conn.execute(
            sa.text(
                "INSERT INTO permissions (id, name, resource, action, description) "
                "VALUES (:id, :name, :resource, :action, :description) "
                "ON CONFLICT (name) DO NOTHING"
            ),
            {
                "id": uuid.uuid4(),
                "name": name,
                "resource": resource,
                "action": action,
                "description": description,
            },
        )

        permission_id = conn.execute(
            sa.text("SELECT id FROM permissions WHERE name = :name"),
            {"name": name},
        ).scalar_one()
        permission_ids[name] = permission_id

    for role, perms in ROLE_PERMISSIONS.items():
        for perm_name in perms:
            permission_id = permission_ids.get(perm_name)
            if permission_id is None:
                continue

            conn.execute(
                sa.text(
                    "INSERT INTO role_permissions (id, role, permission_id) "
                    "VALUES (:id, :role, :permission_id) "
                    "ON CONFLICT (role, permission_id) DO NOTHING"
                ),
                {
                    "id": uuid.uuid4(),
                    "role": role,
                    "permission_id": permission_id,
                },
            )
