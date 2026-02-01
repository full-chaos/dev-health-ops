"""Add RBAC permissions and role_permissions tables.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-01-30 15:00:00

Creates tables for Role-Based Access Control:
- permissions: Permission definitions (resource:action pairs)
- role_permissions: Maps roles to permissions
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
import uuid


revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


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
    )
    op.create_index("ix_permissions_name", "permissions", ["name"], unique=True)
    op.create_index("ix_permissions_resource", "permissions", ["resource"])
    op.create_index(
        "ix_permissions_resource_action", "permissions", ["resource", "action"]
    )
    op.create_unique_constraint(
        "uq_permission_resource_action", "permissions", ["resource", "action"]
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
    )
    op.create_index("ix_role_permissions_role", "role_permissions", ["role"])
    op.create_index(
        "ix_role_permissions_permission_id", "role_permissions", ["permission_id"]
    )
    op.create_unique_constraint(
        "uq_role_permission", "role_permissions", ["role", "permission_id"]
    )

    conn = op.get_bind()
    permission_ids = {}

    for name, resource, action, description in STANDARD_PERMISSIONS:
        perm_id = uuid.uuid4()
        permission_ids[name] = perm_id
        conn.execute(
            sa.text(
                "INSERT INTO permissions (id, name, resource, action, description) "
                "VALUES (:id, :name, :resource, :action, :description)"
            ),
            {
                "id": perm_id,
                "name": name,
                "resource": resource,
                "action": action,
                "description": description,
            },
        )

    for role, perms in ROLE_PERMISSIONS.items():
        for perm_name in perms:
            if perm_name in permission_ids:
                conn.execute(
                    sa.text(
                        "INSERT INTO role_permissions (id, role, permission_id) "
                        "VALUES (:id, :role, :permission_id)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "role": role,
                        "permission_id": permission_ids[perm_name],
                    },
                )


def downgrade() -> None:
    op.drop_table("role_permissions")
    op.drop_table("permissions")
