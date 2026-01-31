"""User, Organization, and Membership models for Enterprise Edition.

This module defines the core multi-tenancy models:
- User: Individual user accounts
- Organization: Multi-tenant organization containers
- Membership: User-Organization relationships with roles

These models support both SaaS and self-hosted deployments.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class MemberRole(str, Enum):
    """Roles for organization membership."""

    OWNER = "owner"  # Full control, can delete org
    ADMIN = "admin"  # Can manage members and settings
    MEMBER = "member"  # Standard access
    VIEWER = "viewer"  # Read-only access


class AuthProvider(str, Enum):
    """Authentication providers for user accounts."""

    LOCAL = "local"  # Email/password
    GITHUB = "github"
    GITLAB = "gitlab"
    GOOGLE = "google"
    SAML = "saml"
    OIDC = "oidc"


class User(Base):
    """User account model.

    Users can belong to multiple organizations via Membership.
    Authentication can be local (email/password) or via OAuth/SAML.
    """

    __tablename__ = "users"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    email = Column(Text, nullable=False, unique=True, index=True)
    username = Column(Text, nullable=True, unique=True, index=True)
    password_hash = Column(Text, nullable=True)  # Null for OAuth users
    full_name = Column(Text, nullable=True)
    avatar_url = Column(Text, nullable=True)

    # Auth provider info
    auth_provider = Column(Text, default=AuthProvider.LOCAL.value)
    auth_provider_id = Column(Text, nullable=True)  # External provider user ID

    # Account status
    is_active = Column(Boolean, default=True, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    is_superuser = Column(Boolean, default=False, nullable=False)

    # Timestamps
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    memberships = relationship(
        "Membership",
        back_populates="user",
        foreign_keys="Membership.user_id",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_users_auth_provider", "auth_provider", "auth_provider_id"),
    )

    def __repr__(self) -> str:
        return f"<User {self.email}>"


class Organization(Base):
    """Organization (tenant) model.

    Organizations are the primary multi-tenancy boundary.
    All data (settings, credentials, metrics) is scoped to an organization.
    """

    __tablename__ = "organizations"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    slug = Column(Text, nullable=False, unique=True, index=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=True)

    # Organization settings (non-sensitive config)
    settings = Column(JSON, default=dict, nullable=False)

    # Billing/tier info (for SaaS)
    tier = Column(Text, default="free", nullable=False)
    stripe_customer_id = Column(Text, nullable=True)

    # Status
    is_active = Column(Boolean, default=True, nullable=False)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    memberships = relationship(
        "Membership",
        back_populates="organization",
        cascade="all, delete-orphan",
    )
    retention_policies = relationship(
        "OrgRetentionPolicy",
        back_populates="organization",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Organization {self.slug}>"


class Membership(Base):
    """User-Organization membership with role.

    Links users to organizations with a specific role.
    A user can belong to multiple organizations.
    An organization can have multiple members.
    """

    __tablename__ = "memberships"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)

    user_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    role = Column(Text, default=MemberRole.MEMBER.value, nullable=False)

    # Who invited this user (optional)
    invited_by_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    # When the user accepted/joined
    joined_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    user = relationship(
        "User",
        back_populates="memberships",
        foreign_keys=[user_id],
    )
    organization = relationship(
        "Organization",
        back_populates="memberships",
    )
    invited_by = relationship(
        "User",
        foreign_keys=[invited_by_id],
    )

    __table_args__ = (
        UniqueConstraint("user_id", "org_id", name="uq_membership_user_org"),
        Index("ix_memberships_org_role", "org_id", "role"),
    )

    def __repr__(self) -> str:
        return f"<Membership user={self.user_id} org={self.org_id} role={self.role}>"


class Permission(Base):
    """Permission definition for RBAC.

    Permissions are defined as resource:action pairs (e.g., metrics:read, settings:write).
    They are assigned to roles via RolePermission.
    """

    __tablename__ = "permissions"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False, unique=True, index=True)
    description = Column(Text, nullable=True)
    resource = Column(Text, nullable=False, index=True)
    action = Column(Text, nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("resource", "action", name="uq_permission_resource_action"),
        Index("ix_permissions_resource_action", "resource", "action"),
    )

    def __repr__(self) -> str:
        return f"<Permission {self.name}>"


class RolePermission(Base):
    """Maps roles to permissions.

    This is a static mapping that defines what each role can do.
    Roles inherit permissions: owner > admin > member > viewer.
    """

    __tablename__ = "role_permissions"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    role = Column(Text, nullable=False, index=True)
    permission_id = Column(
        GUID(),
        ForeignKey("permissions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    permission = relationship("Permission")

    __table_args__ = (
        UniqueConstraint("role", "permission_id", name="uq_role_permission"),
        Index("ix_role_permissions_role", "role"),
    )

    def __repr__(self) -> str:
        return f"<RolePermission role={self.role} permission_id={self.permission_id}>"


# Standard permission definitions
STANDARD_PERMISSIONS = [
    # Analytics
    ("analytics:read", "analytics", "read", "View analytics dashboards and metrics"),
    ("analytics:export", "analytics", "export", "Export analytics data"),
    # Metrics
    ("metrics:read", "metrics", "read", "View computed metrics"),
    ("metrics:compute", "metrics", "compute", "Trigger metric computation jobs"),
    # Work Items
    ("work_items:read", "work_items", "read", "View work items and investments"),
    ("work_items:sync", "work_items", "sync", "Sync work items from providers"),
    # Git Data
    ("git:read", "git", "read", "View git commits and PRs"),
    ("git:sync", "git", "sync", "Sync git data from repositories"),
    # Teams
    ("teams:read", "teams", "read", "View team configurations"),
    ("teams:write", "teams", "write", "Create and modify teams"),
    # Settings
    ("settings:read", "settings", "read", "View organization settings"),
    ("settings:write", "settings", "write", "Modify organization settings"),
    # Integrations/Credentials
    ("integrations:read", "integrations", "read", "View integration configurations"),
    (
        "integrations:write",
        "integrations",
        "write",
        "Configure integrations and credentials",
    ),
    # Members
    ("members:read", "members", "read", "View organization members"),
    ("members:invite", "members", "invite", "Invite new members"),
    ("members:manage", "members", "manage", "Manage member roles and remove members"),
    # Organization
    ("org:read", "org", "read", "View organization details"),
    ("org:write", "org", "write", "Modify organization details"),
    ("org:delete", "org", "delete", "Delete the organization"),
    # Admin
    ("admin:users", "admin", "users", "Manage all users (superuser only)"),
    ("admin:orgs", "admin", "orgs", "Manage all organizations (superuser only)"),
]

# Default permissions for each role (cumulative - higher roles inherit lower)
ROLE_PERMISSIONS = {
    MemberRole.VIEWER: [
        "analytics:read",
        "metrics:read",
        "work_items:read",
        "git:read",
        "teams:read",
        "settings:read",
        "members:read",
        "org:read",
    ],
    MemberRole.MEMBER: [
        # Inherits VIEWER permissions plus:
        "analytics:export",
    ],
    MemberRole.ADMIN: [
        # Inherits MEMBER permissions plus:
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
    MemberRole.OWNER: [
        # Inherits ADMIN permissions plus:
        "org:delete",
    ],
}
