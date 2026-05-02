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
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dev_health_ops.models.git import GUID, Base

if TYPE_CHECKING:
    from dev_health_ops.models.retention import OrgRetentionPolicy


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

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    username: Mapped[str | None] = mapped_column(
        Text, nullable=True, unique=True, index=True
    )
    password_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    full_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Auth provider info
    auth_provider: Mapped[str | None] = mapped_column(
        Text, default=AuthProvider.LOCAL.value
    )
    auth_provider_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Account status
    is_active: Mapped[bool | None] = mapped_column(
        Boolean, default=True, nullable=False
    )
    is_verified: Mapped[bool | None] = mapped_column(
        Boolean, default=False, nullable=False
    )
    is_superuser: Mapped[bool | None] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # Timestamps
    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    memberships: Mapped[list[Membership]] = relationship(
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


class LoginAttempt(Base):
    __tablename__ = "login_attempts"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    attempt_count: Mapped[int | None] = mapped_column(
        Integer, nullable=False, default=0
    )
    first_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (Index("ix_login_attempts_locked_until", "locked_until"),)

    def __repr__(self) -> str:
        return f"<LoginAttempt email={self.email} attempts={self.attempt_count}>"


class Organization(Base):
    """Organization (tenant) model.

    Organizations are the primary multi-tenancy boundary.
    All data (settings, credentials, metrics) is scoped to an organization.
    """

    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Organization settings (non-sensitive config)
    settings: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, default=dict, nullable=False
    )

    # Billing/tier info (for SaaS)
    tier: Mapped[str | None] = mapped_column(Text, default="community", nullable=False)
    stripe_customer_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Status
    is_active: Mapped[bool | None] = mapped_column(
        Boolean, default=True, nullable=False
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    memberships: Mapped[list[Membership]] = relationship(
        "Membership",
        back_populates="organization",
        cascade="all, delete-orphan",
    )
    retention_policies: Mapped[list["OrgRetentionPolicy"]] = relationship(  # noqa: UP037
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

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)

    user_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    role: Mapped[str | None] = mapped_column(
        Text, default=MemberRole.MEMBER.value, nullable=False
    )

    # Who invited this user (optional)
    invited_by_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    # When the user accepted/joined
    joined_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    user: Mapped[User] = relationship(
        "User",
        back_populates="memberships",
        foreign_keys=[user_id],
    )
    organization: Mapped[Organization] = relationship(
        "Organization",
        back_populates="memberships",
    )
    invited_by: Mapped[User | None] = relationship(
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

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    action: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
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

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    role: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    permission_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("permissions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    permission: Mapped[Permission] = relationship("Permission")

    __table_args__ = (
        UniqueConstraint("role", "permission_id", name="uq_role_permission"),
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
