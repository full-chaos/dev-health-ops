"""Audit logging models for Enterprise Edition.

This module defines the audit log infrastructure for tracking user actions
and system events. Audit logging is an Enterprise-tier compliance feature.

Key design principles:
- Immutable records (no updated_at - logs are never modified)
- Org-scoped for multi-tenancy
- Flexible resource tracking via resource_type + resource_id
- JSON fields for extensibility (changes, metadata)
- Optimized indexes for common query patterns
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    Text,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class AuditAction(str, Enum):
    """Standard audit action types."""

    # CRUD operations
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"

    # Auth events
    LOGIN = "login"
    LOGOUT = "logout"
    LOGIN_FAILED = "login_failed"
    PASSWORD_CHANGED = "password_changed"
    PASSWORD_RESET = "password_reset"

    # SSO events
    SSO_LOGIN = "sso_login"
    SSO_LOGOUT = "sso_logout"
    SSO_LINK = "sso_link"
    SSO_UNLINK = "sso_unlink"

    # Permission events
    PERMISSION_GRANTED = "permission_granted"
    PERMISSION_REVOKED = "permission_revoked"
    ROLE_CHANGED = "role_changed"

    # Membership events
    MEMBER_INVITED = "member_invited"
    MEMBER_JOINED = "member_joined"
    MEMBER_REMOVED = "member_removed"

    # Feature/License events
    FEATURE_ENABLED = "feature_enabled"
    FEATURE_DISABLED = "feature_disabled"
    LICENSE_UPDATED = "license_updated"
    LICENSE_VALIDATED = "license_validated"
    LICENSE_VALIDATION_FAILED = "license_validation_failed"
    LICENSE_GRACE_PERIOD_ENTERED = "license_grace_period_entered"
    FEATURE_ACCESS_DENIED = "feature_access_denied"
    LIMIT_EXCEEDED = "limit_exceeded"

    # Data events
    EXPORT = "export"
    IMPORT = "import"
    SYNC = "sync"
    RETENTION_CLEANUP = "retention_cleanup"

    # Security events
    API_KEY_CREATED = "api_key_created"
    API_KEY_REVOKED = "api_key_revoked"
    IP_BLOCKED = "ip_blocked"
    IP_ALLOWED = "ip_allowed"

    # Settings events
    SETTINGS_UPDATED = "settings_updated"
    CREDENTIAL_CREATED = "credential_created"
    CREDENTIAL_UPDATED = "credential_updated"
    CREDENTIAL_DELETED = "credential_deleted"

    # Generic
    OTHER = "other"


class AuditResourceType(str, Enum):
    """Standard resource types for audit logging."""

    USER = "user"
    ORGANIZATION = "organization"
    MEMBERSHIP = "membership"
    TEAM = "team"
    REPOSITORY = "repository"
    WORK_ITEM = "work_item"
    SETTING = "setting"
    CREDENTIAL = "credential"
    SSO_PROVIDER = "sso_provider"
    FEATURE_FLAG = "feature_flag"
    LICENSE = "license"
    API_KEY = "api_key"
    SCHEDULED_JOB = "scheduled_job"
    RETENTION_POLICY = "retention_policy"
    IP_ALLOWLIST = "ip_allowlist"
    AUDIT_LOG = "audit_log"
    METRICS = "metrics"
    SESSION = "session"
    OTHER = "other"


class AuditLog(Base):
    """Audit log entry for tracking user actions and system events.

    Records all significant actions (create, update, delete, permission changes, etc.)
    with full context for compliance and debugging.

    Features:
    - Immutable records (no updates after creation)
    - Org-scoped for multi-tenancy
    - Flexible resource tracking
    - Full context via JSON fields
    - Optimized for time-range and resource queries

    Example usage:
        audit = AuditLog(
            org_id=org.id,
            user_id=user.id,
            action=AuditAction.CREATE.value,
            resource_type=AuditResourceType.CREDENTIAL.value,
            resource_id=str(credential.id),
            description="Created GitHub integration credential",
            changes={"name": "github-prod"},
            metadata={"ip_address": "1.2.3.4", "user_agent": "..."},
        )
    """

    __tablename__ = "audit_logs"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Organization this audit log belongs to",
    )

    # Who performed the action (null for system-triggered actions)
    user_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="User who performed the action (null for system actions)",
    )

    # What action was performed
    action = Column(
        Text,
        nullable=False,
        index=True,
        comment="Action type (create, update, delete, login, etc.)",
    )

    # What resource was affected
    resource_type = Column(
        Text,
        nullable=False,
        index=True,
        comment="Type of resource (user, credential, setting, etc.)",
    )
    resource_id = Column(
        Text,
        nullable=False,
        index=True,
        comment="ID of the affected resource",
    )

    # Context and details
    description = Column(
        Text,
        nullable=True,
        comment="Human-readable description of the action",
    )
    changes = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Before/after values for updates, or created values",
    )
    request_metadata = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Additional context (IP address, user agent, request ID, etc.)",
    )

    # Status tracking
    status = Column(
        Text,
        nullable=False,
        default="success",
        comment="Action status: success or failure",
    )
    error_message = Column(
        Text,
        nullable=True,
        comment="Error message if status is failure",
    )

    # Timestamp (immutable - no updated_at for audit logs)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
        comment="When the action occurred",
    )

    # Relationships
    organization = relationship("Organization")
    user = relationship("User")

    # Indexes for common query patterns
    __table_args__ = (
        # Query by org + time range (most common)
        Index("ix_audit_logs_org_created", "org_id", "created_at"),
        # Query by user + time range
        Index("ix_audit_logs_user_created", "user_id", "created_at"),
        # Query by resource
        Index("ix_audit_logs_resource", "resource_type", "resource_id"),
        # Query by action type + time
        Index("ix_audit_logs_action_created", "action", "created_at"),
        # Query by org + action + time (filter by action type)
        Index("ix_audit_logs_org_action_created", "org_id", "action", "created_at"),
    )

    def __init__(
        self,
        org_id: uuid.UUID,
        action: str,
        resource_type: str,
        resource_id: str,
        user_id: Optional[uuid.UUID] = None,
        description: Optional[str] = None,
        changes: Optional[dict[str, Any]] = None,
        request_metadata: Optional[dict[str, Any]] = None,
        status: str = "success",
        error_message: Optional[str] = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.user_id = user_id
        self.action = action
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.description = description
        self.changes = changes or {}
        self.request_metadata = request_metadata or {}
        self.status = status
        self.error_message = error_message
        self.created_at = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return (
            f"<AuditLog {self.action} {self.resource_type}:{self.resource_id} "
            f"by user={self.user_id}>"
        )

    @classmethod
    def create_entry(
        cls,
        org_id: uuid.UUID,
        action: AuditAction | str,
        resource_type: AuditResourceType | str,
        resource_id: str,
        user_id: Optional[uuid.UUID] = None,
        description: Optional[str] = None,
        changes: Optional[dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        request_id: Optional[str] = None,
        extra_metadata: Optional[dict[str, Any]] = None,
    ) -> "AuditLog":
        """Factory method to create an audit log entry with common metadata.

        Args:
            org_id: Organization ID
            action: Action type (use AuditAction enum values)
            resource_type: Resource type (use AuditResourceType enum values)
            resource_id: ID of the affected resource
            user_id: User who performed the action (optional for system actions)
            description: Human-readable description
            changes: Before/after values for the action
            ip_address: Client IP address
            user_agent: Client user agent string
            request_id: Request correlation ID
            extra_metadata: Additional metadata to include

        Returns:
            AuditLog instance ready to be added to session
        """
        # Normalize enum values to strings
        action_str = action.value if isinstance(action, AuditAction) else action
        resource_type_str = (
            resource_type.value
            if isinstance(resource_type, AuditResourceType)
            else resource_type
        )

        req_metadata: dict[str, Any] = {}
        if ip_address:
            req_metadata["ip_address"] = ip_address
        if user_agent:
            req_metadata["user_agent"] = user_agent
        if request_id:
            req_metadata["request_id"] = request_id
        if extra_metadata:
            req_metadata.update(extra_metadata)

        return cls(
            org_id=org_id,
            user_id=user_id,
            action=action_str,
            resource_type=resource_type_str,
            resource_id=resource_id,
            description=description,
            changes=changes,
            request_metadata=req_metadata if req_metadata else None,
        )
