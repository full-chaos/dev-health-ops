from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, String, Text, CheckConstraint

from dev_health_ops.models.git import Base, GUID


class ImpersonationSession(Base):
    """Represents server-side impersonation sessions between an admin and a target user.

    Tracks active and historical impersonation sessions scoped to an organization.
    """

    __tablename__ = "impersonation_sessions"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)

    # Admin initiating the impersonation
    admin_user_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Target user being impersonated
    target_user_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Organization within which impersonation occurs
    target_org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Role type for the impersonated session
    target_role = Column(String(50), nullable=False)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at = Column(DateTime(timezone=True), nullable=False)
    ended_at = Column(DateTime(timezone=True), nullable=True)

    # Optional client data for auditing
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)

    # Data integrity: admin cannot impersonate themselves
    __table_args__ = (
        CheckConstraint(
            "admin_user_id != target_user_id", name="ck_impersonation_not_self"
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ImpersonationSession id={self.id} admin_user_id={self.admin_user_id} "
            f"target_user_id={self.target_user_id}>"
        )
