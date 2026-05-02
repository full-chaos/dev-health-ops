from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class ImpersonationSession(Base):
    """Represents server-side impersonation sessions between an admin and a target user.

    Tracks active and historical impersonation sessions scoped to an organization.
    """

    __tablename__ = "impersonation_sessions"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)

    # Admin initiating the impersonation
    admin_user_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Target user being impersonated
    target_user_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Organization within which impersonation occurs
    target_org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Role type for the impersonated session
    target_role: Mapped[str] = mapped_column(String(50), nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Optional client data for auditing
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)

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
