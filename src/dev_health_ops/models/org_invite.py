from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, Index, Text

from dev_health_ops.models.git import Base, GUID


class OrgInvite(Base):
    __tablename__ = "org_invites"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    email = Column(Text, nullable=False, index=True)
    role = Column(Text, nullable=False, default="member")
    token_hash = Column(Text, nullable=False, unique=True, index=True)
    invited_by_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    status = Column(Text, nullable=False, default="pending", index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    accepted_at = Column(DateTime(timezone=True), nullable=True)
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

    __table_args__ = (
        Index("ix_org_invites_org_email_status", "org_id", "email", "status"),
        Index("ix_org_invites_org_expires", "org_id", "expires_at"),
    )
