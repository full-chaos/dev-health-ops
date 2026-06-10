"""Server-side refresh token storage for session management."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class RefreshToken(Base):
    """Stores hashed refresh tokens for revocation and rotation."""

    __tablename__ = "refresh_tokens"

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
    )
    token_hash: Mapped[str] = mapped_column(
        Text, nullable=False, unique=True, index=True
    )
    family_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False, index=True)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    replaced_by_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Plain JTI (UUID string) of the successor token, written atomically by
    # rotate_token.  A concurrent request that presents a just-revoked token
    # within the grace window can use this to re-issue the *same* successor
    # JWT instead of triggering family-revocation.  Storing the raw JTI (not
    # its hash) is intentional: reconstruction of the successor JWT requires
    # the JWT_SECRET_KEY too, so DB read-access alone is insufficient.
    successor_jti: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_refresh_tokens_user_family", "user_id", "family_id"),
        Index("ix_refresh_tokens_expires", "expires_at"),
    )
