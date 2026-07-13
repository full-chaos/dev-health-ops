from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base

INTERNAL_SERVICE_TOKEN_PREFIX = "svc_acr_"


def generate_internal_service_token() -> str:
    return f"{INTERNAL_SERVICE_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"


def hash_internal_service_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class InternalServiceCredential(Base):
    __tablename__ = "internal_service_credentials"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    service_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    token_hash: Mapped[str] = mapped_column(
        Text, nullable=False, unique=True, index=True
    )
    token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index(
            "ix_internal_service_credentials_service_active",
            "service_name",
            "revoked_at",
        ),
    )

    @classmethod
    def from_plaintext_token(
        cls,
        *,
        token: str,
        service_name: str,
        scopes: list[str],
        expires_at: datetime | None,
        created_by_user_id: uuid.UUID | None = None,
    ) -> InternalServiceCredential:
        return cls(
            service_name=service_name,
            token_hash=hash_internal_service_token(token),
            token_prefix=token[:16],
            scopes=scopes,
            expires_at=expires_at,
            created_by_user_id=created_by_user_id,
        )

    @classmethod
    def issue(
        cls,
        *,
        service_name: str,
        scopes: list[str],
        created_by_user_id: uuid.UUID | None,
        expires_at: datetime | None = None,
    ) -> tuple[InternalServiceCredential, str]:
        token = generate_internal_service_token()
        return (
            cls.from_plaintext_token(
                token=token,
                service_name=service_name,
                scopes=scopes,
                expires_at=expires_at,
                created_by_user_id=created_by_user_id,
            ),
            token,
        )

    def is_valid(self, now: datetime) -> bool:
        if self.revoked_at is not None:
            return False
        expires_at = self.expires_at
        if expires_at is None:
            return True
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return now <= expires_at

    def public_metadata(self) -> dict[str, str | list[str] | None]:
        return {
            "id": str(self.id),
            "service_name": self.service_name,
            "token_prefix": self.token_prefix,
            "scopes": self.scopes,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "revoked_at": self.revoked_at.isoformat() if self.revoked_at else None,
            "last_used_at": self.last_used_at.isoformat()
            if self.last_used_at
            else None,
        }


class InternalServiceCredentialAudit(Base):
    __tablename__ = "internal_service_credential_audits"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    credential_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("internal_service_credentials.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    requested_org_id: Mapped[str | None] = mapped_column(
        Text, nullable=True, index=True
    )
    action: Mapped[str] = mapped_column(Text, nullable=False)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
