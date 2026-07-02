"""Customer-push source registration and ingest-token models (CHAOS-2696/2712).

See docs/architecture/customer-push-authz.md for the one-active-owner and
token-scoping design rationale. Table names (``external_ingest_sources``/
``external_ingest_tokens``) match the ``external_ingest_*`` feature family
(batches/rejections/payloads) and intentionally avoid the legacy
``/api/v1/ingest`` router's vocabulary.
"""

from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base

TOKEN_PREFIX = "fcpush_"
TOKEN_PREFIX_DISPLAY_LENGTH = 12


class IngestSourceMode(str, Enum):
    FULLCHAOS_SYNC = "fullchaos_sync"
    CUSTOMER_PUSH = "customer_push"
    DISABLED = "disabled"


class IngestWebhookMode(str, Enum):
    """Reserved for CHAOS-2715 (webhook-assisted ingestion).

    v1 only supports ``disabled``/``customer_relay`` -- the admin API 400s on
    ``fullchaos_hosted`` (must-not-foreclose contract, see 0032 migration).
    """

    DISABLED = "disabled"
    CUSTOMER_RELAY = "customer_relay"
    FULLCHAOS_HOSTED = "fullchaos_hosted"


class IngestTokenScope(str, Enum):
    SCHEMA_READ = "schema:read"
    INGEST_WRITE = "ingest:write"
    INGEST_STATUS = "ingest:status"


def generate_ingest_token() -> str:
    """Mint a new plaintext ingest token: ``fcpush_<43-char urlsafe secret>``.

    256 bits of entropy (``secrets.token_urlsafe(32)``) makes a fast hash
    function (see ``hash_ingest_token``) safe for this purpose -- this is a
    high-entropy bearer secret, not a user password.
    """
    return f"{TOKEN_PREFIX}{secrets.token_urlsafe(32)}"


def hash_ingest_token(token: str) -> str:
    """sha256 digest, matching the house convention (RefreshToken,
    PasswordResetToken, OrgInvite, EmailVerificationToken)."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class IngestSource(Base):
    """One row per ``(org_id, system, instance)``.

    Tracks which of ``fullchaos_sync | customer_push | disabled`` currently
    owns that source instance -- the single registry the one-active-owner XOR
    check is enforced against, not just customer-push rows (Design Decision 4).
    """

    __tablename__ = "external_ingest_sources"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    system: Mapped[str] = mapped_column(Text, nullable=False)
    instance: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    mode: Mapped[str] = mapped_column(
        Text, nullable=False, default=IngestSourceMode.DISABLED.value
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    webhook_mode: Mapped[str] = mapped_column(
        Text, nullable=False, default=IngestWebhookMode.DISABLED.value
    )
    # Reserved, unused in v1 (CHAOS-2715 must-not-foreclose contract).
    webhook_secret_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    # CC5: the integration_sources.id resolved by per-provider matching at
    # registration time (or the most recent enable/mode-change re-check).
    # NULL means no managed source currently matches this instance.
    matched_integration_source_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID, nullable=True
    )
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "system",
            "instance",
            name="uq_external_ingest_sources_org_system_instance",
        ),
    )

    def is_write_eligible(self) -> bool:
        return self.enabled and self.mode == IngestSourceMode.CUSTOMER_PUSH.value


class IngestToken(Base):
    """Hashed bearer credential scoped to an org + optionally a single source."""

    __tablename__ = "external_ingest_tokens"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    # NULL means "all sources in this org" -- only legal when scopes are a
    # subset of {schema:read, ingest:status} (never ingest:write); enforced at
    # creation time in the admin endpoint (Design Decision 7).
    source_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("external_ingest_sources.id"),
        nullable=True,
        index=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    token_hash: Mapped[str] = mapped_column(
        Text, nullable=False, unique=True, index=True
    )
    # First 12 chars of the full token (incl. the fcpush_ marker) for
    # human-recognizable UI/audit display -- never the full plaintext secret.
    token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_used_ip: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_external_ingest_tokens_org_active", "org_id", "revoked_at"),
    )

    def is_valid(self, now: datetime) -> bool:
        if self.revoked_at is not None:
            return False
        if self.expires_at is not None and now > self.expires_at:
            return False
        return True
