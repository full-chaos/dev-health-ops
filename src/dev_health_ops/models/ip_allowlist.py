from __future__ import annotations

import ipaddress
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class OrgIPAllowlist(Base):
    __tablename__ = "org_ip_allowlist"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    ip_range = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    created_by_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)

    organization = relationship("Organization")
    created_by = relationship("User")

    __table_args__ = (
        UniqueConstraint("org_id", "ip_range", name="uq_org_ip_allowlist_org_range"),
        Index("ix_org_ip_allowlist_org_active", "org_id", "is_active"),
    )

    def __init__(
        self,
        org_id: uuid.UUID,
        ip_range: str,
        description: Optional[str] = None,
        is_active: bool = True,
        created_by_id: Optional[uuid.UUID] = None,
        expires_at: Optional[datetime] = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.ip_range = ip_range
        self.description = description
        self.is_active = is_active
        self.created_by_id = created_by_id
        self.expires_at = expires_at
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return f"<OrgIPAllowlist {self.ip_range} org={self.org_id}>"

    def matches_ip(self, ip_address: str) -> bool:
        try:
            check_ip = ipaddress.ip_address(ip_address)
            if "/" in str(self.ip_range):
                network = ipaddress.ip_network(self.ip_range, strict=False)
                return check_ip in network
            else:
                allowed_ip = ipaddress.ip_address(self.ip_range)
                return check_ip == allowed_ip
        except ValueError:
            return False

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    @property
    def is_valid(self) -> bool:
        return self.is_active and not self.is_expired


def is_valid_ip_or_cidr(value: str) -> bool:
    try:
        if "/" in value:
            ipaddress.ip_network(value, strict=False)
        else:
            ipaddress.ip_address(value)
        return True
    except ValueError:
        return False
