from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.ip_allowlist import OrgIPAllowlist, is_valid_ip_or_cidr

logger = logging.getLogger(__name__)


@dataclass
class IPAllowlistEntry:
    id: str
    org_id: str
    ip_range: str
    description: Optional[str]
    is_active: bool
    created_by_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime]


class IPAllowlistService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_entry(
        self,
        org_id: uuid.UUID,
        ip_range: str,
        description: Optional[str] = None,
        created_by_id: Optional[uuid.UUID] = None,
        expires_at: Optional[datetime] = None,
    ) -> OrgIPAllowlist:
        if not is_valid_ip_or_cidr(ip_range):
            raise ValueError(f"Invalid IP address or CIDR range: {ip_range}")

        entry = OrgIPAllowlist(
            org_id=org_id,
            ip_range=ip_range,
            description=description,
            created_by_id=created_by_id,
            expires_at=expires_at,
        )

        self.session.add(entry)
        await self.session.flush()

        logger.info("IP allowlist entry created: %s for org=%s", ip_range, org_id)
        return entry

    async def get_entry(
        self, org_id: uuid.UUID, entry_id: uuid.UUID
    ) -> Optional[OrgIPAllowlist]:
        stmt = select(OrgIPAllowlist).where(
            and_(OrgIPAllowlist.id == entry_id, OrgIPAllowlist.org_id == org_id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_entries(
        self,
        org_id: uuid.UUID,
        active_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[Sequence[OrgIPAllowlist], int]:
        conditions = [OrgIPAllowlist.org_id == org_id]

        if active_only:
            conditions.append(OrgIPAllowlist.is_active == True)  # noqa: E712

        count_stmt = select(OrgIPAllowlist).where(and_(*conditions))
        count_result = await self.session.execute(count_stmt)
        total = len(count_result.scalars().all())

        stmt = (
            select(OrgIPAllowlist)
            .where(and_(*conditions))
            .order_by(OrgIPAllowlist.created_at)
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(stmt)
        entries = result.scalars().all()

        return entries, total

    async def update_entry(
        self,
        org_id: uuid.UUID,
        entry_id: uuid.UUID,
        ip_range: Optional[str] = None,
        description: Optional[str] = None,
        is_active: Optional[bool] = None,
        expires_at: Optional[datetime] = None,
    ) -> Optional[OrgIPAllowlist]:
        entry = await self.get_entry(org_id, entry_id)
        if not entry:
            return None

        if ip_range is not None:
            if not is_valid_ip_or_cidr(ip_range):
                raise ValueError(f"Invalid IP address or CIDR range: {ip_range}")
            entry.ip_range = ip_range
        if description is not None:
            entry.description = description
        if is_active is not None:
            entry.is_active = is_active
        if expires_at is not None:
            entry.expires_at = expires_at

        entry.updated_at = datetime.now(timezone.utc)
        await self.session.flush()

        logger.info("IP allowlist entry updated: %s for org=%s", entry_id, org_id)
        return entry

    async def delete_entry(self, org_id: uuid.UUID, entry_id: uuid.UUID) -> bool:
        entry = await self.get_entry(org_id, entry_id)
        if not entry:
            return False

        await self.session.delete(entry)
        await self.session.flush()

        logger.info("IP allowlist entry deleted: %s for org=%s", entry_id, org_id)
        return True

    async def check_ip_allowed(self, org_id: uuid.UUID, ip_address: str) -> bool:
        entries, _ = await self.list_entries(org_id, active_only=True, limit=1000)

        if not entries:
            return True

        now = datetime.now(timezone.utc)
        for entry in entries:
            if entry.expires_at and entry.expires_at < now:
                continue
            if entry.matches_ip(ip_address):
                return True

        return False

    async def get_active_entries_for_org(
        self, org_id: uuid.UUID
    ) -> Sequence[OrgIPAllowlist]:
        now = datetime.now(timezone.utc)
        stmt = select(OrgIPAllowlist).where(
            and_(
                OrgIPAllowlist.org_id == org_id,
                OrgIPAllowlist.is_active == True,  # noqa: E712
            )
        )
        result = await self.session.execute(stmt)
        entries = result.scalars().all()

        return [e for e in entries if not e.expires_at or e.expires_at > now]

    @staticmethod
    def to_entry(allowlist: OrgIPAllowlist) -> IPAllowlistEntry:
        return IPAllowlistEntry(
            id=str(allowlist.id),
            org_id=str(allowlist.org_id),
            ip_range=str(allowlist.ip_range),
            description=str(allowlist.description) if allowlist.description else None,
            is_active=bool(allowlist.is_active),
            created_by_id=str(allowlist.created_by_id)
            if allowlist.created_by_id
            else None,
            created_at=allowlist.created_at,
            updated_at=allowlist.updated_at,
            expires_at=allowlist.expires_at,
        )
