"""Sync configuration service.

Manages provider sync configuration rows (cron-like sync target lists,
sync options, credential association).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import SyncConfiguration


class SyncConfigurationService:
    """Service for managing sync configurations."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def get(
        self, name: str, provider: str | None = None
    ) -> SyncConfiguration | None:
        """Get a sync configuration by name (and optionally provider)."""
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.org_id == self.org_id,
            SyncConfiguration.name == name,
        )
        if provider is not None:
            stmt = stmt.where(SyncConfiguration.provider == provider)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, config_id: str) -> SyncConfiguration | None:
        """Get a sync configuration by ID."""
        import uuid as uuid_module

        try:
            uid = uuid_module.UUID(config_id)
        except (ValueError, AttributeError):
            return None
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.org_id == self.org_id,
            SyncConfiguration.id == uid,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def update(
        self,
        name: str,
        provider: str | None = None,
        sync_targets: list[str] | None = None,
        sync_options: dict[str, Any] | None = None,
        is_active: bool | None = None,
    ) -> SyncConfiguration | None:
        """Update a sync configuration."""
        config: Any | None = await self.get(name, provider=provider)
        if config is None:
            return None

        if sync_targets is not None:
            config.sync_targets = sync_targets
        if sync_options is not None:
            config.sync_options = sync_options
        if is_active is not None:
            config.is_active = is_active

        await self.session.flush()
        return config

    async def list_all(self, active_only: bool = False) -> list[SyncConfiguration]:
        """List all sync configurations."""
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(SyncConfiguration.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete(self, name: str, provider: str | None = None) -> bool:
        """Delete a sync configuration."""
        config = await self.get(name, provider=provider)
        if config is None:
            return False

        await self.session.delete(config)
        await self.session.flush()
        return True
