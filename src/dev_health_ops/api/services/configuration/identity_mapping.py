"""Identity mapping service.

Maps a canonical org-level identity to one or more provider-specific
identities, with optional team membership.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import IdentityMapping


class IdentityMappingService:
    """Service for managing identity mappings."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def get(self, canonical_id: str) -> IdentityMapping | None:
        """Get an identity mapping by canonical ID."""
        stmt = select(IdentityMapping).where(
            IdentityMapping.org_id == self.org_id,
            IdentityMapping.canonical_id == canonical_id,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def find_by_provider_identity(
        self,
        provider: str,
        identity: str,
    ) -> IdentityMapping | None:
        """Find an identity mapping by provider-specific identity."""
        stmt = select(IdentityMapping).where(
            IdentityMapping.org_id == self.org_id,
        )
        result = await self.session.execute(stmt)
        mappings: list[Any] = list(result.scalars().all())

        for mapping in mappings:
            identities = mapping.provider_identities.get(provider, [])
            if identity in identities:
                return mapping
        return None

    async def create_or_update(
        self,
        canonical_id: str,
        display_name: str | None = None,
        email: str | None = None,
        provider_identities: dict[str, list[str]] | None = None,
        team_ids: list[str] | None = None,
    ) -> IdentityMapping:
        """Create or update an identity mapping."""
        mapping: Any | None = await self.get(canonical_id)

        if mapping is None:
            mapping = IdentityMapping(
                canonical_id=canonical_id,
                org_id=self.org_id,
                display_name=display_name,
                email=email,
                provider_identities=provider_identities or {},
                team_ids=team_ids or [],
            )
            self.session.add(mapping)
        else:
            if display_name is not None:
                mapping.display_name = display_name
            if email is not None:
                mapping.email = email
            if provider_identities is not None:
                mapping.provider_identities = provider_identities
            if team_ids is not None:
                mapping.team_ids = team_ids

        await self.session.flush()
        return mapping

    async def add_provider_identity(
        self,
        canonical_id: str,
        provider: str,
        identity: str,
    ) -> IdentityMapping | None:
        """Add a provider identity to an existing mapping."""
        mapping: Any | None = await self.get(canonical_id)
        if mapping is None:
            return None

        identities = mapping.provider_identities.get(provider, [])
        if identity not in identities:
            identities.append(identity)
            mapping.provider_identities = {
                **mapping.provider_identities,
                provider: identities,
            }
            await self.session.flush()

        return mapping

    async def list_all(self, active_only: bool = True) -> list[IdentityMapping]:
        """List all identity mappings."""
        stmt = select(IdentityMapping).where(
            IdentityMapping.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(IdentityMapping.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
