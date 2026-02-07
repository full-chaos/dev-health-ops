from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from dev_health_ops.db import get_postgres_session

router = APIRouter(prefix="/api/v1/licensing", tags=["licensing"])


class EntitlementsResponse(BaseModel):
    org_id: str
    tier: str
    licensed_users: int | None = None
    licensed_repos: int | None = None
    features_override: dict[str, bool] | None = None
    limits_override: dict[str, int | float | None] | None = None
    expires_at: datetime | None = None
    is_valid: bool = True
    limits: dict[str, int | float | None] = Field(default_factory=dict)


@router.get("/entitlements/{org_id}", response_model=EntitlementsResponse)
async def get_entitlements(org_id: str) -> EntitlementsResponse:
    from sqlalchemy import select

    from dev_health_ops.models.licensing import OrgLicense, TIER_LIMITS, Tier
    from dev_health_ops.models.users import Organization

    try:
        org_uuid = uuid.UUID(org_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Organization not found")

    async with get_postgres_session() as session:
        org_result = await session.execute(
            select(Organization).where(Organization.id == org_uuid)
        )
        org = org_result.scalar_one_or_none()
        if org is None:
            raise HTTPException(status_code=404, detail="Organization not found")

        license_result = await session.execute(
            select(OrgLicense).where(OrgLicense.org_id == org_uuid)
        )
        org_license = license_result.scalar_one_or_none()

    try:
        tier_enum = Tier(org.tier)
    except ValueError:
        tier_enum = Tier.FREE

    limits = dict(TIER_LIMITS.get(tier_enum, TIER_LIMITS[Tier.FREE]))
    if org_license and org_license.limits_override:
        limits.update(org_license.limits_override)

    return EntitlementsResponse(
        org_id=str(org.id),
        tier=org.tier,
        licensed_users=org_license.licensed_users if org_license else None,
        licensed_repos=org_license.licensed_repos if org_license else None,
        features_override=org_license.features_override if org_license else None,
        limits_override=org_license.limits_override if org_license else None,
        expires_at=org_license.expires_at if org_license else None,
        is_valid=org_license.is_valid if org_license else True,
        limits=limits,
    )
