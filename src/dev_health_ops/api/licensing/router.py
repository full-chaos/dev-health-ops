from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing.types import LicenseTier

router = APIRouter(prefix="/api/v1/licensing", tags=["licensing"])


class EntitlementsResponse(BaseModel):
    org_id: str
    tier: str
    licensed_users: int | None = None
    licensed_repos: int | None = None
    features: dict[str, bool] = Field(default_factory=dict)
    features_override: dict[str, bool] | None = None
    limits_override: dict[str, int | float | None] | None = None
    expires_at: datetime | None = None
    is_valid: bool = True
    limits: dict[str, int | float | None] = Field(default_factory=dict)


TIER_RANK = {
    LicenseTier.COMMUNITY: 0,
    LicenseTier.TEAM: 1,
    LicenseTier.ENTERPRISE: 2,
}


@router.get("/entitlements/{org_id}", response_model=EntitlementsResponse)
async def get_entitlements(org_id: str) -> EntitlementsResponse:
    from sqlalchemy import select

    from dev_health_ops.models.licensing import (
        FeatureFlag,
        OrgFeatureOverride,
        OrgLicense,
        TIER_LIMITS,
    )
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

        flags_result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.is_enabled == True)  # noqa: E712
        )
        all_flags = flags_result.scalars().all()

        overrides_result = await session.execute(
            select(OrgFeatureOverride).where(OrgFeatureOverride.org_id == org_uuid)
        )
        org_overrides = {str(o.feature_id): o for o in overrides_result.scalars().all()}

    try:
        tier_enum = LicenseTier(org.tier)
    except ValueError:
        tier_enum = LicenseTier.COMMUNITY

    org_rank = TIER_RANK.get(tier_enum, 0)

    features: dict[str, bool] = {}
    for flag in all_flags:
        try:
            min_tier = LicenseTier(flag.min_tier)
        except ValueError:
            min_tier = LicenseTier.COMMUNITY
        features[flag.key] = org_rank >= TIER_RANK.get(min_tier, 0)

    if org_license and org_license.features_override:
        for key, enabled in org_license.features_override.items():
            features[key] = enabled

    for override in org_overrides.values():
        for flag in all_flags:
            if str(flag.id) == str(override.feature_id):
                now = datetime.now().astimezone()
                if override.expires_at and override.expires_at < now:
                    continue
                features[flag.key] = override.is_enabled
                break

    limits = dict(TIER_LIMITS.get(tier_enum, TIER_LIMITS[LicenseTier.COMMUNITY]))
    if org_license and org_license.limits_override:
        limits.update(org_license.limits_override)

    return EntitlementsResponse(
        org_id=str(org.id),
        tier=org.tier,
        licensed_users=org_license.licensed_users if org_license else None,
        licensed_repos=org_license.licensed_repos if org_license else None,
        features=features,
        features_override=org_license.features_override if org_license else None,
        limits_override=org_license.limits_override if org_license else None,
        expires_at=org_license.expires_at if org_license else None,
        is_valid=org_license.is_valid if org_license else True,
        limits=limits,
    )
