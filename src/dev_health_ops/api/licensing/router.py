from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing.types import LicenseTier

router = APIRouter(prefix="/api/v1/licensing", tags=["licensing"])


def _coerce_license_tier(value: object) -> LicenseTier:
    try:
        return LicenseTier(str(value))
    except ValueError:
        return LicenseTier.COMMUNITY


def _coerce_bool_map(value: object) -> dict[str, bool]:
    if not isinstance(value, dict):
        return {}
    return {str(key): bool(enabled) for key, enabled in value.items()}


def _coerce_limits_map(value: object) -> dict[str, int | float | None]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int | float | None] = {}
    for key, raw in value.items():
        if raw is None or isinstance(raw, (int, float)):
            result[str(key)] = raw
    return result


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
async def get_entitlements(
    org_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> EntitlementsResponse:
    # Org-member auth: superusers may query any org; others only their own.
    if not current_user.is_superuser and current_user.org_id != org_id:
        raise HTTPException(status_code=403, detail="Access forbidden")
    from sqlalchemy import select

    from dev_health_ops.models.licensing import (
        TIER_LIMITS,
        FeatureFlag,
        OrgFeatureOverride,
        OrgLicense,
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

    tier = str(org.tier)
    tier_enum = _coerce_license_tier(tier)

    org_rank = TIER_RANK.get(tier_enum, 0)

    features: dict[str, bool] = {}
    for flag in all_flags:
        min_tier = _coerce_license_tier(flag.min_tier)
        features[str(flag.key)] = org_rank >= TIER_RANK.get(min_tier, 0)

    if org_license and org_license.features_override:
        for key, enabled in _coerce_bool_map(org_license.features_override).items():
            features[key] = enabled

    for override in org_overrides.values():
        for flag in all_flags:
            if str(flag.id) == str(override.feature_id):
                now = datetime.now().astimezone()
                if override.expires_at and override.expires_at < now:
                    continue
                features[str(flag.key)] = bool(override.is_enabled)
                break

    limits = _coerce_limits_map(
        TIER_LIMITS.get(tier_enum, TIER_LIMITS[LicenseTier.COMMUNITY])
    )
    if org_license and org_license.limits_override:
        limits.update(_coerce_limits_map(org_license.limits_override))

    return EntitlementsResponse(
        org_id=str(org.id),
        tier=tier,
        licensed_users=(
            int(org_license.licensed_users)
            if org_license and org_license.licensed_users is not None
            else None
        ),
        licensed_repos=(
            int(org_license.licensed_repos)
            if org_license and org_license.licensed_repos is not None
            else None
        ),
        features=features,
        features_override=(
            _coerce_bool_map(org_license.features_override)
            if org_license and org_license.features_override
            else None
        ),
        limits_override=(
            _coerce_limits_map(org_license.limits_override)
            if org_license and org_license.limits_override
            else None
        ),
        expires_at=org_license.expires_at if org_license else None,
        is_valid=bool(org_license.is_valid) if org_license else True,
        limits=limits,
    )
