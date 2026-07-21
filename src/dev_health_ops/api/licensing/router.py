from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing.feature_decisions import evaluate_org_features_async
from dev_health_ops.licensing.registry import get_features_for_tier
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

        stored_feature_keys = tuple(
            str(key) for key in (await session.scalars(select(FeatureFlag.key))).all()
        )
        tier = str(org_license.tier) if org_license is not None else str(org.tier)
        tier_enum = _coerce_license_tier(tier)
        feature_keys = set(get_features_for_tier(tier_enum))
        feature_keys.update(stored_feature_keys)
        if org_license is not None and org_license.features_override:
            feature_keys.update(_coerce_bool_map(org_license.features_override))
        decisions = await evaluate_org_features_async(
            session,
            org_uuid,
            sorted(feature_keys),
        )

    features = {key: decision.allowed for key, decision in decisions.items()}

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
