from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_superuser
from dev_health_ops.api.admin.schemas import (
    FeatureFlagResponse,
    FeatureFlagUpdateRequest,
    FeatureOverrideCreate,
    FeatureOverrideResponse,
    FeatureOverrideUpdate,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride

from .common import get_session

router = APIRouter()


@router.get("/feature-flags", response_model=list[FeatureFlagResponse])
async def list_feature_flags(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> list[FeatureFlagResponse]:
    stmt = select(FeatureFlag).order_by(FeatureFlag.key.asc())
    result = await session.execute(stmt)
    flags = result.scalars().all()
    return [
        FeatureFlagResponse(
            id=str(flag.id),
            key=flag.key,
            name=flag.name,
            description=flag.description,
            category=flag.category,
            min_tier=flag.min_tier,
            is_enabled=bool(flag.is_enabled),
            is_beta=bool(flag.is_beta),
            is_deprecated=bool(flag.is_deprecated),
            created_at=flag.created_at,
        )
        for flag in flags
    ]


@router.get(
    "/orgs/{org_id}/feature-overrides", response_model=list[FeatureOverrideResponse]
)
async def list_feature_overrides(
    org_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> list[FeatureOverrideResponse]:
    org_uuid = uuid.UUID(org_id)
    stmt = (
        select(OrgFeatureOverride, FeatureFlag)
        .join(FeatureFlag, OrgFeatureOverride.feature_id == FeatureFlag.id)
        .where(OrgFeatureOverride.org_id == org_uuid)
        .order_by(OrgFeatureOverride.created_at.desc())
    )
    result = await session.execute(stmt)
    rows = result.all()
    return [
        FeatureOverrideResponse(
            id=str(override.id),
            org_id=str(override.org_id),
            feature_id=str(override.feature_id),
            feature_key=feature.key,
            is_enabled=bool(override.is_enabled),
            expires_at=override.expires_at,
            config=override.config,
            reason=override.reason,
            created_by=str(override.created_by) if override.created_by else None,
            updated_by=str(override.updated_by) if override.updated_by else None,
            created_at=override.created_at,
        )
        for override, feature in rows
    ]


@router.post(
    "/orgs/{org_id}/feature-overrides",
    response_model=FeatureOverrideResponse,
    status_code=201,
)
async def create_feature_override(
    org_id: str,
    payload: FeatureOverrideCreate,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> FeatureOverrideResponse:
    org_uuid = uuid.UUID(org_id)
    feature_uuid = uuid.UUID(payload.feature_id)

    feature_result = await session.execute(
        select(FeatureFlag).where(FeatureFlag.id == feature_uuid)
    )
    feature = feature_result.scalar_one_or_none()
    if feature is None:
        raise HTTPException(status_code=404, detail="Feature flag not found")

    existing_result = await session.execute(
        select(OrgFeatureOverride).where(
            OrgFeatureOverride.org_id == org_uuid,
            OrgFeatureOverride.feature_id == feature_uuid,
        )
    )
    existing = existing_result.scalar_one_or_none()
    if existing is not None:
        raise HTTPException(status_code=409, detail="Feature override already exists")

    override = OrgFeatureOverride(
        org_id=org_uuid,
        feature_id=feature_uuid,
        is_enabled=payload.is_enabled,
        expires_at=payload.expires_at,
        config=payload.config,
        reason=payload.reason,
        created_by=uuid.UUID(current_user.user_id),
    )
    session.add(override)
    await session.flush()

    return FeatureOverrideResponse(
        id=str(override.id),
        org_id=str(override.org_id),
        feature_id=str(override.feature_id),
        feature_key=feature.key,
        is_enabled=bool(override.is_enabled),
        expires_at=override.expires_at,
        config=override.config,
        reason=override.reason,
        created_by=str(override.created_by) if override.created_by else None,
        updated_by=str(override.updated_by) if override.updated_by else None,
        created_at=override.created_at,
    )


@router.patch(
    "/orgs/{org_id}/feature-overrides/{override_id}",
    response_model=FeatureOverrideResponse,
)
async def update_feature_override(
    org_id: str,
    override_id: str,
    payload: FeatureOverrideUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> FeatureOverrideResponse:
    result = await session.execute(
        select(OrgFeatureOverride, FeatureFlag)
        .join(FeatureFlag, OrgFeatureOverride.feature_id == FeatureFlag.id)
        .where(
            OrgFeatureOverride.id == uuid.UUID(override_id),
            OrgFeatureOverride.org_id == uuid.UUID(org_id),
        )
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Feature override not found")

    override, feature = row
    if payload.is_enabled is not None:
        override.is_enabled = payload.is_enabled
    if payload.expires_at is not None:
        override.expires_at = payload.expires_at
    if payload.config is not None:
        override.config = payload.config
    if payload.reason is not None:
        override.reason = payload.reason

    override.updated_by = uuid.UUID(current_user.user_id)
    await session.flush()

    return FeatureOverrideResponse(
        id=str(override.id),
        org_id=str(override.org_id),
        feature_id=str(override.feature_id),
        feature_key=feature.key,
        is_enabled=bool(override.is_enabled),
        expires_at=override.expires_at,
        config=override.config,
        reason=override.reason,
        created_by=str(override.created_by) if override.created_by else None,
        updated_by=str(override.updated_by) if override.updated_by else None,
        created_at=override.created_at,
    )


@router.delete("/orgs/{org_id}/feature-overrides/{override_id}", status_code=204)
async def delete_feature_override(
    org_id: str,
    override_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> None:
    result = await session.execute(
        select(OrgFeatureOverride).where(
            OrgFeatureOverride.id == uuid.UUID(override_id),
            OrgFeatureOverride.org_id == uuid.UUID(org_id),
        )
    )
    override = result.scalar_one_or_none()
    if override is None:
        raise HTTPException(status_code=404, detail="Feature override not found")

    await session.delete(override)
    await session.flush()


@router.patch("/feature-flags/{flag_id}", response_model=FeatureFlagResponse)
async def update_feature_flag(
    flag_id: str,
    payload: FeatureFlagUpdateRequest,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> FeatureFlagResponse:
    """Update patchable properties of a feature flag.

    Only is_enabled, is_beta, and is_deprecated may be updated.
    name, key, min_tier, and category are immutable via this endpoint.
    """
    flag_uuid = uuid.UUID(flag_id)
    result = await session.execute(
        select(FeatureFlag).where(FeatureFlag.id == flag_uuid)
    )
    flag = result.scalar_one_or_none()
    if flag is None:
        raise HTTPException(status_code=404, detail="Feature flag not found")

    if payload.is_enabled is not None:
        flag.is_enabled = payload.is_enabled
    if payload.is_beta is not None:
        flag.is_beta = payload.is_beta
    if payload.is_deprecated is not None:
        flag.is_deprecated = payload.is_deprecated

    await session.flush()

    return FeatureFlagResponse(
        id=str(flag.id),
        key=flag.key,
        name=flag.name,
        description=flag.description,
        category=flag.category,
        min_tier=flag.min_tier,
        is_enabled=bool(flag.is_enabled),
        is_beta=bool(flag.is_beta),
        is_deprecated=bool(flag.is_deprecated),
        created_at=flag.created_at,
    )
