from __future__ import annotations

import uuid
from typing import Any, Protocol, cast

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


class _MutableFeatureFlag(Protocol):
    is_enabled: bool
    is_beta: bool
    is_deprecated: bool


class _MutableFeatureOverride(Protocol):
    is_enabled: bool
    expires_at: object | None
    config: dict[str, Any] | None
    reason: str | None
    updated_by: uuid.UUID | None


def _feature_flag_response(flag: object) -> FeatureFlagResponse:
    return FeatureFlagResponse.model_validate(
        {
            "id": str(getattr(flag, "id")),
            "key": str(getattr(flag, "key")),
            "name": str(getattr(flag, "name")),
            "description": getattr(flag, "description"),
            "category": str(getattr(flag, "category")),
            "min_tier": str(getattr(flag, "min_tier")),
            "is_enabled": getattr(flag, "is_enabled"),
            "is_beta": getattr(flag, "is_beta"),
            "is_deprecated": getattr(flag, "is_deprecated"),
            "created_at": getattr(flag, "created_at"),
        }
    )


def _feature_override_response(
    override: object,
    feature_key: str,
) -> FeatureOverrideResponse:
    return FeatureOverrideResponse.model_validate(
        {
            "id": str(getattr(override, "id")),
            "org_id": str(getattr(override, "org_id")),
            "feature_id": str(getattr(override, "feature_id")),
            "feature_key": feature_key,
            "is_enabled": getattr(override, "is_enabled"),
            "expires_at": getattr(override, "expires_at"),
            "config": getattr(override, "config"),
            "reason": getattr(override, "reason"),
            "created_by": (
                str(getattr(override, "created_by"))
                if getattr(override, "created_by") is not None
                else None
            ),
            "updated_by": (
                str(getattr(override, "updated_by"))
                if getattr(override, "updated_by") is not None
                else None
            ),
            "created_at": getattr(override, "created_at"),
        }
    )


@router.get("/feature-flags", response_model=list[FeatureFlagResponse])
async def list_feature_flags(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> list[FeatureFlagResponse]:
    feature_flag_key = getattr(FeatureFlag, "key")
    stmt = select(FeatureFlag).order_by(feature_flag_key.asc())
    result = await session.execute(stmt)
    flags = result.scalars().all()
    return [_feature_flag_response(flag) for flag in flags]


@router.get(
    "/orgs/{org_id}/feature-overrides", response_model=list[FeatureOverrideResponse]
)
async def list_feature_overrides(
    org_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> list[FeatureOverrideResponse]:
    org_uuid = uuid.UUID(org_id)
    feature_flag_id = getattr(FeatureFlag, "id")
    override_feature_id = getattr(OrgFeatureOverride, "feature_id")
    override_org_id = getattr(OrgFeatureOverride, "org_id")
    override_created_at = getattr(OrgFeatureOverride, "created_at")
    stmt = (
        select(OrgFeatureOverride, FeatureFlag)
        .join(FeatureFlag, override_feature_id == feature_flag_id)
        .where(override_org_id == org_uuid)
        .order_by(override_created_at.desc())
    )
    result = await session.execute(stmt)
    rows = result.all()
    return [
        _feature_override_response(override, str(getattr(feature, "key")))
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

    feature_flag_id = getattr(FeatureFlag, "id")
    feature_result = await session.execute(
        select(FeatureFlag).where(feature_flag_id == feature_uuid)
    )
    feature = feature_result.scalar_one_or_none()
    if feature is None:
        raise HTTPException(status_code=404, detail="Feature flag not found")

    override_org_id = getattr(OrgFeatureOverride, "org_id")
    override_feature_id = getattr(OrgFeatureOverride, "feature_id")
    existing_result = await session.execute(
        select(OrgFeatureOverride).where(
            override_org_id == org_uuid,
            override_feature_id == feature_uuid,
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

    return _feature_override_response(override, str(getattr(feature, "key")))


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
    override_id_col = getattr(OrgFeatureOverride, "id")
    override_org_id = getattr(OrgFeatureOverride, "org_id")
    override_feature_id = getattr(OrgFeatureOverride, "feature_id")
    feature_flag_id = getattr(FeatureFlag, "id")
    result = await session.execute(
        select(OrgFeatureOverride, FeatureFlag)
        .join(FeatureFlag, override_feature_id == feature_flag_id)
        .where(
            override_id_col == uuid.UUID(override_id),
            override_org_id == uuid.UUID(org_id),
        )
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Feature override not found")

    override, feature = row
    mutable_override = cast(_MutableFeatureOverride, override)
    if payload.is_enabled is not None:
        mutable_override.is_enabled = payload.is_enabled
    if payload.expires_at is not None:
        mutable_override.expires_at = payload.expires_at
    if payload.config is not None:
        mutable_override.config = payload.config
    if payload.reason is not None:
        mutable_override.reason = payload.reason

    mutable_override.updated_by = uuid.UUID(current_user.user_id)
    await session.flush()

    return _feature_override_response(override, str(getattr(feature, "key")))


@router.delete("/orgs/{org_id}/feature-overrides/{override_id}", status_code=204)
async def delete_feature_override(
    org_id: str,
    override_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> None:
    override_id_col = getattr(OrgFeatureOverride, "id")
    override_org_id = getattr(OrgFeatureOverride, "org_id")
    result = await session.execute(
        select(OrgFeatureOverride).where(
            override_id_col == uuid.UUID(override_id),
            override_org_id == uuid.UUID(org_id),
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
    feature_flag_id = getattr(FeatureFlag, "id")
    result = await session.execute(
        select(FeatureFlag).where(feature_flag_id == flag_uuid)
    )
    flag = result.scalar_one_or_none()
    if flag is None:
        raise HTTPException(status_code=404, detail="Feature flag not found")

    mutable_flag = cast(_MutableFeatureFlag, flag)
    if payload.is_enabled is not None:
        mutable_flag.is_enabled = payload.is_enabled
    if payload.is_beta is not None:
        mutable_flag.is_beta = payload.is_beta
    if payload.is_deprecated is not None:
        mutable_flag.is_deprecated = payload.is_deprecated

    await session.flush()

    return _feature_flag_response(flag)
