from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.schemas import (
    ImpersonateRequest,
    ImpersonateResponse,
    ImpersonateStatusResponse,
    ImpersonateStopResponse,
    ImpersonatedUserInfo,
)
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.audit import AuditService
from dev_health_ops.api.services.auth import AuthenticatedUser, get_auth_service
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, User

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@asynccontextmanager
async def _session_ctx() -> AsyncGenerator[AsyncSession, None]:
    async with get_postgres_session() as session:
        yield session


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with _session_ctx() as session:
        yield session


def _parse_uuid(value: str, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}") from exc


def _impersonation_ttl_minutes() -> int:
    raw = os.getenv("IMPERSONATION_TTL_MINUTES", "60").strip()
    try:
        ttl = int(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=500, detail="Invalid IMPERSONATION_TTL_MINUTES configuration"
        ) from exc
    if ttl <= 0:
        raise HTTPException(
            status_code=500, detail="IMPERSONATION_TTL_MINUTES must be > 0"
        )
    return ttl


async def _membership_for_org(
    session: AsyncSession,
    user_id: uuid.UUID,
    org_id: uuid.UUID,
) -> Membership | None:
    membership_result = await session.execute(
        select(Membership).where(
            Membership.user_id == user_id, Membership.org_id == org_id
        )
    )
    return membership_result.scalar_one_or_none()


async def _first_membership(
    session: AsyncSession,
    user_id: uuid.UUID,
) -> Membership | None:
    membership_result = await session.execute(
        select(Membership)
        .where(Membership.user_id == user_id)
        .order_by(Membership.created_at.asc())
    )
    return membership_result.scalars().first()


@router.post("/impersonate", response_model=ImpersonateResponse)
async def start_impersonation(
    payload: ImpersonateRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db_session),
) -> ImpersonateResponse:
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")

    target_user_uuid = _parse_uuid(payload.target_user_id, "target_user_id")
    current_org_uuid = _parse_uuid(current_user.org_id, "org_id")

    target_user_result = await session.execute(
        select(User).where(User.id == target_user_uuid)
    )
    target_user = target_user_result.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="Target user not found")

    if target_user.is_superuser:
        raise HTTPException(status_code=403, detail="Cannot impersonate superuser")

    target_membership = await _membership_for_org(
        session=session,
        user_id=target_user_uuid,
        org_id=current_org_uuid,
    )

    if not target_membership:
        if not current_user.is_superuser:
            raise HTTPException(
                status_code=403,
                detail="Cross-organization impersonation is not allowed",
            )
        target_membership = await _first_membership(
            session=session, user_id=target_user_uuid
        )

    if not target_membership:
        raise HTTPException(
            status_code=404,
            detail="Target user has no organization membership",
        )

    ttl_minutes = _impersonation_ttl_minutes()
    expires_delta = timedelta(minutes=ttl_minutes)
    auth_service = get_auth_service()
    access_token = auth_service.create_access_token(
        user_id=str(target_user.id),
        email=str(target_user.email),
        org_id=str(target_membership.org_id),
        role=str(target_membership.role),
        is_superuser=bool(target_user.is_superuser),
        username=str(target_user.username) if target_user.username else None,
        full_name=str(target_user.full_name) if target_user.full_name else None,
        impersonating_user_id=current_user.user_id,
        expires_delta=expires_delta,
    )

    audit_service = AuditService(session)
    await audit_service.log(
        org_id=current_org_uuid,
        action=AuditAction.IMPERSONATION_START,
        resource_type=AuditResourceType.SESSION,
        user_id=uuid.UUID(current_user.user_id),
        resource_id=str(target_user.id),
        user=current_user,
    )

    return ImpersonateResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=ttl_minutes * 60,
        impersonated_user=ImpersonatedUserInfo(
            id=str(target_user.id),
            email=str(target_user.email),
            role=str(target_membership.role),
            org_id=str(target_membership.org_id),
        ),
    )


@router.post("/impersonate/stop", response_model=ImpersonateStopResponse)
async def stop_impersonation(
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db_session),
) -> ImpersonateStopResponse:
    if not current_user.impersonated_by:
        raise HTTPException(status_code=400, detail="Not currently impersonating")

    real_admin_uuid = _parse_uuid(current_user.impersonated_by, "impersonating_user_id")
    current_org_uuid = _parse_uuid(current_user.org_id, "org_id")

    real_user_result = await session.execute(
        select(User).where(User.id == real_admin_uuid)
    )
    real_user = real_user_result.scalar_one_or_none()
    if not real_user:
        raise HTTPException(status_code=404, detail="Impersonating user not found")

    real_membership = await _membership_for_org(
        session=session,
        user_id=real_admin_uuid,
        org_id=current_org_uuid,
    )

    if not real_membership and real_user.is_superuser:
        real_membership = await _first_membership(
            session=session, user_id=real_admin_uuid
        )

    if not real_user.is_superuser and not real_membership:
        raise HTTPException(
            status_code=403,
            detail="Impersonating user is not a member of this organization",
        )

    role = str(real_membership.role) if real_membership else "admin"
    org_id = str(real_membership.org_id) if real_membership else current_user.org_id

    auth_service = get_auth_service()
    access_token = auth_service.create_access_token(
        user_id=str(real_user.id),
        email=str(real_user.email),
        org_id=org_id,
        role=role,
        is_superuser=bool(real_user.is_superuser),
        username=str(real_user.username) if real_user.username else None,
        full_name=str(real_user.full_name) if real_user.full_name else None,
    )

    expires_in = int(timedelta(hours=1).total_seconds())

    audit_service = AuditService(session)
    await audit_service.log(
        org_id=current_org_uuid,
        action=AuditAction.IMPERSONATION_STOP,
        resource_type=AuditResourceType.SESSION,
        user_id=real_admin_uuid,
        resource_id=current_user.user_id,
        user=current_user,
    )

    return ImpersonateStopResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=expires_in,
    )


@router.get("/impersonate/status", response_model=ImpersonateStatusResponse)
async def impersonation_status(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ImpersonateStatusResponse:
    is_impersonating = current_user.impersonated_by is not None
    return ImpersonateStatusResponse(
        is_impersonating=is_impersonating,
        impersonated_user_id=current_user.user_id if is_impersonating else None,
        real_user_id=current_user.impersonated_by if is_impersonating else None,
    )
