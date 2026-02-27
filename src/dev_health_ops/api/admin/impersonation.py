from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.schemas import (
    ImpersonateTargetUser,
    ImpersonationStatusResponse,
    StartImpersonationRequest,
    StartImpersonationResponse,
    StopImpersonationResponse,
)
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.audit import AuditService
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.impersonation_cache import invalidate
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.impersonation import ImpersonationSession
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


@router.post("/impersonate", response_model=StartImpersonationResponse)
async def start_impersonation(
    payload: StartImpersonationRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db_session),
) -> StartImpersonationResponse:
    if not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")

    target_user_uuid = _parse_uuid(payload.target_user_id, "target_user_id")
    admin_user_uuid = _parse_uuid(current_user.user_id, "user_id")

    # Prevent self-impersonation
    if target_user_uuid == admin_user_uuid:
        raise HTTPException(status_code=400, detail="Cannot impersonate yourself")

    # Fetch and validate target user
    target_user_result = await session.execute(
        select(User).where(User.id == target_user_uuid)
    )
    target_user = target_user_result.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="Target user not found")

    if not target_user.is_active:
        raise HTTPException(status_code=400, detail="Target user is not active")

    if target_user.is_superuser:
        raise HTTPException(status_code=403, detail="Cannot impersonate a superuser")

    # Fetch target's first membership for org_id and role
    target_membership = await _first_membership(
        session=session, user_id=target_user_uuid
    )
    if not target_membership:
        raise HTTPException(
            status_code=404,
            detail="Target user has no organization membership",
        )

    # End any existing active session for this admin
    await session.execute(
        update(ImpersonationSession)
        .where(
            ImpersonationSession.admin_user_id == admin_user_uuid,
            ImpersonationSession.ended_at.is_(None),
        )
        .values(ended_at=datetime.now(timezone.utc))
    )

    # Create new ImpersonationSession row
    ttl_minutes = _impersonation_ttl_minutes()
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    new_session = ImpersonationSession(
        admin_user_id=admin_user_uuid,
        target_user_id=target_user_uuid,
        target_org_id=target_membership.org_id,
        target_role=str(target_membership.role),
        expires_at=expires_at,
    )
    session.add(new_session)
    await session.flush()

    # Invalidate cache for this admin
    invalidate(current_user.user_id)

    # Audit log
    target_org_uuid = uuid.UUID(str(target_membership.org_id))
    audit_service = AuditService(session)
    await audit_service.log(
        org_id=target_org_uuid,
        action=AuditAction.IMPERSONATION_START,
        resource_type=AuditResourceType.SESSION,
        user_id=admin_user_uuid,
        resource_id=str(target_user.id),
        user=current_user,
    )

    return StartImpersonationResponse(
        status="active",
        target_user=ImpersonateTargetUser(
            id=str(target_user.id),
            email=str(target_user.email),
            org_id=str(target_membership.org_id),
            role=str(target_membership.role),
        ),
        expires_at=expires_at,
    )


@router.post("/impersonate/stop", response_model=StopImpersonationResponse)
async def stop_impersonation(
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db_session),
) -> StopImpersonationResponse:
    if not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")

    admin_user_uuid = _parse_uuid(current_user.user_id, "user_id")
    now = datetime.now(timezone.utc)

    # Find active session for this admin (DB, not cache)
    active_result = await session.execute(
        select(ImpersonationSession)
        .where(
            ImpersonationSession.admin_user_id == admin_user_uuid,
            ImpersonationSession.ended_at.is_(None),
            ImpersonationSession.expires_at > now,
        )
        .limit(1)
    )
    active_session = active_result.scalar_one_or_none()
    if not active_session:
        raise HTTPException(status_code=400, detail="No active impersonation session")

    # End the session
    active_session.ended_at = now  # type: ignore[assignment]
    await session.flush()

    # Invalidate cache
    invalidate(current_user.user_id)

    # Audit log
    target_org_uuid = uuid.UUID(str(active_session.target_org_id))
    audit_service = AuditService(session)
    await audit_service.log(
        org_id=target_org_uuid,
        action=AuditAction.IMPERSONATION_STOP,
        resource_type=AuditResourceType.SESSION,
        user_id=admin_user_uuid,
        resource_id=str(active_session.target_user_id),
        user=current_user,
    )

    return StopImpersonationResponse(status="stopped")


@router.get("/impersonate/status", response_model=ImpersonationStatusResponse)
async def impersonation_status(
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db_session),
) -> ImpersonationStatusResponse:
    if not current_user.is_superuser:
        return ImpersonationStatusResponse(is_impersonating=False)

    admin_user_uuid = _parse_uuid(current_user.user_id, "user_id")
    now = datetime.now(timezone.utc)

    active_result = await session.execute(
        select(ImpersonationSession)
        .where(
            ImpersonationSession.admin_user_id == admin_user_uuid,
            ImpersonationSession.ended_at.is_(None),
            ImpersonationSession.expires_at > now,
        )
        .limit(1)
    )
    active_session = active_result.scalar_one_or_none()

    if not active_session:
        return ImpersonationStatusResponse(is_impersonating=False)

    # Fetch target email for the response
    target_result = await session.execute(
        select(User).where(User.id == active_session.target_user_id)
    )
    target_user = target_result.scalar_one_or_none()
    target_email = str(target_user.email) if target_user else None

    return ImpersonationStatusResponse(
        is_impersonating=True,
        target_user_id=str(active_session.target_user_id),
        target_email=target_email,
        target_org_id=str(active_session.target_org_id),
        expires_at=active_session.expires_at,
    )
