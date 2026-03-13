from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    IPAllowlistCreate,
    IPAllowlistListResponse,
    IPAllowlistResponse,
    IPAllowlistUpdate,
    IPCheckRequest,
    IPCheckResponse,
)
from dev_health_ops.api.services.ip_allowlist import IPAllowlistService
from dev_health_ops.licensing import require_feature

from .common import get_session, get_user_id

router = APIRouter()

@router.get("/ip-allowlist", response_model=IPAllowlistListResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def list_ip_allowlist_entries(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    active_only: bool = Query(False, description="Filter to active entries only"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> IPAllowlistListResponse:
    svc = IPAllowlistService(session)
    entries, total = await svc.list_entries(
        org_id=uuid.UUID(org_id),
        active_only=active_only,
        limit=limit,
        offset=offset,
    )
    return IPAllowlistListResponse(
        items=[
            IPAllowlistResponse(
                id=str(e.id),
                org_id=str(e.org_id),
                ip_range=str(e.ip_range),
                description=e.description,
                is_active=bool(e.is_active),
                created_by_id=str(e.created_by_id) if e.created_by_id else None,
                created_at=e.created_at,
                updated_at=e.updated_at,
                expires_at=e.expires_at,
            )
            for e in entries
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post("/ip-allowlist", response_model=IPAllowlistResponse, status_code=201)
@require_feature("ip_allowlist", required_tier="enterprise")
async def create_ip_allowlist_entry(
    payload: IPAllowlistCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    user_id: str | None = Depends(get_user_id),
) -> IPAllowlistResponse:
    svc = IPAllowlistService(session)
    try:
        entry = await svc.create_entry(
            org_id=uuid.UUID(org_id),
            ip_range=payload.ip_range,
            description=payload.description,
            created_by_id=uuid.UUID(user_id) if user_id else None,
            expires_at=payload.expires_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return IPAllowlistResponse(
        id=str(entry.id),
        org_id=str(entry.org_id),
        ip_range=str(entry.ip_range),
        description=entry.description,
        is_active=bool(entry.is_active),
        created_by_id=str(entry.created_by_id) if entry.created_by_id else None,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        expires_at=entry.expires_at,
    )


@router.get("/ip-allowlist/{entry_id}", response_model=IPAllowlistResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def get_ip_allowlist_entry(
    entry_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IPAllowlistResponse:
    svc = IPAllowlistService(session)
    entry = await svc.get_entry(
        org_id=uuid.UUID(org_id),
        entry_id=uuid.UUID(entry_id),
    )
    if not entry:
        raise HTTPException(status_code=404, detail="IP allowlist entry not found")
    return IPAllowlistResponse(
        id=str(entry.id),
        org_id=str(entry.org_id),
        ip_range=str(entry.ip_range),
        description=entry.description,
        is_active=bool(entry.is_active),
        created_by_id=str(entry.created_by_id) if entry.created_by_id else None,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        expires_at=entry.expires_at,
    )


@router.patch("/ip-allowlist/{entry_id}", response_model=IPAllowlistResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def update_ip_allowlist_entry(
    entry_id: str,
    payload: IPAllowlistUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IPAllowlistResponse:
    svc = IPAllowlistService(session)
    try:
        entry = await svc.update_entry(
            org_id=uuid.UUID(org_id),
            entry_id=uuid.UUID(entry_id),
            ip_range=payload.ip_range,
            description=payload.description,
            is_active=payload.is_active,
            expires_at=payload.expires_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not entry:
        raise HTTPException(status_code=404, detail="IP allowlist entry not found")
    return IPAllowlistResponse(
        id=str(entry.id),
        org_id=str(entry.org_id),
        ip_range=str(entry.ip_range),
        description=entry.description,
        is_active=bool(entry.is_active),
        created_by_id=str(entry.created_by_id) if entry.created_by_id else None,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        expires_at=entry.expires_at,
    )


@router.delete("/ip-allowlist/{entry_id}")
@require_feature("ip_allowlist", required_tier="enterprise")
async def delete_ip_allowlist_entry(
    entry_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = IPAllowlistService(session)
    deleted = await svc.delete_entry(
        org_id=uuid.UUID(org_id),
        entry_id=uuid.UUID(entry_id),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="IP allowlist entry not found")
    return {"deleted": True}


@router.post("/ip-allowlist/check", response_model=IPCheckResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def check_ip_allowed(
    payload: IPCheckRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IPCheckResponse:
    svc = IPAllowlistService(session)
    allowed = await svc.check_ip_allowed(
        org_id=uuid.UUID(org_id),
        ip_address=payload.ip_address,
    )
    return IPCheckResponse(
        allowed=allowed,
        ip_address=payload.ip_address,
    )
