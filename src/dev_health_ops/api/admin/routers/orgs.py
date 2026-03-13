from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_admin, require_superuser
from dev_health_ops.api.admin.schemas import (
    MembershipCreate,
    MembershipResponse,
    MembershipUpdateRole,
    OrganizationCreate,
    OrganizationResponse,
    OrganizationUpdate,
    OrgInviteCreate,
    OrgInviteResponse,
    OwnershipTransfer,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.invites import create_invite, send_invite_email
from dev_health_ops.api.services.users import MembershipService, OrganizationService
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Organization, User

from .common import (
    _ensure_org_admin_access,
    get_admin_user_key,
    get_session,
    limiter,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/orgs", response_model=list[OrganizationResponse])
async def list_organizations(
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> list[OrganizationResponse]:
    svc = OrganizationService(session)
    orgs = await svc.list_all(limit=limit, offset=offset, active_only=active_only)
    return [
        OrganizationResponse(
            id=str(o.id),
            slug=o.slug,
            name=o.name,
            description=o.description,
            tier=o.tier,
            settings=o.settings or {},
            is_active=o.is_active,
            created_at=o.created_at,
            updated_at=o.updated_at,
        )
        for o in orgs
    ]


@router.get("/orgs/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.get_by_id(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        settings=org.settings or {},
        is_active=org.is_active,
        created_at=org.created_at,
        updated_at=org.updated_at,
    )


@router.post("/orgs", response_model=OrganizationResponse, status_code=201)
async def create_organization(
    payload: OrganizationCreate,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.create(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        settings=payload.settings,
        tier=payload.tier,
        owner_user_id=payload.owner_user_id,
    )
    return OrganizationResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        settings=org.settings or {},
        is_active=org.is_active,
        created_at=org.created_at,
        updated_at=org.updated_at,
    )


@router.patch("/orgs/{org_id}", response_model=OrganizationResponse)
async def update_organization(
    org_id: str,
    payload: OrganizationUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.update(
        org_id=org_id,
        name=payload.name,
        description=payload.description,
        settings=payload.settings,
        tier=payload.tier,
        is_active=payload.is_active,
    )
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        settings=org.settings or {},
        is_active=org.is_active,
        created_at=org.created_at,
        updated_at=org.updated_at,
    )


@router.delete("/orgs/{org_id}")
async def delete_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> dict:
    svc = OrganizationService(session)
    deleted = await svc.delete(org_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Organization not found")
    return {"deleted": True}


@router.get("/orgs/{org_id}/members", response_model=list[MembershipResponse])
async def list_members(
    org_id: str,
    role: str | None = None,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> list[MembershipResponse]:
    await _ensure_org_admin_access(session, org_id, current_user)
    svc = MembershipService(session)
    members = await svc.list_members(org_id, role=role)
    return [
        MembershipResponse(
            id=str(m.id),
            org_id=str(m.org_id),
            user_id=str(m.user_id),
            role=m.role,
            invited_by_id=str(m.invited_by_id) if m.invited_by_id else None,
            joined_at=m.joined_at,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )
        for m in members
    ]


@router.post(
    "/orgs/{org_id}/members", response_model=MembershipResponse, status_code=201
)
async def add_member(
    org_id: str,
    payload: MembershipCreate,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> MembershipResponse:
    await _ensure_org_admin_access(session, org_id, current_user)
    svc = MembershipService(session)
    try:
        membership = await svc.add_member(
            org_id=org_id,
            user_id=payload.user_id,
            role=payload.role,
            invited_by_id=payload.invited_by_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return MembershipResponse(
        id=str(membership.id),
        org_id=str(membership.org_id),
        user_id=str(membership.user_id),
        role=membership.role,
        invited_by_id=str(membership.invited_by_id)
        if membership.invited_by_id
        else None,
        joined_at=membership.joined_at,
        created_at=membership.created_at,
        updated_at=membership.updated_at,
    )


@router.post(
    "/orgs/{org_id}/invites", response_model=OrgInviteResponse, status_code=201
)
@limiter.limit("10/hour", key_func=get_admin_user_key)
async def create_org_invite(
    org_id: str,
    payload: OrgInviteCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> OrgInviteResponse:
    await _ensure_org_admin_access(session, org_id, current_user)

    org_uuid = uuid.UUID(org_id)
    try:
        invited_by_id = uuid.UUID(current_user.user_id)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid user identity") from exc

    org_result = await session.execute(
        select(Organization.id, Organization.name).where(Organization.id == org_uuid)
    )
    org_row = org_result.one_or_none()
    if org_row is None:
        raise HTTPException(status_code=404, detail="Organization not found")

    inviter_result = await session.execute(
        select(User.full_name, User.email).where(User.id == invited_by_id)
    )
    inviter_row = inviter_result.one_or_none()
    inviter_name = (
        str(inviter_row.full_name)
        if inviter_row is not None and inviter_row.full_name
        else (
            str(inviter_row.email)
            if inviter_row is not None and inviter_row.email
            else current_user.email
        )
    )

    try:
        invite, token = await create_invite(
            db=session,
            org_id=org_uuid,
            email=payload.email,
            role=payload.role,
            invited_by_id=invited_by_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    emit_audit_log(
        session,
        org_id=org_uuid,
        action=AuditAction.MEMBER_INVITED,
        resource_type=AuditResourceType.MEMBERSHIP,
        resource_id=str(invite.id),
        user_id=invited_by_id,
        description="Organization invite created",
        changes={
            "email": invite.email,
            "role": invite.role,
            "status": invite.status,
        },
        request=request,
    )

    try:
        await send_invite_email(
            to_email=invite.email,
            org_name=str(org_row.name),
            inviter_name=inviter_name,
            token=token,
        )
    except Exception:
        logger.exception("Failed to send invite email to %s", invite.email)

    return OrgInviteResponse(
        id=str(invite.id),
        org_id=str(invite.org_id),
        email=str(invite.email),
        role=str(invite.role),
        invited_by_id=str(invite.invited_by_id) if invite.invited_by_id else None,
        status=str(invite.status),
        expires_at=invite.expires_at,
        accepted_at=invite.accepted_at,
        created_at=invite.created_at,
        updated_at=invite.updated_at,
    )


@router.patch("/orgs/{org_id}/members/{user_id}", response_model=MembershipResponse)
async def update_member_role(
    org_id: str,
    user_id: str,
    payload: MembershipUpdateRole,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> MembershipResponse:
    await _ensure_org_admin_access(session, org_id, current_user)
    svc = MembershipService(session)
    try:
        membership = await svc.update_role(org_id, user_id, payload.role)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not membership:
        raise HTTPException(status_code=404, detail="Membership not found")
    return MembershipResponse(
        id=str(membership.id),
        org_id=str(membership.org_id),
        user_id=str(membership.user_id),
        role=membership.role,
        invited_by_id=str(membership.invited_by_id)
        if membership.invited_by_id
        else None,
        joined_at=membership.joined_at,
        created_at=membership.created_at,
        updated_at=membership.updated_at,
    )


@router.delete("/orgs/{org_id}/members/{user_id}")
async def remove_member(
    org_id: str,
    user_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> dict:
    await _ensure_org_admin_access(session, org_id, current_user)
    svc = MembershipService(session)
    try:
        deleted = await svc.remove_member(org_id, user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail="Membership not found")
    return {"deleted": True}


@router.post("/orgs/{org_id}/transfer-ownership/{from_user_id}")
async def transfer_ownership(
    org_id: str,
    from_user_id: str,
    payload: OwnershipTransfer,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> dict:
    await _ensure_org_admin_access(session, org_id, current_user)
    svc = MembershipService(session)
    try:
        await svc.transfer_ownership(org_id, from_user_id, payload.new_owner_user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"success": True}
