from __future__ import annotations

import logging
import uuid
from typing import Any

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


def _organization_response(org: object) -> OrganizationResponse:
    return OrganizationResponse.model_validate(
        {
            "id": str(getattr(org, "id")),
            "slug": getattr(org, "slug"),
            "name": getattr(org, "name"),
            "description": getattr(org, "description"),
            "tier": getattr(org, "tier"),
            "settings": getattr(org, "settings") or {},
            "is_active": getattr(org, "is_active"),
            "created_at": getattr(org, "created_at"),
            "updated_at": getattr(org, "updated_at"),
        }
    )


def _membership_response(membership: object) -> MembershipResponse:
    return MembershipResponse.model_validate(
        {
            "id": str(getattr(membership, "id")),
            "org_id": str(getattr(membership, "org_id")),
            "user_id": str(getattr(membership, "user_id")),
            "role": str(getattr(membership, "role")),
            "invited_by_id": (
                str(getattr(membership, "invited_by_id"))
                if getattr(membership, "invited_by_id") is not None
                else None
            ),
            "joined_at": getattr(membership, "joined_at"),
            "created_at": getattr(membership, "created_at"),
            "updated_at": getattr(membership, "updated_at"),
        }
    )


def _org_invite_response(invite: object) -> OrgInviteResponse:
    return OrgInviteResponse.model_validate(
        {
            "id": str(getattr(invite, "id")),
            "org_id": str(getattr(invite, "org_id")),
            "email": str(getattr(invite, "email")),
            "role": str(getattr(invite, "role")),
            "invited_by_id": (
                str(getattr(invite, "invited_by_id"))
                if getattr(invite, "invited_by_id") is not None
                else None
            ),
            "status": str(getattr(invite, "status")),
            "expires_at": getattr(invite, "expires_at"),
            "accepted_at": getattr(invite, "accepted_at"),
            "created_at": getattr(invite, "created_at"),
            "updated_at": getattr(invite, "updated_at"),
        }
    )


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
    return [_organization_response(org) for org in orgs]


@router.get("/orgs/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.get_by_id(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return _organization_response(org)


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
    return _organization_response(org)


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
    return _organization_response(org)


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
    return [_membership_response(member) for member in members]


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
    return _membership_response(membership)


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

    invite_email = str(getattr(invite, "email"))
    org_name = str(getattr(org_row, "name"))
    invite_changes: dict[str, Any] = {
        "email": invite_email,
        "role": str(getattr(invite, "role")),
        "status": str(getattr(invite, "status")),
    }
    emit_audit_log(
        session,
        org_id=org_uuid,
        action=AuditAction.MEMBER_INVITED,
        resource_type=AuditResourceType.MEMBERSHIP,
        resource_id=str(getattr(invite, "id")),
        user_id=invited_by_id,
        description="Organization invite created",
        changes=invite_changes,
        request=request,
    )

    try:
        await send_invite_email(
            to_email=invite_email,
            org_name=org_name,
            inviter_name=inviter_name,
            token=token,
        )
    except Exception:
        logger.exception("Failed to send invite email to %s", invite_email)

    return _org_invite_response(invite)


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
    return _membership_response(membership)


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
