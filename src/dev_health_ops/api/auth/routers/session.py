from __future__ import annotations

import logging
import uuid as uuid_mod
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.api.middleware.rate_limit import AUTH_VALIDATE_LIMIT, limiter
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.refresh_tokens import revoke_token
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, Organization, User

from .common import (
    LoginResponse,
    OrganizationMembershipInfo,
    _issue_membership_tokens,
    _load_org_activity,
    _parse_uuid,
    _require_uuid,
    _to_user_info,
)
from .dependencies import get_current_user, get_current_user_optional

logger = logging.getLogger(__name__)

router = APIRouter()


class TokenValidateRequest(BaseModel):
    token: str


class TokenValidateResponse(BaseModel):
    valid: bool
    user_id: str | None = None
    email: str | None = None
    org_id: str | None = None
    role: str | None = None
    expires_at: str | None = None


class LogoutRequest(BaseModel):
    refresh_token: str


class MeResponse(BaseModel):
    id: str
    email: str
    username: str | None = None
    full_name: str | None = None
    org_id: str
    role: str
    is_superuser: bool = False
    permissions: list[str] = []


class UserOrganizationsResponse(BaseModel):
    active_org_id: str | None = None
    organizations: list[OrganizationMembershipInfo]


class SwitchOrgRequest(BaseModel):
    org_id: str


@router.get("/me", response_model=MeResponse)
async def get_me(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> MeResponse:
    from dev_health_ops.api.services.permissions import get_user_permissions

    permissions = list(get_user_permissions(user))

    return MeResponse(
        id=user.user_id,
        email=user.email,
        username=user.username,
        full_name=user.full_name,
        org_id=user.org_id,
        role=user.role,
        is_superuser=user.is_superuser,
        permissions=permissions,
    )


@router.get("/me/organizations", response_model=UserOrganizationsResponse)
async def get_my_organizations(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> UserOrganizationsResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    user_id = _parse_uuid(user.user_id)
    if user_id is None:
        raise HTTPException(status_code=401, detail=error_detail("Invalid token claims"))

    async with get_postgres_session() as db:
        result = await db.execute(
            select(Membership, Organization)
            .join(Organization, Membership.org_id == Organization.id)
            .where(
                Membership.user_id == user_id,
                Organization.is_active.is_(True),
            )
            .order_by(Membership.joined_at.asc(), Membership.created_at.asc())
        )
        rows = result.all()

    activity_by_org = _load_org_activity(
        _require_uuid(membership.org_id, "membership.org_id")
        for membership, _organization in rows
    )
    organizations = []
    for membership, organization in rows:
        org_id = _require_uuid(membership.org_id, "membership.org_id")
        activity = activity_by_org.get(org_id)
        organizations.append(
            OrganizationMembershipInfo(
                id=str(organization.id),
                slug=str(organization.slug),
                name=str(organization.name),
                tier=str(organization.tier) if organization.tier is not None else None,
                role=str(membership.role),
                joined_at=membership.joined_at,
                has_data=bool(activity and activity.has_data),
                last_metrics_at=activity.last_metrics_at if activity else None,
            )
        )

    return UserOrganizationsResponse(
        active_org_id=user.org_id or None,
        organizations=organizations,
    )


@router.post("/switch-org", response_model=LoginResponse)
async def switch_org(
    payload: SwitchOrgRequest,
    request: Request,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> LoginResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    user_id = _parse_uuid(user.user_id)
    org_id = _parse_uuid(payload.org_id)
    if user_id is None or org_id is None:
        raise HTTPException(status_code=400, detail=error_detail("Invalid organization ID"))

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User, Membership)
            .join(Membership, Membership.user_id == User.id)
            .join(Organization, Membership.org_id == Organization.id)
            .where(
                User.id == user_id,
                User.is_active.is_(True),
                Membership.org_id == org_id,
                Organization.is_active.is_(True),
            )
        )
        row = result.one_or_none()
        if row is None:
            raise HTTPException(
                status_code=403,
                detail=error_detail("User is not a member of the selected organization"),
            )
        db_user, membership = row

        token_pair = await _issue_membership_tokens(db, request, db_user, membership)

        emit_audit_log(
            db,
            org_id=org_id,
            action=AuditAction.LOGIN,
            resource_type=AuditResourceType.SESSION,
            resource_id=user.user_id,
            user_id=user_id,
            description="User switched active organization",
            request=request,
        )
        await db.commit()

    return LoginResponse(
        access_token=token_pair.access_token,
        refresh_token=token_pair.refresh_token,
        token_type=token_pair.token_type,
        expires_in=token_pair.expires_in,
        needs_onboarding=False,
        user=_to_user_info(db_user, membership),
    )


@router.post("/validate", response_model=TokenValidateResponse)
@limiter.limit(AUTH_VALIDATE_LIMIT)
async def validate_token(
    payload: TokenValidateRequest,
    request: Request,
) -> TokenValidateResponse:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(payload.token)

    if not user:
        return TokenValidateResponse(valid=False)

    try:
        user_uuid = uuid_mod.UUID(user.user_id)
    except ValueError:
        return TokenValidateResponse(valid=False)

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User.id, User.is_active).where(User.id == user_uuid)
        )
        db_user = result.one_or_none()

    if not db_user or not db_user.is_active:
        return TokenValidateResponse(valid=False)

    return TokenValidateResponse(
        valid=True,
        user_id=user.user_id,
        email=user.email,
        org_id=user.org_id,
        role=user.role,
    )


@router.post("/logout")
async def logout(
    payload: LogoutRequest,
    request: Request,
    user: Annotated[AuthenticatedUser | None, Depends(get_current_user_optional)],
) -> dict:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    auth_service = get_auth_service()
    refresh_payload = auth_service.validate_token(
        payload.refresh_token, token_type="refresh"
    )
    if refresh_payload and refresh_payload.get("jti"):
        async with get_postgres_session() as db:
            await revoke_token(db, str(refresh_payload["jti"]))

    if user and user.org_id:
        user_uuid = _parse_uuid(user.user_id)
        org_uuid = _parse_uuid(user.org_id)
        if user_uuid is not None and org_uuid is not None:
            async with get_postgres_session() as db:
                emit_audit_log(
                    db,
                    org_id=org_uuid,
                    action=AuditAction.LOGOUT,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=user.user_id,
                    user_id=user_uuid,
                    description="User logged out",
                    request=request,
                )
                await db.commit()

    return {"message": "Logout successful"}
