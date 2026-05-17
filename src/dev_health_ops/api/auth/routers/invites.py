from __future__ import annotations

import logging
import uuid as uuid_mod
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.invites import (
    accept_invite as accept_org_invite,
)
from dev_health_ops.api.services.invites import (
    validate_invite as validate_org_invite,
)
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.errors import error_detail

from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, Organization, User

from .common import _issue_membership_tokens, _require_uuid, _slugify_org_name
from .dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter()


class AcceptInviteRequest(BaseModel):
    token: str


class AcceptInviteResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    org_id: str
    org_name: str
    role: str


class OnboardRequest(BaseModel):
    action: str
    org_name: str | None = None
    invite_code: str | None = None


class OnboardResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    org_id: str
    org_name: str
    role: str


@router.post("/accept-invite", response_model=AcceptInviteResponse)
async def accept_invite(
    payload: AcceptInviteRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    request: Request,
) -> AcceptInviteResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    async with get_postgres_session() as db:
        try:
            user_uuid = uuid_mod.UUID(user.user_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=401,
                detail=error_detail("Invalid user identity"),
            ) from exc

        user_result = await db.execute(select(User).where(User.id == user_uuid))
        db_user = user_result.scalar_one_or_none()
        if db_user is None:
            raise HTTPException(
                status_code=401,
                detail=error_detail("User not found"),
            )

        invite = await validate_org_invite(db, payload.token)
        if invite is None:
            raise HTTPException(
                status_code=400,
                detail=error_detail("Invalid or expired invite"),
            )

        org_result = await db.execute(
            select(Organization).where(Organization.id == invite.org_id)
        )
        org = org_result.scalar_one_or_none()
        if org is None:
            raise HTTPException(
                status_code=404,
                detail=error_detail("Organization not found"),
            )

        try:
            db_user_id = _require_uuid(db_user.id, "db_user.id")
            membership = await accept_org_invite(db, invite, db_user_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=error_detail(str(exc)),
            ) from exc

        invite_org_id = _require_uuid(invite.org_id, "invite.org_id")
        membership_id = _require_uuid(membership.id, "membership.id")
        db_user_id = _require_uuid(db_user.id, "db_user.id")
        emit_audit_log(
            db,
            org_id=invite_org_id,
            action=AuditAction.MEMBER_JOINED,
            resource_type=AuditResourceType.MEMBERSHIP,
            resource_id=str(membership_id),
            user_id=db_user_id,
            description="Invite accepted",
            changes={
                "invite_id": str(invite.id),
                "user_id": str(db_user_id),
                "role": membership.role,
            },
            request=request,
        )

        await db.commit()
        token_pair = await _issue_membership_tokens(db, request, db_user, membership)

        return AcceptInviteResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            org_id=str(org.id),
            org_name=str(org.name),
            role=str(membership.role),
        )


@router.post("/onboard", response_model=OnboardResponse)
async def onboard(
    payload: OnboardRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    request: Request,
) -> OnboardResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    async with get_postgres_session() as db:
        try:
            user_uuid = uuid_mod.UUID(user.user_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=401,
                detail=error_detail("Invalid user identity"),
            ) from exc

        user_result = await db.execute(select(User).where(User.id == user_uuid))
        db_user = user_result.scalar_one_or_none()
        if not db_user:
            raise HTTPException(
                status_code=401,
                detail=error_detail("User not found"),
            )

        membership_result = await db.execute(
            select(Membership.id).where(Membership.user_id == db_user.id)
        )
        if membership_result.first() is not None:
            raise HTTPException(
                status_code=400,
                detail=error_detail("Already onboarded"),
            )

        if payload.action == "join_org":
            if not payload.invite_code:
                raise HTTPException(
                    status_code=400,
                    detail=error_detail("invite_code is required"),
                )

            invite = await validate_org_invite(db, payload.invite_code)
            if invite is None:
                raise HTTPException(
                    status_code=400,
                    detail=error_detail("Invalid or expired invite"),
                )

            org_result = await db.execute(
                select(Organization).where(Organization.id == invite.org_id)
            )
            org = org_result.scalar_one_or_none()
            if org is None:
                raise HTTPException(
                    status_code=404,
                    detail=error_detail("Organization not found"),
                )

            try:
                db_user_id = _require_uuid(db_user.id, "db_user.id")
                membership = await accept_org_invite(db, invite, db_user_id)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=error_detail(str(exc)),
                ) from exc

            org_id = _require_uuid(org.id, "org.id")
            membership_id = _require_uuid(membership.id, "membership.id")
            db_user_id = _require_uuid(db_user.id, "db_user.id")
            emit_audit_log(
                db,
                org_id=org_id,
                action=AuditAction.MEMBER_JOINED,
                resource_type=AuditResourceType.MEMBERSHIP,
                resource_id=str(membership_id),
                user_id=db_user_id,
                description="Invite accepted during onboarding",
                changes={
                    "invite_id": str(invite.id),
                    "user_id": str(db_user_id),
                    "role": membership.role,
                },
                request=request,
            )

            await db.commit()
            token_pair = await _issue_membership_tokens(
                db, request, db_user, membership
            )

            return OnboardResponse(
                access_token=token_pair.access_token,
                refresh_token=token_pair.refresh_token,
                token_type=token_pair.token_type,
                expires_in=token_pair.expires_in,
                org_id=str(org.id),
                org_name=str(org.name),
                role=str(membership.role),
            )

        if payload.action != "create_org":
            raise HTTPException(
                status_code=400,
                detail=error_detail("Invalid action. Use 'create_org' or 'join_org'"),
            )

        org_name = payload.org_name or "My Organization"
        org_slug = f"{_slugify_org_name(org_name)}-{str(db_user.id)[:8]}"

        org = Organization(
            slug=org_slug,
            name=org_name,
            tier="community",
            is_active=True,
        )
        db.add(org)
        await db.flush()
        db_user_id = _require_uuid(db_user.id, "db_user.id")
        org_id = _require_uuid(org.id, "org.id")

        membership = Membership(
            user_id=db_user_id,
            org_id=org_id,
            role="owner",
            joined_at=datetime.now(timezone.utc),
        )
        db.add(membership)

        emit_audit_log(
            db,
            org_id=org_id,
            action=AuditAction.CREATE,
            resource_type=AuditResourceType.ORGANIZATION,
            resource_id=str(org_id),
            user_id=db_user_id,
            description="Organization created during onboarding",
            changes={"organization_name": org_name},
            request=request,
        )

        await db.commit()

        token_pair = await _issue_membership_tokens(db, request, db_user, membership)

        return OnboardResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            org_id=str(org.id),
            org_name=str(org.name),
            role="owner",
        )
