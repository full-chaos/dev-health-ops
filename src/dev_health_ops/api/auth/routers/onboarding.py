from __future__ import annotations

import uuid as uuid_mod
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Membership, Organization, User

from .dependencies import get_current_user

router = APIRouter(prefix="/onboarding")


def _state_response(**kwargs: Any) -> Any:
    from dev_health_ops.api.admin.schemas_flat import OnboardingStateResponse

    return OnboardingStateResponse(**kwargs)


def _parse_user_id(user_id: str) -> uuid_mod.UUID:
    try:
        return uuid_mod.UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="invalid_user") from exc


async def _load_membership(
    db: AsyncSession,
    db_user: User,
    token_org_id: str,
) -> Membership | None:
    stmt = select(Membership).where(Membership.user_id == db_user.id)
    if token_org_id:
        try:
            org_uuid = uuid_mod.UUID(token_org_id)
        except ValueError:
            return None
        stmt = stmt.where(Membership.org_id == org_uuid)
    stmt = stmt.order_by(Membership.joined_at.asc(), Membership.created_at.asc()).limit(
        1
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def _has_connected_integration(db: AsyncSession, org: Organization) -> bool:
    result = await db.execute(
        select(IntegrationCredential.id)
        .where(
            IntegrationCredential.org_id == str(org.id),
            IntegrationCredential.is_active == True,  # noqa: E712
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def _build_onboarding_state(
    db: AsyncSession,
    user: AuthenticatedUser,
) -> Any:
    user_uuid = _parse_user_id(user.user_id)
    db_user = await db.scalar(select(User).where(User.id == user_uuid))
    if db_user is None:
        raise HTTPException(status_code=401, detail="user_not_found")
    if not bool(db_user.is_verified):
        raise HTTPException(status_code=403, detail="email_unverified")

    membership = await _load_membership(db, db_user, user.org_id)
    if membership is None:
        if bool(db_user.is_superuser):
            return _state_response(
                needs_onboarding=False,
                org_created=False,
                first_integration_connected=False,
                integration_skipped=False,
                next_step="dashboard",
            )
        return _state_response(
            needs_onboarding=True,
            org_created=False,
            first_integration_connected=False,
            integration_skipped=False,
            next_step="workspace",
        )

    org = await db.scalar(
        select(Organization).where(Organization.id == membership.org_id)
    )
    if org is None:
        return _state_response(
            needs_onboarding=True,
            org_created=False,
            first_integration_connected=False,
            integration_skipped=False,
            next_step="workspace",
            blocker="organization_not_found",
        )

    integration_connected = await _has_connected_integration(db, org)
    integration_skipped = (
        org.onboarding_integration_skipped_at is not None and not integration_connected
    )
    privileged_dashboard = bool(db_user.is_superuser) or user.role == "admin"
    if privileged_dashboard:
        next_step = "dashboard"
        needs_onboarding = False
    elif integration_connected or integration_skipped:
        next_step = "complete"
        needs_onboarding = False
    else:
        next_step = "integration"
        needs_onboarding = True

    return _state_response(
        needs_onboarding=needs_onboarding,
        org_created=True,
        org_id=str(org.id),
        org_name=str(org.name),
        first_integration_connected=integration_connected,
        integration_skipped=integration_skipped,
        next_step=next_step,
    )


@router.get("/state")
async def onboarding_state(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> Any:
    from dev_health_ops.api.auth.router import get_postgres_session

    async with get_postgres_session() as db:
        return await _build_onboarding_state(db, user)


@router.post("/skip-integration")
async def skip_integration(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> Any:
    from dev_health_ops.api.auth.router import get_postgres_session

    if not user.org_id:
        raise HTTPException(status_code=400, detail="organization_required")
    try:
        org_uuid = uuid_mod.UUID(user.org_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="organization_required") from exc

    async with get_postgres_session() as db:
        membership = await db.scalar(
            select(Membership).where(
                Membership.org_id == org_uuid,
                Membership.user_id == _parse_user_id(user.user_id),
            )
        )
        if membership is None:
            raise HTTPException(
                status_code=403, detail="organization_membership_required"
            )
        org = await db.scalar(select(Organization).where(Organization.id == org_uuid))
        if org is None:
            raise HTTPException(status_code=404, detail="organization_not_found")
        if org.onboarding_integration_skipped_at is None:
            org.onboarding_integration_skipped_at = datetime.now(timezone.utc)
        await db.commit()
        return await _build_onboarding_state(db, user)
