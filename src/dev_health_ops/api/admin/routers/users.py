from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.admin.schemas import (
    UserCreate,
    UserResponse,
    UserSetPassword,
    UserUpdate,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.users import UserService
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.password_policy import validate_password
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership

from .common import (
    ADMIN_PASSWORD_LIMIT,
    _ensure_user_in_scope,
    _get_org_id_for_non_superuser,
    get_admin_user_key,
    get_session,
    limiter,
)

router = APIRouter()


def _user_response(user: object) -> UserResponse:
    return UserResponse.model_validate(
        {
            "id": str(getattr(user, "id")),
            "email": getattr(user, "email"),
            "username": getattr(user, "username"),
            "full_name": getattr(user, "full_name"),
            "avatar_url": getattr(user, "avatar_url"),
            "auth_provider": getattr(user, "auth_provider"),
            "is_active": getattr(user, "is_active"),
            "is_verified": getattr(user, "is_verified"),
            "is_superuser": getattr(user, "is_superuser"),
            "last_login_at": getattr(user, "last_login_at"),
            "created_at": getattr(user, "created_at"),
            "updated_at": getattr(user, "updated_at"),
        }
    )


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
    q: str | None = Query(default=None, min_length=1, max_length=200),
    x_org_id: Annotated[str | None, Header(alias="X-Org-Id")] = None,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> list[UserResponse]:
    svc = UserService(session)
    search = q.strip() if q and q.strip() else None
    # X-Org-Id header present → org-scoped view (admin page).
    # Absent + superuser → global view (superadmin page or impersonation).
    # Absent + non-superuser → scoped to JWT org_id.
    if x_org_id:
        users = await svc.list_by_org(
            x_org_id,
            limit=limit,
            offset=offset,
            active_only=active_only,
            search=search,
        )
    elif current_user.is_superuser:
        users = await svc.list_all(
            limit=limit,
            offset=offset,
            active_only=active_only,
            search=search,
        )
    else:
        org_id = _get_org_id_for_non_superuser(current_user)
        users = await svc.list_by_org(
            org_id,
            limit=limit,
            offset=offset,
            active_only=active_only,
            search=search,
        )
    return [_user_response(user) for user in users]


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> UserResponse:
    org_id = _get_org_id_for_non_superuser(current_user)
    svc = UserService(session)
    user = await svc.get_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await _ensure_user_in_scope(session, user, org_id, current_user)
    return _user_response(user)


@router.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    payload: UserCreate,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    svc = UserService(session)
    try:
        user = await svc.create(
            email=payload.email,
            password=payload.password,
            username=payload.username,
            full_name=payload.full_name,
            auth_provider=payload.auth_provider,
            auth_provider_id=payload.auth_provider_id,
            is_verified=payload.is_verified,
            is_superuser=payload.is_superuser,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _user_response(user)


@router.patch("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str,
    payload: UserUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> UserResponse:
    org_id = _get_org_id_for_non_superuser(current_user)
    svc = UserService(session)
    existing_user = await svc.get_by_id(user_id)
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")
    await _ensure_user_in_scope(session, existing_user, org_id, current_user)
    try:
        user = await svc.update(
            user_id=user_id,
            email=payload.email,
            username=payload.username,
            full_name=payload.full_name,
            avatar_url=payload.avatar_url,
            is_active=payload.is_active,
            is_verified=payload.is_verified,
            is_superuser=payload.is_superuser,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return _user_response(user)


@router.post("/users/{user_id}/password")
@limiter.limit(ADMIN_PASSWORD_LIMIT, key_func=get_admin_user_key)
async def set_user_password(
    request: Request,
    user_id: str,
    payload: UserSetPassword,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> dict:
    import bcrypt

    org_id = _get_org_id_for_non_superuser(current_user)
    password_violations = validate_password(payload.password)
    if password_violations:
        raise HTTPException(status_code=422, detail={"violations": password_violations})

    svc = UserService(session)
    user = await svc.get_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await _ensure_user_in_scope(session, user, org_id, current_user)

    admin_user = await svc.get_by_id(current_user.user_id)
    admin_password_hash = (
        getattr(admin_user, "password_hash") if admin_user is not None else None
    )
    if admin_user is None or admin_password_hash is None:
        raise HTTPException(
            status_code=403,
            detail="Admin password verification failed",
        )

    if not bcrypt.checkpw(
        payload.admin_password.encode("utf-8"),
        str(admin_password_hash).encode("utf-8"),
    ):
        raise HTTPException(
            status_code=403,
            detail="Admin password verification failed",
        )

    try:
        success = await svc.set_password(user_id, payload.password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not success:
        raise HTTPException(status_code=404, detail="User not found")

    audit_org_id: uuid.UUID | None = None
    try:
        audit_org_id = uuid.UUID(org_id) if org_id else None
    except ValueError:
        audit_org_id = None

    if audit_org_id is None:
        membership_result = await session.execute(
            select(Membership.org_id).where(Membership.user_id == user.id).limit(1)
        )
        audit_org_id = membership_result.scalar_one_or_none()

    try:
        actor_user_id = uuid.UUID(current_user.user_id)
    except ValueError:
        actor_user_id = None

    if audit_org_id is not None:
        emit_audit_log(
            session,
            org_id=audit_org_id,
            action=AuditAction.PASSWORD_CHANGED,
            resource_type=AuditResourceType.USER,
            resource_id=str(user.id),
            user_id=actor_user_id,
            description="Admin changed user password",
            request=request,
        )

    return {"success": True}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_admin),
) -> dict:
    org_id = _get_org_id_for_non_superuser(current_user)
    svc = UserService(session)
    user = await svc.get_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await _ensure_user_in_scope(session, user, org_id, current_user)
    deleted = await svc.delete(user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="User not found")
    return {"deleted": True}
