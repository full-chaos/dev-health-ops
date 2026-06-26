from __future__ import annotations

import importlib
import logging
from datetime import datetime, timezone

import bcrypt
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select

from dev_health_ops.api.middleware.rate_limit import (
    AUTH_REGISTER_LIMIT,
    get_forwarded_ip,
    limiter,
)
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.api.utils.password_policy import validate_password
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, Organization, User

from .common import _coerce_uuid, _require_uuid

logger = logging.getLogger(__name__)

router = APIRouter()


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    full_name: str | None = None
    org_name: str | None = None


class RegisterResponse(BaseModel):
    message: str
    user_id: str
    org_id: str | None = None


@router.post("/register", response_model=RegisterResponse, status_code=201)
@limiter.limit(AUTH_REGISTER_LIMIT, key_func=get_forwarded_ip)
async def register(payload: RegisterRequest, request: Request) -> RegisterResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    email_verification_service = importlib.import_module(
        "dev_health_ops.api.services.email_verification"
    )
    create_verification_token = (
        email_verification_service.create_email_verification_token
    )
    send_verification = email_verification_service.send_verification_email

    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        password_violations = validate_password(payload.password)
        if password_violations:
            raise HTTPException(
                status_code=422,
                detail=error_detail(
                    "Password validation failed", errors=password_violations
                ),
            )

        stmt = select(User).where(func.lower(User.email) == email_normalized)
        result = await db.execute(stmt)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            existing_org_result = await db.execute(
                select(Membership.org_id)
                .where(Membership.user_id == existing_user.id)
                .limit(1)
            )
            existing_org_id = existing_org_result.scalar_one_or_none()
            existing_org_uuid = _coerce_uuid(existing_org_id)
            if existing_org_uuid is not None:
                existing_user_id = _require_uuid(existing_user.id, "existing_user.id")
                emit_audit_log(
                    db,
                    org_id=existing_org_uuid,
                    action=AuditAction.CREATE,
                    resource_type=AuditResourceType.USER,
                    resource_id=str(existing_user_id),
                    user_id=existing_user_id,
                    description="User registration failed: email already registered",
                    changes={"email": email_normalized},
                    request=request,
                    status="failure",
                    error_message="Email already registered",
                )
            raise HTTPException(
                status_code=400,
                detail=error_detail("Email already registered"),
            )

        password_hash = bcrypt.hashpw(
            payload.password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        user = User(
            email=email_normalized,
            password_hash=password_hash,
            full_name=payload.full_name,
            auth_provider="local",
            is_active=True,
            is_verified=False,
        )
        db.add(user)
        await db.flush()
        user_id = _require_uuid(user.id, "user.id")

        org_name = payload.org_name or "My Organization"
        org_slug = org_name.lower().replace(" ", "-")[:50]
        org_slug = f"{org_slug}-{str(user.id)[:8]}"

        org = Organization(
            slug=org_slug,
            name=org_name,
            tier="community",
            is_active=True,
        )
        db.add(org)
        await db.flush()
        org_id = _require_uuid(org.id, "org.id")

        membership = Membership(
            user_id=user_id,
            org_id=org_id,
            role="owner",
            joined_at=datetime.now(timezone.utc),
        )
        db.add(membership)

        emit_audit_log(
            db,
            org_id=org_id,
            action=AuditAction.CREATE,
            resource_type=AuditResourceType.USER,
            resource_id=str(user_id),
            user_id=user_id,
            description="User registered",
            changes={"email": email_normalized, "organization_id": str(org_id)},
            request=request,
        )

        verification_token = await create_verification_token(db, user_id)

        await db.commit()

        try:
            await send_verification(
                to_email=str(user.email),
                full_name=str(user.full_name) if user.full_name is not None else None,
                token=verification_token,
            )
        except Exception as exc:
            logger.error(
                "Failed to send verification email for %s: %s: %s",
                sanitize_for_log(payload.email),
                type(exc).__name__,
                sanitize_for_log(str(exc)),
                exc_info=True,
            )

        return RegisterResponse(
            message="Registration successful",
            user_id=str(user_id),
            org_id=str(org_id),
        )
