from __future__ import annotations

import logging
import re
import uuid as uuid_mod
from datetime import datetime, timezone

from fastapi import Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.models.users import Membership, Organization, User

logger = logging.getLogger(__name__)


class UserInfo(BaseModel):
    id: str
    email: str
    username: str | None = None
    full_name: str | None = None
    org_id: str | None = None
    role: str
    is_superuser: bool = False


class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    needs_onboarding: bool = False
    user: UserInfo


class VerifyEmailResponse(BaseModel):
    message: str
    verified: bool | None = None


def _require_uuid(value: object, field_name: str) -> uuid_mod.UUID:
    if isinstance(value, uuid_mod.UUID):
        return value
    raise TypeError(f"{field_name} must be a UUID")


def _optional_uuid(value: object, field_name: str) -> uuid_mod.UUID | None:
    if value is None:
        return None
    return _require_uuid(value, field_name)


def _coerce_uuid(value: object) -> uuid_mod.UUID | None:
    return value if isinstance(value, uuid_mod.UUID) else None


def _slugify_org_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug[:50] or "my-organization"


def _parse_uuid(value: str | None) -> uuid_mod.UUID | None:
    if not value:
        return None
    try:
        return uuid_mod.UUID(value)
    except ValueError:
        return None


def _expiry_to_utc(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return None


async def _resolve_login_audit_org_id(
    db,
    user: User | None,
    payload_org_id: str | None,
) -> uuid_mod.UUID | None:
    parsed_org_id = _parse_uuid(payload_org_id)
    if parsed_org_id is not None:
        org_result = await db.execute(
            select(Organization.id).where(Organization.id == parsed_org_id)
        )
        if org_result.scalar_one_or_none() is not None:
            return parsed_org_id

    if user is None:
        return None

    membership_result = await db.execute(
        select(Membership.org_id).where(Membership.user_id == user.id).limit(1)
    )
    return membership_result.scalar_one_or_none()


async def _issue_membership_tokens(
    db,
    request: Request,
    db_user: User,
    membership: Membership,
):
    from dev_health_ops.api.auth.router import (
        create_refresh_token_record,
        get_auth_service,
    )

    auth_service = get_auth_service()
    token_pair = auth_service.create_token_pair(
        user_id=str(db_user.id),
        email=str(db_user.email),
        org_id=str(membership.org_id),
        role=str(membership.role),
        is_superuser=bool(db_user.is_superuser),
        username=str(db_user.username) if db_user.username is not None else None,
        full_name=str(db_user.full_name) if db_user.full_name is not None else None,
    )

    refresh_payload = auth_service.validate_token(
        token_pair.refresh_token, token_type="refresh"
    )
    if refresh_payload and refresh_payload.get("jti"):
        expires_at = _expiry_to_utc(refresh_payload.get("exp"))
        if expires_at is not None:
            await create_refresh_token_record(
                db=db,
                user_id=str(db_user.id),
                org_id=str(membership.org_id),
                token_hash=str(refresh_payload["jti"]),
                family_id=str(refresh_payload.get("family_id") or uuid_mod.uuid4()),
                expires_at=expires_at,
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
            )

    return token_pair


def _extract_unverified_org_and_subject(
    token: str,
) -> tuple[uuid_mod.UUID | None, str | None]:
    # Intentional unverified decode: audit-logging only — callers invoke this AFTER
    # validate_token has already failed. The returned org_id is never used for
    # authorization; it is passed straight to emit_audit_log.
    import jwt as _jwt
    from jwt.exceptions import InvalidTokenError

    try:
        # nosemgrep: python.jwt.security.unverified-jwt-decode.unverified-jwt-decode
        claims = _jwt.decode(token, options={"verify_signature": False})
    except (InvalidTokenError, ValueError, AttributeError, TypeError) as exc:
        logger.debug("Could not parse unverified claims: %s", exc)
        return None, None

    return _parse_uuid(claims.get("org_id")), claims.get("sub")


__all__ = [
    "LoginResponse",
    "UserInfo",
    "VerifyEmailResponse",
    "_coerce_uuid",
    "_expiry_to_utc",
    "_extract_unverified_org_and_subject",
    "_issue_membership_tokens",
    "_optional_uuid",
    "_parse_uuid",
    "_require_uuid",
    "_resolve_login_audit_org_id",
    "_slugify_org_name",
    "logger",
]
