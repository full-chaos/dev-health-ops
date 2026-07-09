from __future__ import annotations

import logging
import os
import re
import uuid as uuid_mod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.models.users import Membership, Organization, User

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrganizationActivity:
    has_data: bool = False
    last_metrics_at: datetime | None = None


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


class OrganizationMembershipInfo(BaseModel):
    id: str
    slug: str
    name: str
    tier: str | None = None
    role: str
    joined_at: datetime | None = None
    has_data: bool = False
    last_metrics_at: datetime | None = None


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


def _membership_joined_sort_value(membership: Membership) -> datetime:
    joined_at = getattr(membership, "joined_at", None) or getattr(
        membership, "created_at", None
    )
    if joined_at is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if joined_at.tzinfo is None:
        return joined_at.replace(tzinfo=timezone.utc)
    return joined_at.astimezone(timezone.utc)


def _select_active_membership(
    memberships: Sequence[Membership],
    requested_org_id: uuid_mod.UUID | None,
    activity_by_org: dict[uuid_mod.UUID, OrganizationActivity],
) -> Membership | None:
    if requested_org_id is not None:
        return next(
            (
                membership
                for membership in memberships
                if membership.org_id == requested_org_id
            ),
            None,
        )

    if not memberships:
        return None

    def sort_key(membership: Membership) -> tuple[bool, float, datetime]:
        org_id = _require_uuid(membership.org_id, "membership.org_id")
        activity = activity_by_org.get(org_id, OrganizationActivity())
        last_metrics_at = activity.last_metrics_at or datetime.min.replace(
            tzinfo=timezone.utc
        )
        return (
            not activity.has_data,
            -last_metrics_at.timestamp(),
            _membership_joined_sort_value(membership),
        )

    return min(
        memberships,
        key=sort_key,
    )


def _load_org_activity(
    org_ids: Iterable[uuid_mod.UUID],
) -> dict[uuid_mod.UUID, OrganizationActivity]:
    ids = list(dict.fromkeys(org_ids))
    dsn = os.environ.get("CLICKHOUSE_URI") or os.environ.get("DEV_HEALTH_SINK")
    if not ids or not dsn:
        return {org_id: OrganizationActivity() for org_id in ids}

    try:
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    except ImportError as exc:
        logger.debug("ClickHouse unavailable for org activity lookup: %s", exc)
        return {org_id: OrganizationActivity() for org_id in ids}

    sink = None
    activity = {org_id: OrganizationActivity() for org_id in ids}
    metric_tables = (
        "repo_metrics_daily",
        "user_metrics_daily",
        "team_metrics_daily",
        "work_item_metrics_daily",
    )
    try:
        sink = ClickHouseMetricsSink(dsn=dsn)
        for org_id in ids:
            last_metrics_at: datetime | None = None
            has_data = False
            for table in metric_tables:
                try:
                    result = sink.client.query(
                        f"""
                        SELECT count() AS row_count, max(computed_at) AS last_metrics_at
                        FROM {table}
                        WHERE org_id = {{org_id:String}}
                        """,
                        parameters={"org_id": str(org_id)},
                    )
                except Exception as exc:
                    logger.debug(
                        "Skipping org activity lookup for table %s: %s", table, exc
                    )
                    continue

                row = (getattr(result, "result_rows", None) or [(0, None)])[0]
                row_count = int(row[0] or 0)
                table_last_metrics_at = _expiry_to_utc(row[1])
                has_data = has_data or row_count > 0
                if table_last_metrics_at is not None and (
                    last_metrics_at is None or table_last_metrics_at > last_metrics_at
                ):
                    last_metrics_at = table_last_metrics_at
            activity[org_id] = OrganizationActivity(
                has_data=has_data,
                last_metrics_at=last_metrics_at,
            )
    except Exception as exc:
        logger.debug("Org activity lookup unavailable: %s", exc)
    finally:
        if sink is not None:
            sink.close()
    return activity


def _to_user_info(user: User, membership: Membership | None) -> UserInfo:
    return UserInfo(
        id=str(user.id),
        email=str(user.email),
        username=str(user.username) if user.username is not None else None,
        full_name=str(user.full_name) if user.full_name is not None else None,
        org_id=str(membership.org_id) if membership else None,
        role=str(membership.role) if membership else "member",
        is_superuser=bool(user.is_superuser),
    )


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
    membership: Membership | None,
):
    from dev_health_ops.api.auth.router import (
        create_refresh_token_record,
        get_auth_service,
    )

    auth_service = get_auth_service()
    org_id = str(membership.org_id) if membership is not None else ""
    role = str(membership.role) if membership is not None else "member"
    token_pair = auth_service.create_token_pair(
        user_id=str(db_user.id),
        email=str(db_user.email),
        org_id=org_id,
        role=role,
        is_superuser=bool(db_user.is_superuser),
        username=str(db_user.username) if db_user.username is not None else None,
        full_name=str(db_user.full_name) if db_user.full_name is not None else None,
        token_version=int(getattr(db_user, "token_version", 0) or 0),
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
                org_id=org_id,
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
    "OrganizationActivity",
    "OrganizationMembershipInfo",
    "UserInfo",
    "VerifyEmailResponse",
    "_coerce_uuid",
    "_expiry_to_utc",
    "_extract_unverified_org_and_subject",
    "_issue_membership_tokens",
    "_load_org_activity",
    "_optional_uuid",
    "_parse_uuid",
    "_require_uuid",
    "_resolve_login_audit_org_id",
    "_select_active_membership",
    "_slugify_org_name",
    "_to_user_info",
    "logger",
]
