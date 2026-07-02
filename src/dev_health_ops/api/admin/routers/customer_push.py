"""Admin CRUD for customer-push source registration + ingest tokens (CHAOS-2696).

See docs/architecture/customer-push-authz.md for the one-active-owner
(per-provider matching) and token-scoping design. Endpoints live under
``/api/v1/admin/customer-push/*``; the parent admin router already applies
``Depends(require_admin)`` (see ``api/admin/router.py``).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_user
from dev_health_ops.api.admin.schemas.customer_push import (
    IngestSourceCreate,
    IngestSourcePatch,
    IngestSourceResponse,
    IngestTokenCreate,
    IngestTokenCreateResponse,
    IngestTokenResponse,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.ingest_auth import (
    TOKEN_PREFIX_DISPLAY_LENGTH,
    IngestSource,
    IngestSourceMode,
    IngestToken,
    IngestTokenScope,
    generate_ingest_token,
    hash_ingest_token,
)
from dev_health_ops.models.integrations import Integration, IntegrationSource

from .common import get_session

logger = logging.getLogger(__name__)

router = APIRouter()

_VALID_SYSTEMS = {"github", "gitlab", "jira", "linear", "custom"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _org_uuid(org_id: str) -> uuid.UUID:
    return uuid.UUID(org_id)


def _user_uuid(user_id: str | None) -> uuid.UUID | None:
    return uuid.UUID(user_id) if user_id else None


def _validate_system(system: str) -> str:
    normalized = system.strip().lower()
    if normalized not in _VALID_SYSTEMS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid system '{system}'; must be one of {sorted(_VALID_SYSTEMS)}",
        )
    return normalized


def _validate_mode(mode: str) -> IngestSourceMode:
    try:
        return IngestSourceMode(mode)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid mode '{mode}'; must be one of "
                f"{[m.value for m in IngestSourceMode]}"
            ),
        )


def _source_to_response(
    source: IngestSource, warnings: list[str] | None = None
) -> IngestSourceResponse:
    return IngestSourceResponse(
        id=str(source.id),
        org_id=source.org_id,
        system=source.system,
        instance=source.instance,
        display_name=source.display_name,
        mode=source.mode,
        enabled=source.enabled,
        webhook_mode=source.webhook_mode,
        matched_integration_source_id=(
            str(source.matched_integration_source_id)
            if source.matched_integration_source_id is not None
            else None
        ),
        created_at=source.created_at,
        updated_at=source.updated_at,
        warnings=warnings or [],
    )


def _token_to_response(token: IngestToken) -> IngestTokenResponse:
    return IngestTokenResponse(
        id=str(token.id),
        org_id=token.org_id,
        source_id=str(token.source_id) if token.source_id is not None else None,
        name=token.name,
        token_prefix=token.token_prefix,
        scopes=list(token.scopes or []),
        expires_at=token.expires_at,
        revoked_at=token.revoked_at,
        last_used_at=token.last_used_at,
        created_at=token.created_at,
    )


async def _get_org_source(
    session: AsyncSession, org_id: str, source_id: str
) -> IngestSource:
    try:
        source_uuid = uuid.UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Source not found")
    result = await session.execute(
        select(IngestSource).where(
            IngestSource.id == source_uuid, IngestSource.org_id == org_id
        )
    )
    source = result.scalar_one_or_none()
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


async def _get_org_token(
    session: AsyncSession, org_id: str, token_id: str, *, for_update: bool = False
) -> IngestToken:
    try:
        token_uuid = uuid.UUID(token_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Token not found")
    stmt = select(IngestToken).where(
        IngestToken.id == token_uuid, IngestToken.org_id == org_id
    )
    if for_update:
        # Serializes concurrent rotate/revoke on the same token (Postgres;
        # no-op on SQLite, which lacks SELECT ... FOR UPDATE) so two
        # concurrent rotations can't both observe revoked_at IS NULL and
        # each mint a live successor token.
        stmt = stmt.with_for_update()
    result = await session.execute(stmt)
    token = result.scalar_one_or_none()
    if token is None:
        raise HTTPException(status_code=404, detail="Token not found")
    return token


def _linear_is_org_wide_placeholder(source: IntegrationSource) -> bool:
    metadata = source.metadata_ or {}
    if metadata.get("org_wide_placeholder") is True:
        return True
    return (source.external_id or "").strip().lower() == "linear"


def _matches_instance(system: str, instance: str, source: IntegrationSource) -> bool:
    """CC5 per-provider matching (see docs/architecture/customer-push-authz.md)."""
    if system in ("github", "jira"):
        return instance in (source.external_id, source.full_name)
    if system == "gitlab":
        path_with_namespace = (source.metadata_ or {}).get("path_with_namespace")
        return instance in (source.full_name, path_with_namespace, source.external_id)
    if system == "linear":
        if _linear_is_org_wide_placeholder(source):
            return True
        return instance in (source.external_id, source.full_name, source.name)
    return False


async def _resolve_ownership(
    session: AsyncSession, org_id: str, system: str, instance: str
) -> tuple[uuid.UUID | None, list[str]]:
    """Run CC5 per-provider ownership matching against managed integration_sources.

    Returns ``(matched_integration_source_id, warnings)``. Raises 409
    ``source_owned_by_fullchaos_sync`` if a managed source matches this
    instance AND is both source-``is_enabled`` and its parent ``Integration``
    is ``is_active`` (post-critique CC5/CC14; overrules the brief body's
    warn-only Decision 8) -- "one-active-owner": a source row left enabled
    under a since-deactivated integration no longer counts as active
    ownership. ``custom`` systems have no managed equivalent and never
    conflict. A matched-but-not-actively-owned managed source is allowed to
    register and its id is persisted for the accept-time re-check (owned by
    CHAOS-2695). Independently, an active provider-level ``Integration`` row
    for the same provider (any instance) produces a non-blocking warning --
    the surviving half of Decision 8.
    """
    warnings: list[str] = []
    matched_id: uuid.UUID | None = None

    if system == "custom":
        return matched_id, warnings

    # func.lower(...) on both sides: nearby sync-creation paths (e.g.
    # IntegrationCreate) don't enforce lowercase provider values, so a
    # mixed-case managed row ("GitHub") must still be found and block a
    # lowercase "github" customer-push registration -- a bare `==` here
    # would silently bypass the one-active-owner 409.
    candidate_rows = (
        await session.execute(
            select(IntegrationSource, Integration.is_active)
            .join(Integration, IntegrationSource.integration_id == Integration.id)
            .where(
                IntegrationSource.org_id == org_id,
                func.lower(IntegrationSource.provider) == system,
            )
        )
    ).all()
    matches = [
        (source, integration_is_active)
        for source, integration_is_active in candidate_rows
        if _matches_instance(system, instance, source)
    ]
    enabled_match = next(
        (
            source
            for source, integration_is_active in matches
            if source.is_enabled and integration_is_active
        ),
        None,
    )
    if enabled_match is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "source_owned_by_fullchaos_sync",
                "message": (
                    f"A managed {system} sync source already owns '{instance}' "
                    "in this organization; disable it before enabling "
                    "customer-push for the same instance."
                ),
            },
        )
    if matches:
        matched_id = matches[0][0].id

    integration_active = (
        await session.execute(
            select(Integration.id)
            .where(
                Integration.org_id == org_id,
                func.lower(Integration.provider) == system,
                Integration.is_active.is_(True),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if integration_active is not None:
        warnings.append(
            f"Managed sync is also configured for provider '{system}' in this "
            "organization -- verify this is a different repository/workspace."
        )

    return matched_id, warnings


async def _create_token(
    session: AsyncSession,
    request: Request,
    current_user: AuthenticatedUser,
    org_id: str,
    source_id: uuid.UUID | None,
    payload: IngestTokenCreate,
) -> IngestTokenCreateResponse:
    plaintext = generate_ingest_token()
    token = IngestToken(
        org_id=org_id,
        source_id=source_id,
        name=payload.name,
        token_hash=hash_ingest_token(plaintext),
        token_prefix=plaintext[:TOKEN_PREFIX_DISPLAY_LENGTH],
        scopes=list(payload.scopes),
        created_by_user_id=_user_uuid(current_user.user_id),
        expires_at=payload.expires_at,
    )
    session.add(token)
    await session.flush()

    emit_audit_log(
        session,
        org_id=_org_uuid(org_id),
        action=AuditAction.INGEST_TOKEN_CREATED,
        resource_type=AuditResourceType.INGEST_TOKEN,
        resource_id=str(token.id),
        user_id=_user_uuid(current_user.user_id),
        description=f"Created ingest token '{payload.name}'",
        changes={
            "name": payload.name,
            "scopes": list(payload.scopes),
            "source_id": str(source_id) if source_id else None,
        },
        request=request,
    )
    await session.commit()

    return IngestTokenCreateResponse(
        id=str(token.id),
        org_id=org_id,
        source_id=str(source_id) if source_id else None,
        name=token.name,
        token=plaintext,
        token_prefix=token.token_prefix,
        scopes=list(token.scopes),
        expires_at=token.expires_at,
        created_at=token.created_at,
    )


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


@router.post(
    "/customer-push/sources", response_model=IngestSourceResponse, status_code=201
)
async def create_source(
    payload: IngestSourceCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestSourceResponse:
    org_id = current_user.org_id
    system = _validate_system(payload.system)
    mode = _validate_mode(payload.mode)

    matched_id: uuid.UUID | None = None
    warnings: list[str] = []
    if mode == IngestSourceMode.CUSTOMER_PUSH:
        matched_id, warnings = await _resolve_ownership(
            session, org_id, system, payload.instance
        )

    source = IngestSource(
        org_id=org_id,
        system=system,
        instance=payload.instance,
        display_name=payload.display_name,
        mode=mode.value,
        enabled=True,
        webhook_mode=payload.webhook_mode,
        matched_integration_source_id=matched_id,
        created_by_user_id=_user_uuid(current_user.user_id),
    )
    try:
        async with session.begin_nested():
            session.add(source)
            await session.flush()
    except IntegrityError as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                f"A source is already registered for system='{system}' "
                f"instance='{payload.instance}' in this organization"
            ),
        ) from exc

    emit_audit_log(
        session,
        org_id=_org_uuid(org_id),
        action=AuditAction.INGEST_SOURCE_REGISTERED,
        resource_type=AuditResourceType.INGEST_SOURCE,
        resource_id=str(source.id),
        user_id=_user_uuid(current_user.user_id),
        description=f"Registered customer-push source {system}/{payload.instance}",
        changes={"system": system, "instance": payload.instance, "mode": mode.value},
        request=request,
    )
    await session.commit()

    return _source_to_response(source, warnings=warnings)


@router.get("/customer-push/sources", response_model=list[IngestSourceResponse])
async def list_sources(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> list[IngestSourceResponse]:
    result = await session.execute(
        select(IngestSource)
        .where(IngestSource.org_id == current_user.org_id)
        .order_by(IngestSource.created_at.desc())
    )
    return [_source_to_response(s) for s in result.scalars().all()]


@router.get("/customer-push/sources/{source_id}", response_model=IngestSourceResponse)
async def get_source(
    source_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestSourceResponse:
    source = await _get_org_source(session, current_user.org_id, source_id)
    return _source_to_response(source)


@router.patch("/customer-push/sources/{source_id}", response_model=IngestSourceResponse)
async def patch_source(
    source_id: str,
    payload: IngestSourcePatch,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestSourceResponse:
    org_id = current_user.org_id
    source = await _get_org_source(session, org_id, source_id)

    changes: dict[str, Any] = {}
    if payload.mode is not None:
        new_mode = _validate_mode(payload.mode)
        if new_mode.value != source.mode:
            changes["mode"] = {"old": source.mode, "new": new_mode.value}
        source.mode = new_mode.value
    if payload.enabled is not None and payload.enabled != source.enabled:
        changes["enabled"] = {"old": source.enabled, "new": payload.enabled}
        source.enabled = payload.enabled
    if payload.display_name is not None and payload.display_name != source.display_name:
        changes["display_name"] = {
            "old": source.display_name,
            "new": payload.display_name,
        }
        source.display_name = payload.display_name
    if payload.webhook_mode is not None and payload.webhook_mode != source.webhook_mode:
        changes["webhook_mode"] = {
            "old": source.webhook_mode,
            "new": payload.webhook_mode,
        }
        source.webhook_mode = payload.webhook_mode

    # Re-run the CC5 ownership check whenever this PATCH results in the
    # source becoming write-eligible (mode=customer_push AND enabled=True) --
    # "creating/enabling" per CC14 -- so a managed sync source created after
    # the initial registration is still caught here (best-effort; the
    # authoritative guard is the accept-time re-check owned by CHAOS-2695).
    warnings: list[str] = []
    if source.is_write_eligible() and ("mode" in changes or "enabled" in changes):
        matched_id, warnings = await _resolve_ownership(
            session, org_id, source.system, source.instance
        )
        source.matched_integration_source_id = matched_id

    if changes:
        emit_audit_log(
            session,
            org_id=_org_uuid(org_id),
            action=AuditAction.INGEST_SOURCE_MODE_CHANGED,
            resource_type=AuditResourceType.INGEST_SOURCE,
            resource_id=str(source.id),
            user_id=_user_uuid(current_user.user_id),
            description=f"Updated customer-push source {source.system}/{source.instance}",
            changes=changes,
            request=request,
        )
        await session.commit()

    return _source_to_response(source, warnings=warnings)


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


@router.get(
    "/customer-push/sources/{source_id}/tokens",
    response_model=list[IngestTokenResponse],
)
async def list_source_tokens(
    source_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> list[IngestTokenResponse]:
    org_id = current_user.org_id
    source = await _get_org_source(session, org_id, source_id)
    result = await session.execute(
        select(IngestToken)
        .where(IngestToken.org_id == org_id, IngestToken.source_id == source.id)
        .order_by(IngestToken.created_at.desc())
    )
    return [_token_to_response(t) for t in result.scalars().all()]


@router.post(
    "/customer-push/sources/{source_id}/tokens",
    response_model=IngestTokenCreateResponse,
    status_code=201,
)
async def create_source_token(
    source_id: str,
    payload: IngestTokenCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestTokenCreateResponse:
    org_id = current_user.org_id
    source = await _get_org_source(session, org_id, source_id)
    return await _create_token(
        session, request, current_user, org_id, source.id, payload
    )


@router.get("/customer-push/tokens", response_model=list[IngestTokenResponse])
async def list_org_tokens(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> list[IngestTokenResponse]:
    result = await session.execute(
        select(IngestToken)
        .where(IngestToken.org_id == current_user.org_id)
        .order_by(IngestToken.created_at.desc())
    )
    return [_token_to_response(t) for t in result.scalars().all()]


@router.post(
    "/customer-push/tokens",
    response_model=IngestTokenCreateResponse,
    status_code=201,
)
async def create_org_token(
    payload: IngestTokenCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestTokenCreateResponse:
    """Create an org-wide (unbound) token -- never eligible for ingest:write
    (Design Decision 7: NULL source_id is only legal for schema:read/
    ingest:status)."""
    if IngestTokenScope.INGEST_WRITE.value in payload.scopes:
        raise HTTPException(
            status_code=400,
            detail=(
                "ingest:write requires a source-bound token; create it via "
                "POST /customer-push/sources/{source_id}/tokens"
            ),
        )
    return await _create_token(
        session, request, current_user, current_user.org_id, None, payload
    )


@router.post(
    "/customer-push/tokens/{token_id}/rotate",
    response_model=IngestTokenCreateResponse,
)
async def rotate_token(
    token_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestTokenCreateResponse:
    """Hard, immediate cutover (Design Decision 16) -- no grace window."""
    org_id = current_user.org_id
    old_token = await _get_org_token(session, org_id, token_id, for_update=True)
    if old_token.revoked_at is not None:
        raise HTTPException(status_code=400, detail="Token already revoked")

    now = datetime.now(timezone.utc)
    new_expires_at: datetime | None = None
    if old_token.expires_at is not None:
        original_ttl = old_token.expires_at - old_token.created_at
        new_expires_at = now + original_ttl

    old_token.revoked_at = now

    plaintext = generate_ingest_token()
    new_token = IngestToken(
        org_id=org_id,
        source_id=old_token.source_id,
        name=old_token.name,
        token_hash=hash_ingest_token(plaintext),
        token_prefix=plaintext[:TOKEN_PREFIX_DISPLAY_LENGTH],
        scopes=list(old_token.scopes or []),
        created_by_user_id=_user_uuid(current_user.user_id),
        expires_at=new_expires_at,
    )
    session.add(new_token)
    await session.flush()

    emit_audit_log(
        session,
        org_id=_org_uuid(org_id),
        action=AuditAction.INGEST_TOKEN_ROTATED,
        resource_type=AuditResourceType.INGEST_TOKEN,
        resource_id=str(new_token.id),
        user_id=_user_uuid(current_user.user_id),
        description=f"Rotated ingest token '{old_token.name}'",
        changes={
            "old_token_id": str(old_token.id),
            "new_token_id": str(new_token.id),
        },
        request=request,
    )
    await session.commit()

    return IngestTokenCreateResponse(
        id=str(new_token.id),
        org_id=org_id,
        source_id=str(new_token.source_id) if new_token.source_id else None,
        name=new_token.name,
        token=plaintext,
        token_prefix=new_token.token_prefix,
        scopes=list(new_token.scopes),
        expires_at=new_token.expires_at,
        created_at=new_token.created_at,
    )


@router.post(
    "/customer-push/tokens/{token_id}/revoke",
    response_model=IngestTokenResponse,
)
async def revoke_token(
    token_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> IngestTokenResponse:
    org_id = current_user.org_id
    token = await _get_org_token(session, org_id, token_id, for_update=True)
    if token.revoked_at is None:
        token.revoked_at = datetime.now(timezone.utc)
        emit_audit_log(
            session,
            org_id=_org_uuid(org_id),
            action=AuditAction.INGEST_TOKEN_REVOKED,
            resource_type=AuditResourceType.INGEST_TOKEN,
            resource_id=str(token.id),
            user_id=_user_uuid(current_user.user_id),
            description=f"Revoked ingest token '{token.name}'",
            request=request,
        )
        await session.commit()

    return _token_to_response(token)
