"""Admin CRUD for customer-push source registration + ingest tokens (CHAOS-2696).

See docs/architecture/customer-push-authz.md for the one-active-owner
(per-provider matching) and token-scoping design. Endpoints live under
``/api/v1/admin/customer-push/*``; the parent admin router already applies
``Depends(require_admin)`` (see ``api/admin/router.py``).
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_user
from dev_health_ops.api.admin.schemas.customer_push import (
    AdminBatchListItemResponse,
    AdminBatchListResponse,
    AdminBatchResponse,
    AdminRejectedRecordResponse,
    AdminValidateResponse,
    IngestSourceCreate,
    IngestSourcePatch,
    IngestSourceResponse,
    IngestTokenCreate,
    IngestTokenCreateResponse,
    IngestTokenResponse,
)
from dev_health_ops.api.external_ingest import status as ingest_status
from dev_health_ops.api.external_ingest.errors import ExternalIngestError
from dev_health_ops.api.external_ingest.router import _read_body_enforcing_size_limit
from dev_health_ops.api.external_ingest.schemas import (
    MAX_BODY_BYTES_DEFAULT,
    MAX_RECORDS_DEFAULT,
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
    BatchEnvelope,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.licensing import feature_flag_state, resolve_org_tier
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.external_ingest.ownership import find_matching_managed_sources
from dev_health_ops.external_ingest.validate import validate_records
from dev_health_ops.licensing.types import TIER_ORDER, LicenseTier
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.ingest_auth import (
    TOKEN_PREFIX_DISPLAY_LENGTH,
    IngestSource,
    IngestSourceMode,
    IngestToken,
    IngestTokenScope,
    IngestWebhookMode,
    generate_ingest_token,
    hash_ingest_token,
)
from dev_health_ops.models.integrations import Integration
from dev_health_ops.models.licensing import OrgLicense

from .common import get_session

logger = logging.getLogger(__name__)

router = APIRouter()

_VALID_SYSTEMS = {
    "github",
    "gitlab",
    "jira",
    "linear",
    "pagerduty",
    "atlassian",
    "custom",
}
_CUSTOMER_PUSH_FEATURE = "customer_push_ingest"
_CUSTOMER_PUSH_REQUIRED_TIER = LicenseTier.TEAM


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


def _reject_fullchaos_hosted_webhook_mode(webhook_mode: str) -> None:
    """Router business-logic layer of adr-004's two-layer webhookMode contract.

    The Pydantic schema (``api/admin/schemas/customer_push.py``) accepts the
    full 3-value enum -- including ``fullchaos_hosted`` -- so a request body
    containing it passes schema validation (no 422); this is what actually
    rejects it, with a 400, before it is persisted or acted on (Option B /
    FullChaos-hosted webhooks are not built yet, see
    docs/architecture/adr-004-webhook-assisted-customer-push-ingestion.md).
    """
    if webhook_mode == IngestWebhookMode.FULLCHAOS_HOSTED.value:
        raise HTTPException(
            status_code=400,
            detail="fullchaos_hosted webhook mode is not available yet",
        )


def _source_to_response(
    source: IngestSource, warnings: list[str] | None = None
) -> IngestSourceResponse:
    return IngestSourceResponse(
        id=str(source.id),
        org_id=source.org_id,
        system=source.system,
        instance=source.instance,
        entity_family=source.entity_family,
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


async def _require_customer_push_access(session: AsyncSession, org_id: str) -> None:
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Organization not found") from exc

    def _state(sync_session: Any) -> tuple[str, LicenseTier]:
        org_license = (
            sync_session.query(OrgLicense).filter(OrgLicense.org_id == org_uuid).first()
        )
        tier = resolve_org_tier(sync_session, org_uuid, org_license)
        state = feature_flag_state(
            sync_session,
            org_uuid,
            _CUSTOMER_PUSH_FEATURE,
            min_tier=_CUSTOMER_PUSH_REQUIRED_TIER,
        )
        return state, tier

    state, tier = await session.run_sync(_state)
    if state == "enabled":
        return
    if TIER_ORDER.index(tier) < TIER_ORDER.index(_CUSTOMER_PUSH_REQUIRED_TIER):
        raise HTTPException(
            status_code=402,
            detail={
                "error": "feature_not_licensed",
                "feature": _CUSTOMER_PUSH_FEATURE,
                "required_tier": _CUSTOMER_PUSH_REQUIRED_TIER.value,
                "current_tier": tier.value,
            },
        )
    if state == "unregistered":
        raise HTTPException(
            status_code=402,
            detail={
                "error": "feature_not_licensed",
                "feature": _CUSTOMER_PUSH_FEATURE,
                "required_tier": _CUSTOMER_PUSH_REQUIRED_TIER.value,
                "current_tier": "unknown",
            },
        )
    raise HTTPException(
        status_code=403,
        detail={
            "error": "feature_not_enabled",
            "feature": _CUSTOMER_PUSH_FEATURE,
            "message": "Customer push ingest is not enabled for this organization",
        },
    )


async def _resolve_ownership(
    session: AsyncSession,
    org_id: str,
    system: str,
    instance: str,
    entity_family: str,
) -> tuple[uuid.UUID | None, list[str]]:
    """Run CC5 per-provider ownership matching against managed integration_sources.

    The matching predicates live in ``dev_health_ops.external_ingest.ownership``
    (CHAOS-2695, brief decision 12: the SAME logic runs here at registration
    time and in the data-plane accept path's ``resolve_effective_mode``) --
    this wrapper only applies the registration-time POLICY on top.

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

    matches = await find_matching_managed_sources(
        session,
        org_id=org_id,
        system=system,
        instance=instance,
        entity_family=entity_family,
    )
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
    await _require_customer_push_access(session, org_id)
    system = _validate_system(payload.system)
    mode = _validate_mode(payload.mode)
    _reject_fullchaos_hosted_webhook_mode(payload.webhook_mode)

    # Case-insensitive duplicate check BEFORE insert (CHAOS-2695
    # adversarial-review finding): the unique constraint is on the RAW
    # (org, system, instance), but provider instance identifiers are
    # case-insensitive (GitHub full names, GitLab paths, Jira/Linear keys)
    # -- without this, 'Acme/API' and 'acme/api' register as two enabled
    # sources for the same logical repository, splitting the one-active-owner
    # and idempotency namespaces. The stored instance keeps the user's
    # casing (tokens/envelopes then match it exactly); only the collision
    # check folds case. App-level only (this changeset ships no migrations);
    # a DB-level canonical unique index is tracked as a follow-up.
    case_variant = (
        (
            await session.execute(
                select(IngestSource).where(
                    IngestSource.org_id == org_id,
                    IngestSource.system == system,
                    func.lower(IngestSource.instance)
                    == payload.instance.strip().lower(),
                    IngestSource.entity_family == payload.entity_family,
                )
            )
        )
        .scalars()
        .first()
    )
    if case_variant is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"A source is already registered for system='{system}' "
                f"instance='{case_variant.instance}' in this organization "
                "(instance identifiers are case-insensitive)"
            ),
        )

    matched_id: uuid.UUID | None = None
    warnings: list[str] = []
    if mode == IngestSourceMode.CUSTOMER_PUSH:
        matched_id, warnings = await _resolve_ownership(
            session, org_id, system, payload.instance, payload.entity_family
        )

    source = IngestSource(
        org_id=org_id,
        system=system,
        instance=payload.instance,
        entity_family=payload.entity_family,
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
        changes={
            "system": system,
            "instance": payload.instance,
            "entity_family": payload.entity_family,
            "mode": mode.value,
        },
        request=request,
    )
    await session.commit()

    return _source_to_response(source, warnings=warnings)


@router.get("/customer-push/sources", response_model=list[IngestSourceResponse])
async def list_sources(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> list[IngestSourceResponse]:
    await _require_customer_push_access(session, current_user.org_id)
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
    await _require_customer_push_access(session, current_user.org_id)
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
    await _require_customer_push_access(session, org_id)
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
        _reject_fullchaos_hosted_webhook_mode(payload.webhook_mode)
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
            session, org_id, source.system, source.instance, source.entity_family
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
    await _require_customer_push_access(session, org_id)
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
    await _require_customer_push_access(session, org_id)
    source = await _get_org_source(session, org_id, source_id)
    return await _create_token(
        session, request, current_user, org_id, source.id, payload
    )


@router.get("/customer-push/tokens", response_model=list[IngestTokenResponse])
async def list_org_tokens(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> list[IngestTokenResponse]:
    await _require_customer_push_access(session, current_user.org_id)
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
    await _require_customer_push_access(session, current_user.org_id)
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
    await _require_customer_push_access(session, org_id)
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
    await _require_customer_push_access(session, org_id)
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


# ---------------------------------------------------------------------------
# Batches (CHAOS-2694) -- admin-plane read proxies over the CHAOS-2694
# status.py store (same tables the token-authed data-plane GET
# /api/v1/external-ingest/batches* endpoints read). Session-JWT +
# require_admin (already applied at the parent router level, CC25) rather
# than an ingest token.
# ---------------------------------------------------------------------------


def _batch_to_admin_list_item(
    batch: ingest_status.BatchRow,
) -> AdminBatchListItemResponse:
    return AdminBatchListItemResponse(
        ingestion_id=str(batch.ingestion_id),
        status=batch.status,
        source_system=batch.source_system,
        source_instance=batch.source_instance,
        producer=batch.producer,
        items_received=batch.items_received,
        items_accepted=batch.items_accepted,
        items_rejected=batch.items_rejected,
        created_at=batch.created_at,
        completed_at=batch.completed_at,
    )


async def _batch_to_admin_response(
    session: AsyncSession,
    batch: ingest_status.BatchRow,
    *,
    org_id: str,
    rejected_records_limit: int,
    rejected_records_offset: int,
) -> AdminBatchResponse:
    errors, errors_total = await ingest_status.list_rejections(
        session,
        org_id=org_id,
        ingestion_id=batch.ingestion_id,
        limit=rejected_records_limit,
        offset=rejected_records_offset,
    )
    return AdminBatchResponse(
        ingestion_id=str(batch.ingestion_id),
        org_id=batch.org_id,
        status=batch.status,
        attempts=batch.attempts,
        source_system=batch.source_system,
        source_instance=batch.source_instance,
        producer=batch.producer,
        producer_version=batch.producer_version,
        schema_version=batch.schema_version,
        window_started_at=batch.window_started_at,
        window_ended_at=batch.window_ended_at,
        items_received=batch.items_received,
        items_accepted=batch.items_accepted,
        items_rejected=batch.items_rejected,
        record_counts=batch.record_counts,
        error_summary=batch.error_summary,
        created_at=batch.created_at,
        updated_at=batch.updated_at,
        completed_at=batch.completed_at,
        rejected_records=[
            AdminRejectedRecordResponse(
                index=e.record_index,
                kind=e.record_kind,
                external_id=e.external_id,
                code=e.code,
                message=e.message,
                path=e.path,
            )
            for e in errors
        ],
        rejected_records_total=errors_total,
        rejected_records_limit=rejected_records_limit,
        rejected_records_offset=rejected_records_offset,
    )


@router.get(
    "/customer-push/sources/{source_id}/batches",
    response_model=AdminBatchListResponse,
)
async def list_source_batches(
    source_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
    status_filter: str | None = Query(default=None, alias="status"),
    producer: str | None = Query(default=None),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> AdminBatchListResponse:
    org_id = current_user.org_id
    await _require_customer_push_access(session, org_id)
    source = await _get_org_source(session, org_id, source_id)
    rows, total = await ingest_status.list_batches(
        session,
        org_id=org_id,
        source_system=source.system,
        source_instance=source.instance,
        status=status_filter,
        producer=producer,
        created_after=from_,
        created_before=to,
        limit=limit,
        offset=offset,
    )
    return AdminBatchListResponse(
        items=[_batch_to_admin_list_item(row) for row in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/customer-push/batches/{ingestion_id}", response_model=AdminBatchResponse)
async def get_batch_detail(
    ingestion_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
    rejected_records_limit: int = Query(default=50, ge=1, le=200),
    rejected_records_offset: int = Query(default=0, ge=0),
) -> AdminBatchResponse:
    org_id = current_user.org_id
    await _require_customer_push_access(session, org_id)
    try:
        parsed_id = uuid.UUID(ingestion_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Batch not found")
    batch = await ingest_status.get_batch(
        session, org_id=org_id, ingestion_id=parsed_id
    )
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return await _batch_to_admin_response(
        session,
        batch,
        org_id=org_id,
        rejected_records_limit=rejected_records_limit,
        rejected_records_offset=rejected_records_offset,
    )


# ---------------------------------------------------------------------------
# Validate proxy (CHAOS-2695) -- session-auth twin of the token-authed
# data-plane POST /api/v1/external-ingest/validate, for the web console's
# Screen 5. VALIDATE-ONLY: the console-push proxy (POST .../batches,
# producer="web-console") was CUT from v1 (master-spec CC25); the ingestion
# write path stays exclusively token-authed.
# ---------------------------------------------------------------------------


def _validate_failure(
    code: str, message: str, path: str | None
) -> AdminValidateResponse:
    """Envelope-level failure as a 200 ``valid: false`` result row.

    Deliberately NOT a 4xx (see ``AdminValidateResponse``'s docstring): the
    console renders these as validation results; only auth/404 scope errors
    surface as HTTP errors on this route.
    """
    return AdminValidateResponse(
        valid=False,
        items_accepted=0,
        items_rejected=0,
        errors=[
            AdminRejectedRecordResponse(
                index=0,
                kind="unknown",
                external_id=None,
                code=code,
                message=message,
                path=path,
            )
        ],
    )


@router.post(
    "/customer-push/sources/{source_id}/validate",
    response_model=AdminValidateResponse,
)
async def validate_source_payload(
    source_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> AdminValidateResponse:
    """Validate a batch envelope against the wire schemas, per-record.

    Mirrors the data-plane ``POST /validate`` semantics exactly (same
    ``validate_records``, same size/version/count checks) so a payload that
    validates here validates there -- it does NOT check the envelope's
    ``source`` against this route's source (the data plane enforces that at
    push time via the token's source binding, which has no analogue in a
    session-authed console request). The ``source_id`` path segment is a
    tenant/scope check only.
    """
    await _require_customer_push_access(session, current_user.org_id)
    await _get_org_source(session, current_user.org_id, source_id)

    try:
        raw = await _read_body_enforcing_size_limit(request)
    except ExternalIngestError as exc:
        return _validate_failure(exc.code, exc.message, None)

    try:
        envelope = BatchEnvelope.model_validate_json(raw)
    except ValidationError as exc:
        return AdminValidateResponse(
            valid=False,
            items_accepted=0,
            items_rejected=0,
            errors=[
                AdminRejectedRecordResponse(
                    index=0,
                    kind="unknown",
                    external_id=None,
                    code="invalid_envelope",
                    message=err["msg"],
                    path=".".join(str(part) for part in err["loc"]) or None,
                )
                # Cap pathological inputs; the first errors are the
                # actionable ones for a console user.
                for err in exc.errors()[:50]
            ],
        )

    if envelope.schema_version != SCHEMA_VERSION:
        return _validate_failure(
            "unsupported_schema_version",
            f"Unsupported schemaVersion: {envelope.schema_version!r}",
            "schemaVersion",
        )

    max_records = _external_ingest_limits()["maxRecordsPerBatch"]
    if len(envelope.records) > max_records:
        return _validate_failure(
            "batch_too_large",
            f"Batch has {len(envelope.records)} records; max is {max_records}",
            "records",
        )

    errors = validate_records(envelope.records)
    rejected_indices = {item.index for item in errors}
    return AdminValidateResponse(
        valid=not errors,
        items_accepted=len(envelope.records) - len(rejected_indices),
        items_rejected=len(rejected_indices),
        errors=[
            AdminRejectedRecordResponse(
                index=item.index,
                kind=item.kind,
                # ValidationErrorItem has no external_id; enrich from the
                # record wrapper for console-table correlation.
                external_id=(
                    envelope.records[item.index].external_id
                    if 0 <= item.index < len(envelope.records)
                    else None
                ),
                code=item.code,
                message=item.message,
                path=item.path,
            )
            for item in errors
        ],
    )


# ---------------------------------------------------------------------------
# Schemas passthrough (CHAOS-2694) -- thin proxy over the same
# schemas.py-derived payload the data-plane GET /schemas* endpoints return
# (router.py owns those; not modified here). CHAOS-2692's schema registry
# will eventually be the shared source for this once it lands.
# ---------------------------------------------------------------------------


def _external_ingest_limits() -> dict[str, int]:
    return {
        "maxRecordsPerBatch": int(
            os.environ.get("EXTERNAL_INGEST_MAX_RECORDS", str(MAX_RECORDS_DEFAULT))
        ),
        "maxBodyBytes": int(
            os.environ.get(
                "EXTERNAL_INGEST_MAX_BODY_BYTES", str(MAX_BODY_BYTES_DEFAULT)
            )
        ),
    }


@router.get("/customer-push/schemas")
async def admin_list_schemas(
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> dict[str, Any]:
    await _require_customer_push_access(session, current_user.org_id)
    return {
        "schemaVersions": [SCHEMA_VERSION],
        "recordKinds": sorted(RECORD_KIND_MODELS),
        "limits": _external_ingest_limits(),
    }


@router.get("/customer-push/schemas/{schema_version}")
async def admin_get_schema(
    schema_version: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(get_admin_user),
) -> dict[str, Any]:
    await _require_customer_push_access(session, current_user.org_id)
    if schema_version != SCHEMA_VERSION:
        raise HTTPException(
            status_code=404, detail=f"Unknown schema version: {schema_version!r}"
        )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "envelope": BatchEnvelope.model_json_schema(by_alias=True),
        "recordKinds": {
            kind: model.model_json_schema(by_alias=True)
            for kind, model in RECORD_KIND_MODELS.items()
        },
        "limits": _external_ingest_limits(),
    }
