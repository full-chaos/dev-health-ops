"""Real external-ingest auth dependency (CHAOS-2696/2712, master-spec CC14).

Resolves an ``fcpush_`` bearer token against ``external_ingest_tokens``
(``sha256``, matching the house convention -- see
``docs/architecture/customer-push-authz.md``), independently sets the
``org_id`` contextvar (``OrgIdMiddleware`` only understands user JWTs and
takes its anonymous pass-through branch for an ``fcpush_...`` bearer, so it
never sets the contextvar for us), and is the single place 401/403 semantics
for CHAOS-2690's data-plane endpoints are decided.

Deletes CHAOS-2691's interim, flag-gated body (``EXTERNAL_INGEST_INSECURE_AUTH``
+ the ``X-Org-Id`` header path) entirely -- this ticket is the hard pre-GA
blocker for the ``chaos-2690-external-ingest`` integration branch
(master-spec CC14 / reconciliation delta A5). The ``IngestAuthContext`` shape
and ``Depends(...)`` call sites in ``router.py`` are unchanged: both bound
scope dependencies (``_require_schema_read``/``_require_ingest_write``) still
resolve to ``require_ingest_scope(scope)``.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends, Request
from limits import parse as parse_rate_limit
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.middleware.rate_limit import (
    INGEST_AUTH_ATTEMPT_IP_LIMIT,
    INGEST_AUTH_FAILURE_IP_LIMIT,
    get_forwarded_ip,
    limiter,
)
from dev_health_ops.api.services.auth import _current_org_id, set_current_org_id
from dev_health_ops.api.services.licensing import feature_flag_state
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.ingest_auth import (
    TOKEN_PREFIX,
    IngestSource,
    IngestToken,
    hash_ingest_token,
)

from .errors import ExternalIngestError

logger = logging.getLogger(__name__)

_AUTH_ATTEMPT_LIMIT_ITEM = parse_rate_limit(INGEST_AUTH_ATTEMPT_IP_LIMIT)
_AUTH_FAILURE_LIMIT_ITEM = parse_rate_limit(INGEST_AUTH_FAILURE_IP_LIMIT)
_CUSTOMER_PUSH_FEATURE = "customer_push_ingest"
_CUSTOMER_PUSH_REQUIRED_TIER = LicenseTier.TEAM


@dataclass(frozen=True)
class IngestAuthContext:
    """Resolved identity for an authenticated ``/api/v1/external-ingest/*`` request.

    Shape pinned epic-wide (master-spec CC14). ``source`` is ``None`` for
    org-wide tokens -- legal only when scopes are a subset of
    ``{schema:read, ingest:status}`` (never ``ingest:write``), enforced at
    token-creation time (Design Decision 7,
    docs/architecture/customer-push-authz.md), not re-validated here.
    """

    org_id: str
    scopes: frozenset[str]
    token_id: str | None = None
    source: IngestSource | None = None


def _extract_bearer(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if not header or not header.lower().startswith("bearer "):
        return None
    token = header[7:].strip()
    return token or None


async def _emit_failure_audit(
    db: AsyncSession,
    request: Request,
    *,
    org_id: str | None,
    token_id: str | None,
    reason: str,
) -> None:
    """Commit-before-raise (docs/architecture/customer-push-authz.md).

    Every caller does ``await _emit_failure_audit(...)`` immediately
    followed by ``raise ExternalIngestError(...)`` on its own line --
    ``get_postgres_session``'s ambient commit only fires on a clean
    dependency return and rolls back on *any* exception, including a
    deliberately-raised one, which would otherwise silently discard the
    ``INGEST_AUTH_FAILED`` row this function exists to persist (mirrors the
    confirmed live fix pattern in ``api/auth/routers/login.py``).

    ``org_id`` is unknown for a header that never resolved to a token row
    (missing/malformed bearer, unrecognized hash) -- ``AuditLog.org_id`` is a
    required FK to ``organizations.id`` with nothing valid to attach to, so
    those cases log at WARNING only and are never written to Postgres.
    """
    if org_id is None:
        logger.warning(
            "external-ingest auth failure (no org resolved): path=%s reason=%s",
            request.url.path,
            reason,
        )
        return
    try:
        emit_audit_log(
            db,
            org_id=uuid.UUID(org_id),
            action=AuditAction.INGEST_AUTH_FAILED,
            resource_type=AuditResourceType.INGEST_TOKEN,
            resource_id=token_id or "unknown",
            description=f"Ingest auth failed on {request.url.path}: {reason}",
            request=request,
            status="failure",
            error_message=reason,
        )
        await db.commit()
    except Exception:
        logger.exception("Failed to persist ingest auth failure audit log (non-fatal)")


def _auth_attempt_ip_key(request: Request) -> str:
    return f"external-ingest-auth-attempt:{get_forwarded_ip(request)}"


def _auth_failure_ip_key(request: Request) -> str:
    return f"external-ingest-auth-fail:{get_forwarded_ip(request)}"


def _reserve_auth_attempt_or_429(request: Request) -> None:
    """Atomic, unconditional per-IP ceiling on ingest-auth attempts.

    Must be the first thing ``require_ingest_scope``'s dependency does --
    before ``_extract_bearer``, before the token-hash DB lookup, before any
    ``await``. Consumed via ``hit()`` (not ``test()``), which makes this
    atomic with respect to other concurrently-scheduled requests in the same
    asyncio event loop: a synchronous call with no ``await`` inside it
    cannot be interleaved with another coroutine's execution. A
    ``test()``-then-``hit()`` split (as used by
    ``_reject_if_already_ip_throttled`` below) is *not* atomic here, because
    the token-hash DB lookup sits between the two calls and is itself an
    ``await`` point -- a burst of concurrent invalid-token requests can all
    observe the bucket as available and all reach Postgres before any of
    them are counted (2nd-round adversarial-review finding). This bucket is
    the actual DB/app-load protection; see ``INGEST_AUTH_ATTEMPT_IP_LIMIT``'s
    comment (``api/middleware/rate_limit.py``) for why it's generous and
    applies to every attempt regardless of outcome.
    """
    rate_limiter = getattr(limiter, "limiter", None)
    if rate_limiter is None:  # _NoOpLimiter (tests / slowapi unavailable)
        return
    if not rate_limiter.hit(_AUTH_ATTEMPT_LIMIT_ITEM, _auth_attempt_ip_key(request)):
        raise ExternalIngestError(
            429, "rate_limited", "Too many authentication attempts"
        )


def _reject_if_already_ip_throttled(request: Request) -> None:
    """Stricter, failure-only signal layered behind the attempt ceiling
    above -- penalizes repeated *wrong credentials* specifically, not just
    request volume. Its own ``test()``-then-``hit()`` gap is no longer a
    DB-load concern (``_reserve_auth_attempt_or_429`` already bounds that
    unconditionally, atomically, before this ever runs); it exists purely
    as an independent signal. Read-only ``test()`` here (never consumes) so
    a request that's about to succeed never pays for this at all -- only
    ``_record_auth_failure_hit`` (called on the failure paths below)
    consumes from the bucket.
    """
    rate_limiter = getattr(limiter, "limiter", None)
    if rate_limiter is None:  # _NoOpLimiter (tests / slowapi unavailable)
        return
    if not rate_limiter.test(_AUTH_FAILURE_LIMIT_ITEM, _auth_failure_ip_key(request)):
        raise ExternalIngestError(
            429, "rate_limited", "Too many failed authentication attempts"
        )


def _record_auth_failure_hit(request: Request) -> None:
    rate_limiter = getattr(limiter, "limiter", None)
    if rate_limiter is None:
        return
    rate_limiter.hit(_AUTH_FAILURE_LIMIT_ITEM, _auth_failure_ip_key(request))


async def _bump_last_used(token_id: uuid.UUID, ip: str | None) -> None:
    """Best-effort, isolated from the request's main session (Design
    Decision 11, docs/architecture/customer-push-authz.md).

    A later exception raised downstream of this dependency (e.g. the route
    handler's own business logic, which may use its own separate DB session)
    must not roll this back -- "was this token used, even for a rejected
    request" must survive regardless of what happens after ``yield``.
    Awaited synchronously rather than fired via ``asyncio.create_task`` so
    callers (including tests) observe the update deterministically instead
    of racing a detached background task in a short-lived event loop.
    """
    try:
        async with get_postgres_session() as session:
            token = await session.get(IngestToken, token_id)
            if token is not None:
                token.last_used_at = datetime.now(timezone.utc)
                token.last_used_ip = ip
            await session.commit()
    except Exception:
        logger.exception("Failed to record ingest token last_used_at (non-fatal)")


async def _customer_push_feature_state(db: AsyncSession, org_id: str) -> str:
    org_uuid = uuid.UUID(org_id)

    def _state(sync_session: Session) -> str:
        return feature_flag_state(
            sync_session,
            org_uuid,
            _CUSTOMER_PUSH_FEATURE,
            min_tier=_CUSTOMER_PUSH_REQUIRED_TIER,
        )

    return await db.run_sync(_state)


def require_ingest_scope(
    scope: str,
    *,
    require_customer_push_feature: bool = True,
):
    """Single dependency factory for CHAOS-2690's data-plane scope checks.

    Bound once per required scope at router import time (see
    ``api/external_ingest/router.py``'s ``_require_schema_read``/
    ``_require_ingest_write``) -- ``Depends()`` matches
    ``app.dependency_overrides`` by the exact bound callable produced here,
    not by this factory, so unit tests override the bound objects directly.
    """

    async def _dep(
        request: Request,
        db: AsyncSession = Depends(get_postgres_session_dep),
    ) -> AsyncGenerator[IngestAuthContext, None]:
        _reserve_auth_attempt_or_429(request)
        _reject_if_already_ip_throttled(request)

        raw = _extract_bearer(request)
        if raw is None or not raw.startswith(TOKEN_PREFIX):
            _record_auth_failure_hit(request)
            await _emit_failure_audit(
                db,
                request,
                org_id=None,
                token_id=None,
                reason="missing_or_malformed_bearer",
            )
            raise ExternalIngestError(
                401, "invalid_token", "Missing or invalid ingest token"
            )

        token_hash = hash_ingest_token(raw)
        result = await db.execute(
            select(IngestToken).where(IngestToken.token_hash == token_hash)
        )
        token = result.scalar_one_or_none()
        if token is None:
            _record_auth_failure_hit(request)
            await _emit_failure_audit(
                db, request, org_id=None, token_id=None, reason="unknown_token"
            )
            raise ExternalIngestError(
                401, "invalid_token", "Missing or invalid ingest token"
            )

        now = datetime.now(timezone.utc)
        if not token.is_valid(now):
            reason = "revoked" if token.revoked_at is not None else "expired"
            _record_auth_failure_hit(request)
            await _emit_failure_audit(
                db,
                request,
                org_id=token.org_id,
                token_id=str(token.id),
                reason=reason,
            )
            raise ExternalIngestError(
                401, "invalid_token", "Missing or invalid ingest token"
            )

        source: IngestSource | None = None
        if token.source_id is not None:
            source = await db.get(IngestSource, token.source_id)

        # Token is confirmed present + valid -- record usage regardless of
        # whether the scope check below ultimately passes (Design Decision 11).
        client_ip = request.client.host if request.client else None
        await _bump_last_used(token.id, client_ip)

        scopes = frozenset(token.scopes or [])
        if scope not in scopes:
            _record_auth_failure_hit(request)
            await _emit_failure_audit(
                db,
                request,
                org_id=token.org_id,
                token_id=str(token.id),
                reason=f"insufficient_scope:{scope}",
            )
            raise ExternalIngestError(
                403,
                "insufficient_scope",
                f"Token is missing required scope: {scope}",
            )

        if require_customer_push_feature:
            feature_state = await _customer_push_feature_state(db, token.org_id)
            if feature_state != "enabled":
                _record_auth_failure_hit(request)
                await _emit_failure_audit(
                    db,
                    request,
                    org_id=token.org_id,
                    token_id=str(token.id),
                    reason=f"feature_not_enabled:{_CUSTOMER_PUSH_FEATURE}",
                )
                raise ExternalIngestError(
                    403,
                    "feature_not_enabled",
                    "Customer push ingest is not enabled for this organization",
                )

        ctx = IngestAuthContext(
            org_id=token.org_id,
            scopes=scopes,
            token_id=str(token.id),
            source=source,
        )
        # Diagnostics + the rate-limit key func (get_ingest_token_key) key on
        # this once it's been validated here -- never on raw bearer text
        # (adversarial-review finding: an unvalidated raw-text hash lets a
        # caller rotate arbitrary strings to mint a fresh limiter bucket
        # every request).
        request.state.ingest_token_id = ctx.token_id
        org_token = set_current_org_id(
            ctx.org_id
        )  # OrgIdMiddleware doesn't do this for an fcpush_ bearer (Decision 9)
        try:
            yield ctx
        finally:
            _current_org_id.reset(org_token)

    return _dep


def require_matching_source(
    ctx: IngestAuthContext,
    system: str,
    instance: str,
    entity_family: str = "legacy",
) -> IngestSource:
    """Enforce that a write request's declared source matches the token's
    bound source (Design Decision 7 / master-spec CC16 ``source_mismatch``).

    ``require_ingest_scope`` resolves before the request body is available,
    so it cannot check the payload's ``source.system``/``source.instance``
    itself -- call this from the route handler, after parsing the envelope
    (adversarial-review finding: without this check, any source-bound
    ``ingest:write`` token could push data for a *different* source
    instance in the same org, since only ``ctx.org_id`` was previously
    checked).

    An org-wide (unbound) token -- ``ctx.source is None`` -- can only ever
    reach here via ``ingest:write`` if the source-binding invariant enforced
    at token-creation time (Design Decision 7: ``ingest:write`` requires a
    non-null ``source_id``) was somehow bypassed; treat that defensively as
    a mismatch, not a pass-through.
    """
    source = ctx.source
    if (
        source is None
        or source.system != system
        or source.instance != instance
        or (source.entity_family or "legacy") != entity_family
    ):
        raise ExternalIngestError(
            403,
            "source_mismatch",
            "Payload source does not match the token's bound source",
        )
    if not source.is_write_eligible():
        raise ExternalIngestError(
            403,
            "source_disabled",
            "Source is disabled or not in customer_push mode",
        )
    return source


__all__ = ["IngestAuthContext", "require_ingest_scope", "require_matching_source"]
