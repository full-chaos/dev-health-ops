"""Interim external-ingest auth dependency (CHAOS-2691 D7).

INTEGRATION-BRANCH-ONLY: this module's body is intentionally permissive once
``EXTERNAL_INGEST_INSECURE_AUTH=1`` is set (any bearer token + any
``X-Org-Id`` is then accepted) because CHAOS-2691's own acceptance criteria
never test 401/403 — only 202/400/413/openapi/tests-for-shapes. Merging the
``chaos-2690-external-ingest`` integration branch to ``main`` is gated on
CHAOS-2712 landing real DB-backed ``IngestToken`` validation.

CHAOS-2696/CHAOS-2712 replace this function's body; the ``IngestAuthContext``
shape and ``Depends(...)`` call sites in ``router.py`` must not change.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from fastapi import Header

from dev_health_ops.api.services.auth import set_current_org_id

from .errors import ExternalIngestError

logger = logging.getLogger(__name__)


@dataclass
class IngestAuthContext:
    org_id: str
    scopes: set[str] = field(default_factory=set)
    token_id: str | None = None  # populated once CHAOS-2696 lands


def require_ingest_scope(required_scope: str):
    async def _dep(
        authorization: str | None = Header(default=None),
        x_org_id: str | None = Header(default=None, alias="X-Org-Id"),
    ) -> IngestAuthContext:
        # Mechanical guard (master-spec CC14): the interim dependency refuses
        # to run unless explicitly enabled for local/test use. Prevents the
        # any-bearer+X-Org-Id path from ever working in a deployed
        # environment (integration branches DO get deployed to shared envs;
        # process gates slip). CHAOS-2712 deletes this flag together with
        # the interim body.
        if os.environ.get("EXTERNAL_INGEST_INSECURE_AUTH") != "1":
            raise ExternalIngestError(
                status_code=503,
                code="auth_not_configured",
                message="external-ingest auth is not configured on this deployment",
            )
        if not authorization or not authorization.lower().startswith("bearer "):
            # Adversarial-review fix: use the external-ingest error envelope
            # (master-spec CC16 explicitly includes auth failures), not a
            # bare HTTPException — customer SDKs must not need special-case
            # parsing for auth errors on a contract that advertises one
            # stable {"error": {...}} shape.
            raise ExternalIngestError(
                status_code=401,
                code="invalid_token",
                message="Missing or malformed bearer token",
            )
        if not x_org_id:
            # missing_org_header is interim-only: the X-Org-Id header itself
            # disappears once CHAOS-2712 derives org_id from a validated
            # token, so this code is not in master-spec CC16's permanent
            # vocabulary.
            raise ExternalIngestError(
                status_code=400,
                code="missing_org_header",
                message="X-Org-Id header required",
            )
        # INTERIM (CHAOS-2691): token value is opaque and NOT validated
        # against a DB-backed IngestToken/scope model yet. CHAOS-2696
        # replaces this function body. Every interim-mode request is logged
        # at WARNING so this is visible in ops before 2696 lands.
        logger.warning(
            "external-ingest interim auth: unvalidated token accepted for org_id=%s",
            x_org_id,
        )
        ctx = IngestAuthContext(
            org_id=x_org_id,
            scopes={"ingest:write", "ingest:status", "schema:read"},
        )
        if required_scope not in ctx.scopes:
            # Dead code in interim mode (all scopes are granted above) — the
            # check is written now so the Depends() seam doesn't change
            # shape once CHAOS-2696 grants real, narrower scopes.
            raise ExternalIngestError(
                status_code=403,
                code="insufficient_scope",
                message=f"Token is missing required scope: {required_scope}",
            )
        set_current_org_id(ctx.org_id)  # keep ClickHouse auto-scoping consistent
        return ctx

    return _dep


__all__ = ["IngestAuthContext", "require_ingest_scope"]
