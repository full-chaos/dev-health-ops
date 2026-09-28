"""Billing endpoints: Stripe webhooks, checkout, portal, and entitlements."""

from __future__ import annotations

import importlib
import logging
import os
import uuid
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from stripe.params.billing_portal._session_create_params import (
    SessionCreateParams as PortalSessionCreateParams,
)
from stripe.params.checkout._session_create_params import (
    SessionCreateParams as CheckoutSessionCreateParams,
)

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing.audit_service import BillingAuditService
from dev_health_ops.api.billing.reconciliation_service import ReconciliationService
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency
from dev_health_ops.licensing import LicenseTier
from dev_health_ops.models.billing_audit import BillingAuditLog

from .invoice_routes import router as invoice_router
from .plans import router as plans_router
from .refund_routes import router as refund_router
from .stripe_client import (
    get_stripe_client,
    get_tier_price_id,
    get_trial_days,
)
from .subscription_service import has_had_trial

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])
router.include_router(plans_router)
router.include_router(invoice_router)
router.include_router(refund_router)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class CheckoutRequest(BaseModel):
    tier: str
    success_url: str
    cancel_url: str


class CheckoutResponse(BaseModel):
    session_id: str
    url: str


class PortalResponse(BaseModel):
    url: str


class EntitlementResponse(BaseModel):
    tier: str
    features: dict[str, bool]
    limits: dict[str, int]
    is_licensed: bool
    in_grace_period: bool
    is_trialing: bool = False
    trial_ends_at: str | None = None


class BillingAuditLogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    org_id: uuid.UUID
    actor_id: uuid.UUID | None
    action: str
    resource_type: str
    resource_id: uuid.UUID
    description: str
    stripe_event_id: str | None
    local_state: dict[str, Any] | None
    stripe_state: dict[str, Any] | None
    reconciliation_status: str | None
    created_at: datetime | None


class BillingAuditListResponse(BaseModel):
    items: list[BillingAuditLogResponse]
    total: int
    limit: int
    offset: int


class ResolveMismatchRequest(BaseModel):
    resolution: str


def _validate_checkout_url(url: str) -> str:
    if url.startswith("/"):
        return url

    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Invalid checkout URL")

    app_base_url = os.environ.get("APP_BASE_URL", "https://example.com").strip()
    allowed_prefixes: list[str] = []
    if app_base_url:
        allowed_prefixes.append(app_base_url.rstrip("/"))
    allowed_prefixes.extend(
        prefix.strip()
        for prefix in os.environ.get("ALLOWED_CHECKOUT_DOMAINS", "").split(",")
        if prefix.strip()
    )
    if any(prefix and url.startswith(prefix) for prefix in allowed_prefixes):
        return url

    raise HTTPException(
        status_code=400,
        detail="Invalid checkout URL: must be relative or start with an allowed prefix",
    )


async def _maybe_strip_trial(
    trial_days: int | None,
    org_id: str,
    actor_id: str | None,
    session: AsyncSession,
) -> int | None:
    if trial_days is None:
        return None

    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        logger.warning("Trial guard skipped: invalid org_id=%s", org_id)
        return trial_days

    try:
        already_trialed = await has_had_trial(org_uuid, session)
    except Exception:
        logger.exception("Trial guard failed for org_id=%s", org_id)
        return trial_days

    if not already_trialed:
        return trial_days

    logger.info(
        "Trial abuse prevented: org %s already had a trial; stripping trial_period_days",
        org_id,
    )

    actor_uuid: uuid.UUID | None = None
    if actor_id:
        try:
            actor_uuid = uuid.UUID(actor_id)
        except ValueError:
            logger.debug("Skipping actor_id on trial abuse audit: %s", actor_id)

    await BillingAuditService(session).log(
        org_id=org_uuid,
        actor_id=actor_uuid,
        action="trial_abuse_prevented",
        resource_type="checkout",
        resource_id=org_uuid,
        description=("Org already had a trial, creating checkout without trial period"),
        local_state={"requested_trial_days": trial_days},
    )
    return None


def _resolve_trial_days(tier: LicenseTier) -> int | None:
    return get_trial_days(tier)


# ---------------------------------------------------------------------------
# POST /api/v1/billing/checkout
# ---------------------------------------------------------------------------


@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout_session(
    body: CheckoutRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(postgres_session_dependency)],
) -> CheckoutResponse:
    """Create a Stripe Checkout session for the authenticated user's org."""
    try:
        tier_enum = LicenseTier(body.tier.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {body.tier}")

    price_id = get_tier_price_id(tier_enum)
    if not price_id:
        raise HTTPException(
            status_code=400, detail=f"No price configured for tier: {body.tier}"
        )

    success_url = _validate_checkout_url(body.success_url)
    cancel_url = _validate_checkout_url(body.cancel_url)
    trial_days = await _maybe_strip_trial(
        trial_days=_resolve_trial_days(tier_enum),
        org_id=user.org_id,
        actor_id=user.user_id,
        session=session,
    )

    params: CheckoutSessionCreateParams = {
        "success_url": success_url,
        "cancel_url": cancel_url,
        "line_items": [{"price": price_id, "quantity": 1}],
        "mode": "subscription",
        "metadata": {"org_id": user.org_id},
        "client_reference_id": user.org_id,
    }
    if trial_days is not None:
        params["subscription_data"] = {
            "trial_period_days": trial_days,
            "trial_settings": {"end_behavior": {"missing_payment_method": "cancel"}},
        }

    try:
        client = get_stripe_client()
        checkout_session = client.checkout.sessions.create(params=params)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception:
        logger.exception("Failed to create Stripe checkout session")
        raise HTTPException(status_code=502, detail="Failed to create checkout session")

    return CheckoutResponse(
        session_id=checkout_session.id,
        url=checkout_session.url or "",
    )


# ---------------------------------------------------------------------------
# POST /api/v1/billing/portal
# ---------------------------------------------------------------------------


@router.post("/portal", response_model=PortalResponse)
async def create_portal_session(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    return_url: str | None = None,
) -> PortalResponse:
    """Create a Stripe Billing Portal session for the authenticated user's org."""
    customer_id = await _get_customer_id(user.org_id)
    if not customer_id:
        raise HTTPException(
            status_code=404, detail="No billing account found for this organization"
        )

    try:
        client = get_stripe_client()
        portal_params: PortalSessionCreateParams = {
            "customer": customer_id,
            "return_url": return_url or "/",
        }
        portal_session = client.billing_portal.sessions.create(params=portal_params)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception:
        logger.exception("Failed to create Stripe portal session")
        raise HTTPException(status_code=502, detail="Failed to create portal session")

    return PortalResponse(url=portal_session.url or "")


async def _get_customer_id(org_id: str) -> str | None:
    """Look up the Stripe customer ID from OrgLicense for the given org."""
    try:
        from sqlalchemy import select

        from dev_health_ops.db import get_postgres_session
        from dev_health_ops.models.licensing import OrgLicense

        async with get_postgres_session() as session:
            import uuid as uuid_mod

            try:
                org_uuid = uuid_mod.UUID(org_id)
            except ValueError:
                return None

            result = await session.execute(
                select(OrgLicense.customer_id).where(OrgLicense.org_id == org_uuid)
            )
            row = result.scalar_one_or_none()
            return row if isinstance(row, str) else None

    except Exception:
        logger.exception("Failed to look up customer_id for org_id=%s", org_id)
        return None


# ---------------------------------------------------------------------------
# GET /api/v1/billing/entitlements/{org_id}
# ---------------------------------------------------------------------------


async def require_billing_entitlement_access(
    org_id: uuid.UUID,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
) -> None:

    from dev_health_ops.models.users import Membership, User

    try:
        user_id = uuid.UUID(user.user_id)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access forbidden") from None
    current_user = await db.get(User, user_id)
    if current_user is None or not current_user.is_active:
        raise HTTPException(status_code=403, detail="Access forbidden")
    if current_user.is_superuser:
        return
    if user.org_id != str(org_id):
        raise HTTPException(status_code=403, detail="Access forbidden")
    membership = await db.execute(
        select(Membership.id).where(
            Membership.user_id == user_id,
            Membership.org_id == org_id,
        )
    )
    if membership.scalar_one_or_none() is None:
        raise HTTPException(status_code=403, detail="Access forbidden")


@router.get("/entitlements/{org_id}", response_model=EntitlementResponse)
async def get_org_entitlements(
    org_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
    _: Annotated[None, Depends(require_billing_entitlement_access)],
) -> EntitlementResponse:
    gating = importlib.import_module("dev_health_ops.licensing.gating")
    entitlements = await gating.get_org_entitlements_from_db(org_id=org_id, session=db)
    return EntitlementResponse(**entitlements)


router.include_router(
    importlib.import_module("dev_health_ops.api.billing.subscriptions").router
)


@router.get("/audit", response_model=BillingAuditListResponse)
async def list_billing_audit(
    org_id: uuid.UUID,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
    resource_type: str | None = None,
    resource_id: uuid.UUID | None = None,
    action: str | None = None,
    reconciliation_status: str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> BillingAuditListResponse:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superadmin access required")

    svc = BillingAuditService(db)
    items, total = await svc.query(
        org_id=org_id,
        resource_type=resource_type,
        resource_id=resource_id,
        action=action,
        reconciliation_status=reconciliation_status,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        offset=offset,
    )
    return BillingAuditListResponse(
        items=[_to_audit_response(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/audit/{audit_id}", response_model=BillingAuditLogResponse)
async def get_billing_audit(
    audit_id: uuid.UUID,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
) -> BillingAuditLogResponse:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superadmin access required")

    entry = await db.get(BillingAuditLog, audit_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Audit entry not found")
    return _to_audit_response(entry)


@router.post("/audit/{audit_id}/resolve", response_model=BillingAuditLogResponse)
async def resolve_billing_mismatch(
    audit_id: uuid.UUID,
    payload: ResolveMismatchRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
) -> BillingAuditLogResponse:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superadmin access required")

    audit_service = BillingAuditService(db)
    reconciliation_service = ReconciliationService(
        db, get_stripe_client(), audit_service
    )
    resolved = await reconciliation_service.resolve_mismatch(
        audit_log_id=audit_id,
        resolution=payload.resolution,
        actor_id=uuid.UUID(user.user_id),
    )
    if resolved is None:
        raise HTTPException(status_code=404, detail="Audit entry not found")
    return _to_audit_response(resolved)


@router.post("/reconcile")
async def trigger_reconciliation(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
    org_id: uuid.UUID | None = None,
) -> dict:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superadmin access required")

    audit_service = BillingAuditService(db)
    reconciliation_service = ReconciliationService(
        db, get_stripe_client(), audit_service
    )
    report = await reconciliation_service.reconcile_all(org_id=org_id)
    return report.to_dict()


def _to_audit_response(entry: BillingAuditLog) -> BillingAuditLogResponse:
    return BillingAuditLogResponse.model_validate(entry, from_attributes=True)
