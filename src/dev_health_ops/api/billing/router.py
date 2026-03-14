"""Billing endpoints: Stripe webhooks, checkout, portal, and entitlements."""

from __future__ import annotations

import importlib
import logging
import os
import uuid
from datetime import datetime
from typing import Annotated
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from stripe import SignatureVerificationError
except ModuleNotFoundError:

    class SignatureVerificationError(Exception):
        """Fallback when Stripe SDK is not installed."""


from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing.audit_service import BillingAuditService
from dev_health_ops.api.billing.reconciliation_service import ReconciliationService
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session, postgres_session_dependency
from dev_health_ops.licensing import (
    LicenseTier,
    sign_license,
)
from dev_health_ops.models.billing_audit import BillingAuditLog
from dev_health_ops.workers.system_tasks import send_billing_notification

from .invoice_routes import router as invoice_router
from .invoice_service import InvoiceService
from .plans import router as plans_router
from .refund_routes import router as refund_router
from .refund_service import refund_service
from .stripe_client import (
    get_private_key,
    get_stripe_client,
    get_tier_from_line_items,
    get_tier_price_id,
    get_trial_days,
    get_webhook_secret,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])
router.include_router(plans_router)
router.include_router(invoice_router)
router.include_router(refund_router)
invoice_service = InvoiceService()


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
    id: uuid.UUID
    org_id: uuid.UUID
    actor_id: uuid.UUID | None
    action: str
    resource_type: str
    resource_id: uuid.UUID
    description: str
    stripe_event_id: str | None
    local_state: dict | None
    stripe_state: dict | None
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


# ---------------------------------------------------------------------------
# POST /api/v1/billing/webhooks/stripe
# ---------------------------------------------------------------------------


@router.post("/webhooks/stripe")
async def stripe_webhook(request: Request) -> dict:
    """Verify Stripe signature and handle subscription lifecycle events."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        client = get_stripe_client()
        event = client.construct_event(payload, sig_header, get_webhook_secret())
    except SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")
    except RuntimeError as exc:
        logger.error("Stripe config error: %s", exc)
        raise HTTPException(status_code=500, detail="Billing not configured")

    event_type: str = event.type
    data_object = event.data.object

    event_id = getattr(event, "id", None)

    if event_type.startswith("invoice."):
        if event_type == "invoice.payment_failed" and not _invoice_has_org_id(
            data_object
        ):
            customer_id = getattr(data_object, "customer", None)
            logger.warning("Payment failed: customer=%s", customer_id)
            return {"status": "ok"}
        await _handle_invoice_webhook(event_type, data_object, event_id)
    elif event_type == "checkout.session.completed":
        await _handle_checkout_completed(data_object)
    elif event_type == "customer.subscription.created":
        await _process_subscription_event(event)
    elif event_type == "customer.subscription.updated":
        await _process_subscription_event(event)
        await _handle_subscription_updated(data_object)
    elif event_type == "customer.subscription.deleted":
        await _process_subscription_event(event)
        await _handle_subscription_deleted(data_object)
    elif event_type == "invoice.payment_failed":
        _handle_payment_failed(data_object)
    elif event_type in ("charge.refunded", "charge.refund.updated"):
        async with get_postgres_session() as db:
            await refund_service.process_webhook(db=db, event=event)
    else:
        logger.debug("Unhandled Stripe event: %s", event_type)

    return {"status": "ok"}


def _invoice_has_org_id(invoice_payload: object) -> bool:
    metadata = getattr(invoice_payload, "metadata", {}) or {}
    if isinstance(metadata, dict):
        return bool(metadata.get("org_id"))
    return False


async def _handle_invoice_webhook(
    event_type: str,
    invoice_payload: object,
    event_id: str | None,
) -> None:
    pending_email: tuple | None = None

    async with get_postgres_session() as db:
        if event_id:
            payload = dict(getattr(invoice_payload, "metadata", {}) or {})
            is_duplicate = await invoice_service.is_duplicate_event(
                db=db,
                stripe_event_id=event_id,
                event_type=event_type,
                payload=payload,
            )
            if is_duplicate:
                logger.info("Skipping duplicate Stripe invoice event: %s", event_id)
                await db.rollback()
                return

        try:
            invoice = await invoice_service.upsert_invoice(db, invoice_payload)
        except ValueError as exc:
            logger.warning("Skipping invoice webhook event %s: %s", event_type, exc)
            await db.rollback()
            return

        if event_type in {
            "invoice.created",
            "invoice.updated",
            "invoice.finalized",
            "invoice.payment_failed",
            "invoice.paid",
        }:
            await invoice_service.upsert_line_items(
                db=db,
                invoice_id=invoice.id,
                stripe_lines=getattr(invoice_payload, "lines", None),
            )

        if event_type == "invoice.paid":
            await invoice_service.mark_paid(
                db,
                stripe_invoice_id=invoice.stripe_invoice_id,
                payment_intent=getattr(invoice_payload, "payment_intent", None),
            )
        elif event_type == "invoice.payment_failed":
            invoice.status = "payment_failed"
        elif event_type == "invoice.voided":
            await invoice_service.mark_voided(db, invoice.stripe_invoice_id)

        await db.commit()

        # Collect email dispatch parameters before leaving the DB session.
        metadata = getattr(invoice_payload, "metadata", {}) or {}
        org_id_str = metadata.get("org_id") if isinstance(metadata, dict) else None
        if org_id_str:
            try:
                org_uuid = uuid.UUID(org_id_str)
            except ValueError:
                org_uuid = None

            if org_uuid:
                org_str = str(org_uuid)
                if event_type == "invoice.paid":
                    amount_due = getattr(invoice_payload, "amount_due", 0) or 0
                    currency = getattr(invoice_payload, "currency", "usd") or "usd"
                    invoice_url = (
                        getattr(invoice_payload, "hosted_invoice_url", "") or ""
                    )
                    pending_email = (
                        "invoice_receipt",
                        org_str,
                        {
                            "amount_cents": amount_due,
                            "currency": currency,
                            "invoice_url": invoice_url,
                        },
                    )
                elif event_type == "invoice.payment_failed":
                    amount_due = getattr(invoice_payload, "amount_due", 0) or 0
                    currency = getattr(invoice_payload, "currency", "usd") or "usd"
                    attempt_count = getattr(invoice_payload, "attempt_count", 1) or 1
                    pending_email = (
                        "payment_failed",
                        org_str,
                        {
                            "amount_cents": amount_due,
                            "currency": currency,
                            "attempt_count": attempt_count,
                        },
                    )

    # Dispatch email notification after DB session is closed.
    if pending_email:
        email_type, org_str, kwargs = pending_email
        try:
            send_billing_notification.delay(email_type, org_str, **kwargs)
        except Exception:
            logger.debug(
                "Failed to enqueue %s email for org_id=%s", email_type, org_str
            )


async def _handle_checkout_completed(session: object) -> None:
    org_id = getattr(session, "metadata", {}).get("org_id")
    customer_id = getattr(session, "customer", None)
    if not org_id:
        logger.warning("checkout.session.completed missing org_id in metadata")
        return

    try:
        client = get_stripe_client()
        line_items = client.checkout.sessions.list_line_items(
            getattr(session, "id", "")
        )
        items = [
            {"price": {"id": getattr(item.price, "id", None)}}
            for item in line_items.data
        ]
        tier = get_tier_from_line_items(items)
    except Exception:
        logger.exception("Failed to retrieve line items, defaulting to TEAM")
        tier = LicenseTier.TEAM

    try:
        private_key = get_private_key()
        license_key = sign_license(
            private_key,
            org_id=org_id,
            tier=tier,
        )
        logger.info(
            "License generated: org_id=%s tier=%s customer=%s",
            org_id,
            tier.value,
            customer_id,
        )
    except Exception:
        logger.exception("Failed to sign license for org_id=%s", org_id)
        return

    await _persist_license(org_id, tier, license_key, customer_id)


async def _handle_subscription_updated(subscription: object) -> None:
    customer_id = getattr(subscription, "customer", None)
    items_data = getattr(subscription, "items", None)
    if not items_data:
        return

    items = []
    for item in getattr(items_data, "data", []):
        price = getattr(item, "price", None)
        if price:
            items.append({"price": {"id": getattr(price, "id", None)}})

    tier = get_tier_from_line_items(items)
    metadata = getattr(subscription, "metadata", {})
    org_id = metadata.get("org_id") if isinstance(metadata, dict) else None

    if not org_id:
        logger.info(
            "subscription.updated without org_id metadata, customer=%s", customer_id
        )
        return

    # Read current tier BEFORE persisting new one (for change notification).
    old_tier: str | None = None
    try:
        from sqlalchemy import select

        from dev_health_ops.models.users import Organization

        org_uuid = uuid.UUID(org_id)
        async with get_postgres_session() as db:
            result = await db.execute(
                select(Organization.tier).where(Organization.id == org_uuid)
            )
            row = result.first()
            old_tier = str(row.tier) if row and row.tier else None
    except Exception:
        logger.debug("Could not read old tier for org_id=%s", org_id)

    try:
        private_key = get_private_key()
        license_key = sign_license(private_key, org_id=org_id, tier=tier)
        logger.info(
            "License regenerated on subscription update: org_id=%s tier=%s",
            org_id,
            tier.value,
        )
    except Exception:
        logger.exception("Failed to regenerate license for org_id=%s", org_id)
        return

    await _persist_license(org_id, tier, license_key, customer_id)

    # Send subscription changed email if tier actually changed.
    if old_tier is not None and old_tier != str(tier.value):
        try:
            send_billing_notification.delay(
                "subscription_changed",
                org_id,
                old_tier=old_tier,
                new_tier=str(tier.value),
            )
        except Exception:
            logger.debug(
                "Failed to enqueue subscription changed email for org_id=%s", org_id
            )


async def _handle_subscription_deleted(subscription: object) -> None:
    metadata = getattr(subscription, "metadata", {})
    org_id = metadata.get("org_id") if isinstance(metadata, dict) else None
    customer_id = getattr(subscription, "customer", None)

    if org_id:
        logger.info(
            "Subscription deleted: org_id=%s customer=%s — org reverts to COMMUNITY",
            org_id,
            customer_id,
        )

        # Read current tier before revoking (for cancellation email).
        current_tier = "unknown"
        try:
            from sqlalchemy import select

            from dev_health_ops.models.users import Organization

            org_uuid = uuid.UUID(org_id)
            async with get_postgres_session() as db:
                result = await db.execute(
                    select(Organization.tier).where(Organization.id == org_uuid)
                )
                row = result.first()
                current_tier = str(row.tier) if row and row.tier else "unknown"
        except Exception:
            logger.debug(
                "Could not read tier for org_id=%s before cancellation", org_id
            )

        await _revoke_license(org_id)

        try:
            send_billing_notification.delay(
                "subscription_cancelled",
                org_id,
                tier=current_tier,
            )
        except Exception:
            logger.debug(
                "Failed to enqueue subscription cancelled email for org_id=%s", org_id
            )
    else:
        logger.info(
            "subscription.deleted without org_id metadata, customer=%s", customer_id
        )


def _handle_payment_failed(invoice: object) -> None:
    customer_id = getattr(invoice, "customer", None)
    logger.warning("Payment failed: customer=%s", customer_id)


async def _process_subscription_event(event: object) -> None:
    try:
        subscription_module = importlib.import_module(
            "dev_health_ops.api.billing.subscription_service"
        )
        subscription_service = subscription_module.SubscriptionService

        async with get_postgres_session() as session:
            service = subscription_service(session)
            await service.process_event(event)
    except ValueError as exc:
        logger.warning("Skipping malformed subscription event: %s", exc)
    except RuntimeError:
        logger.exception(
            "Billing service unavailable while processing subscription event"
        )
    except Exception:
        logger.exception("Failed to process subscription event")


# ---------------------------------------------------------------------------
# License persistence helpers
# ---------------------------------------------------------------------------


async def _persist_license(
    org_id: str,
    tier: LicenseTier,
    license_key: str,
    customer_id: str | None,
) -> None:
    """Persist license to OrgLicense and update Organization tier."""
    try:
        from datetime import datetime, timezone

        from sqlalchemy import select

        from dev_health_ops.db import get_postgres_session
        from dev_health_ops.models.licensing import OrgLicense
        from dev_health_ops.models.users import Organization

        async with get_postgres_session() as session:
            import uuid as uuid_mod

            try:
                org_uuid = uuid_mod.UUID(org_id)
            except ValueError:
                logger.warning("Invalid org_id UUID: %s", org_id)
                return

            result = await session.execute(
                select(Organization).where(Organization.id == org_uuid)
            )
            org = result.scalar_one_or_none()
            if org:
                org.tier = str(tier.value)

            result = await session.execute(
                select(OrgLicense).where(OrgLicense.org_id == org_uuid)
            )
            org_license = result.scalar_one_or_none()

            if org_license is None:
                org_license = OrgLicense(
                    org_id=org_uuid,
                    tier=str(tier.value),
                    license_type="saas",
                    license_key=license_key,
                )
                session.add(org_license)
            else:
                org_license.tier = str(tier.value)
                org_license.license_key = license_key

            if customer_id:
                org_license.customer_id = customer_id
            org_license.is_valid = True
            org_license.last_validated_at = datetime.now(timezone.utc)

            await session.commit()

    except Exception:
        logger.exception("Failed to persist license for org_id=%s", org_id)


async def _revoke_license(org_id: str) -> None:
    """Mark org license as invalid (subscription cancelled)."""
    try:
        from sqlalchemy import select

        from dev_health_ops.db import get_postgres_session
        from dev_health_ops.models.licensing import OrgLicense
        from dev_health_ops.models.users import Organization

        async with get_postgres_session() as session:
            import uuid as uuid_mod

            try:
                org_uuid = uuid_mod.UUID(org_id)
            except ValueError:
                return

            result = await session.execute(
                select(Organization).where(Organization.id == org_uuid)
            )
            org = result.scalar_one_or_none()
            if org:
                org.tier = str(LicenseTier.COMMUNITY.value)

            result = await session.execute(
                select(OrgLicense).where(OrgLicense.org_id == org_uuid)
            )
            org_license = result.scalar_one_or_none()
            if org_license:
                org_license.is_valid = False
                org_license.tier = str(LicenseTier.COMMUNITY.value)

            await session.commit()

    except Exception:
        logger.exception("Failed to revoke license for org_id=%s", org_id)


# ---------------------------------------------------------------------------
# POST /api/v1/billing/checkout
# ---------------------------------------------------------------------------


@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout_session(
    body: CheckoutRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
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

    try:
        client = get_stripe_client()
        params: dict[str, object] = {
            "success_url": success_url,
            "cancel_url": cancel_url,
            "line_items": [{"price": price_id, "quantity": 1}],
            "mode": "subscription",
            "metadata": {"org_id": user.org_id},
            "client_reference_id": user.org_id,
        }
        trial_days = get_trial_days(tier_enum)
        if trial_days is not None:
            params["subscription_data"] = {
                "trial_period_days": trial_days,
                "trial_settings": {
                    "end_behavior": {"missing_payment_method": "cancel"}
                },
            }
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
        portal_session = client.billing_portal.sessions.create(
            params={
                "customer": customer_id,
                "return_url": return_url or "/",
            }
        )
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


@router.get("/entitlements/{org_id}", response_model=EntitlementResponse)
async def get_org_entitlements(
    org_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)],
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
    resource_type: str | None = None,
    resource_id: uuid.UUID | None = None,
    action: str | None = None,
    reconciliation_status: str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)] = None,
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)] = None,
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
    org_id: uuid.UUID | None = None,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)] = None,
    db: Annotated[AsyncSession, Depends(postgres_session_dependency)] = None,
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
    return BillingAuditLogResponse(
        id=entry.id,
        org_id=entry.org_id,
        actor_id=entry.actor_id,
        action=entry.action,
        resource_type=entry.resource_type,
        resource_id=entry.resource_id,
        description=entry.description,
        stripe_event_id=entry.stripe_event_id,
        local_state=entry.local_state,
        stripe_state=entry.stripe_state,
        reconciliation_status=entry.reconciliation_status,
        created_at=entry.created_at,
    )
