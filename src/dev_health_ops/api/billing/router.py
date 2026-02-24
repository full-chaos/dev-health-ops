"""Billing endpoints: Stripe webhooks, checkout, portal, and entitlements."""

from __future__ import annotations

import importlib
import logging
import os
from typing import Annotated
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

try:
    from stripe import SignatureVerificationError
except ModuleNotFoundError:

    class SignatureVerificationError(Exception):
        """Fallback when Stripe SDK is not installed."""


from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing import (
    LicenseTier,
    get_entitlements,
    sign_license,
)

from .stripe_client import (
    get_private_key,
    get_stripe_client,
    get_tier_from_line_items,
    get_tier_price_id,
    get_webhook_secret,
)
from .invoice_routes import router as invoice_router
from .invoice_service import InvoiceService
from .refund_routes import router as refund_router
from .refund_service import refund_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])
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
        await _revoke_license(org_id)
    else:
        logger.info(
            "subscription.deleted without org_id metadata, customer=%s", customer_id
        )


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
        checkout_session = client.checkout.sessions.create(
            params={
                "success_url": success_url,
                "cancel_url": cancel_url,
                "line_items": [{"price": price_id, "quantity": 1}],
                "mode": "subscription",
                "metadata": {"org_id": user.org_id},
                "client_reference_id": user.org_id,
            }
        )
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
async def get_org_entitlements(org_id: str) -> EntitlementResponse:
    """Return entitlements for the given org from the JWT-backed LicenseManager."""
    entitlements = get_entitlements()
    return EntitlementResponse(**entitlements)


router.include_router(refund_router)
