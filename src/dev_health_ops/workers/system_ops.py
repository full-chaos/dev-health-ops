from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True, queue="ingest", name="dev_health_ops.workers.tasks.run_ingest_consumer"
)
def run_ingest_consumer(self, max_iterations: int = 100):
    """Process buffered ingest stream entries."""
    from dev_health_ops.api.ingest.consumer import consume_streams

    processed = consume_streams(max_iterations=max_iterations)
    return {"processed": processed}


@celery_app.task(bind=True, name="dev_health_ops.workers.tasks.health_check")
def health_check(self) -> dict:
    """Simple health check task to verify worker is running."""
    return {
        "status": "healthy",
        "worker_id": self.request.id,
    }


@celery_app.task(bind=True, max_retries=3, queue="webhooks")
def send_billing_notification(
    self,
    email_type: str,
    org_id: str,
    amount_cents: int = 0,
    currency: str = "usd",
    invoice_url: str = "",
    attempt_count: int = 1,
    old_tier: str = "",
    new_tier: str = "",
    tier: str = "",
) -> dict:
    """Send billing email notification via worker queue.

    Dispatched from billing webhook handlers to decouple email delivery
    from Stripe webhook response time. Retries with exponential backoff
    on transient failures (email service errors, DB connection issues).

    Returns silently (no retry) if org has no owner — that is a data
    condition, not a transient failure.

    Args:
        email_type: One of invoice_receipt, payment_failed,
                    subscription_changed, subscription_cancelled
        org_id: Organization UUID as string
        amount_cents: Invoice amount in cents (invoice emails)
        currency: ISO currency code (invoice emails)
        invoice_url: Hosted invoice URL (invoice_receipt only)
        attempt_count: Payment retry attempt number (payment_failed only)
        old_tier: Previous tier name (subscription_changed only)
        new_tier: New tier name (subscription_changed only)
        tier: Current tier name (subscription_cancelled only)

    Returns:
        dict with send status
    """
    from dev_health_ops.api.services.billing_emails import (
        send_invoice_receipt,
        send_payment_failed,
        send_subscription_cancelled,
        send_subscription_changed,
    )

    dispatch = {
        "invoice_receipt": lambda oid: send_invoice_receipt(
            oid, amount_cents, currency, invoice_url
        ),
        "payment_failed": lambda oid: send_payment_failed(
            oid, amount_cents, currency, attempt_count
        ),
        "subscription_changed": lambda oid: send_subscription_changed(
            oid, old_tier, new_tier
        ),
        "subscription_cancelled": lambda oid: send_subscription_cancelled(oid, tier),
    }

    fn = dispatch.get(email_type)
    if not fn:
        logger.error("Unknown billing email type: %s", email_type)
        return {"status": "error", "reason": f"unknown_email_type: {email_type}"}

    try:
        org_uuid = uuid.UUID(org_id)
        run_async(fn(org_uuid))
        return {"status": "sent", "email_type": email_type, "org_id": org_id}
    except Exception as exc:
        logger.warning(
            "Billing email %s failed for org_id=%s (attempt %d/%d): %s",
            email_type,
            org_id,
            self.request.retries + 1,
            self.max_retries + 1,
            exc,
        )
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))


@celery_app.task(
    bind=True, queue="default", name="dev_health_ops.workers.tasks.phone_home_heartbeat"
)
def phone_home_heartbeat(self) -> dict[str, Any]:
    import hashlib
    import time

    import httpx
    from sqlalchemy import func, select

    from dev_health_ops import __version__
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.audit import AuditAction, AuditLog, AuditResourceType
    from dev_health_ops.models.licensing import OrgLicense
    from dev_health_ops.models.users import Organization, User

    endpoint = os.getenv("TELEMETRY_ENDPOINT")

    org_count = 0
    user_count = 0
    tier = "community"
    license_hash: str | None = None
    org_id_for_audit = None

    with get_postgres_session_sync() as session:
        org_count = int(
            session.execute(select(func.count(Organization.id))).scalar() or 0
        )
        user_count = int(session.execute(select(func.count(User.id))).scalar() or 0)

        first_org = session.execute(
            select(Organization.id).limit(1)
        ).scalar_one_or_none()
        org_id_for_audit = first_org

        org_license = session.execute(select(OrgLicense).limit(1)).scalar_one_or_none()
        if org_license is not None:
            tier = str(org_license.tier or "community")
            if org_license.license_key:
                license_hash = hashlib.sha256(
                    org_license.license_key.encode("utf-8")
                ).hexdigest()[:16]

        payload = {
            "instance_id": os.getenv("INSTANCE_ID", "unknown"),
            "version": __version__,
            "org_count": org_count,
            "user_count": user_count,
            "tier": tier,
            "license_hash": license_hash,
            "uptime_seconds": time.monotonic(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if org_id_for_audit is not None:
            session.add(
                AuditLog(
                    org_id=org_id_for_audit,
                    action=AuditAction.OTHER.value,
                    resource_type=AuditResourceType.OTHER.value,
                    resource_id="phone_home_heartbeat",
                    description="Background phone-home heartbeat recorded",
                    changes=payload,
                    request_metadata={"source": "celery", "endpoint": endpoint},
                )
            )
            session.flush()

    if endpoint:
        try:
            resp = httpx.post(endpoint, json=payload, timeout=10.0)
            logger.info("Phone-home heartbeat sent: status=%d", resp.status_code)
        except Exception as exc:
            logger.warning("Phone-home heartbeat failed: %s", exc)
    else:
        logger.debug("No TELEMETRY_ENDPOINT configured, recorded heartbeat locally")

    return {"status": "ok", "endpoint_configured": bool(endpoint), "payload": payload}
