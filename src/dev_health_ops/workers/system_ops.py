from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, cast

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


@celery_app.task(
    bind=True,
    queue="ingest",
    name="dev_health_ops.workers.tasks.run_product_telemetry_consumer",
)
def run_product_telemetry_consumer(self, max_iterations: int = 100):
    """Process buffered product telemetry stream entries."""
    from dev_health_ops.api.product_telemetry.consumer import (
        consume_product_telemetry_streams,
    )

    processed = consume_product_telemetry_streams(max_iterations=max_iterations)
    return {"processed": processed}


@celery_app.task(
    bind=True,
    queue="external-ingest",
    name="dev_health_ops.workers.tasks.run_external_ingest_consumer",
)
def run_external_ingest_consumer(self, max_iterations: int = 100):
    """Process buffered external-ingest stream entries (CHAOS-2693)."""
    from dev_health_ops.api.external_ingest.consumer import (
        consume_external_ingest_streams,
    )

    processed = consume_external_ingest_streams(max_iterations=max_iterations)
    return {"processed": processed}


@celery_app.task(
    bind=True,
    queue="monitoring",
    name="dev_health_ops.workers.tasks.external_ingest_stream_health",
)
def external_ingest_stream_health(self) -> dict:
    """Log external-ingest stream depth/lag telemetry (CHAOS-2693 D9)."""
    from dev_health_ops.api.external_ingest.stream_health import report_stream_health

    return report_stream_health()


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
    email_type: str | None = None,
    org_id: str | None = None,
    amount_cents: int = 0,
    currency: str = "usd",
    invoice_url: str = "",
    attempt_count: int = 1,
    old_tier: str = "",
    new_tier: str = "",
    tier: str = "",
    days_remaining: int = 0,
    trial_end_date: str = "",
    durable_notification_id: str | None = None,
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
        days_remaining: Trial days remaining (trial_expiring only)
        trial_end_date: Trial end ISO date (trial_started/trial_expiring only)

    Returns:
        dict with send status
    """
    from dev_health_ops.api.services import billing_emails

    if durable_notification_id:
        durable = _load_billing_notification(durable_notification_id)
        if durable is None:
            return {"status": "dropped", "reason": "missing_durable_notification"}
        email_type, org_id, attributes = durable
        amount_cents = int(cast(Any, attributes.get("amount_cents", 0)))
        currency = str(attributes.get("currency", "usd"))
        invoice_url = str(attributes.get("invoice_url", ""))
        attempt_count = int(cast(Any, attributes.get("attempt_count", 1)))
        old_tier = str(attributes.get("old_tier", ""))
        new_tier = str(attributes.get("new_tier", ""))
        tier = str(attributes.get("tier", ""))
        days_remaining = int(cast(Any, attributes.get("days_remaining", 0)))
        trial_end_date = str(attributes.get("trial_end_date", ""))

    if not email_type or not org_id:
        return {"status": "dropped", "reason": "missing_notification_identity"}

    dispatch = {
        "invoice_receipt": lambda oid: billing_emails.send_invoice_receipt(
            oid, amount_cents, currency, invoice_url
        ),
        "payment_failed": lambda oid: billing_emails.send_payment_failed(
            oid, amount_cents, currency, attempt_count
        ),
        "subscription_changed": lambda oid: billing_emails.send_subscription_changed(
            oid, old_tier, new_tier
        ),
        "subscription_cancelled": lambda oid: (
            billing_emails.send_subscription_cancelled(oid, tier)
        ),
        "trial_started": lambda oid: getattr(billing_emails, "send_trial_started")(
            oid, tier, trial_end_date
        ),
        "trial_expiring": lambda oid: getattr(billing_emails, "send_trial_expiring")(
            oid, tier, days_remaining, trial_end_date
        ),
        "trial_expired": lambda oid: getattr(billing_emails, "send_trial_expired")(
            oid, tier
        ),
    }

    fn = dispatch.get(email_type)
    if not fn:
        logger.error("Unknown billing email type: %s", email_type)
        return {"status": "error", "reason": f"unknown_email_type: {email_type}"}

    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        # A malformed org_id is permanently bad — retrying can never succeed.
        # Seen via Stripe TEST webhooks whose metadata carries fixture ids like
        # "org-abc"; the retry loop just spams the worker.
        logger.error(
            "Billing email %s dropped: org_id=%r is not a UUID (non-retryable)",
            email_type,
            org_id,
        )
        return {"status": "dropped", "reason": "invalid_org_id", "org_id": org_id}

    try:
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


def _load_billing_notification(
    durable_notification_id: str,
) -> tuple[str, str, dict[str, object]] | None:
    try:
        notification_uuid = uuid.UUID(durable_notification_id)
    except ValueError:
        return None
    try:
        from sqlalchemy import select

        from dev_health_ops.db import get_postgres_session_sync
        from dev_health_ops.models.operational_deliveries import BillingNotification

        with get_postgres_session_sync() as session:
            notification = session.scalar(
                select(BillingNotification).where(
                    BillingNotification.id == notification_uuid
                )
            )
            if notification is None:
                return None
            return (
                notification.notification_type,
                str(notification.org_id),
                dict(notification.attributes),
            )
    except Exception:
        logger.exception(
            "Unable to load durable billing notification id=%s", durable_notification_id
        )
        raise


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
            license_key = getattr(org_license, "license_key", None)
            if isinstance(license_key, str) and license_key:
                license_hash = hashlib.sha256(license_key.encode("utf-8")).hexdigest()[
                    :16
                ]

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
