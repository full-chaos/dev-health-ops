from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.system_ops_metrics import (
    BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL,
)

logger = logging.getLogger(__name__)


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
    idempotency_key: str | None = None,
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
        durable_notification_id: Row id in ``billing_notifications``; when set,
                    all other fields are loaded from that row and this call
                    is subject to the completion fence below (CHAOS-3952).
        idempotency_key: The bridge's own copy of the durable row's
                    idempotency key. Optional (older callers omit it), but
                    when present it must match the row's stored key —
                    a mismatch means the two sides disagree about identity
                    and is never safe to act on.

    Returns:
        dict with send status
    """
    from dev_health_ops.api.services import billing_emails

    if durable_notification_id:
        durable = _load_billing_notification(durable_notification_id)
        if durable is None:
            return {"status": "dropped", "reason": "missing_durable_notification"}
        email_type, org_id, attributes, durable_idempotency_key, completed_at = durable
        if idempotency_key is not None and idempotency_key != durable_idempotency_key:
            logger.error("Billing notification dropped: idempotency key mismatch")
            BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                outcome="key_mismatch"
            ).inc()
            return {"status": "dropped", "reason": "idempotency_key_mismatch"}
        if completed_at is not None:
            # CHAOS-3952: the durable row already recorded a completed send.
            # This call is a retry that replayed the same row unchanged (the
            # HTTP response back to Go was lost after Python already sent) —
            # skip the effect entirely rather than sending the email again.
            BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                outcome="duplicate_suppressed"
            ).inc()
            return {
                "status": "sent",
                "email_type": email_type,
                "org_id": org_id,
                "already_sent": True,
            }
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
        # email_type comes from the task payload and may contain customer-supplied
        # data. Keep the operational signal without sending it to application logs.
        logger.error("Billing notification dropped: unsupported email type")
        return {"status": "error", "reason": f"unknown_email_type: {email_type}"}

    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        # A malformed org_id is permanently bad — retrying can never succeed.
        # Seen via Stripe TEST webhooks whose metadata carries fixture ids like
        # "org-abc"; the retry loop just spams the worker.
        logger.error("Billing notification dropped: invalid organization identifier")
        return {"status": "dropped", "reason": "invalid_org_id", "org_id": org_id}

    try:
        run_async(fn(org_uuid))
        if durable_notification_id:
            # Record the fence only after the send call has returned
            # successfully — never before — so a crash mid-send still
            # allows a legitimate retry to send.
            _mark_billing_notification_completed(durable_notification_id)
            BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(outcome="sent").inc()
        return {"status": "sent", "email_type": email_type, "org_id": org_id}
    except Exception as exc:
        logger.warning(
            "Billing notification delivery failed (attempt %d/%d)",
            self.request.retries + 1,
            self.max_retries + 1,
        )
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))


def _load_billing_notification(
    durable_notification_id: str,
) -> tuple[str, str, dict[str, object], str, datetime | None] | None:
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
                notification.idempotency_key,
                notification.completed_at,
            )
    except Exception:
        logger.error("Unable to load durable billing notification")
        raise


def _mark_billing_notification_completed(durable_notification_id: str) -> None:
    """Set the CHAOS-3952 completion fence for one durable billing row.

    A single ``UPDATE ... WHERE completed_at IS NULL`` is the atomicity
    boundary: PostgreSQL serializes concurrent updates to the same row, so
    whichever caller's UPDATE commits first is the one that actually claims
    the fence — a second, truly concurrent caller matches zero rows rather
    than racing a read-then-write.
    """
    try:
        notification_uuid = uuid.UUID(durable_notification_id)
    except ValueError:
        return
    from sqlalchemy import update

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.operational_deliveries import BillingNotification

    with get_postgres_session_sync() as session:
        # get_postgres_session_sync commits on clean exit from this block.
        session.execute(
            update(BillingNotification)
            .where(
                BillingNotification.id == notification_uuid,
                BillingNotification.completed_at.is_(None),
            )
            .values(completed_at=datetime.now(timezone.utc))
        )


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
