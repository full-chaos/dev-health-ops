from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.system_ops_metrics import (
    BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL,
)

logger = logging.getLogger(__name__)


class _PermanentBillingDrop(Exception):
    """Signals a permanently-invalid billing notification, post-claim.

    Codex round 3 (P1 x2, both EXECUTED) found that everything between a
    successful claim and the guarded send call — attribute coercion,
    identity checks, email-type lookup, org_id parsing — was UNGUARDED: an
    exception or an early ``return`` there left the claim held with no
    release, and a Go retry hitting that still-fresh, unresolved claim
    reported `already_sent: true` for an email that was never attempted.
    Every one of those conditions is data-permanent (retrying does not
    change a malformed stored value), so they all funnel through this one
    exception, caught once, released once, reported once — not repeated
    per call site the way the un-guarded returns were.
    """

    def __init__(self, reason: str, **extra: object) -> None:
        super().__init__(reason)
        self.reason = reason
        self.extra = extra


# CHAOS-3952: a claim older than this with no completion is reported as
# stale rather than silently treated as an ordinary in-flight duplicate.
# River's own backoff for this task tops out well under this window (three
# retries at 30/60/120s), so a claim still unclaimed-but-uncompleted past it
# is far more likely a crashed/stuck attempt than a slow one. No reaper /
# auto-resend acts on this in this PR — it is surfaced for an operator.
_STALE_CLAIM_THRESHOLD_SECONDS = 900


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

    claimed_durable_notification_id: str | None = None
    if durable_notification_id:
        durable = _load_billing_notification(durable_notification_id)
        if durable is None:
            return {"status": "dropped", "reason": "missing_durable_notification"}
        email_type, org_id, attributes, durable_idempotency_key = durable
        if idempotency_key is not None and idempotency_key != durable_idempotency_key:
            logger.error("Billing notification dropped: idempotency key mismatch")
            BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                outcome="key_mismatch"
            ).inc()
            return {"status": "dropped", "reason": "idempotency_key_mismatch"}
        # CHAOS-3952: claim the fence BEFORE sending, not after. A read-then
        # -send-then-write ordering (check completed_at, send, mark complete)
        # leaves two open windows: two concurrent attempts can both observe
        # completed_at=NULL and both send (codex round 1, P2, executed), and a
        # process crash between a successful send and the write leaves the
        # fence unset for a retry to duplicate (codex round 1, P1, executed).
        # Claiming first via a single atomic `UPDATE ... WHERE claimed_at IS
        # NULL` makes the DECISION itself atomic — its rowcount, not a
        # separate read, says who won — closing both windows.
        #
        # The residual risk moves rather than disappears: a crash strictly
        # between the claim committing and the send call starting now means
        # the email is SKIPPED, not duplicated — the except branch below
        # releases the claim on every raised exception so an ordinary send
        # failure still retries; only an unhandled process death in that
        # narrow window is unrecoverable by a normal retry. For a
        # customer-visible billing email this is the intended trade (a rare
        # silent miss over a rare duplicate) and matches the ticket's
        # "checked before send" wording — but "intended" does not mean
        # invisible: claimed_at and completed_at are separate columns
        # specifically so a claim that never completed is a queryable,
        # observable state, not indistinguishable from a real send. This PR
        # surfaces it (below, `stale_claim_detected`) rather than acting on
        # it — no reaper/auto-resend here.
        claim = _claim_billing_notification_completion(durable_notification_id)
        if not claim.claimed:
            if claim.completed_at is not None:
                BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                    outcome="duplicate_suppressed"
                ).inc()
                return {
                    "status": "sent",
                    "email_type": email_type,
                    "org_id": org_id,
                    "already_sent": True,
                }
            stale = (
                claim.claimed_at is not None
                and (datetime.now(timezone.utc) - claim.claimed_at).total_seconds()
                > _STALE_CLAIM_THRESHOLD_SECONDS
            )
            if stale:
                # A claim this old with no completion did not merely lose a
                # concurrent race (that resolves in seconds) — the attempt
                # that made it almost certainly crashed before sending.
                # Report it as its own distinct, non-"sent" outcome rather
                # than masquerading as a duplicate: we do NOT know whether
                # the email went out.
                logger.error(
                    "Billing notification claim is stale: claimed but never completed"
                )
                BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                    outcome="stale_claim_detected"
                ).inc()
                return {
                    "status": "error",
                    "reason": "stale_claim",
                    "email_type": email_type,
                    "org_id": org_id,
                }
            # Within the normal in-flight/backoff window: treat as an
            # ordinary duplicate suppression. If the other attempt is itself
            # stuck, a later retry's claim will find the same claimed_at and
            # cross the staleness threshold above instead.
            BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                outcome="duplicate_suppressed"
            ).inc()
            return {
                "status": "sent",
                "email_type": email_type,
                "org_id": org_id,
                "already_sent": True,
            }
        claimed_durable_notification_id = durable_notification_id

    # CHAOS-3952, codex round 3 (P1 x2, both EXECUTED): everything from here
    # through the send attempt runs ONLY after a claim may already be held
    # (`claimed_durable_notification_id` set). Every exit from this block —
    # exception or deliberate drop — must release that claim before
    # returning/propagating, or a Go retry hitting the still-fresh,
    # unresolved claim reports `already_sent: true` for an email that was
    # never attempted. One try/except covers the whole block instead of
    # guarding each call site individually, which is exactly what round 3
    # found un-guarded (attribute coercion, identity checks, email-type
    # lookup, org_id parsing all returned/raised directly).
    try:
        if durable_notification_id:
            # A stored value's shape does not change between retries — a
            # malformed one is a permanent-drop condition (codex round 3,
            # P1, EXECUTED), same family as an invalid org_id below, not a
            # transient failure worth self.retry()'s backoff.
            try:
                amount_cents = int(cast(Any, attributes.get("amount_cents", 0)))
                currency = str(attributes.get("currency", "usd"))
                invoice_url = str(attributes.get("invoice_url", ""))
                attempt_count = int(cast(Any, attributes.get("attempt_count", 1)))
                old_tier = str(attributes.get("old_tier", ""))
                new_tier = str(attributes.get("new_tier", ""))
                tier = str(attributes.get("tier", ""))
                days_remaining = int(cast(Any, attributes.get("days_remaining", 0)))
                trial_end_date = str(attributes.get("trial_end_date", ""))
            except (TypeError, ValueError) as exc:
                raise _PermanentBillingDrop(
                    "malformed_notification_attributes"
                ) from exc

        if not email_type or not org_id:
            raise _PermanentBillingDrop("missing_notification_identity")

        dispatch = {
            "invoice_receipt": lambda oid: billing_emails.send_invoice_receipt(
                oid, amount_cents, currency, invoice_url
            ),
            "payment_failed": lambda oid: billing_emails.send_payment_failed(
                oid, amount_cents, currency, attempt_count
            ),
            "subscription_changed": (
                lambda oid: billing_emails.send_subscription_changed(
                    oid, old_tier, new_tier
                )
            ),
            "subscription_cancelled": lambda oid: (
                billing_emails.send_subscription_cancelled(oid, tier)
            ),
            "trial_started": lambda oid: getattr(billing_emails, "send_trial_started")(
                oid, tier, trial_end_date
            ),
            "trial_expiring": lambda oid: getattr(
                billing_emails, "send_trial_expiring"
            )(oid, tier, days_remaining, trial_end_date),
            "trial_expired": lambda oid: getattr(billing_emails, "send_trial_expired")(
                oid, tier
            ),
        }

        fn = dispatch.get(email_type)
        if not fn:
            # email_type comes from the task payload and may contain
            # customer-supplied data. Keep the operational signal without
            # sending it to application logs.
            raise _PermanentBillingDrop("unknown_email_type") from None

        try:
            org_uuid = uuid.UUID(org_id)
        except ValueError:
            # A malformed org_id is permanently bad — retrying can never
            # succeed. Seen via Stripe TEST webhooks whose metadata carries
            # fixture ids like "org-abc"; the retry loop just spams the
            # worker.
            raise _PermanentBillingDrop("invalid_org_id", org_id=org_id) from None

        run_async(fn(org_uuid))
    except _PermanentBillingDrop as drop:
        if claimed_durable_notification_id:
            _release_billing_notification_completion_claim(
                claimed_durable_notification_id
            )
        if drop.reason == "missing_notification_identity":
            return {"status": "dropped", "reason": drop.reason}
        if drop.reason == "unknown_email_type":
            logger.error("Billing notification dropped: unsupported email type")
            return {"status": "error", "reason": f"unknown_email_type: {email_type}"}
        if drop.reason == "invalid_org_id":
            logger.error(
                "Billing notification dropped: invalid organization identifier"
            )
            return {
                "status": "dropped",
                "reason": "invalid_org_id",
                "org_id": drop.extra.get("org_id"),
            }
        logger.error("Billing notification dropped: durable row attributes malformed")
        return {"status": "dropped", "reason": "malformed_notification_attributes"}
    except Exception as exc:
        if claimed_durable_notification_id:
            # The claim above already reserved this row before the send was
            # attempted; the SEND ITSELF failed here, so nothing went out —
            # release the claim, or the fence would permanently skip a
            # delivery that never actually happened.
            _release_billing_notification_completion_claim(
                claimed_durable_notification_id
            )
        logger.warning(
            "Billing notification delivery failed (attempt %d/%d)",
            self.request.retries + 1,
            self.max_retries + 1,
        )
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))

    # The send succeeded — the email is out and self.retry() must never run
    # again for this notification (retrying now would duplicate it).
    # Recording completion is bookkeeping ON TOP of that fact, not a gate on
    # it: a failure writing `completed_at` here (codex round 2, P1, EXECUTED
    # — a broad except around both the send and this write let a transient
    # DB error on the write alone release the claim and duplicate the
    # email on retry) must NOT release the claim and must NOT retry the
    # task. The claim stays held with completed_at unset — exactly the
    # "stale claim" state a later contention already classifies and
    # surfaces (`stale_claim_detected`), which is the correct outcome here:
    # the email went out, only the bookkeeping is stuck.
    if claimed_durable_notification_id:
        try:
            _mark_billing_notification_completed(claimed_durable_notification_id)
        except Exception:
            logger.error(
                "Billing notification sent but its completion fence write failed"
            )
            BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(
                outcome="sent_fence_write_failed"
            ).inc()
            return {"status": "sent", "email_type": email_type, "org_id": org_id}
        BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL.labels(outcome="sent").inc()
    return {"status": "sent", "email_type": email_type, "org_id": org_id}


def _load_billing_notification(
    durable_notification_id: str,
) -> tuple[str, str, dict[str, object], str] | None:
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
            )
    except Exception:
        logger.error("Unable to load durable billing notification")
        raise


@dataclass(frozen=True, slots=True)
class _ClaimResult:
    """Outcome of one `_claim_billing_notification_completion` call.

    ``claimed`` True means this call won and must send the email. Otherwise
    ``completed_at``/``claimed_at`` describe the row AS OF THE FAILED CLAIM,
    so the caller can tell a genuine prior success (`completed_at` set) from
    an unresolved claim (`claimed_at` set, `completed_at` still NULL) —
    which the caller further classifies as normal-in-flight or stale by age.
    """

    claimed: bool
    claimed_at: datetime | None = None
    completed_at: datetime | None = None


def _claim_billing_notification_completion(
    durable_notification_id: str,
) -> _ClaimResult:
    """Atomically claim the CHAOS-3952 dedup fence for one durable row.

    A single ``UPDATE ... WHERE claimed_at IS NULL`` is both the decision
    and the write: PostgreSQL serializes concurrent updates to the same row,
    so exactly one concurrent caller's UPDATE matches (rowcount 1) and every
    other one matches zero rows — there is no separate read to race against.
    On a lost claim, one follow-up read reports the row's current
    claimed_at/completed_at so the caller can classify why.
    """
    try:
        notification_uuid = uuid.UUID(durable_notification_id)
    except ValueError:
        return _ClaimResult(claimed=False)
    from sqlalchemy import select, update
    from sqlalchemy.engine import CursorResult

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.operational_deliveries import BillingNotification

    with get_postgres_session_sync() as session:
        # get_postgres_session_sync commits on clean exit from this block.
        result = cast(
            CursorResult,
            session.execute(
                update(BillingNotification)
                .where(
                    BillingNotification.id == notification_uuid,
                    BillingNotification.claimed_at.is_(None),
                )
                .values(claimed_at=datetime.now(timezone.utc))
            ),
        )
        if result.rowcount == 1:
            return _ClaimResult(claimed=True)
        row = session.execute(
            select(
                BillingNotification.claimed_at, BillingNotification.completed_at
            ).where(BillingNotification.id == notification_uuid)
        ).one_or_none()
        if row is None:
            return _ClaimResult(claimed=False)
        return _ClaimResult(
            claimed=False, claimed_at=row.claimed_at, completed_at=row.completed_at
        )


def _mark_billing_notification_completed(durable_notification_id: str) -> None:
    """Record that the send this attempt claimed actually went out.

    Only called after `run_async(fn(org_uuid))` has returned without
    raising. Separate from the claim (see `BillingNotification.claimed_at`
    vs `completed_at`) so a claim with no matching completion is a queryable
    fact, not an assumption.
    """
    try:
        notification_uuid = uuid.UUID(durable_notification_id)
    except ValueError:
        return
    from sqlalchemy import update

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.operational_deliveries import BillingNotification

    with get_postgres_session_sync() as session:
        session.execute(
            update(BillingNotification)
            .where(BillingNotification.id == notification_uuid)
            .values(completed_at=datetime.now(timezone.utc))
        )


def _release_billing_notification_completion_claim(
    durable_notification_id: str,
) -> None:
    """Undo a claim this attempt made but never delivered on.

    Called from every exception/permanent-drop exit after a claim is held
    (the ordinary self.retry() path, and the permanent-drop paths added in
    codex round 3) — never after a successful send. Restoring
    ``claimed_at`` to NULL lets the next attempt claim and actually send;
    without this, a failure here would claim the fence once and then
    silently never send at all.

    NEVER RAISES (codex round 3, P1, EXECUTED): this call sits inside an
    already-caught exception's handler. If the release write itself failed
    and were allowed to propagate, it would REPLACE the original exception
    — silently skipping self.retry() and any drop reporting, and leaving
    the caller to believe cleanup happened when it did not. A failure here
    is therefore logged and swallowed; the claim is left held, which the
    existing stale-claim detection surfaces on a later contending attempt
    (the same "observable, not silently lost" outcome the claimed_at/
    completed_at split exists for).
    """
    try:
        notification_uuid = uuid.UUID(durable_notification_id)
    except ValueError:
        return
    from sqlalchemy import update

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.operational_deliveries import BillingNotification

    try:
        with get_postgres_session_sync() as session:
            session.execute(
                update(BillingNotification)
                .where(BillingNotification.id == notification_uuid)
                .values(claimed_at=None)
            )
    except Exception:
        logger.error(
            "Billing notification claim release failed; claim stays held "
            "(will surface as a stale claim on a later attempt)"
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
