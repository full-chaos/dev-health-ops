from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="dev_health_ops.workers.tasks.health_check")
def health_check(self) -> dict:
    """Simple health check task to verify worker is running."""
    return {
        "status": "healthy",
        "worker_id": self.request.id,
    }


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
