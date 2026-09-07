"""Startup drift check: are any ``go_api_routing_state`` rows actually live?

The defect this exists to make impossible to repeat: on 2026-09-01 an SDL
change (#2065, ``33b3f3f21d``) moved the canonical schema digest from
``sha256:67b87d38…`` to ``sha256:29d509cd…``. Twelve routing rows had
been seeded hours earlier at the OLD digest, all ``mode=canary``,
``rollout_percentage=100``. Because
``go_api_routing_state``'s primary key includes ``schema_digest``, every
one of them became unreachable the moment that commit landed. The Python
dispatcher fell back to Python on every request, ``PostgresSwitch``
returned ``false`` on every lookup, and both did so **exactly as
designed** -- a missing row is the documented safe default. Nothing
distinguished "correctly serving Python because nothing is enabled" from
"serving Python because the enablement silently died". It went unnoticed
for six days.

Failing closed is right. Failing closed SILENTLY is the bug. This module
is the missing signal, and it deliberately reports the state that is
otherwise indistinguishable from normal: rows exist, but none of them at
the digest this process computes.

Called once per process from :mod:`dev_health_ops.api._lifespan`. It is
best-effort by contract -- a routing table that cannot be read must never
stop the API from starting, since the API serves every request from
Python perfectly well without it. That is why every failure path here
logs and returns rather than raising.
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from .go_api_registry_telemetry import GO_API_ROUTING_DIGEST_DRIFT_TOTAL
from .go_api_routing_admin import count_rows_by_schema_digest
from .go_api_schema_digest import current_schema_digest

logger = logging.getLogger(__name__)

__all__ = ["check_routing_digest_drift"]


async def check_routing_digest_drift(session: AsyncSession) -> str:
    """Compare the routing table's schema digests against the live SDL's.

    Returns the outcome label it counted (``live``/``stale``/``empty``/
    ``error``) so a caller -- and the test suite -- can assert on the
    decision without parsing a log line.
    """
    try:
        live_digest = current_schema_digest()
        counts = await count_rows_by_schema_digest(session)
    except Exception:
        GO_API_ROUTING_DIGEST_DRIFT_TOTAL.labels(result="error").inc()
        logger.exception(
            "go_api_routing.drift_check_failed -- cannot tell whether any "
            "Go-API routing row is reachable; dispatch is unaffected (it "
            "fails closed to Python), but this check's signal is absent"
        )
        return "error"

    if not counts:
        GO_API_ROUTING_DIGEST_DRIFT_TOTAL.labels(result="empty").inc()
        logger.info(
            "go_api_routing.no_rows: go_api_routing_state is empty at "
            "live schema digest %s -- no operation is enabled for Go, "
            "every request is served by Python (this is the default "
            "posture, not an incident)",
            live_digest,
        )
        return "empty"

    live_count = counts.get(live_digest, 0)
    if live_count > 0:
        GO_API_ROUTING_DIGEST_DRIFT_TOTAL.labels(result="live").inc()
        logger.info(
            "go_api_routing.rows_live: %d row(s) at live schema digest %s "
            "(%d row(s) at other digests)",
            live_count,
            live_digest,
            sum(counts.values()) - live_count,
        )
        return "live"

    GO_API_ROUTING_DIGEST_DRIFT_TOTAL.labels(result="stale").inc()
    stale_summary = ", ".join(
        f"{digest}={count}" for digest, count in sorted(counts.items())
    )
    logger.error(
        "go_api_routing.rows_stale: %d rows at %s, 0 at %s -- every Go-API "
        "routing row is keyed to a schema digest this process does not "
        "compute, so NO operation is reachable and every request silently "
        "falls back to Python. The SDL moved after these rows were "
        "written. Re-enable with `dev-hops go-api routing enable` AFTER the "
        "query-api image is rebuilt from this SDL; see the 'When the schema "
        "digest moves' section of "
        "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md",
        sum(counts.values()),
        stale_summary,
        live_digest,
    )
    return "stale"
