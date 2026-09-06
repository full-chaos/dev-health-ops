"""Promote every checked-in worker job route off the Celery rollback route.

Revision ID: 0125
Revises: 0124

DATA ONLY. This touches at most one row per checked-in kind. It creates,
drops and alters nothing.

CHAOS-5320 deletes ``resolve_worker_job_route``'s acceptance of a
``celery``-transport row as a legal alternate to the checked-in policy route
(``job_routes.py``): a row still on ``celery`` now raises ``WorkerJobRouteError``
("worker job route drifts from checked-in policy") rather than resolving.

That code change is only safe fleet-wide because Celery has no executor left
to be safe FOR. ``0066_activate_river_worker_job_routes`` already promoted 23
of 24 kinds off ``celery`` in the CHAOS-3033 cutover, and prod Celery itself
stopped entirely on 2026-08-19 (CHAOS-4054 step 4) -- so, unlike 0107's
narrower promotion (which predates the fleet-wide Celery stop and therefore
had to distinguish an untouched seed row from a live, still-meaningful
operator rollback), there is no longer any scenario in which a
``celery``-transport row is a deliberate, functional configuration: nothing
anywhere executes a Celery-routed job today, regardless of how or when that
row got there. A row on ``celery`` is unconditionally broken, for every kind,
independent of its generation history. This migration therefore promotes ANY
row whose transport is still ``celery`` to that kind's own checked-in policy
route (``river`` for every kind in ``migration-state.json`` today) -- it does
not gate on generation the way 0107 does, because 0107's generation==1 guard
answers a question ("was this row touched since the pre-cutover seed") that
no longer distinguishes a safe row from an unsafe one now that celery itself
is uniformly non-executable.

What IS preserved, matching 0107's caution: ``paused`` is left exactly as it
was. A paused row stays paused (``resolve_worker_job_route`` already raises
on a paused row regardless of transport, so promoting transport alone changes
nothing observable about a paused kind) -- this migration only ever changes
``transport``, on kinds this migration owns, and bumps ``generation`` to
record that a change happened, same convention as 0107.

Kinds this migration owns: every kind checked into ``migration-state.json``
as of this revision (``KNOWN_KINDS`` below), matching the exact set CHAOS-5320
moved off ``celery_removed``-eligible ``rollback_route``. A kind NOT in this
list is left untouched even if its transport happens to be ``celery`` --
narrower is safer than a table-wide UPDATE with no kind allowlist at all.

WHY downgrade() IS A DOCUMENTED NO-OP
--------------------------------------
Same reasoning as 0107: no predicate can distinguish a row this migration
promoted from one that was already on the policy route for an unrelated
reason, and the only place a downgrade could send it back to (``celery``) is a
transport nothing can execute. An explicitly irreversible data migration,
with the reason stated, beats a reversible one that reintroduces a black
hole. To roll a kind back deliberately, an operator uses
``dev-health-workerctl job-routes rollback`` -- except CHAOS-5320 also
removes ``celery`` as a legal rollback target in the producer, so that
command's own celery path is retired by this same change; the supported
mitigation for a bad Go rollout is now pausing the kind, not rolling it back
to a dead transport.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

revision: str = "0125"
down_revision: str | None = "0124"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "worker_job_routes"
_CELERY = "celery"

#: The kind -> checked-in policy route this migration promotes a lingering
#: ``celery`` row to, frozen at this revision's own migration-state.json
#: (every kind whose rollback_route moved from "celery" to "none" in the same
#: PR, plus sync.provider_unit's already-river_canary target left alone here
#: since 0107 already owns that kind's promotion and its policy route is not
#: plain "river"). Intentionally NOT re-read from the live JSON file at
#: migration time -- a migration's data effect must be pinned to what it says
#: it does, not to whatever the checked-in policy happens to be the day it
#: runs.
_KIND_TARGET_ROUTE: dict[str, str] = {
    "investment.materialize": "river",
    "metrics.daily_dispatch": "river",
    "metrics.daily_finalize": "river",
    "metrics.daily_partition": "river",
    "metrics.remaining.capacity": "river",
    "metrics.remaining.complexity": "river",
    "metrics.remaining.dora": "river",
    "metrics.remaining.membership_backfill": "river",
    "metrics.remaining.recommendations": "river",
    "metrics.remaining.release_impact": "river",
    "operational.billing_notification": "river",
    "operational.webhook_delivery": "river",
    "report.execute_on_demand": "river",
    "report.execute_scheduled": "river",
    "sync.team_autoimport": "river",
    "system.heartbeat": "river",
    "system.retention_cleanup": "river",
    "workgraph.build": "river",
}


def upgrade() -> None:
    routes = sa.table(
        _TABLE,
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    now = datetime.now(UTC)
    total_promoted = 0
    bind = op.get_bind()
    for kind, target_route in _KIND_TARGET_ROUTE.items():
        result = bind.execute(
            routes.update()
            .where(
                sa.and_(
                    routes.c.job_kind == kind,
                    routes.c.transport == _CELERY,
                )
            )
            .values(
                transport=target_route,
                generation=routes.c.generation + 1,
                updated_at=now,
            )
        )
        # -1 means the DBAPI driver didn't report a row count; treat that as
        # "unknown", not zero, so an operator reading this log doesn't mistake
        # a silent driver limitation for a real no-op kind.
        rowcount = result.rowcount
        total_promoted += max(rowcount, 0)
        logger.info(
            "0125: promoted worker_job_routes off celery kind=%s target_route=%s rows=%s",
            kind,
            target_route,
            rowcount,
        )
    logger.info(
        "0125: promotion complete kinds_checked=%d rows_promoted=%d",
        len(_KIND_TARGET_ROUTE),
        total_promoted,
    )


def downgrade() -> None:
    """Intentionally does nothing. See "WHY downgrade() IS A DOCUMENTED NO-OP".

    Not an oversight and not a stub: reversing would have to guess whether the
    current route was set by this migration or was already there for an
    unrelated reason, and the only value it could guess into (``celery``) is a
    transport nothing can execute.
    """
