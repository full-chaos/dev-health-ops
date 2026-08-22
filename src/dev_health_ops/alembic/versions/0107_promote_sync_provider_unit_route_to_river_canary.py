"""Move the fresh-install ``sync.provider_unit`` route off Celery, which no
longer has an executor (CHAOS-4054 step 4).

Revision ID: 0107
Revises: 0106

DATA ONLY. This touches one row. It creates, drops and alters nothing, so the
``worker_job_routes`` and ``sync_dispatch_transport_routes`` tables are left
exactly as they were -- their retirement is CHAOS-4082's, not this migration's.

Why the row is wrong. 0055 seeds every kind ``celery``; 0061 re-seeds
``sync.provider_unit`` ``celery`` on purpose, so that promoting it to the
canary route stays an explicit operator decision; and 0066's wholesale cutover
deliberately EXCLUDES this one kind for the same reason. That was coherent
while Celery was the rollback executor: a fresh database started on a
transport that could actually run the work, and an operator chose when to move
it.

CHAOS-4026 stopped the Celery fleet fleet-wide, and CHAOS-4054 step 4 deleted
the Python provider-unit dispatch path outright. ``celery`` is now a route
with no executor behind it. The producer therefore fails closed on it rather
than staging outbox rows the Go relay would release forever
(``internal/joboutbox/relay.go``), which is correct -- but it means a freshly
migrated environment lands on ``celery`` and dispatches NO provider unit at
all until an operator runs ``dev-health-workerctl job-routes apply``. Nobody
would know to: nothing in the bring-up path says so.

Production is already past this. A route-table dump taken for CHAOS-4082 shows
``sync.provider_unit`` at ``river_canary``, generation 2 -- an operator applied
it long ago, which is why the gap was invisible until the fail-closed producer
made it load-bearing. This migration makes a fresh install land where
production already is.

``river_canary``, not plain ``river``. Whether this kind graduates to full
River ownership is an operator decision with its own evidence bar (0066's own
comment, and CHAOS-4082); this migration only removes a state that cannot
work, and matches what production actually runs.

Idempotent and operator-safe. The predicate matches ONLY a row still sitting
on ``celery``, so an environment an operator already promoted -- or one that
is deliberately rolled back at the moment this runs -- is left untouched
rather than having its decision overwritten. The generation bump mirrors what
``workerctl job-routes apply`` itself does, so the operator fence stays
monotonic and a concurrent CAS on the old generation still loses.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0107"
down_revision: str | None = "0106"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "worker_job_routes"
_KIND = "sync.provider_unit"
_CELERY = "celery"
_RIVER_CANARY = "river_canary"


def _routes() -> sa.TableClause:
    return sa.table(
        _TABLE,
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )


def _move(from_transport: str, to_transport: str) -> None:
    routes = _routes()
    op.execute(
        routes.update()
        .where(
            sa.and_(
                routes.c.job_kind == _KIND,
                routes.c.transport == from_transport,
            )
        )
        .values(
            transport=to_transport,
            generation=routes.c.generation + 1,
            updated_at=datetime.now(UTC),
        )
    )


def upgrade() -> None:
    _move(_CELERY, _RIVER_CANARY)


def downgrade() -> None:
    """Reverse only what upgrade() moved.

    Symmetric with the upgrade predicate: a row an operator has since moved
    somewhere else (``river``, or paused into a rollback) is not dragged back
    to ``celery`` by a downgrade that did not put it there.
    """

    _move(_RIVER_CANARY, _CELERY)
