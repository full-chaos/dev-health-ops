"""Move the fresh-install ``sync.provider_unit`` route off Celery, which no
longer has an executor (CHAOS-4054 step 4).

Revision ID: 0107
Revises: 0106

DATA ONLY. This touches at most one row. It creates, drops and alters nothing,
so the ``worker_job_routes`` and ``sync_dispatch_transport_routes`` tables are
left exactly as they were -- their retirement is CHAOS-4082's, not this
migration's.

Why the row is wrong. 0055 seeds every kind ``celery``; 0061 re-seeds
``sync.provider_unit`` ``celery`` on purpose, so that promoting it to the
canary route stays an explicit operator decision; and 0066's wholesale cutover
deliberately EXCLUDES this one kind for the same reason. That was coherent
while Celery was the rollback executor: a fresh database started on a
transport that could actually run the work.

CHAOS-4026 stopped the Celery fleet fleet-wide, and CHAOS-4054 step 4 deleted
the Python provider-unit dispatch path outright, so ``celery`` is now a route
with no executor behind it. The producer fails closed on it rather than
staging outbox rows the Go relay would release forever
(``internal/joboutbox/relay.go``), which is correct -- but it means a freshly
migrated environment lands on ``celery`` and dispatches NO provider unit at
all until an operator runs ``dev-health-workerctl job-routes apply``. Nothing
in the bring-up path would say so.

Production is already past this: a route dump taken for CHAOS-4082 shows
``sync.provider_unit`` at ``river_canary``, generation 2 -- an operator applied
it long ago, which is why the gap stayed invisible until the fail-closed
producer made it load-bearing. This lands a fresh install where production
already is.

``river_canary``, not plain ``river``. Whether this kind graduates to full
River ownership is an operator decision with its own evidence bar (0066's own
comment, and CHAOS-4082). This only removes a state that cannot work.

WHAT THIS MIGRATION OWNS, EXACTLY
---------------------------------
Only the untouched 0061 seed: ``transport='celery'`` AND ``generation=1`` AND
``paused=false``. Generation is the provenance test. Every operator mutation
bumps it -- ``jobroute.Controller.ApplyCheckedIn`` and ``.Rollback`` both
``SET ... generation = generation + 1`` (``internal/jobroute/control.go:160``
and ``:233``) -- and 0055's ``ck_worker_job_route_generation`` pins the floor
at 1. So ``generation = 1`` means precisely "no operator has ever moved this
row since 0061 seeded it", which is the only state this migration has any
business changing.

An earlier cut of this migration matched on ``transport`` alone and claimed
operator-safety it did not have: adversarial review demonstrated that it
promoted a deliberate rollback sitting at generation 3, promoted a PAUSED row
while leaving it paused, and -- on downgrade -- moved production's
operator-set ``river_canary`` into ``celery``. The predicate below is as
narrow as the claim.

WHY downgrade() IS A DOCUMENTED NO-OP
------------------------------------
This is a deliberate exception to the house rule that downgrades are
reversible, taken with review sign-off rather than by omission.

No predicate can distinguish a row this migration promoted from one an
operator promoted: both are ``river_canary``, and both sit at the same
generation -- production is literally the operator-promoted case, at
generation 2, which is exactly what this migration produces from the seed. A
downgrade therefore cannot tell whose decision it would be reversing, and the
only place it could reverse to is ``celery``, a transport with no executor.

An explicitly irreversible data migration, with the reason stated, beats a
reversible one that corrupts production. To undo this deliberately, an
operator uses ``dev-health-workerctl job-routes rollback``, which is the
supported path, takes the quiescence barrier, and records who did it.

Quiescence. ``workerctl job-routes apply`` gates promotion behind a Celery
quiescence prover, and a bare UPDATE skips it. With the fingerprint above that
is vacuous: a row at generation 1 has never been moved by an operator, so
there is no live handoff between two running execution owners to sequence --
the same reasoning 0066 documented for its own wholesale cutover, narrowed
here to a single untouched row.
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
#: The generation 0061 seeds. Any operator mutation moves it off this value.
_UNTOUCHED_GENERATION = 1


def upgrade() -> None:
    routes = sa.table(
        _TABLE,
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("paused", sa.Boolean()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    op.execute(
        routes.update()
        .where(
            sa.and_(
                routes.c.job_kind == _KIND,
                routes.c.transport == _CELERY,
                routes.c.generation == _UNTOUCHED_GENERATION,
                routes.c.paused.is_(False),
            )
        )
        .values(
            transport=_RIVER_CANARY,
            generation=routes.c.generation + 1,
            updated_at=datetime.now(UTC),
        )
    )


def downgrade() -> None:
    """Intentionally does nothing. See "WHY downgrade() IS A DOCUMENTED NO-OP".

    Not an oversight and not a stub: reversing would have to guess whether the
    current route was set by this migration or by an operator, and would guess
    into a transport that cannot execute the work.
    """
