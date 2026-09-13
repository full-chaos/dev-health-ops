"""Promote the ``sync.provider_unit`` route off canary AND off Celery.

Revision ID: 0131
Revises: 0130

DATA ONLY. This touches at most one row. It creates, drops and alters
nothing.

0107 moved a fresh install's ``sync.provider_unit`` route from ``celery`` to
``river_canary`` -- deliberately NOT plain ``river``, because at the time
whether this kind graduated to full River ownership was an open operator
decision with its own evidence bar, and only if the row was still exactly at
0061's untouched seed (transport ``celery``, generation 1, unpaused). That
bar is now cleared: ``contracts/jobs/v1/migration-state.json`` pins this
kind's checked-in policy route to plain ``river`` (rollback_route ``none``)
-- its Go handler (``internal/jobs/providerunit``) already covers every
provider/dataset pair the checked-in matrix lists, and no Celery consumer
has served this queue in any deployment for a long time, so the canary
label had stopped gating anything real.

Both ``resolve_worker_job_route`` (``jobs/routes.py``) and
``jobroute.Controller``'s own ``allowed()`` check now require a row's
transport to be EXACTLY the checked-in route or the checked-in rollback
route -- with rollback_route now ``none``, that closed set is just
``{"river"}``. Any row still sitting on ``river_canary`` (0107's own
promotion) OR still sitting on ``celery`` (a row 0107's narrow generation==1
guard never touched -- e.g. an operator paused it, or otherwise mutated it,
before 0107 ever ran) drifts the instant the policy file above ships. This
is the same shape 0125 fixed for the twelve kinds whose rollback route left
``celery`` in that PR, applied here to the one kind 0125 explicitly left
alone (its own comment: "sync.provider_unit's already-river_canary target
left alone here since 0107 already owns that kind's promotion and its
policy route is not plain river") -- covering BOTH transports 0107's own
narrower migration could have left this kind on, not just the one it
successfully promoted to.

Unconditional on generation, matching 0125's reasoning rather than 0107's:
0107 needed to distinguish an untouched pre-cutover seed from a live,
meaningful operator decision, because at that point ``river_canary`` was
itself a legitimate, executable end state an operator might have chosen
deliberately and want left alone, and a still-``celery`` row past
generation 1 was a deliberate rollback with nowhere else to go. Neither
distinction has anything left to protect: every row on ``river_canary`` for
this kind is the SAME state (there was only ever one canary meaning for
it), and ``celery`` is unconditionally broken for this kind exactly the way
0125's own docstring establishes for its twelve -- nothing anywhere
executes a Celery-routed job today, regardless of how or when the row got
there. A row on either legacy transport, at any generation, is promoted.

What IS preserved, matching both 0107 and 0125: ``paused`` is left exactly
as it was, and only ``transport``/``generation``/``updated_at`` change.

WHY downgrade() IS A DOCUMENTED NO-OP
--------------------------------------
Same reasoning as 0107 and 0125. No predicate can distinguish a row this
migration promoted from one that already read ``river`` for an unrelated
reason, and the only places a downgrade could send it back to
(``river_canary`` or ``celery``) are transports the checked-in policy no
longer accepts -- ``resolve_worker_job_route`` would immediately fail it
closed again. An explicitly irreversible data migration, with the reason
stated, beats a reversible one that reintroduces a route nothing can
legally resolve to. To roll this kind back deliberately, an operator pauses
it; there is no supported rollback target below ``river`` any more.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

revision: str = "0131"
down_revision: str | None = "0130"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "worker_job_routes"
_KIND = "sync.provider_unit"
_LEGACY_TRANSPORTS = ("river_canary", "celery")
_RIVER = "river"


def upgrade() -> None:
    routes = sa.table(
        _TABLE,
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    bind = op.get_bind()
    now = datetime.now(UTC)
    total_promoted = 0
    for legacy_transport in _LEGACY_TRANSPORTS:
        result = bind.execute(
            routes.update()
            .where(
                sa.and_(
                    routes.c.job_kind == _KIND,
                    routes.c.transport == legacy_transport,
                )
            )
            .values(
                transport=_RIVER,
                generation=routes.c.generation + 1,
                updated_at=now,
            )
        )
        # -1 means the DBAPI driver didn't report a row count; treat that as
        # "unknown", not zero, so an operator reading this log doesn't
        # mistake a silent driver limitation for a real no-op.
        rowcount = result.rowcount
        total_promoted += max(rowcount, 0)
        logger.info(
            "0131: promoted worker_job_routes kind=%s %s->river rows=%s",
            _KIND,
            legacy_transport,
            rowcount,
        )
    logger.info(
        "0131: promotion complete kind=%s rows_promoted=%d",
        _KIND,
        total_promoted,
    )


def downgrade() -> None:
    """Intentionally does nothing. See "WHY downgrade() IS A DOCUMENTED NO-OP".

    Not an oversight and not a stub: reversing would have to guess whether the
    current route was set by this migration or was already there for an
    unrelated reason, and the only values it could guess into
    (``river_canary`` or ``celery``) are transports the checked-in policy no
    longer accepts.
    """
