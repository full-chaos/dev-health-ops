"""Retire the Celery rollback route for the four sync-dispatch transport kinds.

Revision ID: 0132
Revises: 0131

``contracts/sync-dispatch/v1/transport-routes.json`` pinned ``rollback_route:
celery`` for ``dispatch_sync_run``, ``finalize_sync_run``, ``post_sync`` and
``reference_discovery`` long after every Celery bridge route for these four
kinds was deleted (79551fa2, 0bd32a1e): no Celery producer has served any of
them in any deployment since. The checked-in contract now completes that
promotion the same way ``migration-state.json`` completed
``sync.provider_unit`` (0131): ``route: river`` stays, ``rollback_route``
moves to ``none``.

``sync_dispatch_transport_routes.rollback_transport`` is the DB-persisted
mirror of that declaration, gated by ``ck_sync_dispatch_transport_routes_-
rollback``. Both ``internal/syncroute.Fence.Check`` and
``internal/syncroute.Controller`` compare a row's ``rollback_transport``
against the checked-in registry's ``RollbackRoute`` for that kind (not a
hardcoded literal), so shipping the contract change alone -- without moving
this column -- would fail every one of these four rows closed with
``ErrDrift`` the moment that binary deployed. Promoting a kind out of a
rollback route needs the row migrated in lockstep with the policy file, not
just the JSON.

The check constraint is WIDENED (``= 'celery'`` becomes ``IN ('celery',
'none')``), not replaced outright: several Postgres-backed Python tests
(``tests/_helpers.py``, ``tests/test_dispatch_outbox.py``) hand-construct
rows with ``transport='celery', rollback_transport='celery'`` via
``Base.metadata.create_all`` to exercise claim/lock/pause mechanics
generically, independent of what the real checked-in contract says for these
kinds today -- a hard ``= 'none'`` constraint would break every one of them.

DATA CHANGE. Touches at most four rows. Creates and drops nothing but the one
check constraint it replaces.

WHY downgrade() IS A DOCUMENTED NO-OP
--------------------------------------
Same reasoning as 0125 and 0131. Reversing would have to put these four rows
back on a ``rollback_transport`` the checked-in contract no longer declares --
``internal/syncroute.Controller.Resume`` already refuses ``celery`` as a
transport for a kind whose registry ``RollbackRoute`` is ``none`` -- so a
downgrade would immediately be unusable through the one path that reads
these rows. There is no supported rollback target left for any of these four
kinds; an operator who needs one pauses the route instead.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

revision: str = "0132"
down_revision: str | None = "0131"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_dispatch_transport_routes"
_CONSTRAINT = "ck_sync_dispatch_transport_routes_rollback"
_KINDS = (
    "dispatch_sync_run",
    "finalize_sync_run",
    "post_sync",
    "reference_discovery",
)


def upgrade() -> None:
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")

    routes = sa.table(
        _TABLE,
        sa.column("kind", sa.String()),
        sa.column("rollback_transport", sa.String()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    bind = op.get_bind()
    now = datetime.now(UTC)
    result = bind.execute(
        routes.update()
        .where(
            sa.and_(
                routes.c.kind.in_(_KINDS),
                routes.c.rollback_transport != "none",
            )
        )
        .values(
            rollback_transport="none",
            generation=routes.c.generation + 1,
            updated_at=now,
        )
    )
    # -1 means the DBAPI driver didn't report a row count; logged as-is
    # (rather than clamped to zero) so an operator reading this doesn't
    # mistake a silent driver limitation for a real no-op.
    logger.info(
        "0132: retired rollback_transport celery->none rows=%s",
        result.rowcount,
    )

    op.create_check_constraint(
        _CONSTRAINT,
        _TABLE,
        "rollback_transport IN ('celery', 'none')",
    )


def downgrade() -> None:
    """Intentionally does nothing. See "WHY downgrade() IS A DOCUMENTED NO-OP".

    Not an oversight and not a stub: the only place this column is read
    (``internal/syncroute``) resolves every row's expected rollback transport
    from the checked-in contract, which no longer declares ``celery`` for any
    of these four kinds. Reintroducing it here would not restore a usable
    rollback path.
    """
