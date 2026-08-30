"""Index sync_dispatch_outbox for the CHAOS-4583 terminal-close candidate scan.

Revision ID: 0118
Revises: 0117
Create Date: 2026-08-30 00:00:00

CHAOS-4583: sync_dispatch_outbox rows never reach a terminal status. Every
native completion path (native_dispatch_sync_run.go, native_finalize_sync_run.go,
native_post_sync.go, native_reference_discovery.go's stampSuccess) arms its
SUCCESSOR kind's outbox row but never closes the row it was itself dispatched
for, so a 'dispatched' row whose owner has long since gone terminal is
stranded forever. Local readback: 6568/6568 rows non-terminal, ~99.7% per
kind already owned by a terminal run/ledger.

The new ``TerminalOutboxClose`` reconciler step (internal/syncreconciler/
terminal_outbox_close.go) and its backlog reaper
(``dev-health-workerctl sync-dispatch-outbox close-backlog``) both scan by
``kind`` + ``status = 'dispatched'`` independent of ``sync_run_id`` -- the
existing ``uq_sync_dispatch_outbox_run_kind`` unique index (sync_run_id, kind)
does not serve this access path at all, and ``ix_sync_dispatch_outbox_due``
(status, available_at) puts ``kind`` nowhere in its key. Without a
kind-leading index, the terminal-close candidate scan is a full sequential
scan of the whole table on every reconciler tick, which is exactly the shape
0111/0101/CHAOS-4262 already found and fixed for the sibling materializer/
terminal-repair joins on ``sync_runs`` -- this is the same defect class one
table over, caught before merge rather than in production.

``available_at`` trails the key even though today's terminal-close queries do
not filter or order by it: it matches ``ix_sync_dispatch_outbox_due``'s own
column order convention for this table (kind, then the two columns that
convention already pairs) and costs nothing extra to carry, in case a future
per-kind ordered scan wants it.

Concurrent-index retry safety follows 0111/0093/0019's pattern: ``DROP INDEX
CONCURRENTLY IF EXISTS`` before create, so a retry after a failed
CONCURRENTLY build always converges on a VALID index rather than silently
skipping a stale invalid one.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0118"
down_revision: str | None = "0117"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

# _TABLE and _INDEX are module-level compile-time constants, never operator
# or request input, so the f-string DDL below carries no injection surface --
# the nosemgrep suppressions on each op.execute mirror 0111/0113/0089's own
# identical justification for the identical CONCURRENTLY-index shape (a
# migration's DDL cannot be parameterized the way a DML statement can; alembic
# and psycopg2 offer no bind-parameter path for identifiers, and CREATE INDEX
# CONCURRENTLY additionally cannot run inside the transaction op.execute's
# parameterized path would open).
_TABLE = "sync_dispatch_outbox"
_INDEX = "ix_sync_dispatch_outbox_kind_status_available_at"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"
        )
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"CREATE INDEX CONCURRENTLY {_INDEX} ON public.{_TABLE} "
            "(kind, status, available_at)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"
        )
