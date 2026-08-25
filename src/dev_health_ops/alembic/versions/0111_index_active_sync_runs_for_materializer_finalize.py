"""Index active sync runs for the materializer's finalize/dispatch candidate scan.

Revision ID: 0111
Revises: 0110
Create Date: 2026-08-25 00:00:00

CHAOS-4262: ``materializeFinalizeSQL`` (internal/syncreconciler/materializer.go)
scans ``sync_runs`` directly with ``status NOT IN ('success', 'partial_failed',
'failed')``, the same predicate shape CHAOS-4092 already found unindexed for
the terminal-delivery repair join (0101_index_active_sync_runs_for_terminal_repair
added ``ix_sync_runs_status_id (status, id)`` for that sibling). A plain
``(status, id)`` index does not help a ``NOT IN`` predicate the way it helps
an equality lookup, so the planner keeps choosing a full ``Seq Scan`` whose
estimated cost grows with ALL of ``sync_runs`` history rather than with the
active (non-terminal) set -- and once that estimate crosses
``jit_above_cost`` (100000, the Postgres default), the planner compiles a JIT
plan for a statement that, in steady state, touches zero rows.

Verified empirically (testcontainers Postgres, 4536 sync_runs / 1478
schedule-triggered / 20000-row scheduled_sync_occurrences, matching prod's
"4536 rows removed by filter, 0 rows out" shape): the planner's estimated
total cost for ``materializeFinalizeSQL`` was 1,714,780 without this index
(JIT fired: 30 functions, ~134ms compile) and 45,146 with it -- a ~38x drop,
and back under ``jit_above_cost`` even with JIT left on. See
internal/syncreconciler/materializer_jit_cost_integration_test.go for the
executable version of this proof.

The index predicate is written as the EXACT ``NOT IN`` the query itself uses,
not rewritten to a positive ``IN`` allowlist -- CHAOS-4107 (the sibling
runaway-report ticket, same predicate) already documented why a positive
rewrite fails closed for a reporting/candidate query: a status value added
later would silently stop being "active". An index predicate that mirrors
the query's own NOT IN exactly does not have that problem, because the two
stay definitionally identical -- a new status is "not in the terminal three"
in both places at once, so it is never silently dropped from either side.

Concurrent-index retry safety follows 0019/0093's pattern: ``DROP INDEX
CONCURRENTLY IF EXISTS`` before create, so a retry after a failed
CONCURRENTLY build always converges on a VALID index rather than silently
skipping a stale invalid one.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0111"
down_revision: str | None = "0110"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_runs"
_INDEX = "ix_sync_runs_active_candidates"
_WHERE = "status NOT IN ('success', 'partial_failed', 'failed')"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
        op.execute(
            f"CREATE INDEX CONCURRENTLY {_INDEX} ON public.{_TABLE} "
            f"(created_at, id) WHERE {_WHERE}"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
