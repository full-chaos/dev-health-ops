"""Add the durable executed-proof ledger and backfill it once (CHAOS-4114).

Revision ID: 0109
Revises: 0108
Create Date: 2026-08-22 00:00:00

The CHAOS-4060 route-readiness gate derived its evidence from a whole-table
``GROUP BY`` over ``sync_run_units`` with per-row JSON extraction. That table
only grows, so the query got monotonically slower until it outran both of its
deadlines: the scheduler's 15s ``StepTimeout`` on the periodic refresh, and
the 30s startup budget. On 2026-08-22 a startup refresh timed out, the
first-failure path installed an EMPTY Degraded snapshot, and every non-waived
provider/dataset pair was blocked from planning for eight hours -- only the
single waived route survived (CHAOS-4124).

The house rule is that timeouts never fix capacity; durable truth does. This
table is that durable truth: one row per ``(provider, dataset_key)`` pair --
roughly a hundred rows for the entire route matrix, independent of how much
sync history exists -- maintained by the same transactions that write the
units it summarizes. The refresh becomes a bounded read of the route matrix.

Deliberately ORG-AGNOSTIC. The query this replaces had no ``org_id`` in its
``GROUP BY``, and that is not an oversight: the gate answers "has this
provider route's WRITER ever landed a real row", a statement about code
capability, not about any tenant's data. Adding an org dimension here would
change the gate's decision, not merely its storage.

The columns are normalized-lowercase identifiers and two timestamps. No org
id, no payloads, no error text, no credentials.

Operating this migration
------------------------

The backfill below is ONE full scan of ``sync_run_units`` -- the same scan the
scheduler could no longer afford, run once, offline. It is the expensive part
of this migration and it is deliberately atomic with the ``CREATE TABLE``.

That is a considered tradeoff, not an oversight, and the alternative is worse.
A partially-populated ledger does not read as "incomplete": a pair with no
ledger row reads as NEVER ATTEMPTED, which is the gate's permissive state. So
any staged, batched, or deferred backfill would silently disable the CHAOS-4060
gate for every pair it had not reached yet -- fail-OPEN, invisibly, which is
precisely the direction this whole ticket exists to close. Creating the table
and filling it in one transaction is the only shape where the ledger is never a
partial truth: either the gate reads the complete history, or the migration
rolled back and there is no table at all (which fails CLOSED and loudly, via
the scheduler's executed_proof_evidence readiness check).

What it costs, concretely:

* The read takes ACCESS SHARE on ``sync_run_units``. It does NOT block
  concurrent INSERTs or UPDATEs, so syncing continues normally while it runs.
  The write side touches only the brand-new table, which nothing else can be
  using yet.
* It is one sequential pass with per-row JSON extraction. The 15s/30s deadlines
  it blew in the scheduler were budgets for a statement competing with hourly
  sync load, not a measurement of how long the scan takes -- but treat the
  duration as unknown and give the migration a window rather than a timeout.
* It is re-runnable and monotone (see ``_BACKFILL_SQL``), so an operator who
  wants the cost off the deployment's critical path can apply this revision
  ahead of rolling the binaries. Deploying the binaries FIRST is the one
  ordering that does not work: until the table exists, the evidence read and
  both ledger writes fail.

The row count and elapsed time are printed so the pass is observed rather than
silent -- a backfill that wrote zero rows on a database with real sync history
means something is wrong, and that must not look like success.

Cutover windows, and why they are safe
--------------------------------------

Two windows exist where a ``sync_run_units`` row can land without a matching
ledger write: rows committed after this scan's snapshot, and rows committed by
a pre-deploy worker during a rolling rollout. Both are real. Both leave the
ledger BEHIND reality, and behind is the safe direction:

* Missing ``proven_at``. The pair reads attempted-but-unproven and is blocked.
  Note what that pair looked like BEFORE this migration: it was unproven then
  too, so CHAOS-4060 was already blocking it. The ledger does not newly block
  anything -- it delays an unblocking by one cycle, until the next successful
  completion on a ledger-aware worker stamps the proof. Proof is monotone, so
  the stamp is permanent once it lands.
* Missing ``attempted_at``. For the ledger to miss this, EVERY row that pair
  has ever had must have been committed inside the window -- which makes it a
  genuinely brand-new pair, and "never attempted" is then the correct verdict,
  not a mistake. A pair with any older history is caught by the scan.

The ledger can therefore be stale, but it cannot be stale in a way that
manufactures proof a route has not earned. Nothing here can move a pair from
blocked to planning on false evidence.

If a route does look starved after a rollout, the remedy is the backfill
itself: ``_BACKFILL_SQL`` is monotone and idempotent, so re-running it against
the live database reconciles the ledger with whatever ``sync_run_units`` now
says, without disturbing any proving instant already recorded.
``test_0109_backfill_repairs_a_ledger_that_missed_rows`` proves exactly that.
"""

from __future__ import annotations

import time
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0109"
down_revision: str | None = "0108"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_executed_proof_ledger"

#: One-time rebuild of the ledger from the authoritative table, evaluating the
#: IDENTICAL predicate ``executedProofProvenPredicateSQL`` in
#: ``internal/providersync/executed_proof.go`` -- and therefore the identical
#: predicate the pre-ledger evidence query used. Kept as one uninterpolated
#: literal on purpose: every consumer of that predicate has to read the same
#: bytes, and an f-string or a concatenation here is exactly how the two would
#: quietly stop meaning the same thing. Pinned byte-for-byte by
#: ``tests/test_executed_proof_ledger_predicate_parity.py``.
#:
#: This is the same full scan the scheduler could no longer afford, run ONCE,
#: offline, in migration context where neither the 15s nor the 30s deadline
#: exists.
#:
#: ``attempted_at``/``proven_at`` are derived from the units themselves rather
#: than stamped with ``now()``, so a backfilled ledger reads as the honest
#: history it is. Both aggregates merge monotonically into whatever the ledger
#: already holds, which is what makes re-running this a no-op.
#:
#: The ``btrim(...) <> ''`` filter drops history whose provider or dataset_key
#: is blank. Such a row cannot produce a ledger key any route lookup would
#: match (the gate looks pairs up by trimmed, lowercased identity), and it
#: would violate the table's normalization CHECK and abort the whole
#: migration. ``sync_run_units`` declares both columns NOT NULL and every
#: planner sets them, so this is a guard against corrupt history rather than
#: an expected case.
_BACKFILL_SQL = """
INSERT INTO public.sync_executed_proof_ledger AS ledger
  (provider, dataset_key, attempted_at, proven_at)
SELECT
  btrim(lower(unit.provider)),
  btrim(lower(unit.dataset_key)),
  min(unit.created_at),
  min(unit.updated_at) FILTER (WHERE
    unit.status = 'success'
    AND (
      COALESCE(
        (unit.result #>> '{go_provider_route,records}') ~ '^[0-9]{1,18}$'
        AND (unit.result #>> '{go_provider_route,records}')::bigint > 0,
        false
      )
      OR COALESCE(
        (unit.result ->> 'persisted') ~ '^[0-9]{1,18}$'
        AND (unit.result ->> 'persisted')::bigint > 0,
        false
      )
    )
  )
FROM public.sync_run_units AS unit
WHERE btrim(unit.provider) <> '' AND btrim(unit.dataset_key) <> ''
GROUP BY 1, 2
ON CONFLICT (provider, dataset_key) DO UPDATE
  SET attempted_at = LEAST(ledger.attempted_at, EXCLUDED.attempted_at),
      proven_at = COALESCE(ledger.proven_at, EXCLUDED.proven_at)
"""


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("dataset_key", sa.Text(), nullable=False),
        # When this pair FIRST had a sync_run_units row of any status. The
        # evidence query this replaces had no WHERE clause, so a pair became
        # "attempted" the instant planning minted its first row, whatever
        # became of it. NOT NULL: a ledger row that is not attempted cannot
        # exist, because a row is only ever created by an attempt.
        sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=False),
        # When this pair FIRST completed a unit with a positive persisted-row
        # count. NULL means attempted-but-never-proven -- the CHAOS-4048/4049
        # shape the gate exists to catch -- and is a real, load-bearing state,
        # not missing data.
        sa.Column("proven_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("provider", "dataset_key"),
        # The lowercase normalization is an INVARIANT of this table, not a
        # convention its writers are trusted to remember. The query being
        # replaced applied lower() at read time over the whole table; the
        # ledger applies it at write time, and a writer that forgot would
        # otherwise create a second, invisible row for the same pair --
        # splitting one route's proof in half and letting an already-proven
        # route read as never-attempted. The database refuses instead.
        sa.CheckConstraint(
            "provider = btrim(lower(provider)) AND provider <> ''",
            name="ck_sync_executed_proof_ledger_provider_normalized",
        ),
        sa.CheckConstraint(
            "dataset_key = btrim(lower(dataset_key)) AND dataset_key <> ''",
            name="ck_sync_executed_proof_ledger_dataset_normalized",
        ),
        # There is deliberately NO `proven_at >= attempted_at` constraint.
        # attempted_at is stamped by the scheduler process and proven_at by a
        # worker process, on different hosts: a few milliseconds of clock skew
        # between them would abort the terminalizing transaction and lose a
        # completed unit over a purely cosmetic ordering invariant. Only the
        # NULL-ness of proven_at is load-bearing to the gate.
    )
    started = time.monotonic()
    result = op.get_bind().execute(sa.text(_BACKFILL_SQL))
    elapsed = time.monotonic() - started
    # Printed, not silent: on a database with real sync history a zero-row
    # backfill means the ledger is empty, and an empty ledger reads as "no pair
    # has ever been attempted" -- it would disable the gate rather than
    # populate it. That failure has to be visible in the migration output, not
    # discovered later from routes that stopped being blocked.
    print(
        f"0109: executed-proof ledger backfilled {result.rowcount} "
        f"provider/dataset pairs from public.sync_run_units in {elapsed:.1f}s"
    )


def downgrade() -> None:
    op.drop_table(_TABLE)
