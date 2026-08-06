"""Add sync run unit deferral-exhaustion columns (CHAOS-3412).

``sync/budget_guard.py`` defers a unit whose estimate does not fit its bucket
by re-stamping it ``retrying`` with a fresh ``available_at``. That deferral
does NOT increment ``attempts``, and until CHAOS-3412 no counter tracked the
budget episode at all -- so a permanently oversized unit (a HEAVY dataset on
a wide ``initial_sync_depth``) looped forever: never run, never failed, never
visible.

Three columns, two different lifecycles:

* ``budget_deferrals`` / ``budget_first_deferred_at`` give the BUDGET episode
  the same count-plus-wall-clock exhaustion state the rate-limit episode
  already has in ``rate_limit_deferrals`` / ``rate_limit_first_seen_at``
  (revision 0022). Per-episode columns are reset whenever a DIFFERENT episode
  begins, so they measure one continuous episode.
* ``first_blocked_at`` is the AGGREGATE clock and is deliberately NOT
  per-episode. Each episode kind clears the other's counters, so a unit that
  alternates between budget deferral and rate-limit cooldown keeps both
  per-episode caps permanently out of reach while never running -- the same
  invisible-forever outcome by a different route. ``first_blocked_at`` is set
  once when a unit first becomes blocked for ANY reason and survives episode
  changes; it is cleared only when the unit is actually dispatched or
  succeeds.

Lock safety: each ``ADD COLUMN`` takes a brief ACCESS EXCLUSIVE lock on
``sync_run_units``, which is hot. On PostgreSQL 11+ a constant (non-volatile)
``DEFAULT`` does not rewrite the table, so the lock is held only for the
catalog update -- the real risk is queueing behind a long-running transaction
and blocking every reader behind it. A 5s ``lock_timeout`` bounds that: the
migration fails fast instead of stalling production. If it fails with
``canceling statement due to lock timeout``, find and end the long
transaction (``pg_stat_activity`` / ``pg_locks``) and re-run -- the column
adds are individually guarded, so a retry resumes rather than failing on a
duplicate column.

Retry safety: each column is guarded individually so a rerun after a partial
failure resumes instead of failing on a duplicate column. The downgrade
mirrors that by dropping columns only when present.

Revision ID: 0085
Revises: 0084
Create Date: 2026-08-05 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0085"
down_revision: str | None = "0084"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    _set_lock_timeout()
    _add_column_if_missing(
        "sync_run_units",
        sa.Column(
            "budget_deferrals",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column(
            "budget_first_deferred_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    _add_column_if_missing(
        "sync_run_units",
        sa.Column("first_blocked_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    _set_lock_timeout()
    _drop_column_if_present("sync_run_units", "first_blocked_at")
    _drop_column_if_present("sync_run_units", "budget_first_deferred_at")
    _drop_column_if_present("sync_run_units", "budget_deferrals")


def _set_lock_timeout() -> None:
    """Bound how long this migration waits for a table lock, to 5 seconds.

    ``SET LOCAL`` scopes it to the migration's own transaction, so it cannot
    leak into any other session. Skipped on non-PostgreSQL backends (the unit
    suite runs migrations against SQLite, which has no such setting).

    The statement is a literal rather than an interpolated string: there is no
    caller-supplied value here, and building even a constant timeout through
    ``sa.text(f"...")`` trips
    ``python.sqlalchemy.security.audit.avoid-sqlalchemy-text`` and reads like
    the SQL-injection shape that rule exists to catch. Every other migration
    in this tree issues raw DDL as a plain ``op.execute("...")`` string; this
    now matches.
    """
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.execute("SET LOCAL lock_timeout = '5s'")


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if column.name not in _column_names(table_name):
        op.add_column(table_name, column)


def _drop_column_if_present(table_name: str, column_name: str) -> None:
    if column_name in _column_names(table_name):
        op.drop_column(table_name, column_name)


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}
