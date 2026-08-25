"""Retire the extra_metrics/team_metrics remaining-metric route rows and family values.

Revision ID: 0110
Revises: 0109

CHAOS-4243: ``metrics.remaining.extra_metrics``/``metrics.remaining.team_metrics``
were registered Go handlers with zero producer anywhere -- retired (deleted
from the registry, the worker, and every checked-in contract) rather than
left dormant. This migration cleans up the two pieces of durable Postgres
state 0064/0066 and 0058 left behind for them:

* the seeded ``worker_job_routes`` rows, so a fresh database no longer
  carries route control-plane state for a kind nothing will ever dispatch;
* the ``remaining_metric_runs`` family CHECK constraint, so a row can no
  longer be inserted for either retired family.

Neither family ever had a producer, so no ``remaining_metric_runs`` row for
either exists in any measured environment. That is asserted, not assumed:
``upgrade()`` checks two places work for either retired kind could be staged
or already claimed, and refuses to proceed if it finds anything:

* ``worker_job_outbox`` (Alembic-owned, always present by this revision) for
  any ``pending``/``claimed`` row -- the reconciler stages envelopes there
  before the relay inserts them into River, so a row can exist even if River
  never saw the kind;
* River's own job table, schema-qualified via the same ``RIVER_DATABASE_SCHEMA``
  environment variable the Go runtime uses (default ``river``; resolved and
  validated as a plain lowercase Postgres identifier before use, since it
  cannot be a bind parameter), for any row (any state) of either kind.

A River job row or a pending/claimed outbox row surviving would mean
something DID enqueue the kind at some point, which would falsify this
migration's entire premise. The River check is a no-op (not an error) when
the configured schema/table does not exist yet in the database being
migrated -- River owns that schema via its own Go migration tool, not
Alembic, and a database that has never run River's migrations trivially has
no such rows.

Known residual gap: the outbox/River checks and the subsequent DELETE are not
fenced against a concurrent INSERT between the check and the commit. This is
accepted rather than closed with locking machinery because no code path, in
this revision's tree or any earlier one, ever creates a
``worker_job_outbox``/``river_job`` row for either kind -- that is the same
"registered handler with zero producer anywhere" finding CHAOS-4243 verified
against production log evidence before retiring them, not a live producer
this migration is racing against.
"""

from __future__ import annotations

import os
import re
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.selectable import TableClause

revision: str = "0110"
down_revision: str | None = "0109"
branch_labels = None
depends_on = None

_RETIRED_KINDS = (
    "metrics.remaining.extra_metrics",
    "metrics.remaining.team_metrics",
)
_SAFE_ROLLBACK_TRANSPORT = "celery"
_WIDE_FAMILY_CHECK = (
    "family IN ('capacity', 'complexity', 'dora', 'extra_metrics', "
    "'membership_backfill', 'recommendations', 'release_impact', "
    "'team_metrics')"
)
_NARROW_FAMILY_CHECK = (
    "family IN ('capacity', 'complexity', 'dora', "
    "'membership_backfill', 'recommendations', 'release_impact')"
)
_RIVER_SCHEMA_ENV = "RIVER_DATABASE_SCHEMA"
_DEFAULT_RIVER_SCHEMA = "river"
_SAFE_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _routes() -> TableClause:
    return sa.table(
        "worker_job_routes",
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("paused", sa.Boolean()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )


def _river_schema() -> str:
    """Resolve the configured River Postgres schema, matching the Go runtime.

    Mirrors ``internal/platform/config/config.go``'s ``validateIdentifier``:
    a plain lowercase identifier, 1-63 characters. Schema names cannot be
    bind parameters, so this validation is what makes it safe to interpolate
    directly into the query text below.
    """
    schema = os.environ.get(_RIVER_SCHEMA_ENV, "").strip() or _DEFAULT_RIVER_SCHEMA
    if len(schema) > 63 or not _SAFE_IDENTIFIER_RE.match(schema):
        raise RuntimeError(
            f"{_RIVER_SCHEMA_ENV}={schema!r} is not a lowercase Postgres identifier"
        )
    return schema


def _assert_no_pending_outbox_rows_for_retired_kinds() -> None:
    bind = op.get_bind()
    found = bind.execute(
        sa.text(
            "SELECT job_kind, status, count(*) AS n FROM worker_job_outbox "
            "WHERE job_kind = ANY(:kinds) AND status IN ('pending', 'claimed') "
            "GROUP BY job_kind, status ORDER BY job_kind, status"
        ),
        {"kinds": list(_RETIRED_KINDS)},
    ).all()
    if found:
        details = ", ".join(f"{row.job_kind}:{row.status}={row.n}" for row in found)
        raise RuntimeError(
            "refusing to retire remaining-metrics kinds with pending/claimed "
            f"worker_job_outbox rows -- CHAOS-4243's premise is that no "
            f"producer ever enqueued them: {details}"
        )


def _assert_no_river_job_rows_for_retired_kinds() -> None:
    bind = op.get_bind()
    schema = _river_schema()
    table_exists = bind.execute(
        sa.text(f"SELECT to_regclass('{schema}.river_job') IS NOT NULL")
    ).scalar_one()
    if not table_exists:
        return
    found = bind.execute(
        sa.text(
            f"SELECT kind, count(*) AS n FROM {schema}.river_job "
            "WHERE kind = ANY(:kinds) GROUP BY kind ORDER BY kind"
        ),
        {"kinds": list(_RETIRED_KINDS)},
    ).all()
    if found:
        details = ", ".join(f"{row.kind}={row.n}" for row in found)
        raise RuntimeError(
            f"refusing to retire remaining-metrics kinds with existing "
            f"{schema}.river_job rows (any state) -- CHAOS-4243's premise is "
            f"that no producer ever enqueued them: {details}"
        )


def upgrade() -> None:
    _assert_no_pending_outbox_rows_for_retired_kinds()
    _assert_no_river_job_rows_for_retired_kinds()
    routes = _routes()
    op.execute(routes.delete().where(routes.c.job_kind.in_(_RETIRED_KINDS)))
    op.drop_constraint(
        "ck_remaining_metric_run_family", "remaining_metric_runs", type_="check"
    )
    op.create_check_constraint(
        "ck_remaining_metric_run_family",
        "remaining_metric_runs",
        _NARROW_FAMILY_CHECK,
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_remaining_metric_run_family", "remaining_metric_runs", type_="check"
    )
    op.create_check_constraint(
        "ck_remaining_metric_run_family",
        "remaining_metric_runs",
        _WIDE_FAMILY_CHECK,
    )
    routes = _routes()
    now = datetime.now(UTC)
    op.execute(
        routes.insert().values(
            [
                {
                    "job_kind": kind,
                    "transport": _SAFE_ROLLBACK_TRANSPORT,
                    "paused": False,
                    "generation": 1,
                    "updated_at": now,
                }
                for kind in _RETIRED_KINDS
            ]
        )
    )
