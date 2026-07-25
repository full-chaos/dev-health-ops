"""Seed safe rollback routes for every checked-in worker job kind.

Revision ID: 0064
Revises: 0063

The Go outbox relay resolves every checked-in descriptor during startup.  Route
rows must therefore exist even for kinds whose implementation remains deferred.
All checked-in rollback transports at this revision are Celery.  The one active
canary, ``sync.provider_unit``, is deliberately kept on Celery unless an
operator has already made a valid generation-bumped canary decision.
"""

from __future__ import annotations

from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import RowMapping
from sqlalchemy.sql.selectable import TableClause

revision: str = "0064"
down_revision: str | None = "0063"
branch_labels = None
depends_on = None

_SAFE_ROLLBACK_TRANSPORT = "celery"
_SYNC_PROVIDER_KIND = "sync.provider_unit"
_KINDS = (
    "investment.chunk",
    "investment.dispatch",
    "investment.finalize",
    "investment.materialize",
    "metrics.daily_dispatch",
    "metrics.daily_finalize",
    "metrics.daily_partition",
    "metrics.remaining.capacity",
    "metrics.remaining.complexity",
    "metrics.remaining.dora",
    "metrics.remaining.extra_metrics",
    "metrics.remaining.membership_backfill",
    "metrics.remaining.recommendations",
    "metrics.remaining.release_impact",
    "metrics.remaining.team_metrics",
    "operational.billing_notification",
    "operational.webhook_delivery",
    "report.execute_on_demand",
    "report.execute_scheduled",
    "sync.provider_unit",
    "sync.team_autoimport",
    "system.heartbeat",
    "system.retention_cleanup",
    "workgraph.build",
)


def _routes() -> TableClause:
    return sa.table(
        "worker_job_routes",
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("paused", sa.Boolean()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )


def _is_seeded_baseline(row: RowMapping) -> bool:
    return (
        row["transport"] == _SAFE_ROLLBACK_TRANSPORT
        and not bool(row["paused"])
        and row["generation"] == 1
    )


def _is_valid_operator_managed_row(kind: str, row: RowMapping) -> bool:
    """Accept only states an authenticated route transition could have made."""

    if kind != _SYNC_PROVIDER_KIND or bool(row["paused"]) or row["generation"] < 2:
        return False
    return row["transport"] in {_SAFE_ROLLBACK_TRANSPORT, "river_canary"}


def _existing_rows(routes: TableClause) -> dict[str, RowMapping]:
    bind = op.get_bind()
    return {
        row["job_kind"]: row
        for row in bind.execute(
            sa.select(
                routes.c.job_kind,
                routes.c.transport,
                routes.c.paused,
                routes.c.generation,
            ).where(routes.c.job_kind.in_(_KINDS))
        ).mappings()
    }


def upgrade() -> None:
    routes = _routes()
    existing = _existing_rows(routes)

    # Validate before inserting so direct execution, too, cannot leave a
    # partially seeded table when an operator-owned or corrupt route conflicts.
    for kind, row in existing.items():
        if _is_seeded_baseline(row) or _is_valid_operator_managed_row(kind, row):
            continue
        raise RuntimeError(f"worker job route {kind!r} conflicts with safe baseline")

    missing = [kind for kind in _KINDS if kind not in existing]
    if not missing:
        return
    now = datetime.now(UTC)
    op.get_bind().execute(
        routes.insert(),
        [
            {
                "job_kind": kind,
                "transport": _SAFE_ROLLBACK_TRANSPORT,
                "paused": False,
                "generation": 1,
                "updated_at": now,
            }
            for kind in missing
        ],
    )


def downgrade() -> None:
    # The table records no seed provenance.  Deleting a baseline-looking row
    # could erase an operator's intentional rollback, so this downgrade is
    # deliberately conservative and leaves all route decisions intact.
    return
