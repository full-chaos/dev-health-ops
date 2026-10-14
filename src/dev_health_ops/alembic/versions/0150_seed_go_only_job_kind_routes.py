"""Seed the worker_job_routes rows of the Go-only job kinds.

Revision ID: 0150
Revises: 0149
Create Date: 2026-10-13 00:00:00

CHAOS-9607. system.llm_budget_stale_reservations (CHAOS-9476) is registered
Go-only (registry.json go_only, migration-state.json river/none) and shipped
with no worker_job_routes row. The Go migrator (dev-health-worker-migrate,
``migrate river``) seeds such a row, but only when MIGRATION_DATABASE_URI is
configured; where that step is skipped, internal/jobroute's
Controller.DeferredKinds finds no row for the kind on its first reconciler step
and every Go worker and the reconciler exit with "job route is unknown". This
revision seeds the row in the Alembic chain, the way 0094, 0115 and 0123 did
for their kinds, so the route no longer depends on that step. It also seeds the
other Go-only kind, system.dimension_fold, which had the same dependency.

The seed is the River route the Go migrator would insert (transport "river",
not paused, generation 1). A row that already exists (the Go migrator ran
first) is left exactly as it is; a row on another transport is a conflict.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.selectable import TableClause

revision: str = "0150"
down_revision: str | None = "0149"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_KINDS = (
    "system.dimension_fold",
    "system.llm_budget_stale_reservations",
)
_TRANSPORT = "river"


def _routes() -> TableClause:
    return sa.table(
        "worker_job_routes",
        sa.column("job_kind", sa.String()),
        sa.column("transport", sa.String()),
        sa.column("paused", sa.Boolean()),
        sa.column("generation", sa.BigInteger()),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )


def upgrade() -> None:
    routes = _routes()
    bind = op.get_bind()
    existing = {
        row["job_kind"]: row
        for row in bind.execute(
            sa.select(
                routes.c.job_kind,
                routes.c.transport,
                routes.c.generation,
            )
            .where(routes.c.job_kind.in_(_KINDS))
            .with_for_update()
        ).mappings()
    }
    for kind, row in existing.items():
        if row["transport"] != _TRANSPORT or int(row["generation"]) < 1:
            raise RuntimeError(
                f"worker job route {kind!r} conflicts with the native River route"
            )
    missing = [kind for kind in _KINDS if kind not in existing]
    if missing:
        now = datetime.now(UTC)
        bind.execute(
            routes.insert(),
            [
                {
                    "job_kind": kind,
                    "transport": _TRANSPORT,
                    "paused": False,
                    "generation": 1,
                    "updated_at": now,
                }
                for kind in missing
            ],
        )
    # Heap order of this tiny table follows its update history (the River
    # activation rewrote every row), so a new row lands at a position that a
    # table rebuilt from the baseline dump does not reproduce. Rewriting the
    # heap in primary key order gives both the same physical order; the
    # clustering flag CLUSTER leaves on the index is cleared again.
    op.execute(sa.text("CLUSTER worker_job_routes USING worker_job_routes_pkey"))
    op.execute(sa.text("ALTER TABLE worker_job_routes SET WITHOUT CLUSTER"))


def downgrade() -> None:
    # Remove only untouched rows this revision can have created. A paused or
    # generation-bumped row records a later operator decision.
    routes = _routes()
    op.get_bind().execute(
        routes.delete().where(
            routes.c.job_kind.in_(_KINDS),
            routes.c.transport == _TRANSPORT,
            routes.c.paused.is_(False),
            routes.c.generation == 1,
        )
    )
