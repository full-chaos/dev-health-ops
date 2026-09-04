"""Seed the native work_item_attribution backstop route.

Revision ID: 0123
Revises: 0122
Create Date: 2026-09-04 00:00:00

CHAOS-3092 PR-B: metrics.remaining.work_item_attribution has no Celery
implementation or rollback route -- it re-derives work_item_team_attributions
for the staleness window the sync-time deriver's incremental watermark leaves
open, and is Go-native from day one (the retired Python daily sweep it
replaces was an unconditional full recompute, not a predecessor this kind
rolls back to). This revision creates its durable route directly on River,
on queue "metrics" (the queue every other native remaining-metrics kind --
capacity, complexity, dora, membership_backfill, recommendations,
release_impact -- already runs on), same pattern as 0094's
system.sync_coverage_refresh seed and 0115's
sync.team_repo_ownership_derivation seed.

Root-caused via a live reconciler failure (dev-health-reconciler exiting
at startup with "worker outbox database unavailable"): internal/jobroute's
Controller.DeferredKinds iterates every registered kind and queries
public.worker_job_routes for each -- a kind with no seeded row there fails
the whole reconciler step instantly (pgx.ErrNoRows -> ErrUnknownRoute ->
joboutbox.ErrUnavailable), and produces no Postgres server-side error to
find, because a SELECT returning zero rows isn't one. Existing route
decisions are never rewritten: the only accepted pre-existing state is the
same River route at a valid generation.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.selectable import TableClause

revision: str = "0123"
down_revision: str | None = "0122"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_KIND = "metrics.remaining.work_item_attribution"
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
    existing = (
        bind.execute(
            sa.select(
                routes.c.transport,
                routes.c.paused,
                routes.c.generation,
            )
            .where(routes.c.job_kind == _KIND)
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    if existing is not None:
        if existing["transport"] != _TRANSPORT or int(existing["generation"]) < 1:
            raise RuntimeError(
                f"worker job route {_KIND!r} conflicts with the native River route"
            )
        return

    bind.execute(
        routes.insert().values(
            job_kind=_KIND,
            transport=_TRANSPORT,
            paused=False,
            generation=1,
            updated_at=datetime.now(UTC),
        )
    )


def downgrade() -> None:
    # Remove only the untouched row this revision itself can have created.
    # A paused or generation-bumped row records a later operator decision and
    # cannot safely be inferred to belong to this migration.
    routes = _routes()
    op.get_bind().execute(
        routes.delete().where(
            routes.c.job_kind == _KIND,
            routes.c.transport == _TRANSPORT,
            routes.c.paused.is_(False),
            routes.c.generation == 1,
        )
    )
