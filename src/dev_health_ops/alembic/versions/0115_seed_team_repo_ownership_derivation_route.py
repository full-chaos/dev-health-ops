"""Seed the native team_repo_ownership-derivation route.

Revision ID: 0115
Revises: 0114
Create Date: 2026-08-28 00:00:00

CHAOS-4365 item 1b: sync.team_repo_ownership_derivation has no Celery
implementation or rollback route -- it re-derives team_repo_ownership from
already-synced ClickHouse data (team_project_ownership, work_items,
work_item_dependencies, work_graph_issue_pr), never fetches from a
provider, and is Go-native from day one. This revision creates its
durable route directly on River, on queue "sync" (the queue every other
post-sync-fanout handoff -- daily, complexity, workgraph, investment,
membership_backfill, dora, team_autoimport -- already runs on), same
pattern as 0094's system.sync_coverage_refresh seed. Existing route
decisions are never rewritten: the only accepted pre-existing state is
the same River route at a valid generation.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.selectable import TableClause

revision: str = "0115"
down_revision: str | None = "0114"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_KIND = "sync.team_repo_ownership_derivation"
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
