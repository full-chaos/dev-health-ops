"""Seed the native sync-coverage refresh route.

Revision ID: 0094
Revises: 0093
Create Date: 2026-08-12 00:00:00

The historical 0064 baseline is deliberately pinned to the 24 job kinds that
existed when it shipped.  Sync coverage refresh has no Celery implementation
or rollback route, so this revision creates its durable route directly on
River.  Existing route decisions are never rewritten: the only accepted
pre-existing state is the same River route at a valid generation.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.selectable import TableClause

revision: str = "0094"
down_revision: str | None = "0093"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_KIND = "system.sync_coverage_refresh"
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
