"""Seed the safe sync-provider route baseline and audit actions.

Revision ID: 0061
Revises: 0060
"""

from __future__ import annotations

from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import RowMapping
from sqlalchemy.sql.selectable import TableClause

revision: str = "0061"
down_revision: str | None = "0060"
branch_labels = None
depends_on = None

_KIND = "sync.provider_unit"
_ROUTE_ACTIONS = (
    "jobs.cancel",
    "jobs.retry",
    "queues.pause",
    "queues.resume",
    "workers.drain",
    "job_routes.apply_checked_in",
    "job_routes.rollback",
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
        row["transport"] == "celery"
        and not bool(row["paused"])
        and row["generation"] == 1
    )


def _seed_baseline_route() -> None:
    bind = op.get_bind()
    routes = _routes()
    row = (
        bind.execute(
            sa.select(routes.c.transport, routes.c.paused, routes.c.generation).where(
                routes.c.job_kind == _KIND
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        bind.execute(
            routes.insert().values(
                job_kind=_KIND,
                transport="celery",
                paused=False,
                generation=1,
                updated_at=datetime.now(UTC),
            )
        )
        return
    if not _is_seeded_baseline(row):
        raise RuntimeError("sync provider route conflicts with safe Celery baseline")


def _remove_seeded_baseline_route() -> None:
    bind = op.get_bind()
    routes = _routes()
    row = (
        bind.execute(
            sa.select(routes.c.transport, routes.c.paused, routes.c.generation).where(
                routes.c.job_kind == _KIND
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return
    if not _is_seeded_baseline(row):
        raise RuntimeError("sync provider route changed after baseline seed")
    bind.execute(routes.delete().where(routes.c.job_kind == _KIND))


def _replace_audit_action_constraint(actions: tuple[str, ...]) -> None:
    with op.batch_alter_table("worker_operator_audits") as batch_op:
        batch_op.drop_constraint("ck_worker_operator_audits_action", type_="check")
        quoted = ", ".join(f"'{action}'" for action in actions)
        batch_op.create_check_constraint(
            "ck_worker_operator_audits_action", f"action IN ({quoted})"
        )


def upgrade() -> None:
    _replace_audit_action_constraint(_ROUTE_ACTIONS)
    _seed_baseline_route()


def downgrade() -> None:
    _remove_seeded_baseline_route()
    _replace_audit_action_constraint(_ROUTE_ACTIONS[:5])
