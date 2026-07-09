"""Add bounded-recompute visibility columns + job log (CHAOS-2699).

Extends CHAOS-2694's ``external_ingest_batches`` table with a summary of the
bounded recompute triggered for that batch, plus a companion
``external_ingest_recompute_jobs`` table logging each individual Celery
dispatch (a single debounced flush can fan out to N per-repo
``run_daily_metrics``/``run_work_graph_build`` chains + one
``dispatch_investment_materialize_partitioned`` call).

``recompute_status`` enum is pinned epic-wide (master-spec CC21):
``not_applicable | pending | dispatched | skipped_no_scope | failed``,
server_default ``'not_applicable'`` (a batch whose record kinds never
trigger recompute -- e.g. ``repository.v1``-only -- stays at the default
forever).

Renumbered from this issue's original 0033 sketch: 0033 = CHAOS-2696 (ingest
auth), 0034 = CHAOS-2694 (status store) -- this is 0035, chained onto 0034.

Guarded per the 0025/0030/0031/0032/0033 create-if-missing convention so a
partial rerun resumes cleanly.

Revision ID: 0035
Revises: 0034
Create Date: 2026-07-02 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0035"
down_revision: str | None = "0034"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_BATCHES_TABLE = "external_ingest_batches"
_JOBS_TABLE = "external_ingest_recompute_jobs"
_JOBS_SCOPE_INDEX = "ix_external_ingest_recompute_jobs_scope"


def upgrade() -> None:
    _add_column_if_missing(
        _BATCHES_TABLE,
        sa.Column(
            "recompute_status",
            sa.Text(),
            nullable=False,
            server_default="not_applicable",
        ),
    )
    _add_column_if_missing(
        _BATCHES_TABLE, sa.Column("recompute_scope", sa.JSON(), nullable=True)
    )
    _add_column_if_missing(
        _BATCHES_TABLE,
        sa.Column("recompute_dispatched_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        _BATCHES_TABLE,
        sa.Column("recompute_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        _BATCHES_TABLE, sa.Column("recompute_error", sa.Text(), nullable=True)
    )

    if not _table_exists(_JOBS_TABLE):
        op.create_table(
            _JOBS_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("source_instance", sa.Text(), nullable=False),
            sa.Column("celery_task_name", sa.Text(), nullable=False),
            # Nullable: dispatch_recompute() can legitimately produce a
            # per-repo daily-metrics job whose Celery AsyncResult has no
            # `.parent` (chain result-metadata unavailable), in which case
            # the task id it captured is None -- the Celery dispatch itself
            # still succeeded, so this must not block persisting the
            # recompute outcome (adversarial-review finding).
            sa.Column("celery_task_id", sa.Text(), nullable=True),
            sa.Column("queue", sa.Text(), nullable=False),
            sa.Column("repo_id", sa.Text(), nullable=True),
            sa.Column("status", sa.Text(), nullable=False, server_default="dispatched"),
            sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
    _create_index_if_missing(
        _JOBS_SCOPE_INDEX,
        _JOBS_TABLE,
        ["org_id", "source_system", "source_instance", "dispatched_at"],
    )


def downgrade() -> None:
    if _table_exists(_JOBS_TABLE):
        op.drop_table(_JOBS_TABLE)
    for col in (
        "recompute_status",
        "recompute_scope",
        "recompute_dispatched_at",
        "recompute_completed_at",
        "recompute_error",
    ):
        if _table_exists(_BATCHES_TABLE) and col in _column_names(_BATCHES_TABLE):
            op.drop_column(_BATCHES_TABLE, col)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if column.name not in _column_names(table_name):
        op.add_column(table_name, column)


def _create_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing_indexes = {
        index["name"] for index in sa.inspect(bind).get_indexes(table_name)
    }
    if index_name not in existing_indexes:
        op.create_index(index_name, table_name, columns)
