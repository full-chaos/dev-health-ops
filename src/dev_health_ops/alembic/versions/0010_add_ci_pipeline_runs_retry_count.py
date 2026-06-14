"""Add retry_count column to ci_pipeline_runs for TestOps rerun_rate.

Revision ID: 0010
Revises: 0009
Create Date: 2026-06-13 00:00:00

Background (CHAOS-2380)
-----------------------
``CiPipelineRun.retry_count`` was added to the SQLAlchemy model and is now
written on the *base* ``sync_cicd`` path (``CicdMixin.insert_ci_pipeline_runs``
includes it in both the inserted row and the ON CONFLICT update set). The
ClickHouse side already migrates this column idempotently in
``migrations/clickhouse/029_testops_tables.sql``
(``ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS retry_count ...``),
but Postgres had no matching migration.

The git data-plane tables (``repos``, ``ci_pipeline_runs``, ``deployments``,
``incidents``, ``security_alerts`` …) are *not* created by the Alembic 0001
initial schema — they are bootstrapped via ``Base.metadata.create_all``.
``create_all`` only creates missing *tables*; it never adds a new *column* to a
table that already exists. So an already-provisioned Postgres deployment whose
``ci_pipeline_runs`` table predates this column would generate an
INSERT/ON CONFLICT statement referencing a nonexistent ``retry_count`` column
and fail the whole base CI/CD sync batch (not merely report a flat rerun_rate).

This migration closes that gap idempotently: it adds
``ci_pipeline_runs.retry_count INTEGER NOT NULL DEFAULT 0`` only when the table
exists and the column is absent. It is therefore a no-op on:
  * fresh deployments where ``create_all`` already materialised the column
    (the model now declares it), and
  * deployments that ran this migration once already.

Mirrors the ClickHouse ``UInt32 DEFAULT 0`` semantics and the model's
``server_default="0"``.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "ci_pipeline_runs"
_COLUMN = "retry_count"


def _has_column(conn: sa.Connection, table: str, column: str) -> bool:
    inspector = sa.inspect(conn)
    if table not in inspector.get_table_names():
        # Table not bootstrapped yet (create_all will materialise it with the
        # column already present). Nothing to add.
        return True
    return any(col["name"] == column for col in inspector.get_columns(table))


def upgrade() -> None:
    conn = op.get_bind()
    if _has_column(conn, _TABLE, _COLUMN):
        return
    op.add_column(
        _TABLE,
        sa.Column(
            _COLUMN,
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="number of automatic re-runs for this pipeline run "
            "(GitHub run_attempt - 1); drives TestOps rerun_rate",
        ),
    )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if _TABLE not in inspector.get_table_names():
        return
    if any(col["name"] == _COLUMN for col in inspector.get_columns(_TABLE)):
        op.drop_column(_TABLE, _COLUMN)
