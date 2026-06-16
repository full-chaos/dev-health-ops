"""Add ScheduledJob unique constraint for sync dispatch markers.

Revision ID: 0013
Revises: 0012
Create Date: 2026-06-16 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "scheduled_jobs"
_CONSTRAINT = "uq_scheduled_job_org_sync_config_type"


def _has_unique_constraint(conn: sa.Connection, table: str, name: str) -> bool:
    inspector = sa.inspect(conn)
    if table not in inspector.get_table_names():
        return True
    return any(
        constraint.get("name") == name
        for constraint in inspector.get_unique_constraints(table)
    )


def upgrade() -> None:
    conn = op.get_bind()
    if conn.dialect.name != "postgresql":
        return
    if _has_unique_constraint(conn, _TABLE, _CONSTRAINT):
        return

    op.execute(
        """
        DELETE FROM scheduled_jobs
        WHERE id IN (
            SELECT id FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_id, sync_config_id, job_type
                        ORDER BY updated_at DESC, created_at DESC
                    ) AS rn
                FROM scheduled_jobs
                WHERE sync_config_id IS NOT NULL
            ) ranked
            WHERE rn > 1
        )
        """
    )
    op.create_unique_constraint(
        _CONSTRAINT,
        _TABLE,
        ["org_id", "sync_config_id", "job_type"],
    )


def downgrade() -> None:
    conn = op.get_bind()
    if conn.dialect.name != "postgresql":
        return
    if _has_unique_constraint(conn, _TABLE, _CONSTRAINT):
        op.drop_constraint(_CONSTRAINT, _TABLE, type_="unique")
