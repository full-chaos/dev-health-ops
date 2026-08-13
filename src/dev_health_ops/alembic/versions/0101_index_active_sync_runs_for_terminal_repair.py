"""Index active sync runs for bounded River terminal-delivery repair.

Revision ID: 0101
Revises: 0100
Create Date: 2026-08-13 00:00:00

The reconciler starts terminal-delivery repair from active sync runs. Retained
terminal history must not turn that one-second maintenance path into a table
scan, so the deployed schema needs the same status/id access path declared by
the SQLAlchemy model.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0101"
down_revision: str | None = "0100"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "sync_runs"
_INDEX = "ix_sync_runs_status_id"


def upgrade() -> None:
    op.create_index(_INDEX, _TABLE, ["status", "id"])


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
