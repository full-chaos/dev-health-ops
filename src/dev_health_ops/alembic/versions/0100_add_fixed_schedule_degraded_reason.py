"""Persist fixed-schedule degraded verdicts with their occurrence.

Revision ID: 0100
Revises: 0099
Create Date: 2026-08-13 00:00:00

The occurrence row is the replica-safe transaction boundary for a producer
evaluation. A separate nullable column is required because ``skip_reason``
describes why an occurrence produced no work, while a degraded condition may
coexist with materialized work.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0100"
down_revision: str | None = "0099"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "fixed_schedule_occurrences"
_COLUMN = "degraded_reason"


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column(_COLUMN, sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(_TABLE, _COLUMN)
