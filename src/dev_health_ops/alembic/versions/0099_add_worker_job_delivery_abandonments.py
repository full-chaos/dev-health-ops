"""Retain minimal evidence for abandoned worker-job delivery.

Revision ID: 0099
Revises: 0098
Create Date: 2026-08-13 00:00:00

Terminal outbox rows have bounded retention, but deleting a dead row must not
make an exhausted delivery indistinguishable from one that was never
published. This write-once table keeps only the stable identity and bounded
terminal metadata; job arguments and detailed errors remain subject to outbox
retention.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0099"
down_revision: str | None = "0098"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "worker_job_delivery_abandonments"
_INDEX = "ix_worker_job_delivery_abandonments_kind_time"
_ATTEMPT_COUNT_CHECK = "ck_worker_job_delivery_abandonments_attempt_count"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("dedupe_key", sa.String(length=256), nullable=False),
        sa.Column("job_kind", sa.String(length=96), nullable=False),
        sa.Column("abandoned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name=_ATTEMPT_COUNT_CHECK,
        ),
        sa.PrimaryKeyConstraint("dedupe_key"),
    )
    op.create_index(_INDEX, _TABLE, ["job_kind", "abandoned_at"])


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
