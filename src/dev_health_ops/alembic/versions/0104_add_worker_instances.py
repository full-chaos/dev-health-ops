"""Add expiring Go worker-instance registrations.

Revision ID: 0104
Revises: 0103
Create Date: 2026-08-15 00:00:00

Rows contain bounded process identity and liveness only. They never contain
River arguments, job payloads, provider credentials, or error text.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0104"
down_revision: str | None = "0103"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "worker_instances"
_INDEX = "ix_worker_instances_group_expiry"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("instance_id", UUID(as_uuid=True), nullable=False),
        sa.Column("worker_group", sa.String(length=64), nullable=False),
        # The application stores a canonical, sorted, unique JSON queue array.
        # The migration only enforces that the value is non-empty because the
        # queue contract is owned by the Go worker boundary.
        sa.Column("queues", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(worker_group) BETWEEN 1 AND 64",
            name="ck_worker_instance_worker_group",
        ),
        sa.CheckConstraint(
            "length(queues) > 2",
            name="ck_worker_instance_queues",
        ),
        sa.CheckConstraint(
            "state IN ('accepting', 'draining')",
            name="ck_worker_instance_state",
        ),
        sa.PrimaryKeyConstraint("instance_id"),
    )
    op.create_index(_INDEX, _TABLE, ["worker_group", "expires_at"])


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
