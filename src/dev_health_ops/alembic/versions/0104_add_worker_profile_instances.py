"""Add expiring Go worker profile process registrations.

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

_TABLE = "worker_profile_instances"
_INDEX = "ix_worker_profile_instances_profile_expiry"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("instance_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(profile) BETWEEN 1 AND 32",
            name="ck_worker_profile_instance_profile",
        ),
        sa.CheckConstraint(
            "state IN ('active', 'draining')",
            name="ck_worker_profile_instance_state",
        ),
        sa.PrimaryKeyConstraint("instance_id"),
    )
    op.create_index(_INDEX, _TABLE, ["profile", "expires_at"])


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
