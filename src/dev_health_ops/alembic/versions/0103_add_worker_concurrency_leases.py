"""Add durable cross-process Go worker concurrency leases.

Revision ID: 0103
Revises: 0102
Create Date: 2026-08-15 00:00:00

The table contains only bounded policy identity and lease state. It never
stores River arguments, job payloads, provider credentials, or error text.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0103"
down_revision: str | None = "0102"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "worker_concurrency_leases"
_INDEX = "ix_worker_concurrency_leases_budget_expiry"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("budget_key", sa.String(length=320), nullable=False),
        sa.Column("job_kind", sa.String(length=96), nullable=False),
        sa.Column("concurrency_scope", sa.String(length=16), nullable=False),
        sa.Column("organization_id", UUID(as_uuid=True), nullable=True),
        sa.Column("owner_token", UUID(as_uuid=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "concurrency_scope IN ('fleet', 'organization')",
            name="ck_worker_concurrency_lease_scope",
        ),
        sa.CheckConstraint(
            "(concurrency_scope = 'fleet' AND organization_id IS NULL) OR "
            "(concurrency_scope = 'organization' AND organization_id IS NOT NULL)",
            name="ck_worker_concurrency_lease_scope_org",
        ),
        sa.CheckConstraint(
            "length(budget_key) BETWEEN 1 AND 320",
            name="ck_worker_concurrency_lease_budget_key",
        ),
        sa.CheckConstraint(
            "length(job_kind) BETWEEN 1 AND 96",
            name="ck_worker_concurrency_lease_job_kind",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("owner_token", name="uq_worker_concurrency_lease_owner"),
    )
    op.create_index(_INDEX, _TABLE, ["budget_key", "lease_expires_at"])


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
