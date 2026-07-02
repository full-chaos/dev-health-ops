"""Add provider_rate_limit_observations table (CHAOS-2758).

Durable, org-scoped store of provider rate-limit observations. Every
``RateLimitException`` that defers a sync unit writes one normalized row here
in the SAME transaction as the unit's RETRYING stamp
(``workers/sync_units.py``), so a later consumer (cross-unit cooldown gating,
CHAOS-2760) can consult recent provider/integration/route-family cooldowns.

Postgres, not ClickHouse -- every durable store the dispatch path already
consults is Postgres (``sync_dispatch_outbox`` 0020, the per-unit rate-limit
deferral columns 0022, ``sync_compute_checkpoints`` 0025), and BudgetGuard's
own reservation runs inside a Postgres advisory-lock transaction
(``sync/budget_guard.py``). See
``docs/providers/rate-limit-policy.md`` "Observation store" section.

Retry safety: table + index creation are guarded (per the 0025 / 0020
"create-if-missing" convention) so a rerun after a partial failure resumes
instead of failing on "relation already exists".

Revision ID: 0031
Revises: 0030
Create Date: 2026-07-01 00:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0031"
down_revision: str | None = "0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE_NAME = "provider_rate_limit_observations"
_COOLDOWN_INDEX_NAME = "ix_provider_rate_limit_observations_cooldown"
_ORG_INDEX_NAME = "ix_provider_rate_limit_observations_org_id"


def upgrade() -> None:
    if not _table_exists(_TABLE_NAME):
        op.create_table(
            _TABLE_NAME,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("provider", sa.Text(), nullable=False),
            sa.Column("host", sa.Text(), nullable=True),
            sa.Column("integration_id", UUID(as_uuid=True), nullable=False),
            sa.Column("sync_run_id", UUID(as_uuid=True), nullable=False),
            sa.Column("sync_run_unit_id", UUID(as_uuid=True), nullable=False),
            sa.Column("route_family", sa.Text(), nullable=True),
            sa.Column("dimension", sa.Text(), nullable=True),
            sa.Column("retry_after_seconds", sa.Float(), nullable=True),
            sa.Column("reset_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("request_id", sa.Text(), nullable=True),
            sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
    _create_index_if_missing(_ORG_INDEX_NAME, _TABLE_NAME, ["org_id"])
    _create_index_if_missing(
        _COOLDOWN_INDEX_NAME,
        _TABLE_NAME,
        ["provider", "integration_id", "route_family", "observed_at"],
    )


def downgrade() -> None:
    if _table_exists(_TABLE_NAME):
        op.drop_table(_TABLE_NAME)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _create_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing_indexes = {
        index["name"] for index in sa.inspect(bind).get_indexes(table_name)
    }
    if index_name not in existing_indexes:
        op.create_index(index_name, table_name, columns)
