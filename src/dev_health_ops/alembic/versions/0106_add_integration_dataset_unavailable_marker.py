"""Surface repeated provider_dataset_unavailable terminalizations on the
IntegrationDataset row the owner-facing API already reads, instead of only
per-unit failure rows nobody watches (CHAOS-4048).

Revision ID: 0106
Revises: 0105

Nullable, no backfill: a dataset with no recorded outage has no marker, same
as any dataset synced before this migration. unavailable_since is set once
per outage and unavailable_last_seen_at advances on every repeat failure;
both are cleared by the next successful unit for that dataset (self-heal,
not an operator action).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0106"
down_revision: str | None = "0105"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "integration_datasets"


def upgrade() -> None:
    op.add_column(
        _TABLE, sa.Column("unavailable_reason", sa.String(length=64), nullable=True)
    )
    op.add_column(
        _TABLE,
        sa.Column("unavailable_since", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        _TABLE,
        sa.Column(
            "unavailable_last_seen_at", sa.DateTime(timezone=True), nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column(_TABLE, "unavailable_last_seen_at")
    op.drop_column(_TABLE, "unavailable_since")
    op.drop_column(_TABLE, "unavailable_reason")
