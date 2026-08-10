"""Persist public Ask Dev SSE events for cursor-based run resume."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0092"
down_revision: str | None = "0091"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


def upgrade() -> None:
    op.create_table(
        "dev_run_stream_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("event", sa.String(length=32), nullable=False),
        sa.Column("event_data", sa.JSON(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("sequence >= 0", name="ck_dev_run_stream_events_sequence"),
        sa.ForeignKeyConstraint(
            ["run_id", "org_id", "user_id"],
            ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
            name="fk_dev_run_stream_events_run_owner",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "sequence", name="uq_dev_run_stream_events_run_sequence"
        ),
    )
    op.create_index(
        "ix_dev_run_stream_events_owner_run_sequence",
        "dev_run_stream_events",
        ["org_id", "user_id", "run_id", "sequence"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_dev_run_stream_events_owner_run_sequence",
        table_name="dev_run_stream_events",
    )
    op.drop_table("dev_run_stream_events")
