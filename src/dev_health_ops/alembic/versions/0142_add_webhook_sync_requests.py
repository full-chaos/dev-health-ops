"""Add webhook_sync_requests (CHAOS-6695).

Revision ID: 0142
Revises: 0141

A webhook delivery that routes to a sync configuration used to write the
scheduled_jobs marker, the scheduled_sync_occurrences row and its
sync_manual_triggers payload from the webhook worker, which runs on the
domain role. Those are coordinator-owned scheduling writes under the Option B
role split, so the domain role could never make them (42501).

The webhook worker now records one durable request per delivery in this
table, on the domain role, and the Go scheduler claims it on the coordinator
role and mints the occurrence through the same hand-off every other trigger
uses (``internal/synchandoff``). The row stays until the mint commits (then it
is deleted in the same transaction) or the scheduler refuses it for good:

- ``attempts``/``next_attempt_at``/``last_error``: a failed mint is retried
  with backoff; ``last_error`` is stage-named and never carries payload data.
- ``refused_at``/``refused_reason``: a request the scheduler will not mint
  (too old, its configuration gone or inactive) keeps its row with the reason.

No data changes. Downgrade drops the table.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0142"
down_revision: str | None = "0141"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "webhook_sync_requests"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        # One request per webhook delivery: a redelivered job finds its row.
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("org_id", sa.Text(), nullable=False),
        sa.Column("sync_config_id", postgresql.UUID(as_uuid=True), nullable=False),
        # The manual trigger payload the scheduler mints with.
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("source_ids", postgresql.ARRAY(sa.Text()), nullable=True),
        # The delivery's own created_at: the occurrence identity derives from
        # it, so a replayed request mints the same occurrence.
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "attempts", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("refused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refused_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["sync_config_id"],
            ["sync_configurations.id"],
            name="fk_webhook_sync_requests_sync_config_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("delivery_id", name="pk_webhook_sync_requests"),
    )
    # The scheduler's claim reads pending rows in arrival order.
    op.create_index(
        "ix_webhook_sync_requests_pending",
        _TABLE,
        ["created_at"],
        postgresql_where=sa.text("refused_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_webhook_sync_requests_pending", table_name=_TABLE)
    op.drop_table(_TABLE)
