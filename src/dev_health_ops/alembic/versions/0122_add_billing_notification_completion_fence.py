"""Add a durable claim/completion fence to billing_notifications.

Revision ID: 0122
Revises: 0121
Create Date: 2026-09-02 00:00:00

A lost HTTP response between the Go operational worker and the internal
Python bridge (dev_health_ops.api.internal.worker_operational) leaves Go
retrying a billing_notification job whose email Python already sent, because
nothing durable recorded that the send completed.

Two columns, kept deliberately separate so a stuck attempt is a queryable,
observable state rather than indistinguishable from a real completion:

``claimed_at`` is the dedup gate — claimed atomically, via
``UPDATE ... WHERE claimed_at IS NULL``, BEFORE the email send is attempted.
Its rowcount, not a prior read, is the decision, so two concurrent attempts
can't both win.

``completed_at`` is set only AFTER the send call returns successfully. A row
with ``claimed_at`` set and ``completed_at`` still NULL is either genuinely
in flight or a stale/crashed claim if old enough (see
``system_ops.py``'s ``_STALE_CLAIM_THRESHOLD_SECONDS``).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0122"
down_revision = "0121"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "billing_notifications",
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "billing_notifications",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("billing_notifications", "completed_at")
    op.drop_column("billing_notifications", "claimed_at")
