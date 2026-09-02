"""Add a durable completion fence to billing_notifications.

Revision ID: 0122
Revises: 0121
Create Date: 2026-09-02 00:00:00

A lost HTTP response between the Go operational worker and the internal
Python bridge (dev_health_ops.api.internal.worker_operational) leaves Go
retrying a billing_notification job whose email Python already sent, because
nothing durable recorded that the send completed. ``completed_at`` is that
fence: set once, atomically, only after the email dispatch call returns
successfully; a retry that reloads the same row sees it set and skips
re-sending.
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
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("billing_notifications", "completed_at")
