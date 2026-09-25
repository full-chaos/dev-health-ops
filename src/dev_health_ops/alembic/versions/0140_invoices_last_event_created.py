"""Invoices: record the Stripe time of the newest applied event.

Revision ID: 0140
Revises: 0139

The Stripe invoice webhook applies an event only when it was created at or
after the newest event already applied to that invoice, so a delayed or
retried older event cannot overwrite newer state. ``last_event_created``
holds that Stripe ``created`` time (Unix seconds); NULL for rows written
before this revision, which accept the next event. No data changes.

Downgrade drops the column.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0140"
down_revision: str | None = "0139"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "invoices", sa.Column("last_event_created", sa.BigInteger(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("invoices", "last_event_created")
