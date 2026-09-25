"""Refunds: write the row before Stripe answers, and key client retries.

Revision ID: 0139
Revises: 0138

The refund route writes its row, pending, before it calls Stripe, so a
refund Stripe makes always has a local row. Until Stripe answers, the row
has no Stripe refund or charge id: ``stripe_refund_id`` and
``stripe_charge_id`` become nullable (the unique constraint on
``stripe_refund_id`` stays; NULLs do not collide). A client's
``Idempotency-Key`` is stored in the new ``idempotency_key`` column, unique
per org where set, so a retried request finds its first refund. No data
changes.

Downgrade restores NOT NULL and drops the column and its index. It fails
while a row without a Stripe id exists.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0139"
down_revision: str | None = "0138"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "uq_refunds_org_idempotency_key"


def upgrade() -> None:
    op.alter_column(
        "refunds", "stripe_refund_id", existing_type=sa.Text(), nullable=True
    )
    op.alter_column(
        "refunds", "stripe_charge_id", existing_type=sa.Text(), nullable=True
    )
    op.add_column("refunds", sa.Column("idempotency_key", sa.Text(), nullable=True))
    op.create_index(
        _INDEX,
        "refunds",
        ["org_id", "idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="refunds")
    op.drop_column("refunds", "idempotency_key")
    op.alter_column(
        "refunds", "stripe_charge_id", existing_type=sa.Text(), nullable=False
    )
    op.alter_column(
        "refunds", "stripe_refund_id", existing_type=sa.Text(), nullable=False
    )
