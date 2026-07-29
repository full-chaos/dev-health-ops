"""Add durable reservations and usage for organization BYO LLM budgets.

Revision ID: 0071
Revises: 0070
Create Date: 2026-07-29 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0071"
down_revision: str | None = "0070"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "byo_llm_budget_reservations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=False),
        sa.Column("reserved_micro_usd", sa.BigInteger(), nullable=False),
        sa.Column("actual_micro_usd", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("pricing_version", sa.String(length=64), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("cached_input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('reserved', 'succeeded', 'failed', 'cancelled', "
            "'voided', 'usage_unavailable')",
            name="ck_byo_llm_budget_reservation_status",
        ),
        sa.CheckConstraint(
            "reserved_micro_usd >= 0",
            name="ck_byo_llm_budget_reserved_nonnegative",
        ),
        sa.CheckConstraint(
            "actual_micro_usd IS NULL OR actual_micro_usd >= 0",
            name="ck_byo_llm_budget_actual_nonnegative",
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_byo_llm_budget_input_nonnegative",
        ),
        sa.CheckConstraint(
            "cached_input_tokens IS NULL OR cached_input_tokens >= 0",
            name="ck_byo_llm_budget_cached_input_nonnegative",
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_byo_llm_budget_output_nonnegative",
        ),
        sa.CheckConstraint(
            "cached_input_tokens IS NULL OR input_tokens IS NULL OR "
            "cached_input_tokens <= input_tokens",
            name="ck_byo_llm_budget_cached_within_input",
        ),
        sa.CheckConstraint(
            "(status = 'reserved' AND actual_micro_usd IS NULL "
            "AND reconciled_at IS NULL) OR "
            "(status = 'usage_unavailable' AND actual_micro_usd IS NULL "
            "AND reconciled_at IS NOT NULL) OR "
            "(status = 'voided' AND actual_micro_usd = 0 "
            "AND input_tokens = 0 AND cached_input_tokens = 0 "
            "AND output_tokens = 0 AND reconciled_at IS NOT NULL) OR "
            "(status IN ('succeeded', 'failed', 'cancelled') "
            "AND actual_micro_usd IS NOT NULL AND input_tokens IS NOT NULL "
            "AND cached_input_tokens IS NOT NULL AND output_tokens IS NOT NULL "
            "AND reconciled_at IS NOT NULL)",
            name="ck_byo_llm_budget_reconciliation_state",
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "window_start",
            "idempotency_key",
            name="uq_byo_llm_budget_reservation_attempt",
        ),
    )
    op.create_index(
        "ix_byo_llm_budget_org_window_status",
        "byo_llm_budget_reservations",
        ["org_id", "window_start", "status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_byo_llm_budget_org_window_status",
        table_name="byo_llm_budget_reservations",
    )
    op.drop_table("byo_llm_budget_reservations")
