"""Add dev_qua_shadow_budget_reservations (CHAOS-3452 isolated shadow quota).

Revision ID: 0087
Revises: 0086
Create Date: 2026-08-06 00:00:00

Additive only: one new reservation table, structurally identical to 0071's
``byo_llm_budget_reservations`` (same reservation/reconciliation CHECK
constraints) but deliberately SEPARATE -- see
``dev_health_ops.models.llm_budget.QUAShadowBudgetReservation`` and
``dev_health_ops.llm.qua_shadow_budget`` for why a shadow attempt must never
be recorded in, or read back from, the live BYO budget table.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0087"
down_revision: str | None = "0086"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dev_qua_shadow_budget_reservations",
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
            name="ck_qua_shadow_budget_reservation_status",
        ),
        sa.CheckConstraint(
            "reserved_micro_usd >= 0",
            name="ck_qua_shadow_budget_reserved_nonnegative",
        ),
        sa.CheckConstraint(
            "actual_micro_usd IS NULL OR actual_micro_usd >= 0",
            name="ck_qua_shadow_budget_actual_nonnegative",
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_qua_shadow_budget_input_nonnegative",
        ),
        sa.CheckConstraint(
            "cached_input_tokens IS NULL OR cached_input_tokens >= 0",
            name="ck_qua_shadow_budget_cached_input_nonnegative",
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_qua_shadow_budget_output_nonnegative",
        ),
        sa.CheckConstraint(
            "cached_input_tokens IS NULL OR input_tokens IS NULL OR "
            "cached_input_tokens <= input_tokens",
            name="ck_qua_shadow_budget_cached_within_input",
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
            name="ck_qua_shadow_budget_reconciliation_state",
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "window_start",
            "idempotency_key",
            name="uq_qua_shadow_budget_reservation_attempt",
        ),
    )
    op.create_index(
        "ix_qua_shadow_budget_org_window_status",
        "dev_qua_shadow_budget_reservations",
        ["org_id", "window_start", "status"],
    )


def downgrade() -> None:
    # Same posture as 0086's own downgrade guard: this table holds real
    # monetary reservation/usage evidence once the shadow flag is on in any
    # environment, so an unconditional drop would silently erase it on a
    # routine downgrade.
    bind = op.get_bind()
    if bind.dialect.has_table(bind, "dev_qua_shadow_budget_reservations"):
        row_count = bind.execute(
            sa.select(sa.func.count()).select_from(
                sa.table("dev_qua_shadow_budget_reservations")
            )
        ).scalar()
        if row_count:
            raise RuntimeError(
                f"refusing to downgrade 0087: dev_qua_shadow_budget_reservations "
                f"has {row_count} row(s); this downgrade is for pre-release "
                "rehearsal only"
            )
    op.drop_index(
        "ix_qua_shadow_budget_org_window_status",
        table_name="dev_qua_shadow_budget_reservations",
    )
    op.drop_table("dev_qua_shadow_budget_reservations")
