"""Durable reservations and reconciled usage for organization BYO LLM budgets."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .git import GUID, Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


class BYOLLMBudgetReservation(Base):
    """One provider attempt's committed maximum exposure and final usage."""

    __tablename__ = "byo_llm_budget_reservations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    reserved_micro_usd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actual_micro_usd: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="reserved")
    pricing_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cached_input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    reconciled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "window_start",
            "idempotency_key",
            name="uq_byo_llm_budget_reservation_attempt",
        ),
        CheckConstraint(
            "status IN ('reserved', 'succeeded', 'failed', 'cancelled', "
            "'voided', 'usage_unavailable')",
            name="ck_byo_llm_budget_reservation_status",
        ),
        CheckConstraint(
            "reserved_micro_usd >= 0",
            name="ck_byo_llm_budget_reserved_nonnegative",
        ),
        CheckConstraint(
            "actual_micro_usd IS NULL OR actual_micro_usd >= 0",
            name="ck_byo_llm_budget_actual_nonnegative",
        ),
        CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_byo_llm_budget_input_nonnegative",
        ),
        CheckConstraint(
            "cached_input_tokens IS NULL OR cached_input_tokens >= 0",
            name="ck_byo_llm_budget_cached_input_nonnegative",
        ),
        CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_byo_llm_budget_output_nonnegative",
        ),
        CheckConstraint(
            "cached_input_tokens IS NULL OR input_tokens IS NULL OR "
            "cached_input_tokens <= input_tokens",
            name="ck_byo_llm_budget_cached_within_input",
        ),
        CheckConstraint(
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
        Index(
            "ix_byo_llm_budget_org_window_status",
            "org_id",
            "window_start",
            "status",
        ),
    )


class QUAShadowBudgetReservation(Base):
    """One QUA shadow-call attempt's committed maximum exposure and final
    usage (CHAOS-3452).

    Structurally identical in shape to ``BYOLLMBudgetReservation`` -- same
    reservation/reconciliation lifecycle, same CHECK-constraint discipline --
    but a DELIBERATELY SEPARATE table. ``get_budget_status`` in
    ``llm/budget.py`` sums every reservation for an (org_id, window_start)
    pair regardless of provider/model, so a shadow attempt recorded in the
    live ``byo_llm_budget_reservations`` table would draw down the same
    organization monetary ceiling the live investigation call needs (the
    exact CHAOS-3389 incident this ticket closes -- design comment
    16bd8fed). This table is read and written ONLY by
    ``llm/qua_shadow_budget.py``; nothing in the live BYO budget path
    (``llm/budget.py``, ``attach_agent_budget_guard``, ``guard_byo_call``)
    ever queries it.
    """

    __tablename__ = "dev_qua_shadow_budget_reservations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    reserved_micro_usd: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actual_micro_usd: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="reserved")
    pricing_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cached_input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    reconciled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "window_start",
            "idempotency_key",
            name="uq_qua_shadow_budget_reservation_attempt",
        ),
        CheckConstraint(
            "status IN ('reserved', 'succeeded', 'failed', 'cancelled', "
            "'voided', 'usage_unavailable')",
            name="ck_qua_shadow_budget_reservation_status",
        ),
        CheckConstraint(
            "reserved_micro_usd >= 0",
            name="ck_qua_shadow_budget_reserved_nonnegative",
        ),
        CheckConstraint(
            "actual_micro_usd IS NULL OR actual_micro_usd >= 0",
            name="ck_qua_shadow_budget_actual_nonnegative",
        ),
        CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_qua_shadow_budget_input_nonnegative",
        ),
        CheckConstraint(
            "cached_input_tokens IS NULL OR cached_input_tokens >= 0",
            name="ck_qua_shadow_budget_cached_input_nonnegative",
        ),
        CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_qua_shadow_budget_output_nonnegative",
        ),
        CheckConstraint(
            "cached_input_tokens IS NULL OR input_tokens IS NULL OR "
            "cached_input_tokens <= input_tokens",
            name="ck_qua_shadow_budget_cached_within_input",
        ),
        CheckConstraint(
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
        Index(
            "ix_qua_shadow_budget_org_window_status",
            "org_id",
            "window_start",
            "status",
        ),
    )
