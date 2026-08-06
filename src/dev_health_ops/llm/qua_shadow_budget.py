"""CHAOS-3452: isolated shadow-quota budget guard for the QUA shadow seam.

Structurally separate from the live BYO LLM budget path in ``llm/budget.py``:
own reservation table (``QUAShadowBudgetReservation`` /
``dev_qua_shadow_budget_reservations``), own Postgres advisory-lock
namespace, own in-process per-org ``asyncio.Lock``, and its own operator-wide
monetary ceiling. A shadow call reserves against, exhausts, and is
reconciled ENTIRELY within this pool -- it can never read, contend with, or
draw down the live org's ``byo_llm_budget_reservations`` pool.

Why this had to be a genuinely separate pool, not just a separate provider
object: ``llm.budget.get_budget_status`` sums every reservation for an
``(org_id, window_start)`` pair regardless of provider/model, so recording a
shadow attempt in the live table would still consume the same organization
monetary ceiling the live investigation call needs -- the exact incident
CHAOS-3389 shipped ``provider=None`` to avoid (design comment 16bd8fed on
that ticket has the full rationale). This module is the follow-on that makes
a REAL provider safe to wire.

Conservative-by-default and NOT organization-configurable (no new admin
surface here -- out of scope for CHAOS-3452): the ceiling is a single
operator env var, defaulting to a few cents per organization per calendar
month, entirely independent of and unrelated to that organization's own
``BYO_LLM_MAX_BUDGET_MICRO_USD`` / configured monetary ceiling.

Fails CLOSED where the live path fails open: ``guard_byo_call`` lets an
unpriced custom provider through unbudgeted (preserving pre-budget
behavior for BYO configurations the pricing table doesn't cover).
``guard_qua_shadow_call`` never does that -- an unpriced provider/model pair
cannot be bounded, and staying bounded is this module's entire purpose, so
it resolves to ``BUDGET_UNAVAILABLE`` (a typed skip at the call site,
``QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED`` -- see ``qua_shadow.py``)
instead.

### Scope boundary, stated explicitly (Codex round 2, both CONFIRMED, both
### deliberately NOT fixed here -- tracked on CHAOS-3464, see the
### CHAOS-3452 design comment for the full rationale)

* **This isolates the LOCAL Postgres monetary reservation, not the
  underlying provider account.** For a BYO org, the shadow provider
  instance is built from the SAME credentials/base_url as the live one
  (``production_runtime.py``'s ``_build_qua_shadow_provider`` -- a
  genuinely separate Python object and HTTP client, never the SAME
  instance, but the SAME external account). This module cannot isolate
  that account's own provider-side rate limits, request quotas, or
  monetary credits -- only ``guard_byo_call``'s and
  ``guard_qua_shadow_call``'s OWN independent Postgres-tracked ceilings are
  isolated from each other. **For BYO orgs, this means the isolated
  shadow quota is spending the CUSTOMER'S OWN provider credits on our
  shadow experiment**, not just risking rate-limit contention with their
  live calls -- a real cost/trust surface, not merely a performance one.
  A genuinely separate credential/account for shadow traffic is the SAME
  "separately certified ``question_understanding`` role provider" the
  CHAOS-3389 design comment already named as explicit follow-on work, not
  new scope discovered here (tracked on CHAOS-3464). Bound in practice by
  construction: the shadow call is synchronously awaited inline within one
  investigation run (never fire-and-forget), so shadow request VOLUME
  against the shared account is structurally capped at exactly 1x live
  request volume for orgs with both wave_3_1 and the shadow flag on --
  never unbounded, never higher than doubling load on that account.
* **The admission reservation is a conservative ESTIMATE, not a proven
  upper bound.** It sizes ``maximum_input_tokens`` from
  ``len(json.dumps({messages, tools, response_schema}).encode())`` --
  exactly ``llm.budget.attach_agent_budget_guard``'s own, already-shipped
  formula for the live path, deliberately kept at parity rather than
  reimplemented, per CHAOS-3452's constraint against changing live-path
  behavior. Reconciliation after the real call already corrects the
  reservation to ACTUAL usage (or falls back to the reserved worst-case
  when usage is unavailable -- see ``qua_shadow_budget_status``), so a rare
  underestimate is a bounded post-hoc accounting drift, not an unbounded
  one; building a formula proven to always dominate the provider's own
  request-construction logic is deferred as the SAME kind of pre-existing,
  accepted estimate the live guard has always used (tracked as the second
  residual on CHAOS-3464 -- if ever tightened, tighten both guards
  together rather than letting shadow diverge from live).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from threading import Lock
from typing import Any, Final, TypeVar

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.llm_budget import QUAShadowBudgetReservation

from .agent.errors import AgentProviderError, AgentProviderErrorCode
from .budget import PRICING_VERSION, _window, cost_micro_usd

__all__ = [
    "DEFAULT_QUA_SHADOW_MAX_MICRO_USD",
    "QUA_SHADOW_BUDGET_ENV_KEY",
    "QUAShadowBudgetAccountingError",
    "QUAShadowBudgetExceeded",
    "attach_qua_shadow_budget_guard",
    "guard_qua_shadow_call",
    "qua_shadow_budget_status",
    "qua_shadow_maximum_micro_usd",
]

#: Config-gated: an unset env var means the conservative default below,
#: never "unlimited" (there is deliberately no env value that disables
#: enforcement -- see ``qua_shadow_maximum_micro_usd``).
QUA_SHADOW_BUDGET_ENV_KEY: Final = "ASK_DEV_QUA_SHADOW_MAX_BUDGET_MICRO_USD"
#: ~$0.50/organization/calendar month -- enough for a meaningful sample of
#: shadow evidence at the ~18-active-project scale this seam runs at today
#: (CHAOS-3389 design comment 16bd8fed), far below any live BYO ceiling an
#: org would configure for its real investigation traffic.
DEFAULT_QUA_SHADOW_MAX_MICRO_USD: Final = 500_000


class QUAShadowBudgetExceeded(RuntimeError):
    """The isolated shadow quota is exhausted for this organization/window."""


class QUAShadowBudgetAccountingError(RuntimeError):
    """A shadow reservation, pricing decision, or reconciliation failed."""


_T = TypeVar("_T")

#: Deliberately a SEPARATE ContextVar from llm.budget's own
#: ``_idempotency_key`` -- a caller binding one for the live call must never
#: leak into a concurrent shadow call sharing the same asyncio task tree.
_shadow_idempotency_key: ContextVar[str | None] = ContextVar(
    "qua_shadow_budget_idempotency_key", default=None
)

#: Deliberately a SEPARATE in-process lock table from llm.budget's own
#: ``_org_locks`` -- a live reservation transaction for an org must never
#: block behind (or be blocked by) a concurrent shadow reservation
#: transaction for the SAME org. Both are held only briefly, for the
#: reservation-admission step, never across the network call.
_shadow_locks_guard = Lock()
_shadow_org_locks: dict[str, asyncio.Lock] = {}


def qua_shadow_maximum_micro_usd() -> int:
    """The operator-wide shadow ceiling. Unset -> the conservative default."""

    raw = os.getenv(QUA_SHADOW_BUDGET_ENV_KEY)
    if raw is None:
        return DEFAULT_QUA_SHADOW_MAX_MICRO_USD
    try:
        value = int(raw)
    except ValueError:
        return 0
    return max(0, value)


def _shadow_org_lock(org_id: str) -> asyncio.Lock:
    with _shadow_locks_guard:
        lock = _shadow_org_locks.get(org_id)
        if lock is None:
            lock = asyncio.Lock()
            _shadow_org_locks[org_id] = lock
        return lock


async def _acquire_shadow_advisory_lock(session: AsyncSession, org_id: str) -> None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return
    # Deliberately a DIFFERENT hash input than llm/budget.py's own
    # "byo-llm-budget:{org_id}" advisory lock -- a live reservation and a
    # shadow reservation for the SAME org must never serialize on the same
    # Postgres advisory lock, or a slow/contended shadow reservation could
    # stall the live call's own reservation transaction.
    digest = hashlib.sha256(f"qua-shadow-llm-budget:{org_id}".encode()).digest()
    lock_key = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key}
    )


@dataclass(frozen=True, slots=True)
class QUAShadowBudgetStatus:
    used_micro_usd: int
    limit_micro_usd: int
    remaining_micro_usd: int
    exhausted: bool


async def qua_shadow_budget_status(
    session: AsyncSession, *, org_id: str, now: datetime | None = None
) -> QUAShadowBudgetStatus:
    effective_now = now or datetime.now(UTC)
    window_start, _ = _window(effective_now)
    limit = qua_shadow_maximum_micro_usd()
    try:
        org_uuid = uuid.UUID(org_id)
    except (TypeError, ValueError):
        return QUAShadowBudgetStatus(
            used_micro_usd=0,
            limit_micro_usd=limit,
            remaining_micro_usd=limit,
            exhausted=limit <= 0,
        )
    result = await session.execute(
        select(QUAShadowBudgetReservation).where(
            QUAShadowBudgetReservation.org_id == org_uuid,
            QUAShadowBudgetReservation.window_start == window_start,
        )
    )
    reservations = list(result.scalars().all())
    # Codex round 1 (HIGH, confirmed): matches llm.budget.get_budget_status's
    # OWN filter exactly -- ``!= "voided"``, nothing else excluded. A
    # ``usage_unavailable`` reservation is STILL a dispatched call whose real
    # cost could not be confirmed; counting it at its reserved (worst-case)
    # amount is what keeps it real monetary exposure until reconciled,
    # exactly as the live path's own comment states. The original
    # ``not in ("voided", "usage_unavailable")`` filter let repeated
    # dispatched-but-unreconciled shadow calls advance zero quota, silently
    # defeating the cap this module exists to enforce.
    used = sum(
        row.actual_micro_usd
        if row.actual_micro_usd is not None
        else row.reserved_micro_usd
        for row in reservations
        if row.status != "voided"
    )
    remaining = max(0, limit - used)
    return QUAShadowBudgetStatus(
        used_micro_usd=used,
        limit_micro_usd=limit,
        remaining_micro_usd=remaining,
        exhausted=remaining <= 0,
    )


async def guard_qua_shadow_call(
    *,
    session: AsyncSession,
    org_id: str,
    provider: str,
    model: str,
    base_url: str | None,
    idempotency_key: str,
    maximum_input_tokens: int,
    maximum_output_tokens: int,
    invoke: Callable[[], Awaitable[_T]],
    usage: Callable[[_T], tuple[int | None, int | None, int | None]],
) -> tuple[_T, int | None]:
    """Reserve against the ISOLATED shadow pool, execute, then reconcile.

    Mirrors ``llm.budget.guard_byo_call``'s reservation/execute/reconcile
    shape, but every step below touches ONLY
    ``QUAShadowBudgetReservation`` / ``dev_qua_shadow_budget_reservations``
    -- never ``BYOLLMBudgetReservation`` / ``byo_llm_budget_reservations``,
    never the live path's in-process lock or advisory-lock namespace.
    """

    maximum_cost = cost_micro_usd(
        provider=provider,
        model=model,
        base_url=base_url,
        input_tokens=max(0, maximum_input_tokens),
        output_tokens=max(0, maximum_output_tokens),
        cached_input_tokens=0,
    )
    reservation: QUAShadowBudgetReservation | None = None
    async with _shadow_org_lock(org_id):
        try:
            await _acquire_shadow_advisory_lock(session, org_id)
            if maximum_cost is None:
                await session.rollback()
                # Unlike the live path, an unpriced provider/model is never
                # let through unbudgeted here -- staying bounded is this
                # module's entire purpose (see module docstring).
                raise QUAShadowBudgetAccountingError(
                    "QUA shadow pricing is unavailable for this provider/model."
                )
            status = await qua_shadow_budget_status(session, org_id=org_id)
            if status.exhausted or maximum_cost > status.remaining_micro_usd:
                await session.rollback()
                raise QUAShadowBudgetExceeded(
                    "Isolated QUA shadow quota is exhausted for this organization."
                )
            reservation = await _create_shadow_reservation(
                session,
                org_id=org_id,
                provider=provider,
                model=model,
                idempotency_key=idempotency_key,
                reserved_micro_usd=maximum_cost,
            )
            await session.commit()
        except (QUAShadowBudgetAccountingError, QUAShadowBudgetExceeded):
            if session.in_transaction():
                await session.rollback()
            raise
        except Exception as accounting_exc:
            await session.rollback()
            raise QUAShadowBudgetAccountingError(
                "QUA shadow budget reservation is temporarily unavailable."
            ) from accounting_exc

    if reservation is None:
        raise QUAShadowBudgetAccountingError(
            "QUA shadow budget reservation was not created."
        )

    # The reservation is committed and both locks released before invoking
    # the provider -- same network transaction boundary as the live path.
    try:
        result = await invoke()
    except BaseException as exc:
        provider_dispatched = getattr(exc, "provider_dispatched", True)
        # Codex round 1 (HIGH, confirmed): a dispatched-but-failed call can
        # still carry REAL billable usage -- AgentProviderError.usage is the
        # SAME AgentUsage type AgentDecisionResult.usage is, so
        # ``_shadow_agent_usage`` (below) reads it identically; discarding
        # it here reconciled every such call as ``usage_unavailable``
        # regardless of whether real usage was available, one more way the
        # cap could be silently defeated before the ``used`` filter fix
        # above. Mirrors llm.budget's own exception-usage extraction
        # (``_usage_from_exception``).
        reported = (
            (0, 0, 0) if provider_dispatched is False else _shadow_agent_usage(exc)
        )
        outcome = (
            "voided"
            if provider_dispatched is False
            else ("cancelled" if isinstance(exc, asyncio.CancelledError) else "failed")
        )
        try:
            await _reconcile_shadow_reservation(
                session,
                reservation=reservation,
                provider=provider,
                model=model,
                base_url=base_url,
                outcome=outcome,
                reported=reported,
            )
        except Exception as accounting_exc:
            await session.rollback()
            raise QUAShadowBudgetAccountingError(
                "QUA shadow budget accounting is temporarily unavailable."
            ) from accounting_exc
        raise

    try:
        billed = await _reconcile_shadow_reservation(
            session,
            reservation=reservation,
            provider=provider,
            model=model,
            base_url=base_url,
            outcome="succeeded",
            reported=usage(result),
        )
    except Exception as accounting_exc:
        await session.rollback()
        raise QUAShadowBudgetAccountingError(
            "QUA shadow budget accounting is temporarily unavailable."
        ) from accounting_exc
    # Codex round 2 (MEDIUM, confirmed): unlike the live BYO guard -- where
    # an unpriceable SUCCESSFUL call must still fail closed to protect the
    # ORG'S OWN configured monetary ceiling (guard_byo_call's own
    # equivalent check) -- a successfully-decided shadow call is NEVER
    # discarded just because its cost could not be confirmed (e.g. the
    # provider omitted cache-usage detail, an OpenAICompatibleAgentProvider-
    # documented possibility via ``_normalize_usage``). The admission
    # reservation ALREADY counts its RESERVED, worst-case amount toward
    # `used` regardless of how reconciliation turns out (see
    # ``qua_shadow_budget_status``'s filter), so the quota cannot be
    # evaded by this. Discarding the result here would only throw away the
    # evaluated evidence the shadow seam exists to collect, for free --
    # there is no organization-facing monetary guarantee at stake to
    # protect the way there is on the live path.
    return result, billed


async def _create_shadow_reservation(
    session: AsyncSession,
    *,
    org_id: str,
    provider: str,
    model: str,
    idempotency_key: str,
    reserved_micro_usd: int,
) -> QUAShadowBudgetReservation:
    window_start, _ = _window(datetime.now(UTC))
    org_uuid = uuid.UUID(org_id)
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    existing = await session.execute(
        select(QUAShadowBudgetReservation.id).where(
            QUAShadowBudgetReservation.org_id == org_uuid,
            QUAShadowBudgetReservation.window_start == window_start,
            QUAShadowBudgetReservation.idempotency_key == digest,
        )
    )
    if existing.scalar_one_or_none() is not None:
        raise QUAShadowBudgetAccountingError(
            "QUA shadow request has already been reserved."
        )
    reservation = QUAShadowBudgetReservation(
        org_id=org_uuid,
        window_start=window_start,
        idempotency_key=digest,
        provider=provider,
        model=model,
        reserved_micro_usd=reserved_micro_usd,
        status="reserved",
        pricing_version=PRICING_VERSION,
    )
    session.add(reservation)
    return reservation


async def _reconcile_shadow_reservation(
    session: AsyncSession,
    *,
    reservation: QUAShadowBudgetReservation,
    provider: str,
    model: str,
    base_url: str | None,
    outcome: str,
    reported: tuple[int | None, int | None, int | None],
) -> int | None:
    input_tokens, output_tokens, cached_input_tokens = reported
    row = await session.get(QUAShadowBudgetReservation, reservation.id)
    if row is None:
        raise RuntimeError(
            "QUA shadow budget reservation disappeared before reconciliation"
        )
    await session.refresh(row)
    if row.status != "reserved":
        raise RuntimeError(
            "QUA shadow budget reservation is no longer owned by this attempt"
        )
    row.input_tokens = input_tokens
    row.output_tokens = output_tokens
    row.cached_input_tokens = cached_input_tokens
    row.reconciled_at = datetime.now(UTC)
    if outcome == "voided":
        row.status = "voided"
        row.input_tokens = 0
        row.output_tokens = 0
        row.cached_input_tokens = 0
        row.actual_micro_usd = 0
        await session.commit()
        return 0
    if input_tokens is None or output_tokens is None or cached_input_tokens is None:
        row.status = "usage_unavailable"
        row.actual_micro_usd = None
        await session.commit()
        return None
    billed = cost_micro_usd(
        provider=provider,
        model=model,
        base_url=base_url,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
    )
    if billed is None:
        row.status = "usage_unavailable"
        row.actual_micro_usd = None
        await session.commit()
        return None
    row.status = outcome
    row.actual_micro_usd = billed
    await session.commit()
    return billed


def _shadow_agent_usage(item: Any) -> tuple[int | None, int | None, int | None]:
    """Mirrors ``llm.budget._agent_usage`` -- duplicated rather than
    imported to keep this module decoupled from the live guard's private
    surface (see module docstring on why the two paths must never share
    state)."""

    usage = getattr(item, "usage", None)
    if usage is None:
        return None, None, None
    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)
    cached_input_tokens = getattr(usage, "cached_input_tokens", None)
    if not isinstance(input_tokens, int) or isinstance(input_tokens, bool):
        input_tokens = None
    if not isinstance(output_tokens, int) or isinstance(output_tokens, bool):
        output_tokens = None
    if not isinstance(cached_input_tokens, int) or isinstance(
        cached_input_tokens, bool
    ):
        cached_input_tokens = None
    if input_tokens == 0 and output_tokens == 0:
        return None, None, None
    return input_tokens, output_tokens, cached_input_tokens


def attach_qua_shadow_budget_guard(
    provider_instance: Any,
    *,
    org_id: str,
    provider: str,
    model: str,
    base_url: str | None,
) -> Any:
    """Decorate a provider instance DEDICATED to the QUA shadow seam.

    The caller must pass a freshly-constructed provider instance -- never
    the same object ``llm.budget.attach_agent_budget_guard`` has already
    decorated for the live investigation call. This function does not (and
    cannot) verify that; see ``production_runtime.py``'s call site for the
    structural guarantee that a second, independent instance is what's
    passed here.
    """

    original = provider_instance.decide
    call_number = 0

    async def decide(
        messages: Sequence[Any],
        tools: Sequence[Any],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: Any = None,
    ) -> Any:
        nonlocal call_number
        call_number += 1
        # Codex round 1 (HIGH, confirmed): the reservation must bound the
        # COMPLETE wire request, not just the messages -- the real
        # provider call also sends `tools` and `response_schema` (this
        # seam's own JSON Schema can be sizeable once the shortlist is
        # large), so a messages-only estimate was not a guaranteed upper
        # bound and could let concurrent admissions jointly reconcile above
        # the cap. Mirrors llm.budget.attach_agent_budget_guard's own
        # payload-sizing formula exactly.
        payload = json.dumps(
            {
                "messages": [str(item) for item in messages],
                "tools": [str(item) for item in tools],
                "response_schema": response_schema,
            },
            sort_keys=True,
            default=str,
        )
        try:
            async with get_postgres_session() as budget_session:
                result, billed = await guard_qua_shadow_call(
                    session=budget_session,
                    org_id=org_id,
                    provider=provider,
                    model=model,
                    base_url=base_url,
                    idempotency_key=(
                        _shadow_idempotency_key.get()
                        or f"qua-shadow:{id(provider_instance)}:{call_number}"
                    ),
                    maximum_input_tokens=max(1, len(payload.encode("utf-8"))),
                    maximum_output_tokens=max_output_tokens,
                    invoke=lambda: original(
                        messages,
                        tools,
                        response_schema,
                        timeout_seconds,
                        max_output_tokens,
                        signal,
                    ),
                    usage=_shadow_agent_usage,
                )
        except (QUAShadowBudgetExceeded, QUAShadowBudgetAccountingError) as exc:
            code = (
                AgentProviderErrorCode.BUDGET_EXHAUSTED
                if isinstance(exc, QUAShadowBudgetExceeded)
                else AgentProviderErrorCode.BUDGET_UNAVAILABLE
            )
            raise AgentProviderError(code) from None
        if billed is not None:
            result = replace(
                result,
                usage=replace(result.usage, estimated_cost_microusd=billed),
            )
        return result

    setattr(provider_instance, "decide", decide)
    return provider_instance
