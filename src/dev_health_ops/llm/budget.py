"""Durable organization monetary budgets for shared BYO LLM execution.

Budget configuration is stored outside the encrypted ``llm`` credential
category. PostgreSQL advisory locking serializes short reservation transactions
across processes; provider network execution happens only after that reservation
is committed and the transaction lock is released.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from threading import Lock
from typing import Any, Final, Literal, TypeVar
from urllib.parse import urlsplit

from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.db import get_postgres_session
from dev_health_ops.llm.errors import LLMError
from dev_health_ops.llm.providers.base import CompletionResult, LLMProvider
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.llm_budget import BYOLLMBudgetReservation

BUDGET_CATEGORY: Final = "llm_budget"
BUDGET_LIMIT_KEY: Final = "limit_micro_usd"
PRICING_VERSION: Final = "openai-public-2025-08-07.v1"
BUDGET_WINDOW: Final = "calendar_month_utc"
DEFAULT_OPERATOR_MAX_MICRO_USD: Final = 100_000_000
LICENSE_LIMIT_KEY: Final = "byo_llm_budget_micro_usd"

# Integer micro-USD per one million tokens.  Only exact, server-certified
# provider/model pairs are present.  An absent pair is unavailable, never zero.
# Source snapshot: https://developers.openai.com/api/docs/models/gpt-5-mini
_PRICE_PER_MILLION: Final[dict[tuple[str, str], tuple[int, int, int]]] = {
    # input, cached input, output
    ("openai", "gpt-5-mini"): (250_000, 25_000, 2_000_000),
}

BudgetReason = Literal[
    "available",
    "budget_not_configured",
    "pricing_unavailable",
    "usage_unavailable",
    "budget_exhausted",
]

_idempotency_key: ContextVar[str | None] = ContextVar(
    "byo_llm_budget_idempotency_key", default=None
)
_attempt_request_key: ContextVar[str | None] = ContextVar(
    "byo_llm_budget_attempt_request_key", default=None
)


@dataclass(frozen=True, slots=True)
class BYOBudgetStatus:
    used_micro_usd: int | None
    limit_micro_usd: int | None
    remaining_micro_usd: int | None
    window: str
    reset_at: datetime
    enforcement_available: bool
    reason: BudgetReason
    maximum_limit_micro_usd: int
    pricing_version: str | None


class BYOBudgetExceeded(LLMError):
    """The configured organization BYO monetary ceiling denied admission."""


class BYOBudgetAccountingError(LLMError):
    """A budget reservation, pricing decision, or reconciliation could not complete."""


_T = TypeVar("_T")
_locks_guard = Lock()
_org_locks: dict[str, asyncio.Lock] = {}


def operator_maximum_micro_usd() -> int:
    raw = os.getenv("BYO_LLM_MAX_BUDGET_MICRO_USD")
    if raw is None:
        return DEFAULT_OPERATOR_MAX_MICRO_USD
    try:
        value = int(raw)
    except ValueError:
        return 0
    return max(0, value)


async def provisioned_maximum_micro_usd(settings: SettingsService) -> int:
    """Resolve the lower of operator and optional licensed organization maxima."""

    operator_maximum = operator_maximum_micro_usd()
    try:
        org_uuid = uuid.UUID(settings.org_id)
    except (TypeError, ValueError):
        return operator_maximum
    bind = settings.session.get_bind()
    if bind is not None and bind.dialect.name == "sqlite":
        has_license_table = await settings.session.run_sync(_has_org_license_table)
        if not has_license_table:
            return operator_maximum
    result = await settings.session.execute(
        select(OrgLicense.limits_override).where(OrgLicense.org_id == org_uuid)
    )
    overrides = result.scalar_one_or_none()
    if not isinstance(overrides, dict) or LICENSE_LIMIT_KEY not in overrides:
        return operator_maximum
    licensed_maximum = _nonnegative_int(str(overrides[LICENSE_LIMIT_KEY]))
    if licensed_maximum is None:
        return 0
    return min(operator_maximum, licensed_maximum)


def _org_lock(org_id: str) -> asyncio.Lock:
    with _locks_guard:
        lock = _org_locks.get(org_id)
        if lock is None:
            lock = asyncio.Lock()
            _org_locks[org_id] = lock
        return lock


def _has_org_license_table(sync_session: Any) -> bool:
    bind = sync_session.get_bind()
    if bind is None:
        return False
    return inspect(bind).has_table(OrgLicense.__tablename__)


@contextmanager
def budget_idempotency_scope(key: str):
    """Bind a stable caller-owned identity to one provider attempt."""

    token = _idempotency_key.set(key)
    try:
        yield
    finally:
        _idempotency_key.reset(token)


def _window(now: datetime) -> tuple[datetime, datetime]:
    value = now.astimezone(UTC)
    start = datetime(value.year, value.month, 1, tzinfo=UTC)
    if value.month == 12:
        reset = datetime(value.year + 1, 1, 1, tzinfo=UTC)
    else:
        reset = datetime(value.year, value.month + 1, 1, tzinfo=UTC)
    return start, reset


def _normalized_model(model: str) -> str:
    value = model.strip().lower()
    if value == "gpt-5-mini" or value.startswith("gpt-5-mini-"):
        return "gpt-5-mini"
    return value


def _official_openai_endpoint(base_url: str | None) -> bool:
    if not (base_url or "").strip():
        return True
    try:
        parsed = urlsplit(str(base_url))
    except ValueError:
        return False
    return parsed.scheme == "https" and parsed.hostname == "api.openai.com"


def reliable_price(
    *, provider: str, model: str, base_url: str | None
) -> tuple[int, int, int] | None:
    normalized_provider = provider.strip().lower()
    if normalized_provider != "openai" or not _official_openai_endpoint(base_url):
        return None
    return _PRICE_PER_MILLION.get((normalized_provider, _normalized_model(model)))


def cost_micro_usd(
    *,
    provider: str,
    model: str,
    base_url: str | None,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int | None,
) -> int | None:
    price = reliable_price(provider=provider, model=model, base_url=base_url)
    if price is None:
        return None
    if (
        input_tokens < 0
        or output_tokens < 0
        or cached_input_tokens is None
        or cached_input_tokens < 0
        or cached_input_tokens > input_tokens
    ):
        return None
    input_rate, cached_input_rate, output_rate = price
    uncached_input_tokens = input_tokens - cached_input_tokens
    numerator = (
        uncached_input_tokens * input_rate
        + cached_input_tokens * cached_input_rate
        + output_tokens * output_rate
    )
    return math.ceil(numerator / 1_000_000)


async def set_budget_limit(settings: SettingsService, limit_micro_usd: int) -> None:
    maximum = await provisioned_maximum_micro_usd(settings)
    if limit_micro_usd < 0 or limit_micro_usd > maximum:
        raise ValueError(f"budget_limit_micro_usd must be between 0 and {maximum}")
    async with _org_lock(settings.org_id):
        await _acquire_advisory_lock(settings.session, settings.org_id)
        await settings.set(
            BUDGET_LIMIT_KEY,
            str(limit_micro_usd),
            BUDGET_CATEGORY,
            description=(
                "Organization BYO LLM monetary ceiling in integer micro-USD; "
                "stored separately from provider credentials"
            ),
        )


async def get_budget_status(
    settings: SettingsService,
    *,
    provider: str,
    model: str,
    base_url: str | None,
    now: datetime | None = None,
) -> BYOBudgetStatus:
    effective_now = now or datetime.now(UTC)
    window_start, reset_at = _window(effective_now)
    maximum = await provisioned_maximum_micro_usd(settings)
    raw_limit = await settings.get(BUDGET_LIMIT_KEY, BUDGET_CATEGORY)
    parsed_limit = _nonnegative_int(raw_limit)
    limit = min(parsed_limit, maximum) if parsed_limit is not None else None
    if reliable_price(provider=provider, model=model, base_url=base_url) is None:
        return BYOBudgetStatus(
            used_micro_usd=None,
            limit_micro_usd=limit,
            remaining_micro_usd=None,
            window=BUDGET_WINDOW,
            reset_at=reset_at,
            enforcement_available=False,
            reason=(
                "budget_not_configured" if limit is None else "pricing_unavailable"
            ),
            maximum_limit_micro_usd=maximum,
            pricing_version=None,
        )

    try:
        org_uuid = uuid.UUID(settings.org_id)
    except (TypeError, ValueError):
        org_uuid = None
    reservations: list[BYOLLMBudgetReservation] = []
    if org_uuid is not None:
        result = await settings.session.execute(
            select(BYOLLMBudgetReservation).where(
                BYOLLMBudgetReservation.org_id == org_uuid,
                BYOLLMBudgetReservation.window_start == window_start,
            )
        )
        reservations = list(result.scalars().all())
    if any(row.status == "usage_unavailable" for row in reservations):
        return BYOBudgetStatus(
            used_micro_usd=None,
            limit_micro_usd=limit,
            remaining_micro_usd=None,
            window=BUDGET_WINDOW,
            reset_at=reset_at,
            enforcement_available=False,
            reason="usage_unavailable",
            maximum_limit_micro_usd=maximum,
            pricing_version=PRICING_VERSION,
        )
    # A committed reservation is monetary exposure until reconciled, so it
    # participates in admission immediately even while the provider is running.
    used = sum(
        row.actual_micro_usd
        if row.actual_micro_usd is not None
        else row.reserved_micro_usd
        for row in reservations
        if row.status != "voided"
    )
    if limit is None:
        return BYOBudgetStatus(
            used_micro_usd=used,
            limit_micro_usd=None,
            remaining_micro_usd=None,
            window=BUDGET_WINDOW,
            reset_at=reset_at,
            enforcement_available=False,
            reason="budget_not_configured",
            maximum_limit_micro_usd=maximum,
            pricing_version=PRICING_VERSION,
        )
    remaining = max(0, limit - used)
    exhausted = remaining == 0
    return BYOBudgetStatus(
        used_micro_usd=used,
        limit_micro_usd=limit,
        remaining_micro_usd=remaining,
        window=BUDGET_WINDOW,
        reset_at=reset_at,
        enforcement_available=True,
        reason="budget_exhausted" if exhausted else "available",
        maximum_limit_micro_usd=maximum,
        pricing_version=PRICING_VERSION,
    )


async def guard_byo_call(
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
    """Reserve, execute outside a transaction, then reconcile one BYO call."""

    settings = SettingsService(session, org_id)
    maximum_cost = cost_micro_usd(
        provider=provider,
        model=model,
        base_url=base_url,
        input_tokens=max(0, maximum_input_tokens),
        output_tokens=max(0, maximum_output_tokens),
        cached_input_tokens=0,
    )
    reservation: BYOLLMBudgetReservation | None = None
    budget_configured = False
    unpriced_without_budget = False
    async with _org_lock(org_id):
        try:
            await _acquire_advisory_lock(session, org_id)
            status = await get_budget_status(
                settings, provider=provider, model=model, base_url=base_url
            )
            budget_configured = status.limit_micro_usd is not None
            if maximum_cost is None:
                await session.rollback()
                if budget_configured:
                    raise BYOBudgetAccountingError(
                        "BYO LLM pricing is unavailable for the configured budget.",
                        provider=provider,
                        model=model,
                    )
                # Preserve pre-budget custom-provider behavior. The read
                # transaction is closed before the network call below.
                unpriced_without_budget = True
            elif budget_configured and status.reason == "usage_unavailable":
                await session.rollback()
                raise BYOBudgetAccountingError(
                    "BYO LLM usage is unavailable for the configured budget.",
                    provider=provider,
                    model=model,
                )
            elif budget_configured and (
                status.remaining_micro_usd is None
                or maximum_cost > status.remaining_micro_usd
            ):
                await session.rollback()
                raise BYOBudgetExceeded(
                    "Organization BYO LLM monetary budget is exhausted.",
                    provider=provider,
                    model=model,
                )
            elif maximum_cost is not None:
                reservation = await _create_reservation(
                    session,
                    org_id=org_id,
                    provider=provider,
                    model=model,
                    idempotency_key=idempotency_key,
                    reserved_micro_usd=maximum_cost,
                )
                await session.commit()
        except (BYOBudgetAccountingError, BYOBudgetExceeded):
            if session.in_transaction():
                await session.rollback()
            raise
        except Exception as accounting_exc:
            await session.rollback()
            raise BYOBudgetAccountingError(
                "BYO LLM budget reservation is temporarily unavailable.",
                provider=provider,
                model=model,
                original=accounting_exc,
            ) from accounting_exc

    if unpriced_without_budget:
        return await invoke(), None
    if reservation is None:
        raise BYOBudgetAccountingError(
            "BYO LLM budget reservation was not created.",
            provider=provider,
            model=model,
        )

    # The reservation is committed and both locks are released before invoking
    # the provider. This line is the network transaction boundary.
    try:
        result = await invoke()
    except BaseException as exc:
        provider_dispatched = getattr(exc, "provider_dispatched", True)
        reported = (
            (0, 0, 0) if provider_dispatched is False else _usage_from_exception(exc)
        )
        if provider_dispatched is False:
            outcome = "voided"
        else:
            outcome = (
                "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
            )
        try:
            await _reconcile_reservation(
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
            raise BYOBudgetAccountingError(
                "BYO LLM budget accounting is temporarily unavailable.",
                provider=provider,
                model=model,
                original=accounting_exc,
            ) from accounting_exc
        raise

    try:
        billed = await _reconcile_reservation(
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
        raise BYOBudgetAccountingError(
            "BYO LLM budget accounting is temporarily unavailable.",
            provider=provider,
            model=model,
            original=accounting_exc,
        ) from accounting_exc
    if billed is None and budget_configured:
        raise BYOBudgetAccountingError(
            "BYO LLM usage is unavailable for the configured budget.",
            provider=provider,
            model=model,
        )
    return result, billed


async def _create_reservation(
    session: AsyncSession,
    *,
    org_id: str,
    provider: str,
    model: str,
    idempotency_key: str,
    reserved_micro_usd: int,
) -> BYOLLMBudgetReservation:
    window_start, _ = _window(datetime.now(UTC))
    org_uuid = uuid.UUID(org_id)
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    existing = await session.execute(
        select(BYOLLMBudgetReservation.id).where(
            BYOLLMBudgetReservation.org_id == org_uuid,
            BYOLLMBudgetReservation.window_start == window_start,
            BYOLLMBudgetReservation.idempotency_key == digest,
        )
    )
    if existing.scalar_one_or_none() is not None:
        raise BYOBudgetAccountingError(
            "BYO LLM request has already been reserved.",
            provider=provider,
            model=model,
        )
    reservation = BYOLLMBudgetReservation(
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


async def _reconcile_reservation(
    session: AsyncSession,
    *,
    reservation: BYOLLMBudgetReservation,
    provider: str,
    model: str,
    base_url: str | None,
    outcome: str,
    reported: tuple[int | None, int | None, int | None],
) -> int | None:
    input_tokens, output_tokens, cached_input_tokens = reported
    row = await session.get(BYOLLMBudgetReservation, reservation.id)
    if row is None:
        raise RuntimeError(
            "BYO LLM budget reservation disappeared before reconciliation"
        )
    await session.refresh(row)
    if row.status != "reserved":
        raise RuntimeError(
            "BYO LLM budget reservation is no longer owned by this attempt"
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


def attach_llm_budget_guard(
    provider_instance: LLMProvider,
    *,
    org_id: str,
    provider: str,
    model: str,
    base_url: str | None,
    maximum_output_tokens: int = 4096,
) -> LLMProvider:
    """Decorate the shared provider instance without changing its concrete type."""

    original = provider_instance.complete
    attempt_setter = getattr(provider_instance, "set_attempt_executor", None)
    if provider.strip().lower() == "openai" and callable(attempt_setter):

        async def execute_attempt(
            attempt_number: int,
            maximum_input_tokens: int,
            attempt_maximum_output_tokens: int,
            invoke: Callable[[], Awaitable[Any]],
        ) -> Any:
            request_key = _attempt_request_key.get()
            if request_key is None:
                raise BYOBudgetAccountingError(
                    "BYO LLM attempt is missing its request identity.",
                    provider=provider,
                    model=model,
                )
            async with get_postgres_session() as session:
                result, _ = await guard_byo_call(
                    session=session,
                    org_id=org_id,
                    provider=provider,
                    model=model,
                    base_url=base_url,
                    idempotency_key=f"{request_key}:attempt:{attempt_number}",
                    maximum_input_tokens=maximum_input_tokens,
                    maximum_output_tokens=attempt_maximum_output_tokens,
                    invoke=invoke,
                    usage=_provider_response_usage,
                )
                return result

        attempt_setter(execute_attempt)

        async def complete(prompt: str) -> CompletionResult:
            request_key = _idempotency_key.get() or f"shared:{uuid.uuid4()}"
            token = _attempt_request_key.set(request_key)
            try:
                return await original(prompt)
            finally:
                _attempt_request_key.reset(token)

    else:

        async def complete(prompt: str) -> CompletionResult:
            request_key = _idempotency_key.get() or f"shared:{uuid.uuid4()}"
            async with get_postgres_session() as session:
                result, _ = await guard_byo_call(
                    session=session,
                    org_id=org_id,
                    provider=provider,
                    model=model,
                    base_url=base_url,
                    idempotency_key=request_key,
                    maximum_input_tokens=max(1, len(prompt.encode("utf-8"))),
                    maximum_output_tokens=maximum_output_tokens,
                    invoke=lambda: original(prompt),
                    usage=lambda item: (
                        item.input_tokens,
                        item.output_tokens,
                        item.cached_input_tokens,
                    ),
                )
                return result

    setattr(provider_instance, "complete", complete)
    original_submit = getattr(provider_instance, "submit_batch", None)
    if callable(original_submit):

        async def submit_batch(items: Any) -> Any:
            # The existing batch result contract does not retain cached-token
            # detail and Batch API pricing differs from synchronous pricing.
            # Configured budgets therefore fail closed before execution. An
            # unbudgeted organization retains its existing batch behavior.
            async with get_postgres_session() as session:
                settings = SettingsService(session, org_id)
                limit = _nonnegative_int(
                    await settings.get(BUDGET_LIMIT_KEY, BUDGET_CATEGORY)
                )
                await session.rollback()
            if limit is not None:
                raise BYOBudgetAccountingError(
                    "BYO LLM batch pricing is unavailable for the configured budget.",
                    provider=provider,
                    model=model,
                )
            return await original_submit(items)

        setattr(provider_instance, "submit_batch", submit_batch)
    return provider_instance


def attach_agent_budget_guard(
    provider_instance: Any,
    *,
    session: AsyncSession,
    org_id: str,
    provider: str,
    model: str,
    base_url: str | None,
) -> Any:
    """Decorate Ask Dev's provider-neutral decision seam with the shared guard."""

    original = provider_instance.decide
    call_number = 0
    _ = session

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
                result, billed = await guard_byo_call(
                    session=budget_session,
                    org_id=org_id,
                    provider=provider,
                    model=model,
                    base_url=base_url,
                    idempotency_key=(
                        _idempotency_key.get()
                        or f"ask-dev:{id(provider_instance)}:{call_number}"
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
                    usage=_agent_usage,
                )
        except (BYOBudgetExceeded, BYOBudgetAccountingError) as exc:
            from dev_health_ops.llm.agent.errors import (
                AgentProviderError,
                AgentProviderErrorCode,
            )

            code = (
                AgentProviderErrorCode.BUDGET_EXHAUSTED
                if isinstance(exc, BYOBudgetExceeded)
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


async def _acquire_advisory_lock(session: AsyncSession, org_id: str) -> None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return
    digest = hashlib.sha256(f"byo-llm-budget:{org_id}".encode()).digest()
    lock_key = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key}
    )


def _usage_from_exception(
    exc: BaseException,
) -> tuple[int | None, int | None, int | None]:
    usage = getattr(exc, "usage", None)
    if usage is None:
        return None, None, None
    details = (
        usage.get("prompt_tokens_details") or usage.get("input_tokens_details")
        if isinstance(usage, dict)
        else getattr(usage, "prompt_tokens_details", None)
        or getattr(usage, "input_tokens_details", None)
    )
    cached_tokens = _usage_value(details, "cached_tokens")
    if cached_tokens is None:
        # Ask Dev's provider-neutral AgentUsage contract (attached to
        # AgentProviderError -- CHAOS-3285) reports cached tokens as a flat
        # field rather than the raw provider's nested *_tokens_details
        # object; fall back to it when there is no nested detail to read.
        cached_tokens = _usage_value(usage, "cached_input_tokens")
    return (
        _usage_value(usage, "prompt_tokens", "input_tokens"),
        _usage_value(usage, "completion_tokens", "output_tokens"),
        cached_tokens,
    )


def _agent_usage(item: Any) -> tuple[int | None, int | None, int | None]:
    usage = getattr(item, "usage", None)
    if usage is None:
        return None, None, None
    input_tokens = _usage_value(usage, "input_tokens")
    output_tokens = _usage_value(usage, "output_tokens")
    cached_input_tokens = _usage_value(usage, "cached_input_tokens")
    # The OpenAI-compatible adapter uses its contract's zero defaults when the
    # provider omitted the usage object. A valid completion cannot consume no
    # input and no output tokens, so preserve that state as unknown, not $0.
    if input_tokens == 0 and output_tokens == 0:
        return None, None, None
    return input_tokens, output_tokens, cached_input_tokens


def _provider_response_usage(
    item: Any,
) -> tuple[int | None, int | None, int | None]:
    usage = getattr(item, "usage", None)
    if usage is None:
        return None, None, None
    details = (
        usage.get("input_tokens_details") or usage.get("prompt_tokens_details")
        if isinstance(usage, dict)
        else getattr(usage, "input_tokens_details", None)
        or getattr(usage, "prompt_tokens_details", None)
    )
    return (
        _usage_value(usage, "input_tokens", "prompt_tokens"),
        _usage_value(usage, "output_tokens", "completion_tokens"),
        _usage_value(details, "cached_tokens"),
    )


def _usage_value(usage: Any, *names: str) -> int | None:
    for name in names:
        value = (
            usage.get(name) if isinstance(usage, dict) else getattr(usage, name, None)
        )
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    return None


def _nonnegative_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


__all__ = [
    "BUDGET_CATEGORY",
    "BUDGET_LIMIT_KEY",
    "BYOBudgetAccountingError",
    "BYOBudgetExceeded",
    "BYOBudgetStatus",
    "attach_agent_budget_guard",
    "attach_llm_budget_guard",
    "budget_idempotency_scope",
    "cost_micro_usd",
    "get_budget_status",
    "guard_byo_call",
    "operator_maximum_micro_usd",
    "provisioned_maximum_micro_usd",
    "reliable_price",
    "set_budget_limit",
]
