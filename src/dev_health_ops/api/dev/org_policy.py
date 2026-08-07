"""Organization-owned Ask Dev policy shared by both interaction surfaces."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.models.settings import SettingCategory

ASK_DEV_RETENTION_KEY = "ask_dev_retention_days"
ASK_DEV_FALLBACK_KEY = "ask_dev_platform_fallback"
ASK_DEV_EMERGENCY_DISABLED_KEY = "ask_dev_emergency_disabled"
ASK_DEV_PLATFORM_MONTHLY_REQUEST_LIMIT_KEY = "ask_dev_platform_monthly_request_limit"
ASK_DEV_PLATFORM_MONTHLY_COST_LIMIT_KEY = "ask_dev_platform_monthly_cost_microusd"

PLATFORM_MONTHLY_REQUEST_LIMIT_MIN = 100
PLATFORM_MONTHLY_REQUEST_LIMIT_DEFAULT = 1_000
PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX = 5_000
PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD = 10_000_000
PLATFORM_MONTHLY_COST_LIMIT_DEFAULT_MICROUSD = 100_000_000
PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD = 500_000_000
ASK_DEV_RUN_COST_HARD_MAX_MICROUSD = 5_000_000

#: The one definition of "this dev_run is finished". Previously duplicated
#: (persistence/service.py's own ``_TERMINAL_RUN_STATES``, and a third bare
#: literal inline in ask_dev.py's platform-allowance query) -- the same class
#: of "two implementations of the same fact" as platform_month_window()
#: above. persistence/service.py keeps a private ``_TERMINAL_RUN_STATES``
#: alias importing this, for its own existing call sites and the test that
#: names it directly.
TERMINAL_RUN_STATES: frozenset[str] = frozenset(
    {
        "completed",
        "insufficient_evidence",
        "refused",
        "failed",
        "cancelled",
    }
)


def _bounded_operator_limit(
    name: str, *, default: int, minimum: int, hard_maximum: int
) -> int:
    raw = os.getenv(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return min(hard_maximum, max(minimum, value))


def platform_operator_request_limit() -> int:
    return _bounded_operator_limit(
        "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX",
        default=PLATFORM_MONTHLY_REQUEST_LIMIT_DEFAULT,
        minimum=PLATFORM_MONTHLY_REQUEST_LIMIT_MIN,
        hard_maximum=PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX,
    )


def platform_operator_cost_limit_microusd() -> int:
    return _bounded_operator_limit(
        "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD",
        default=PLATFORM_MONTHLY_COST_LIMIT_DEFAULT_MICROUSD,
        minimum=PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD,
        hard_maximum=PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD,
    )


def platform_month_window(now: datetime) -> tuple[datetime, datetime]:
    """The calendar-month-UTC window platform allowance is billed against.

    CHAOS-3522: this is the ONE definition. Before, ``ask_dev.py``'s admin
    usage read and ``persistence/service.py``'s admission enforcement each
    hand-rolled their own copy of "start of this UTC month" / "start of next
    UTC month" -- two implementations of "this month" that happened to agree
    only because nobody had touched either in a while. The Valkey allowance
    counter key (``askdev:allowance:{org_id}:{YYYY-MM}``) and its TTL are
    also derived from this function, so a key an admission writes and a key
    a read or the SQL fallback computes are always the same key by
    construction, never by two authors independently getting the same
    calendar math right.
    """

    start = datetime(now.year, now.month, 1, tzinfo=UTC)
    if now.month == 12:
        return start, datetime(now.year + 1, 1, 1, tzinfo=UTC)
    return start, datetime(now.year, now.month + 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class AskDevOrgPolicy:
    retention_days: Literal[0, 30] = 30
    fallback_policy: Literal["fail_closed", "platform"] = "platform"
    emergency_disabled: bool = False
    platform_monthly_request_limit: int = PLATFORM_MONTHLY_REQUEST_LIMIT_DEFAULT
    platform_monthly_cost_limit_microusd: int = (
        PLATFORM_MONTHLY_COST_LIMIT_DEFAULT_MICROUSD
    )


def _stored_limit(raw: str | None, *, default: int, minimum: int, maximum: int) -> int:
    if raw is None:
        return min(maximum, max(minimum, default))
    try:
        value = int(raw)
    except ValueError:
        return minimum
    return min(maximum, max(minimum, value))


async def load_ask_dev_org_policy(settings: SettingsService) -> AskDevOrgPolicy:
    """Read bounded settings and fail closed on malformed stored values."""

    category = SettingCategory.ASK_DEV.value
    retention = await settings.get(ASK_DEV_RETENTION_KEY, category)
    fallback = await settings.get(ASK_DEV_FALLBACK_KEY, category)
    if fallback is None:
        # Wave 2 stored the first explicit-fallback switch beside the BYO
        # provider settings. Preserve that decision while all new writes use
        # the dedicated Ask Dev category.
        fallback = await settings.get(ASK_DEV_FALLBACK_KEY, SettingCategory.LLM.value)
    emergency_disabled = await settings.get(ASK_DEV_EMERGENCY_DISABLED_KEY, category)
    request_limit = await settings.get(
        ASK_DEV_PLATFORM_MONTHLY_REQUEST_LIMIT_KEY, category
    )
    cost_limit = await settings.get(ASK_DEV_PLATFORM_MONTHLY_COST_LIMIT_KEY, category)
    operator_request_limit = platform_operator_request_limit()
    operator_cost_limit = platform_operator_cost_limit_microusd()

    retention_days: Literal[0, 30] = 0 if retention == "0" else 30
    fallback_policy: Literal["fail_closed", "platform"]
    if fallback is None or fallback in {"platform", "true"}:
        fallback_policy = "platform"
    else:
        # Explicit fail_closed and malformed stored values both remain closed.
        fallback_policy = "fail_closed"
    # Unknown values are treated as disabled rather than silently reopening a
    # tenant surface after a corrupt or partial administrative write.
    disabled = emergency_disabled not in {None, "", "false", "0"}
    return AskDevOrgPolicy(
        retention_days=retention_days,
        fallback_policy=fallback_policy,
        emergency_disabled=disabled,
        platform_monthly_request_limit=_stored_limit(
            request_limit,
            default=PLATFORM_MONTHLY_REQUEST_LIMIT_DEFAULT,
            minimum=PLATFORM_MONTHLY_REQUEST_LIMIT_MIN,
            maximum=operator_request_limit,
        ),
        platform_monthly_cost_limit_microusd=_stored_limit(
            cost_limit,
            default=PLATFORM_MONTHLY_COST_LIMIT_DEFAULT_MICROUSD,
            minimum=PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD,
            maximum=operator_cost_limit,
        ),
    )
