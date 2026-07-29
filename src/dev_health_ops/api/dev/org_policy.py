"""Organization-owned Ask Dev policy shared by both interaction surfaces."""

from __future__ import annotations

import os
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class AskDevOrgPolicy:
    retention_days: Literal[0, 30] = 30
    fallback_policy: Literal["fail_closed", "platform"] = "fail_closed"
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
    fallback_policy: Literal["fail_closed", "platform"] = (
        "platform" if fallback in {"platform", "true"} else "fail_closed"
    )
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
