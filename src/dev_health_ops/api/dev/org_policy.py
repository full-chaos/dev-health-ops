"""Organization-owned Ask Dev policy shared by both interaction surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.models.settings import SettingCategory

ASK_DEV_RETENTION_KEY = "ask_dev_retention_days"
ASK_DEV_FALLBACK_KEY = "ask_dev_platform_fallback"
ASK_DEV_EMERGENCY_DISABLED_KEY = "ask_dev_emergency_disabled"


@dataclass(frozen=True, slots=True)
class AskDevOrgPolicy:
    retention_days: Literal[0, 30] = 30
    fallback_policy: Literal["fail_closed", "platform"] = "fail_closed"
    emergency_disabled: bool = False


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
    )
