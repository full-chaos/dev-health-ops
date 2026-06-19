from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.schemas import LLMSettingsResponse, LLMSettingsUpsert
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.api.services.licensing import FeatureService, resolve_org_tier
from dev_health_ops.licensing.types import TIER_ORDER, LicenseTier
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import SettingCategory
from dev_health_ops.models.users import Organization

LLM_SETTING_KEYS = ("provider", "model", "api_key", "base_url", "concurrency")
BYO_LLM_MIN_TIER = LicenseTier.TEAM


@dataclass(frozen=True)
class LLMSettingsAccessError(Exception):
    status_code: int
    detail: dict[str, str]

    @property
    def message(self) -> str:
        if self.detail.get("error") == "feature_not_licensed":
            return (
                "BYO LLM settings require "
                f"{self.detail['required_tier']} tier; "
                f"current tier is {self.detail['current_tier']}"
            )
        return self.detail.get("message", "BYO LLM settings access denied")


async def require_byo_llm_access(session: AsyncSession, org_id: str) -> None:
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError as exc:
        raise LLMSettingsAccessError(
            status_code=404,
            detail={
                "error": "organization_not_found",
                "message": "Organization not found",
            },
        ) from exc

    org_result = await session.execute(
        select(Organization.id).where(Organization.id == org_uuid)
    )
    if org_result.scalar_one_or_none() is None:
        raise LLMSettingsAccessError(
            status_code=404,
            detail={
                "error": "organization_not_found",
                "message": "Organization not found",
            },
        )

    license_result = await session.execute(
        select(OrgLicense).where(OrgLicense.org_id == org_uuid)
    )
    org_license = license_result.scalar_one_or_none()

    def _resolve(sync_session):
        return resolve_org_tier(sync_session, org_uuid, org_license)

    tier = await session.run_sync(_resolve)
    if TIER_ORDER.index(tier) < TIER_ORDER.index(BYO_LLM_MIN_TIER):
        raise LLMSettingsAccessError(
            status_code=402,
            detail={
                "error": "feature_not_licensed",
                "feature": "byo_llm",
                "required_tier": BYO_LLM_MIN_TIER.value,
                "current_tier": tier.value,
            },
        )

    # CHAOS-2551: in addition to the tier gate above, require the byo_llm
    # feature flag to be enabled (global flag + per-org override). If the flag
    # is not registered in this environment (pre-migration / minimal DB), skip
    # this check so behavior matches the prior tier-only gate.
    def _flag_access(sync_session):
        return FeatureService(sync_session).check_feature_access(org_uuid, "byo_llm")

    try:
        access = await session.run_sync(_flag_access)
    except Exception:
        access = None
    if access is not None and not access.allowed:
        reason = access.reason or ""
        # Tier already passed above, so a flag-registered denial here is a
        # disabled global flag or a per-org override, not a tier shortfall.
        if not reason.startswith("Unknown feature"):
            raise LLMSettingsAccessError(
                status_code=403,
                detail={
                    "error": "feature_not_enabled",
                    "feature": "byo_llm",
                    "message": "BYO LLM is not enabled for this organization",
                },
            )


def mask_api_key(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "********"
    return f"{value[:4]}…{value[-4:]}"


async def get_llm_settings_response(svc: SettingsService) -> LLMSettingsResponse:
    provider = await svc.get("provider", SettingCategory.LLM.value)
    model = await svc.get("model", SettingCategory.LLM.value)
    api_key = await svc.get("api_key", SettingCategory.LLM.value)
    base_url = await svc.get("base_url", SettingCategory.LLM.value)
    concurrency = await svc.get("concurrency", SettingCategory.LLM.value)
    return LLMSettingsResponse(
        provider=provider,
        model=model,
        api_key=mask_api_key(api_key),
        base_url=base_url,
        concurrency=int(concurrency) if concurrency else None,
    )


async def upsert_llm_settings(
    svc: SettingsService,
    payload: LLMSettingsUpsert,
) -> LLMSettingsResponse:
    await svc.set(
        "provider",
        payload.provider.strip().lower(),
        SettingCategory.LLM.value,
        description="BYO LLM provider for this organization",
    )
    await svc.set(
        "model",
        payload.model,
        SettingCategory.LLM.value,
        description="BYO LLM model for this organization",
    )
    if payload.api_key is not None:
        await svc.set(
            "api_key",
            payload.api_key,
            SettingCategory.LLM.value,
            encrypt=True,
            description="Encrypted BYO LLM API key for this organization",
        )
    await svc.set(
        "base_url",
        payload.base_url,
        SettingCategory.LLM.value,
        description="BYO LLM base URL for this organization",
    )
    if payload.concurrency is not None:
        await svc.set(
            "concurrency",
            str(payload.concurrency),
            SettingCategory.LLM.value,
            description="BYO LLM maximum concurrent categorizations for this organization",
        )
    return await get_llm_settings_response(svc)


async def delete_llm_settings(svc: SettingsService) -> bool:
    deleted = False
    for key in LLM_SETTING_KEYS:
        deleted = (await svc.delete(key, SettingCategory.LLM.value)) or deleted
    return deleted
