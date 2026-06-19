from __future__ import annotations

from dev_health_ops.api.admin.schemas import LLMSettingsResponse, LLMSettingsUpsert
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.models.settings import SettingCategory

LLM_SETTING_KEYS = ("provider", "model", "api_key", "base_url")


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
    return LLMSettingsResponse(
        provider=provider,
        model=model,
        api_key=mask_api_key(api_key),
        base_url=base_url,
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
    return await get_llm_settings_response(svc)


async def delete_llm_settings(svc: SettingsService) -> bool:
    deleted = False
    for key in LLM_SETTING_KEYS:
        deleted = (await svc.delete(key, SettingCategory.LLM.value)) or deleted
    return deleted
