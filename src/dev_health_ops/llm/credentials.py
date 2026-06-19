from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from dev_health_ops.llm.errors import LLMAuthError


@dataclass(frozen=True)
class LLMCredentials:
    api_key: str = field(default="", repr=False)
    base_url: str = ""


_API_KEY_ENV_BY_PROVIDER: dict[str, tuple[str, ...]] = {
    "openai": ("LLM_API_KEY", "OPENAI_API_KEY"),
    "anthropic": ("LLM_API_KEY", "ANTHROPIC_API_KEY"),
    "gemini": ("LLM_API_KEY", "GEMINI_API_KEY"),
    "qwen": ("LLM_API_KEY", "QWEN_API_KEY", "DASHSCOPE_API_KEY"),
    "local": ("LLM_API_KEY", "LOCAL_LLM_API_KEY"),
    "ollama": ("LLM_API_KEY", "LOCAL_LLM_API_KEY"),
    "lmstudio": ("LMSTUDIO_API_KEY", "LLM_API_KEY", "LOCAL_LLM_API_KEY"),
    "qwen-local": ("LLM_API_KEY", "LOCAL_LLM_API_KEY"),
    "qwen-lmstudio": ("LLM_API_KEY", "LOCAL_LLM_API_KEY"),
}

_BASE_URL_ENV_BY_PROVIDER: dict[str, tuple[str, ...]] = {
    "openai": ("LLM_BASE_URL", "OPENAI_BASE_URL"),
    "anthropic": ("LLM_BASE_URL", "ANTHROPIC_BASE_URL"),
    "gemini": ("LLM_BASE_URL", "GEMINI_BASE_URL"),
    "qwen": ("LLM_BASE_URL", "DASHSCOPE_BASE_URL"),
    "local": ("LLM_BASE_URL", "LOCAL_LLM_BASE_URL"),
    "ollama": ("LLM_BASE_URL", "OLLAMA_BASE_URL"),
    "lmstudio": ("LLM_BASE_URL", "LMSTUDIO_BASE_URL"),
    "qwen-local": ("LLM_BASE_URL", "OLLAMA_BASE_URL"),
    "qwen-lmstudio": ("LLM_BASE_URL", "LMSTUDIO_BASE_URL"),
}

_API_KEY_REQUIRED_PROVIDERS = {"openai", "anthropic", "gemini", "qwen"}
_LLM_PROVIDER_KEY = "provider"
_LLM_MODEL_KEY = "model"
_LLM_API_KEY_KEY = "api_key"
_LLM_BASE_URL_KEY = "base_url"


def _first_env(names: tuple[str, ...]) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return ""


def _normalize_provider(provider: str) -> str:
    return (provider or "auto").strip().lower()


def _load_org_llm_settings(org_id: str | None) -> dict[str, str]:
    if not org_id:
        return {}

    try:
        from sqlalchemy import select

        from dev_health_ops.core.encryption import decrypt_value
        from dev_health_ops.db import get_postgres_session_sync
        from dev_health_ops.models.settings import Setting, SettingCategory
    except Exception:
        return {}

    try:
        with get_postgres_session_sync() as session:
            result = session.execute(
                select(Setting).where(
                    Setting.org_id == org_id,
                    Setting.category == SettingCategory.LLM.value,
                )
            )
            rows: list[Any] = list(result.scalars().all())
    except Exception:
        return {}

    settings: dict[str, str] = {}
    for row in rows:
        value = row.value or ""
        if row.is_encrypted and value:
            try:
                value = decrypt_value(value)
            except ValueError:
                continue
        if value:
            settings[str(row.key)] = str(value)
    return settings


def resolve_llm_org_settings_provider(*, org_id: str | None = None) -> str:
    return _load_org_llm_settings(org_id).get(_LLM_PROVIDER_KEY, "")


def resolve_llm_org_settings_model(provider: str, *, org_id: str | None = None) -> str:
    settings = _load_org_llm_settings(org_id)
    configured_provider = _normalize_provider(settings.get(_LLM_PROVIDER_KEY, ""))
    requested_provider = _normalize_provider(provider)
    if configured_provider and requested_provider not in {"auto", configured_provider}:
        return ""
    return settings.get(_LLM_MODEL_KEY, "")


def resolve_llm_org_settings_credentials(
    provider: str, *, org_id: str | None = None
) -> LLMCredentials:
    settings = _load_org_llm_settings(org_id)
    configured_provider = _normalize_provider(settings.get(_LLM_PROVIDER_KEY, ""))
    requested_provider = _normalize_provider(provider)
    if configured_provider and requested_provider not in {"auto", configured_provider}:
        return LLMCredentials()
    return LLMCredentials(
        api_key=settings.get(_LLM_API_KEY_KEY, ""),
        base_url=settings.get(_LLM_BASE_URL_KEY, ""),
    )


def resolve_llm_credentials(
    provider: str,
    *,
    org_id: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> LLMCredentials:
    provider_name = _normalize_provider(provider)
    env_api_key = _first_env(_API_KEY_ENV_BY_PROVIDER.get(provider_name, ()))
    env_base_url = _first_env(_BASE_URL_ENV_BY_PROVIDER.get(provider_name, ()))
    org_credentials = resolve_llm_org_settings_credentials(provider_name, org_id=org_id)

    resolved = LLMCredentials(
        api_key=str(api_key or env_api_key or org_credentials.api_key or ""),
        base_url=str(base_url or env_base_url or org_credentials.base_url or ""),
    )
    if provider_name in _API_KEY_REQUIRED_PROVIDERS and not resolved.api_key:
        env_names = ", ".join(_API_KEY_ENV_BY_PROVIDER.get(provider_name, ()))
        raise LLMAuthError(
            f"Missing API key for LLM provider '{provider_name}'. Set --llm-api-key, {env_names}, or org-scoped LLM settings.",
            provider=provider_name,
        )
    return resolved
