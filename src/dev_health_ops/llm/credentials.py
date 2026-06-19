from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

from dev_health_ops.llm.errors import LLMAuthError

logger = logging.getLogger(__name__)


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
_LLM_CONCURRENCY_KEY = "concurrency"


def _first_env(names: tuple[str, ...]) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return ""


def _normalize_provider(provider: str) -> str:
    return (provider or "auto").strip().lower()


def _safe_log_value(value: str) -> str:
    """Strip CR/LF before logging to prevent log injection from org-provided
    values (provider names come from tenant-controlled settings)."""
    return value.replace("\r", "").replace("\n", "")


def _apply_byo_llm_flag_gate(
    session: Any, org_id: str, settings: dict[str, str]
) -> dict[str, str]:
    """Apply the byo_llm feature-flag gate to already-loaded org BYO settings.

    Called only when the org HAS BYO settings. Returns the settings unchanged
    when the flag is enabled OR unregistered (pre-migration / minimal DB), and
    {} when the flag is explicitly disabled (global flag, per-org override, or
    insufficient tier) -- the org reverts to the platform default.

    A genuine flag-lookup failure for a BYO-configured org is NOT swallowed: it
    raises LLMAuthError so the resolver fails loudly instead of silently
    rerouting the tenant's BYO traffic to the platform LLM (a data-residency
    boundary). Orgs without BYO settings never reach this path, so transient
    licensing-store errors do not disrupt them.
    """
    import uuid as _uuid

    from dev_health_ops.api.services.licensing import byo_llm_flag_state

    try:
        state = byo_llm_flag_state(session, _uuid.UUID(org_id))
    except Exception as exc:
        raise LLMAuthError(
            "Unable to determine the byo_llm feature flag state for an "
            "organization with BYO LLM settings; refusing to resolve so the "
            "tenant's BYO traffic is not silently rerouted to the platform "
            "default LLM.",
            provider="auto",
            model="none",
        ) from exc
    if state == "disabled":
        return {}
    return settings


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

            if not settings:
                # No org BYO configured: nothing to gate and no data-residency
                # concern -> platform default (skip the flag lookup entirely).
                return {}

            # Org HAS BYO settings: gate on the byo_llm feature flag.
            return _apply_byo_llm_flag_gate(session, org_id, settings)
    except LLMAuthError:
        # Controlled flag-lookup failure for a BYO-configured org: propagate so
        # the resolver does NOT silently reroute the tenant to the platform LLM.
        raise
    except Exception:
        return {}


def resolve_llm_org_settings_provider(*, org_id: str | None = None) -> str:
    return _load_org_llm_settings(org_id).get(_LLM_PROVIDER_KEY, "")


def resolve_llm_org_settings_model(provider: str, *, org_id: str | None = None) -> str:
    settings = _load_org_llm_settings(org_id)
    configured_provider = _normalize_provider(settings.get(_LLM_PROVIDER_KEY, ""))
    requested_provider = _normalize_provider(provider)
    if configured_provider and requested_provider not in {"auto", configured_provider}:
        return ""
    return settings.get(_LLM_MODEL_KEY, "")


def resolve_llm_org_settings_concurrency(*, org_id: str | None = None) -> int | None:
    raw = _load_org_llm_settings(org_id).get(_LLM_CONCURRENCY_KEY, "")
    if not raw:
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return None


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


def _env_llm_credentials(provider_name: str) -> LLMCredentials:
    """Platform/default credentials sourced *only* from environment variables.

    Source-bound: both api_key and base_url come from the environment, never
    mixed with org-scoped or per-call values (CHAOS-2550 credential-isolation
    invariant).
    """
    return LLMCredentials(
        api_key=_first_env(_API_KEY_ENV_BY_PROVIDER.get(provider_name, ())),
        base_url=_first_env(_BASE_URL_ENV_BY_PROVIDER.get(provider_name, ())),
    )


def _llm_credentials_complete(provider_name: str, credentials: LLMCredentials) -> bool:
    """Whether a credential bundle satisfies the provider's hard requirements.

    Only API-key-required providers (openai/anthropic/gemini/qwen) can be
    "incomplete"; local/self-hosted providers are usable without a key.
    """
    if provider_name in _API_KEY_REQUIRED_PROVIDERS and not credentials.api_key:
        return False
    return True


def org_byo_provider_matches(provider_name: str, org_id: str | None) -> bool:
    """True iff the org configured THIS provider with a complete bundle.

    Identifies when org BYO is the active credential source for a provider so
    model resolution can stay source-bound (org model, not platform env model).
    Does not log; the incomplete-config warning is emitted during provider and
    credential resolution.
    """
    org_credentials = resolve_llm_org_settings_credentials(provider_name, org_id=org_id)
    if not (org_credentials.api_key or org_credentials.base_url):
        return False
    return _llm_credentials_complete(provider_name, org_credentials)


def _is_known_llm_provider(provider_name: str) -> bool:
    """Whether ``provider_name`` is a real provider this resolver can build.

    Backed by the per-provider env-var maps (the canonical set of supported
    real providers; excludes mock/none which are explicit-only).
    """
    return (
        provider_name in _API_KEY_ENV_BY_PROVIDER
        or provider_name in _BASE_URL_ENV_BY_PROVIDER
    )


def resolve_usable_org_llm_provider(*, org_id: str | None = None) -> str:
    """Return the org's BYO provider iff it is configured AND complete.

    Returns "" when the org has no BYO provider, names mock/none, names an
    unrecognized provider, supplies no credential material, or is configured
    but missing required credentials. In the not-usable cases a warning is
    logged (where the config is non-trivial) and "" is returned so callers
    transparently fall back to the platform default instead of crashing
    (CHAOS-2550 decision #1: "silent-but-missing > crashing").
    """
    settings = _load_org_llm_settings(org_id)
    provider_name = _normalize_provider(settings.get(_LLM_PROVIDER_KEY, ""))
    if not provider_name or provider_name in {"auto", "mock", "none"}:
        return ""
    if not _is_known_llm_provider(provider_name):
        logger.warning(
            "Org BYO LLM provider '%s' is not a recognized provider; "
            "falling back to the platform default.",
            _safe_log_value(provider_name),
        )
        return ""
    credentials = LLMCredentials(
        api_key=settings.get(_LLM_API_KEY_KEY, ""),
        base_url=settings.get(_LLM_BASE_URL_KEY, ""),
    )
    if not (credentials.api_key or credentials.base_url):
        # Provider named but no credential material: not an active BYO config.
        # Treat as "not configured" so BOTH provider and credential resolution
        # fall back to the platform default and stay in agreement (the
        # _resolve_org_byo_credentials path returns None on empty material too).
        return ""
    if not _llm_credentials_complete(provider_name, credentials):
        logger.warning(
            "Org BYO LLM provider '%s' is configured but incomplete "
            "(missing required credentials); falling back to the platform default.",
            _safe_log_value(provider_name),
        )
        return ""
    return provider_name


def _resolve_org_byo_credentials(
    provider_name: str, org_id: str | None
) -> LLMCredentials | None:
    """Org-scoped BYO credentials for ``provider_name`` when complete, else None.

    Returns None when the org has not configured this provider. When the org
    HAS configured it but the bundle is incomplete, logs a warning and returns
    None so the caller falls back to the platform default (CHAOS-2550
    decision #1).
    """
    org_credentials = resolve_llm_org_settings_credentials(provider_name, org_id=org_id)
    if not (org_credentials.api_key or org_credentials.base_url):
        return None
    if not _llm_credentials_complete(provider_name, org_credentials):
        logger.warning(
            "Org BYO LLM credentials for provider '%s' are incomplete; "
            "falling back to the platform default.",
            _safe_log_value(provider_name),
        )
        return None
    return org_credentials


def resolve_llm_credentials(
    provider: str,
    *,
    org_id: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> LLMCredentials:
    provider_name = _normalize_provider(provider)

    # Source-bound resolution (CHAOS-2550): never mix api_key/base_url across
    # sources. A platform key must never reach an org-provided base_url, and an
    # org key must never reach a platform base_url.
    if api_key or base_url:
        # Per-call override: isolated to the explicitly provided values.
        resolved = LLMCredentials(
            api_key=str(api_key or ""),
            base_url=str(base_url or ""),
        )
    else:
        org_credentials = _resolve_org_byo_credentials(provider_name, org_id)
        if org_credentials is not None:
            # Org BYO: both fields come from the org settings only.
            resolved = org_credentials
        else:
            # Platform default: both fields come from the environment only.
            resolved = _env_llm_credentials(provider_name)

    if provider_name in _API_KEY_REQUIRED_PROVIDERS and not resolved.api_key:
        env_names = ", ".join(_API_KEY_ENV_BY_PROVIDER.get(provider_name, ()))
        raise LLMAuthError(
            f"Missing API key for LLM provider '{provider_name}'. Set --llm-api-key, {env_names}, or org-scoped LLM settings.",
            provider=provider_name,
        )
    return resolved
