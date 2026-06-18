from __future__ import annotations

import logging
import os

from dev_health_ops.llm.credentials import resolve_llm_credentials
from dev_health_ops.llm.errors import LLMAuthError

from .base import (
    DEFAULT_MODEL_BY_PROVIDER,
    CompletionResult,
    LLMProvider,
    LLMProviderBase,
)

logger = logging.getLogger(__name__)

_LOGGED_PROVIDER_MODELS: set[tuple[str, str | None]] = set()

_MODEL_ENV_BY_PROVIDER: dict[str, tuple[str, ...]] = {
    "openai": ("LLM_MODEL_OPENAI",),
    "anthropic": ("LLM_MODEL_ANTHROPIC",),
    "gemini": ("LLM_MODEL_GEMINI", "GEMINI_MODEL"),
    "local": ("LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"),
    "ollama": ("LLM_MODEL_OLLAMA", "OLLAMA_MODEL"),
    "lmstudio": ("LLM_MODEL_LMSTUDIO", "LMSTUDIO_MODEL"),
    "qwen": ("LLM_MODEL_QWEN", "QWEN_MODEL"),
    "qwen-local": ("LLM_MODEL_QWEN_LOCAL", "QWEN_LOCAL_MODEL"),
    "qwen-lmstudio": ("LLM_MODEL_QWEN_LMSTUDIO", "LMSTUDIO_MODEL"),
}

_KNOWN_PROVIDERS = {
    "anthropic",
    "gemini",
    "lmstudio",
    "local",
    "mock",
    "none",
    "ollama",
    "openai",
    "qwen",
    "qwen-lmstudio",
    "qwen-local",
}


def _normalize_provider_name(name: str) -> str:
    return (name or "auto").strip().lower()


def _configured_provider() -> str | None:
    if os.getenv("OPENAI_API_KEY"):
        return "openai"
    if os.getenv("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.getenv("GEMINI_API_KEY"):
        return "gemini"
    if os.getenv("LOCAL_LLM_BASE_URL"):
        return "local"
    if os.getenv("DASHSCOPE_API_KEY") or os.getenv("QWEN_API_KEY"):
        return "qwen"
    if os.getenv("OLLAMA_MODEL") or os.getenv("OLLAMA_BASE_URL"):
        return "ollama"
    if os.getenv("LMSTUDIO_MODEL") or os.getenv("LMSTUDIO_BASE_URL"):
        return "lmstudio"
    return None


def _provider_has_required_config(
    name: str, *, api_key: str | None = None, base_url: str | None = None
) -> bool:
    if name == "mock":
        return True
    if name == "none":
        return False
    if name not in _KNOWN_PROVIDERS:
        return False
    try:
        resolve_llm_credentials(name, api_key=api_key, base_url=base_url)
        return True
    except LLMAuthError:
        return False


def _missing_provider_error(name: str) -> LLMAuthError:
    if name == "auto":
        if os.getenv("LLM_API_KEY") or os.getenv("LLM_BASE_URL"):
            return LLMAuthError(
                "A generic LLM_API_KEY/LLM_BASE_URL was provided but no provider "
                "could be auto-detected. A bare credential does not identify which "
                "provider API to call. Set --llm-provider or LLM_PROVIDER "
                "(e.g. openai, anthropic, gemini, qwen, local).",
                provider="auto",
                model="none",
            )
        return LLMAuthError(
            "No LLM provider is configured for auto. Set --llm-provider mock for "
            "fixtures/testing, or configure LLM_PROVIDER plus provider credentials "
            "such as OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, "
            "QWEN_API_KEY/DASHSCOPE_API_KEY, LOCAL_LLM_BASE_URL, OLLAMA_BASE_URL, "
            "or OLLAMA_MODEL.",
            provider="auto",
            model="none",
        )
    if name == "openai":
        env_hint = "OPENAI_API_KEY"
    elif name == "anthropic":
        env_hint = "ANTHROPIC_API_KEY"
    elif name == "gemini":
        env_hint = "GEMINI_API_KEY"
    elif name == "qwen":
        env_hint = "QWEN_API_KEY or DASHSCOPE_API_KEY"
    else:
        env_hint = "LLM_PROVIDER"
    return LLMAuthError(
        f"LLM provider '{name}' is not configured. Set {env_hint} or choose "
        "--llm-provider mock for fixtures/testing.",
        provider=name,
        model="none",
    )


def resolve_provider_name(name: str = "auto") -> str:
    requested = _normalize_provider_name(name)
    if requested != "auto":
        return requested

    env_name = _normalize_provider_name(os.getenv("LLM_PROVIDER", "auto"))
    if env_name != "auto":
        return env_name

    detected = _configured_provider()
    if detected:
        return detected
    raise _missing_provider_error("auto")


def resolve_model_name(provider: str, model: str | None = None) -> str | None:
    provider = _normalize_provider_name(provider)
    if provider == "mock":
        return "mock"
    if provider == "none":
        return None
    if model:
        return model
    for env_name in _MODEL_ENV_BY_PROVIDER.get(provider, ()):
        if os.getenv(env_name):
            return os.getenv(env_name)
    if os.getenv("LLM_MODEL"):
        return os.getenv("LLM_MODEL")
    return DEFAULT_MODEL_BY_PROVIDER.get(provider)


def _log_resolved_provider_model(provider: str, model: str | None) -> None:
    key = (provider, model)
    if key in _LOGGED_PROVIDER_MODELS:
        return
    _LOGGED_PROVIDER_MODELS.add(key)
    logger.info(
        "Resolved LLM provider: provider=%s model=%s", provider, model or "none"
    )


def is_llm_available(name: str = "auto") -> bool:
    try:
        resolved = resolve_provider_name(name)
    except LLMAuthError:
        return False
    return _provider_has_required_config(resolved)


def get_provider(
    name: str = "auto",
    model: str | None = None,
    *,
    api_key: str | None = None,
    base_url: str | None = None,
) -> LLMProvider:
    try:
        provider_name = resolve_provider_name(name)
    except LLMAuthError:
        if (api_key or base_url) and _normalize_provider_name(name) == "auto":
            raise LLMAuthError(
                "A per-call LLM credential (--llm-api-key/--llm-base-url) was "
                "provided but no provider could be auto-detected. Set "
                "--llm-provider or LLM_PROVIDER to choose the provider API.",
                provider="auto",
                model="none",
            ) from None
        raise
    model_name = resolve_model_name(provider_name, model)

    if provider_name == "none":
        from .none import NoneProvider

        _log_resolved_provider_model(provider_name, model_name)
        return NoneProvider()

    if not _provider_has_required_config(
        provider_name, api_key=api_key, base_url=base_url
    ):
        raise _missing_provider_error(provider_name)

    _log_resolved_provider_model(provider_name, model_name)
    credentials = resolve_llm_credentials(
        provider_name, api_key=api_key, base_url=base_url
    )

    if provider_name == "mock":
        from .mock import MockProvider

        return MockProvider()

    if provider_name == "openai":
        from .openai import OpenAIProvider

        return OpenAIProvider(
            api_key=credentials.api_key,
            base_url=credentials.base_url or None,
            model=model_name,
        )

    if provider_name == "anthropic":
        from .anthropic import AnthropicProvider

        return AnthropicProvider(
            api_key=credentials.api_key,
            base_url=credentials.base_url or None,
            model=model_name,
        )

    if provider_name == "gemini":
        from .gemini import GeminiProvider

        return GeminiProvider(
            api_key=credentials.api_key,
            base_url=credentials.base_url or None,
            model=model_name,
        )

    if provider_name == "local":
        from .local import LocalProvider

        return LocalProvider(
            api_key=credentials.api_key or None,
            base_url=credentials.base_url or None,
            model=model_name,
        )

    if provider_name == "ollama":
        from .local import OllamaProvider

        return OllamaProvider(base_url=credentials.base_url or None, model=model_name)

    if provider_name == "lmstudio":
        if model_name and model_name.startswith("openai/gpt-oss"):
            from .local import LMStudioGPT5Provider

            return LMStudioGPT5Provider(
                base_url=credentials.base_url or None, model=model_name
            )

        from .local import LMStudioProvider

        return LMStudioProvider(base_url=credentials.base_url or None, model=model_name)

    if provider_name == "qwen":
        from .qwen import QwenProvider

        return QwenProvider(
            api_key=credentials.api_key,
            base_url=credentials.base_url or None,
            model=model_name,
        )

    if provider_name == "qwen-local":
        from .qwen import QwenLocalProvider

        return QwenLocalProvider(
            base_url=credentials.base_url or None, model=model_name
        )

    if provider_name == "qwen-lmstudio":
        from .qwen import QwenLMStudioProvider

        return QwenLMStudioProvider(
            base_url=credentials.base_url or None, model=model_name
        )

    raise ValueError(f"Unknown LLM provider: {provider_name}")


__all__ = [
    "CompletionResult",
    "LLMProvider",
    "LLMProviderBase",
    "get_provider",
    "is_llm_available",
    "resolve_model_name",
    "resolve_provider_name",
]
