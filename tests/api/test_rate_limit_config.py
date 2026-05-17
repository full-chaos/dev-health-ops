from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest


def _reload_rate_limit(
    monkeypatch,
    *,
    redis_url: str | None,
    environment: str | None = None,
) -> tuple[ModuleType, dict[str, object]]:
    """Reload rate_limit module under controlled env, using a FakeLimiter to capture kwargs."""
    if redis_url is None:
        monkeypatch.delenv("REDIS_URL", raising=False)
    else:
        monkeypatch.setenv("REDIS_URL", redis_url)

    if environment is None:
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        monkeypatch.delenv("APP_ENV", raising=False)
        monkeypatch.delenv("ENV", raising=False)
    else:
        monkeypatch.setenv("ENVIRONMENT", environment)

    captured: dict[str, object] = {}

    class _FakeLimiter:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    module_name = "dev_health_ops.api.middleware.rate_limit"
    sys.modules.pop(module_name, None)
    monkeypatch.setattr("slowapi.Limiter", _FakeLimiter)

    module = importlib.import_module(module_name)
    return module, captured


def _reload_rate_limit_noop(
    monkeypatch,
    *,
    environment: str | None = None,
) -> ModuleType:
    """Reload rate_limit with slowapi made unimportable (simulates missing install)."""
    if environment is None:
        monkeypatch.delenv("ENVIRONMENT", raising=False)
    else:
        monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.delenv("REDIS_URL", raising=False)

    module_name = "dev_health_ops.api.middleware.rate_limit"
    sys.modules.pop(module_name, None)

    # Make slowapi unimportable so the try/except in rate_limit.py sets Limiter = None.
    monkeypatch.setitem(sys.modules, "slowapi", None)  # type: ignore[arg-type]
    monkeypatch.setitem(sys.modules, "slowapi.util", None)  # type: ignore[arg-type]

    return importlib.import_module(module_name)


# ---------------------------------------------------------------------------
# CHAOS-1553: key_func must be get_forwarded_ip
# ---------------------------------------------------------------------------


def test_rate_limiter_key_func_is_get_forwarded_ip(monkeypatch):
    """key_func must be get_forwarded_ip, not get_remote_address (CHAOS-1553)."""
    module, captured = _reload_rate_limit(
        monkeypatch,
        redis_url="redis://localhost:6379/0",
        environment="development",
    )
    # Compare against the function from the *same* reloaded module instance.
    assert captured.get("key_func") is module.get_forwarded_ip


def test_rate_limiter_accepts_redis_storage_uri(monkeypatch):
    """When REDIS_URL is set, the limiter must use it as storage_uri."""
    redis_url = "redis://localhost:6379/0"
    _module, captured = _reload_rate_limit(
        monkeypatch, redis_url=redis_url, environment="development"
    )
    assert captured["storage_uri"] == redis_url


def test_rate_limiter_backend_redis_when_redis_url_set(monkeypatch):
    """LIMITER_BACKEND must be 'redis' when REDIS_URL is configured."""
    module, _ = _reload_rate_limit(
        monkeypatch,
        redis_url="redis://localhost:6379/0",
        environment="development",
    )
    assert module.LIMITER_BACKEND == "redis"


# ---------------------------------------------------------------------------
# CHAOS-1554: no silent fallback to memory:// in prod; no silent NoOp in prod
# ---------------------------------------------------------------------------


def test_rate_limiter_raises_in_prod_without_redis(monkeypatch):
    """Production startup must raise RuntimeError when REDIS_URL is missing (CHAOS-1554)."""
    with pytest.raises(RuntimeError, match="REDIS_URL must be set"):
        _reload_rate_limit(monkeypatch, redis_url=None, environment="production")


def test_rate_limiter_memory_allowed_in_development(monkeypatch):
    """Dev environment may fall back to in-memory storage."""
    module, captured = _reload_rate_limit(
        monkeypatch, redis_url=None, environment="development"
    )
    assert captured["storage_uri"] == "memory://"
    assert module.LIMITER_BACKEND == "memory"


def test_rate_limiter_memory_allowed_in_test_env(monkeypatch):
    """Test environment may fall back to in-memory storage."""
    module, captured = _reload_rate_limit(
        monkeypatch, redis_url=None, environment="test"
    )
    assert captured["storage_uri"] == "memory://"
    assert module.LIMITER_BACKEND == "memory"


def test_noop_limiter_raises_in_prod(monkeypatch):
    """slowapi absent in prod must raise RuntimeError, never silently disable limits (CHAOS-1554)."""
    with pytest.raises(RuntimeError, match="slowapi is not installed"):
        _reload_rate_limit_noop(monkeypatch, environment="production")


def test_noop_limiter_allowed_in_development(monkeypatch):
    """slowapi absent in dev is acceptable — NoOp limiter is used."""
    module = _reload_rate_limit_noop(monkeypatch, environment="development")
    assert module.LIMITER_BACKEND == "noop"
