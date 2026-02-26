from __future__ import annotations

import importlib
import sys


def _load_rate_limit_module(monkeypatch, redis_url: str | None):
    captured: dict[str, object] = {}

    class _FakeLimiter:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    if redis_url is None:
        monkeypatch.delenv("REDIS_URL", raising=False)
    else:
        monkeypatch.setenv("REDIS_URL", redis_url)

    module_name = "dev_health_ops.api.middleware.rate_limit"
    sys.modules.pop(module_name, None)
    monkeypatch.setattr("slowapi.Limiter", _FakeLimiter)

    module = importlib.import_module(module_name)
    return module, captured


def test_rate_limiter_uses_memory_storage_when_redis_url_missing(monkeypatch):
    _module, limiter_kwargs = _load_rate_limit_module(monkeypatch, redis_url=None)

    assert limiter_kwargs["storage_uri"] == "memory://"


def test_rate_limiter_accepts_redis_storage_uri(monkeypatch):
    redis_url = "redis://localhost:6379/0"
    _module, limiter_kwargs = _load_rate_limit_module(monkeypatch, redis_url=redis_url)

    assert limiter_kwargs["storage_uri"] == redis_url
