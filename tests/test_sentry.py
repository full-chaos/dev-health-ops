from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

from dev_health_ops import sentry


class _Integration:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


def _install_integration_module(
    monkeypatch: Any,
    module_name: str,
    class_name: str,
) -> type[_Integration]:
    module = ModuleType(module_name)
    integration_class = type(class_name, (_Integration,), {})
    setattr(module, class_name, integration_class)
    monkeypatch.setitem(sys.modules, module_name, module)
    return integration_class


def test_init_sentry_configures_strawberry_async_execution(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    sentry_sdk = ModuleType("sentry_sdk")

    def init(**kwargs: Any) -> None:
        captured.update(kwargs)

    sentry_sdk.init = init  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentry_sdk", sentry_sdk)
    monkeypatch.setitem(
        sys.modules, "sentry_sdk.integrations", ModuleType("integrations")
    )
    _install_integration_module(
        monkeypatch,
        "sentry_sdk.integrations.celery",
        "CeleryIntegration",
    )
    _install_integration_module(
        monkeypatch,
        "sentry_sdk.integrations.fastapi",
        "FastApiIntegration",
    )
    _install_integration_module(
        monkeypatch,
        "sentry_sdk.integrations.logging",
        "LoggingIntegration",
    )
    _install_integration_module(
        monkeypatch,
        "sentry_sdk.integrations.starlette",
        "StarletteIntegration",
    )
    strawberry_integration = _install_integration_module(
        monkeypatch,
        "sentry_sdk.integrations.strawberry",
        "StrawberryIntegration",
    )
    monkeypatch.setattr(sentry, "_initialized", False)
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")

    assert sentry.init_sentry() is True

    strawberry_instances = [
        integration
        for integration in captured["integrations"]
        if isinstance(integration, strawberry_integration)
    ]
    assert len(strawberry_instances) == 1
    assert strawberry_instances[0].kwargs == {"async_execution": True}
