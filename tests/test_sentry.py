from __future__ import annotations

import sys
from copy import deepcopy
from types import ModuleType
from typing import Any

from sentry_sdk.types import Event, Hint

from dev_health_ops import sentry
from dev_health_ops.sync.error_sanitize import REDACTION_MARKER


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


def test_before_send_scrubs_nested_secrets_without_mutating_input() -> None:
    event: Event = {
        "message": "request rejected with Bearer value-to-redact",
        "extra": {
            "deployment": "production",
            "providers": [
                {"PagerDuty_API_Token": "pd-token-value"},
                {"clientSecret": "client-value", "region": "eu"},
            ],
        },
        "exception": {
            "values": [{"type": "ProviderError", "value": "client_secret=client-value"}]
        },
        "breadcrumbs": {
            "values": [{"message": "upstream rejected Bearer breadcrumb-value"}]
        },
        "request": {
            "url": "https://app.example.test/callback?access_token=value-to-redact",
            "data": {
                "oauth": {"client_secret": "client-value"},
                "csrf": "anti-forgery-value",
            },
            "headers": {"Cookie": "session-value", "X-Request-ID": "request-123"},
        },
    }
    original = deepcopy(event)

    hint: Hint = {}
    scrubbed = sentry.before_send(event, hint)

    assert event == original
    assert scrubbed.get("message") == "request rejected with [REDACTED]"
    assert scrubbed.get("extra") == {
        "deployment": "production",
        "providers": [
            {"PagerDuty_API_Token": REDACTION_MARKER},
            {"clientSecret": REDACTION_MARKER, "region": "eu"},
        ],
    }
    assert scrubbed.get("exception") == {
        "values": [{"type": "ProviderError", "value": REDACTION_MARKER}]
    }
    assert scrubbed.get("breadcrumbs") == {
        "values": [{"message": "upstream rejected [REDACTED]"}]
    }
    request = scrubbed.get("request")
    assert isinstance(request, dict)
    assert request["url"] == "https://app.example.test/callback?[REDACTED]"
    assert request["data"] == {"oauth": REDACTION_MARKER, "csrf": REDACTION_MARKER}
    assert request["headers"] == {
        "Cookie": REDACTION_MARKER,
        "X-Request-ID": "request-123",
    }


def test_before_send_preserves_non_mapping_context_values() -> None:
    event: Any = {"contexts": {"runtime": "Python 3.13"}}

    scrubbed = sentry.before_send(event, {})

    assert scrubbed.get("contexts") == {"runtime": {"value": "Python 3.13"}}


def test_init_sentry_disables_default_pii_and_registers_scrubber(
    monkeypatch: Any,
) -> None:
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
    _install_integration_module(
        monkeypatch,
        "sentry_sdk.integrations.strawberry",
        "StrawberryIntegration",
    )
    monkeypatch.setattr(sentry, "_initialized", False)
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")

    assert sentry.init_sentry() is True

    assert captured["send_default_pii"] is False
    assert captured["before_send"] is sentry.before_send
