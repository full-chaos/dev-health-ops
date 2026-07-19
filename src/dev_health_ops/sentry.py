"""Sentry SDK integration for dev-health-ops.

Initializes Sentry for FastAPI and Celery when SENTRY_DSN is set.
Safe to call multiple times (no-op if already initialised or DSN absent).

Compatible with Sentry SaaS, self-hosted Sentry, and BugSink.
When using BugSink, set SENTRY_TRACES_RATE=0 (BugSink ignores traces).

Environment variables:
    SENTRY_DSN            — Sentry DSN (required to activate)
    SENTRY_ENVIRONMENT    — Environment tag (default: production)
    SENTRY_TRACES_RATE    — Traces sample rate 0.0–1.0 (default: 0.0)
    SENTRY_PROFILES_RATE  — Profiles sample rate 0.0–1.0 (default: 0.0)
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final, TypeVar

from dev_health_ops.sync.error_sanitize import REDACTION_MARKER, sanitize_error_text

if TYPE_CHECKING:
    from sentry_sdk.types import Event, Hint

logger = logging.getLogger(__name__)

_initialized = False
_MappingKey = TypeVar("_MappingKey")
_SENSITIVE_KEY_PARTS: Final = frozenset(
    {
        "apikey",
        "auth",
        "authorization",
        "cookie",
        "csrf",
        "oauth",
        "pagerduty",
        "password",
        "secret",
        "token",
    }
)


def _is_sensitive_key(key: str) -> bool:
    normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key).lower()
    parts = frozenset(re.split(r"[^a-z0-9]+", normalized))
    return not _SENSITIVE_KEY_PARTS.isdisjoint(parts)


def _scrub_sentry_value(value: object) -> object:
    if isinstance(value, str):
        return sanitize_error_text(value, max_length=None)
    if isinstance(value, Mapping):
        return _scrub_sentry_mapping(value)
    if isinstance(value, list):
        return [_scrub_sentry_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_scrub_sentry_value(item) for item in value)
    return value


def _scrub_sentry_mapping(
    value: Mapping[_MappingKey, object],
) -> dict[_MappingKey, object]:
    return {
        key: REDACTION_MARKER
        if isinstance(key, str) and _is_sensitive_key(key)
        else _scrub_sentry_value(nested)
        for key, nested in value.items()
    }


def _scrub_sentry_context(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return _scrub_sentry_mapping(value)
    return {"value": _scrub_sentry_value(value)}


def before_send(
    event: Event,
    _hint: Hint,
) -> Event:
    """Return a copy of an event with nested credential-bearing data redacted."""
    scrubbed = event.copy()
    if "extra" in event:
        scrubbed["extra"] = _scrub_sentry_mapping(event["extra"])
    if "request" in event:
        scrubbed["request"] = _scrub_sentry_mapping(event["request"])
    if "user" in event:
        scrubbed["user"] = _scrub_sentry_mapping(event["user"])
    if "logentry" in event:
        scrubbed["logentry"] = _scrub_sentry_mapping(event["logentry"])
    if "contexts" in event:
        scrubbed["contexts"] = {
            key: _scrub_sentry_context(value)
            for key, value in event["contexts"].items()
        }
    if "exception" in event:
        exception = event["exception"].copy()
        exception["values"] = [
            _scrub_sentry_mapping(value) for value in event["exception"]["values"]
        ]
        scrubbed["exception"] = exception
    if "breadcrumbs" in event:
        breadcrumbs = event["breadcrumbs"]
        if isinstance(breadcrumbs, dict):
            scrubbed["breadcrumbs"] = {
                "values": [
                    _scrub_sentry_mapping(value) for value in breadcrumbs["values"]
                ]
            }
    if "message" in event:
        scrubbed["message"] = (
            sanitize_error_text(event["message"], max_length=None) or ""
        )
    if "tags" in event:
        scrubbed["tags"] = {
            key: REDACTION_MARKER
            if _is_sensitive_key(key)
            else sanitize_error_text(value, max_length=None) or ""
            for key, value in event["tags"].items()
        }
    return scrubbed


def init_sentry() -> bool:
    """Initialise Sentry SDK from environment variables.

    Returns True if Sentry was activated, False if skipped (no DSN).
    """
    global _initialized
    if _initialized:
        return True

    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        logger.debug("SENTRY_DSN not set — Sentry disabled")
        return False

    try:
        import sentry_sdk
        from sentry_sdk.integrations.celery import CeleryIntegration
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        from sentry_sdk.integrations.strawberry import StrawberryIntegration

        environment = os.getenv("SENTRY_ENVIRONMENT", "production")
        traces_rate = float(os.getenv("SENTRY_TRACES_RATE", "0.0"))
        profiles_rate = float(os.getenv("SENTRY_PROFILES_RATE", "0.0"))
        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            traces_sample_rate=traces_rate,
            profiles_sample_rate=profiles_rate,
            integrations=[
                StarletteIntegration(transaction_style="endpoint"),
                FastApiIntegration(transaction_style="endpoint"),
                StrawberryIntegration(async_execution=True),
                CeleryIntegration(monitor_beat_tasks=True),
                LoggingIntegration(
                    level=logging.INFO,
                    event_level=logging.ERROR,
                ),
            ],
            send_default_pii=False,
            before_send=before_send,
        )
        _initialized = True
        logger.info(
            "Sentry initialised",
            extra={"environment": environment, "traces_rate": traces_rate},
        )
        return True

    except ImportError:
        logger.warning(
            "sentry-sdk not installed — install sentry-sdk[fastapi,celery] to enable Sentry"
        )
        return False
    except Exception as exc:
        logger.warning("Sentry initialisation failed: %s", exc)
        return False
