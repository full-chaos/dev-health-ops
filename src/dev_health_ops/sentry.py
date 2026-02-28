"""Sentry SDK integration for dev-health-ops.

Initializes Sentry for FastAPI and Celery when SENTRY_DSN is set.
Safe to call multiple times (no-op if already initialised or DSN absent).

Environment variables:
    SENTRY_DSN            — Sentry DSN (required to activate)
    SENTRY_ENVIRONMENT    — Environment tag (default: production)
    SENTRY_TRACES_RATE    — Traces sample rate 0.0–1.0 (default: 0.1)
    SENTRY_PROFILES_RATE  — Profiles sample rate 0.0–1.0 (default: 0.0)
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_initialized = False


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
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        from sentry_sdk.integrations.celery import CeleryIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        environment = os.getenv("SENTRY_ENVIRONMENT", "production")
        traces_rate = float(os.getenv("SENTRY_TRACES_RATE", "0.1"))
        profiles_rate = float(os.getenv("SENTRY_PROFILES_RATE", "0.0"))

        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            traces_sample_rate=traces_rate,
            profiles_sample_rate=profiles_rate,
            integrations=[
                StarletteIntegration(transaction_style="endpoint"),
                FastApiIntegration(transaction_style="endpoint"),
                CeleryIntegration(monitor_beat_tasks=True),
                LoggingIntegration(
                    level=logging.INFO,
                    event_level=logging.ERROR,
                ),
            ],
            # Strip personally identifiable information from events
            send_default_pii=False,
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
