"""Observability instrumentation (OpenTelemetry + Prometheus).

Extracted from ``api.main`` so that ``main.py`` remains composition-only.
``register_observability`` mirrors the original inline behavior: it wires
OpenTelemetry FastAPI instrumentation and, when the optional
``prometheus-fastapi-instrumentator`` dependency is installed, exposes a
``/metrics`` endpoint.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dev_health_ops.tracing import instrument_fastapi_app

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


def register_observability(app: FastAPI) -> None:
    """Attach tracing + metrics instrumentation to ``app``.

    OpenTelemetry instrumentation is always applied. Prometheus instrumentation
    is best-effort: if ``prometheus-fastapi-instrumentator`` is not installed,
    a warning is logged and ``/metrics`` is simply not exposed (matching the
    original behavior).
    """
    # OpenTelemetry FastAPI instrumentation (must run after app is created).
    instrument_fastapi_app(app)

    # Prometheus metrics — expose /metrics endpoint when the optional
    # dependency is present.
    try:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator(
            should_group_status_codes=True,
            should_ignore_untemplated=True,
            excluded_handlers=["/health", "/ready", "/metrics"],
        ).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
        logger.info("Prometheus /metrics endpoint enabled")
    except ImportError:
        logger.warning(
            "prometheus-fastapi-instrumentator not installed — "
            "/metrics endpoint disabled"
        )


__all__ = ["register_observability"]
