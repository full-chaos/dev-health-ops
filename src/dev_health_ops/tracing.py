"""OpenTelemetry distributed tracing for dev-health-ops.

Configures the OpenTelemetry SDK with an OTLP gRPC exporter and
instruments FastAPI, HTTPX, SQLAlchemy, and Celery.

Environment variables:
    OTEL_ENABLED              — set to "false" to disable (default: true)
    OTEL_EXPORTER_OTLP_ENDPOINT — OTLP gRPC endpoint
                                  (default: http://localhost:4317)
    OTEL_SERVICE_NAME         — service name tag (default: dev-health-ops)
    OTEL_ENVIRONMENT          — deployment environment tag (default: production)
    OTEL_SAMPLE_RATE          — head-based sample rate 0.0–1.0 (default: 0.1)

Usage:
    from dev_health_ops.tracing import init_tracing, instrument_fastapi_app
    init_tracing()
    instrument_fastapi_app(app)   # call after the FastAPI app is created
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_initialized = False


def init_tracing() -> bool:
    """Initialise the OpenTelemetry tracer with OTLP gRPC export.

    Returns True if tracing was activated, False if skipped or unavailable.
    """
    global _initialized
    if _initialized:
        return True

    enabled = os.getenv("OTEL_ENABLED", "true").lower() not in ("false", "0", "no")
    if not enabled:
        logger.debug("OpenTelemetry tracing disabled via OTEL_ENABLED=false")
        return False

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        service_name = os.getenv("OTEL_SERVICE_NAME", "dev-health-ops")
        environment = os.getenv("OTEL_ENVIRONMENT", "production")
        otlp_endpoint = os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
        )
        sample_rate = float(os.getenv("OTEL_SAMPLE_RATE", "0.1"))

        # Build head-based sampler
        if sample_rate >= 1.0:
            from opentelemetry.sdk.trace.sampling import ALWAYS_ON as sampler
        elif sample_rate <= 0.0:
            from opentelemetry.sdk.trace.sampling import ALWAYS_OFF as sampler
        else:
            from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

            sampler = TraceIdRatioBased(sample_rate)

        resource = Resource.create(
            {
                "service.name": service_name,
                "deployment.environment": environment,
            }
        )

        exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        provider = TracerProvider(resource=resource, sampler=sampler)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        _initialized = True
        logger.info(
            "OpenTelemetry tracing initialised",
            extra={
                "otlp_endpoint": otlp_endpoint,
                "service_name": service_name,
                "sample_rate": sample_rate,
            },
        )
        return True

    except ImportError as exc:
        logger.warning(
            "opentelemetry packages not installed — tracing disabled: %s", exc
        )
        return False
    except Exception as exc:
        logger.warning("OpenTelemetry initialisation failed: %s", exc)
        return False


def instrument_fastapi_app(app: object) -> None:
    """Instrument a FastAPI app instance with OpenTelemetry.

    Must be called after init_tracing() and after the FastAPI app object
    is created but before the first request is handled.
    """
    if not _initialized:
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(
            app,  # type: ignore[arg-type]
            excluded_urls="/health,/ready,/metrics",
        )

        # HTTPX (outbound HTTP calls)
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()

        # SQLAlchemy (Postgres session)
        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            SQLAlchemyInstrumentor().instrument()
        except Exception:
            pass  # SQLAlchemy instrumentation is optional

        logger.info("FastAPI OpenTelemetry instrumentation applied")
    except ImportError as exc:
        logger.warning("FastAPI OTel instrumentation unavailable: %s", exc)
    except Exception as exc:
        logger.warning("FastAPI OTel instrumentation failed: %s", exc)


def instrument_celery() -> None:
    """Instrument Celery tasks with OpenTelemetry."""
    if not _initialized:
        return
    try:
        from opentelemetry.instrumentation.celery import CeleryInstrumentor

        CeleryInstrumentor().instrument()
        logger.info("Celery OpenTelemetry instrumentation applied")
    except ImportError as exc:
        logger.warning("Celery OTel instrumentation unavailable: %s", exc)
    except Exception as exc:
        logger.warning("Celery OTel instrumentation failed: %s", exc)
