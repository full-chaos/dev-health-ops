"""Custom Prometheus metrics for dev-health-ops.

Defines application-level counters, histograms, and gauges for:
  - Celery task execution
  - ClickHouse query latency
  - LLM API calls (OpenAI / Anthropic)

Usage:
    from dev_health_ops.metrics.prometheus import (
        CELERY_TASKS_TOTAL,
        CELERY_TASK_DURATION_SECONDS,
        record_celery_task,
        CLICKHOUSE_QUERY_DURATION_SECONDS,
        LLM_REQUESTS_TOTAL,
        LLM_TOKENS_TOTAL,
        record_llm_call,
    )
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Generator

try:
    from prometheus_client import Counter, Histogram
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False


def _noop_counter(*args, **kwargs):
    class _Noop:
        def labels(self, **kw):
            return self
        def inc(self, amount=1):
            pass
        def observe(self, amount):
            pass
    return _Noop()


def _noop_histogram(*args, **kwargs):
    return _noop_counter()


if _PROMETHEUS_AVAILABLE:
    # ---------------------------------------------------------------------------
    # Celery metrics
    # ---------------------------------------------------------------------------
    CELERY_TASKS_TOTAL = Counter(
        "devhealth_celery_tasks_total",
        "Total number of Celery task executions",
        ["task_name", "state"],
    )

    CELERY_TASK_DURATION_SECONDS = Histogram(
        "devhealth_celery_task_duration_seconds",
        "Celery task execution duration in seconds",
        ["task_name"],
        buckets=(0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0),
    )

    # ---------------------------------------------------------------------------
    # ClickHouse metrics
    # ---------------------------------------------------------------------------
    CLICKHOUSE_QUERY_DURATION_SECONDS = Histogram(
        "devhealth_clickhouse_query_duration_seconds",
        "ClickHouse query latency in seconds",
        ["query_type"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )

    CLICKHOUSE_QUERIES_TOTAL = Counter(
        "devhealth_clickhouse_queries_total",
        "Total ClickHouse queries executed",
        ["query_type", "status"],
    )

    # ---------------------------------------------------------------------------
    # LLM metrics
    # ---------------------------------------------------------------------------
    LLM_REQUESTS_TOTAL = Counter(
        "devhealth_llm_requests_total",
        "Total LLM API requests",
        ["provider", "model", "status"],
    )

    LLM_TOKENS_TOTAL = Counter(
        "devhealth_llm_tokens_total",
        "Total LLM tokens consumed",
        ["provider", "model", "token_type"],
    )

    LLM_REQUEST_DURATION_SECONDS = Histogram(
        "devhealth_llm_request_duration_seconds",
        "LLM API request latency in seconds",
        ["provider", "model"],
        buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    )

else:
    # Graceful no-ops when prometheus_client is unavailable
    CELERY_TASKS_TOTAL = _noop_counter()
    CELERY_TASK_DURATION_SECONDS = _noop_histogram()
    CLICKHOUSE_QUERY_DURATION_SECONDS = _noop_histogram()
    CLICKHOUSE_QUERIES_TOTAL = _noop_counter()
    LLM_REQUESTS_TOTAL = _noop_counter()
    LLM_TOKENS_TOTAL = _noop_counter()
    LLM_REQUEST_DURATION_SECONDS = _noop_histogram()


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def record_celery_task(task_name: str, state: str, duration_seconds: float) -> None:
    """Record Celery task completion metrics."""
    CELERY_TASKS_TOTAL.labels(task_name=task_name, state=state).inc()
    if state == "success":
        CELERY_TASK_DURATION_SECONDS.labels(task_name=task_name).observe(
            duration_seconds
        )


def record_llm_call(
    provider: str,
    model: str,
    status: str,
    duration_seconds: float,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
) -> None:
    """Record an LLM API call with token usage."""
    LLM_REQUESTS_TOTAL.labels(provider=provider, model=model, status=status).inc()
    LLM_REQUEST_DURATION_SECONDS.labels(provider=provider, model=model).observe(
        duration_seconds
    )
    if prompt_tokens > 0:
        LLM_TOKENS_TOTAL.labels(
            provider=provider, model=model, token_type="prompt"
        ).inc(prompt_tokens)
    if completion_tokens > 0:
        LLM_TOKENS_TOTAL.labels(
            provider=provider, model=model, token_type="completion"
        ).inc(completion_tokens)


@contextmanager
def clickhouse_query_timer(query_type: str = "query") -> Generator[None, None, None]:
    """Context manager that records ClickHouse query latency."""
    start = time.perf_counter()
    status = "success"
    try:
        yield
    except Exception:
        status = "error"
        raise
    finally:
        duration = time.perf_counter() - start
        CLICKHOUSE_QUERY_DURATION_SECONDS.labels(query_type=query_type).observe(
            duration
        )
        CLICKHOUSE_QUERIES_TOTAL.labels(
            query_type=query_type, status=status
        ).inc()
