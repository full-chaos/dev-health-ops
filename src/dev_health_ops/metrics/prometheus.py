"""Custom Prometheus metrics for dev-health-ops.

Defines application-level counters, histograms, and gauges for:
  - Celery task execution
  - ClickHouse query latency
  - LLM API calls (OpenAI / Anthropic)
  - GitHub API calls (requests by endpoint/status, rate limit remaining)

Usage:
    from dev_health_ops.metrics.prometheus import (
        CELERY_TASKS_TOTAL,
        CELERY_TASK_DURATION_SECONDS,
        record_celery_task,
        CLICKHOUSE_QUERY_DURATION_SECONDS,
        LLM_REQUESTS_TOTAL,
        LLM_TOKENS_TOTAL,
        record_llm_call,
        GITHUB_API_REQUESTS_TOTAL,
        GITHUB_RATE_LIMIT_REMAINING,
        record_github_api_request,
        record_github_rate_limit,
    )
"""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from importlib import import_module
from typing import Any

try:
    _prometheus_client_module: Any = import_module("prometheus_client")
except ImportError:
    _prometheus_client_module = None

_PROMETHEUS_AVAILABLE = _prometheus_client_module is not None


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


def _noop_gauge(*args, **kwargs):
    class _NoopGauge:
        def labels(self, **kw):
            return self

        def set(self, value):
            pass

        def inc(self, amount=1):
            pass

        def dec(self, amount=1):
            pass

    return _NoopGauge()


if _PROMETHEUS_AVAILABLE:
    assert _prometheus_client_module is not None

    # ---------------------------------------------------------------------------
    # Celery metrics
    # ---------------------------------------------------------------------------
    CELERY_TASKS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_celery_tasks_total",
        "Total number of Celery task executions",
        ["task_name", "state"],
    )

    CELERY_TASK_DURATION_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_celery_task_duration_seconds",
        "Celery task execution duration in seconds",
        ["task_name"],
        buckets=(0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0),
    )

    # ---------------------------------------------------------------------------
    # ClickHouse metrics
    # ---------------------------------------------------------------------------
    CLICKHOUSE_QUERY_DURATION_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_clickhouse_query_duration_seconds",
        "ClickHouse query latency in seconds",
        ["query_type"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )

    CLICKHOUSE_QUERIES_TOTAL = _prometheus_client_module.Counter(
        "devhealth_clickhouse_queries_total",
        "Total ClickHouse queries executed",
        ["query_type", "status"],
    )

    # ---------------------------------------------------------------------------
    # LLM metrics
    # ---------------------------------------------------------------------------
    LLM_REQUESTS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_llm_requests_total",
        "Total LLM API requests",
        ["provider", "model", "status"],
    )

    LLM_TOKENS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_llm_tokens_total",
        "Total LLM tokens consumed",
        ["provider", "model", "token_type"],
    )

    LLM_REQUEST_DURATION_SECONDS = _prometheus_client_module.Histogram(
        "devhealth_llm_request_duration_seconds",
        "LLM API request latency in seconds",
        ["provider", "model"],
        buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    )

    BYO_LLM_BASE_URL_FALLBACK_TOTAL = _prometheus_client_module.Counter(
        "devhealth_byo_llm_base_url_fallback_total",
        "Org BYO LLM base_url fallbacks by provider and reason",
        ["provider", "reason_code", "audit_inserted"],
    )

    BYO_LLM_BASE_URL_FALLBACK_ALERT_TOTAL = _prometheus_client_module.Counter(
        "devhealth_byo_llm_base_url_fallback_alert_total",
        "Sustained org BYO LLM base_url fallback alert signals",
        ["provider", "reason_code"],
    )

    # ---------------------------------------------------------------------------
    # GitHub API metrics
    # ---------------------------------------------------------------------------
    GITHUB_API_REQUESTS_TOTAL = _prometheus_client_module.Counter(
        "devhealth_github_api_requests_total",
        "Total GitHub API requests by endpoint and status code",
        ["endpoint", "status_code"],
    )

    GITHUB_RATE_LIMIT_REMAINING = _prometheus_client_module.Gauge(
        "devhealth_github_rate_limit_remaining",
        "GitHub API rate limit remaining calls by resource type",
        ["resource"],
    )

    INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL = _prometheus_client_module.Counter(
        "devhealth_investment_membership_scope_stale_total",
        "Investment reads that fell back to unscoped results due to stale membership projection",
        ["scope_mode"],
    )

    INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS = _prometheus_client_module.Gauge(
        "devhealth_investment_membership_scope_lag_seconds",
        "Lag between latest work_unit_investments row and latest membership run when stale",
        ["scope_mode"],
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
    BYO_LLM_BASE_URL_FALLBACK_TOTAL = _noop_counter()
    BYO_LLM_BASE_URL_FALLBACK_ALERT_TOTAL = _noop_counter()
    GITHUB_API_REQUESTS_TOTAL = _noop_counter()
    GITHUB_RATE_LIMIT_REMAINING = _noop_gauge()
    INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL = _noop_counter()
    INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS = _noop_gauge()


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


def record_byo_llm_base_url_fallback(
    *, provider: str, reason_code: str, audit_inserted: str
) -> None:
    BYO_LLM_BASE_URL_FALLBACK_TOTAL.labels(
        provider=provider,
        reason_code=reason_code,
        audit_inserted=audit_inserted,
    ).inc()


def record_byo_llm_base_url_fallback_alert(
    *, provider: str, reason_code: str, threshold: str, window_seconds: str
) -> None:
    _ = (threshold, window_seconds)
    BYO_LLM_BASE_URL_FALLBACK_ALERT_TOTAL.labels(
        provider=provider,
        reason_code=reason_code,
    ).inc()


def record_github_api_request(endpoint: str, status_code: str) -> None:
    """Record a GitHub API request with endpoint and HTTP status code."""
    GITHUB_API_REQUESTS_TOTAL.labels(endpoint=endpoint, status_code=status_code).inc()


def record_github_rate_limit(resource: str, remaining: int) -> None:
    """Update the GitHub rate limit remaining gauge for a resource type."""
    GITHUB_RATE_LIMIT_REMAINING.labels(resource=resource).set(remaining)


def record_investment_membership_scope_stale(
    *, lag_seconds: int, scope_mode: str
) -> None:
    INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL.labels(scope_mode=scope_mode).inc()
    INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS.labels(scope_mode=scope_mode).set(
        lag_seconds
    )


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
        CLICKHOUSE_QUERIES_TOTAL.labels(query_type=query_type, status=status).inc()
