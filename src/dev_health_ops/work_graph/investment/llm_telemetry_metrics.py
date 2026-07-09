"""Prometheus collectors for investment LLM telemetry."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _noop_metric(*args: Any, **kwargs: Any) -> Any:
    class _Noop:
        def labels(self, **values: str) -> _Noop:
            return self

        def inc(self, amount: float = 1) -> None:
            return None

        def observe(self, amount: float) -> None:
            return None

    return _Noop()


try:
    _prometheus: Any = import_module("prometheus_client")
except ImportError:
    _prometheus = None


def _counter(name: str, description: str, labels: list[str]) -> Any:
    if _prometheus is None:
        return _noop_metric()
    return _prometheus.Counter(name, description, labels)


def _histogram(
    name: str, description: str, labels: list[str], buckets: tuple[float, ...]
) -> Any:
    if _prometheus is None:
        return _noop_metric()
    return _prometheus.Histogram(name, description, labels, buckets=buckets)


COMMON_LABELS = ["provider", "model", "stage", "prompt_kind", "prompt_version"]

REQUESTS_TOTAL = _counter(
    "devhealth_investment_llm_requests_total",
    "Investment LLM requests by bounded provider, model, prompt, stage, and outcome",
    [*COMMON_LABELS, "outcome"],
)
REQUEST_DURATION_SECONDS = _histogram(
    "devhealth_investment_llm_request_duration_seconds",
    "Investment LLM request latency",
    COMMON_LABELS,
    (0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 60, 120),
)
REQUEST_ERRORS_TOTAL = _counter(
    "devhealth_investment_llm_request_errors_total",
    "Investment LLM request failures by bounded family",
    [*COMMON_LABELS, "error_family"],
)
TOKENS_TOTAL = _counter(
    "devhealth_investment_llm_tokens_total",
    "Investment LLM tokens",
    [*COMMON_LABELS, "direction"],
)
OUTPUT_CHARS = _histogram(
    "devhealth_investment_llm_output_chars",
    "Investment LLM output characters",
    COMMON_LABELS,
    (100, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000),
)
VALIDATION_TOTAL = _counter(
    "devhealth_investment_llm_validation_total",
    "Investment categorization validation outcomes",
    [*COMMON_LABELS, "result"],
)
VALIDATION_FAILURES_TOTAL = _counter(
    "devhealth_investment_llm_validation_failures_total",
    "Investment categorization validation failures by bounded family",
    [*COMMON_LABELS, "error_family"],
)
CATEGORIZATION_OUTCOMES_TOTAL = _counter(
    "devhealth_investment_llm_categorization_outcomes_total",
    "Terminal investment categorization outcomes",
    ["provider", "model", "prompt_kind", "prompt_version", "status"],
)
EXPLANATION_PARSE_TOTAL = _counter(
    "devhealth_investment_llm_explanation_parse_total",
    "Investment explanation parse outcomes",
    ["provider", "model", "prompt_kind", "prompt_version", "status"],
)
