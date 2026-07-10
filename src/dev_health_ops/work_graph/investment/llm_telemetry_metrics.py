"""Prometheus and OpenTelemetry instruments for investment LLM telemetry."""

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

try:
    _otel_metrics: Any = import_module("opentelemetry.metrics")
    _meter: Any = _otel_metrics.get_meter(__name__)
except ImportError:
    _meter = None


class _DualCounter:
    def __init__(self, prometheus: Any, otel: Any) -> None:
        self._prometheus = prometheus
        self._otel = otel

    def labels(self, **values: str) -> _BoundCounter:
        prometheus = (
            self._prometheus.labels(**values) if self._prometheus is not None else None
        )
        return _BoundCounter(prometheus, self._otel, values)


class _BoundCounter:
    def __init__(self, prometheus: Any, otel: Any, attributes: dict[str, str]) -> None:
        self._prometheus = prometheus
        self._otel = otel
        self._attributes = attributes

    def inc(self, amount: float = 1) -> None:
        if self._prometheus is not None:
            self._prometheus.inc(amount)
        if self._otel is not None:
            self._otel.add(amount, attributes=self._attributes)


class _DualHistogram:
    def __init__(self, prometheus: Any, otel: Any) -> None:
        self._prometheus = prometheus
        self._otel = otel

    def labels(self, **values: str) -> _BoundHistogram:
        prometheus = (
            self._prometheus.labels(**values) if self._prometheus is not None else None
        )
        return _BoundHistogram(prometheus, self._otel, values)


class _BoundHistogram:
    def __init__(self, prometheus: Any, otel: Any, attributes: dict[str, str]) -> None:
        self._prometheus = prometheus
        self._otel = otel
        self._attributes = attributes

    def observe(self, amount: float) -> None:
        if self._prometheus is not None:
            self._prometheus.observe(amount)
        if self._otel is not None:
            self._otel.record(amount, attributes=self._attributes)


def _counter(
    name: str,
    description: str,
    labels: list[str],
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    prometheus_counter = (
        prometheus.Counter(name, description, labels)
        if prometheus is not None
        else None
    )
    otel_counter = (
        meter.create_counter(name, description=description)
        if meter is not None
        else None
    )
    if prometheus_counter is None and otel_counter is None:
        return _noop_metric()
    return _DualCounter(prometheus_counter, otel_counter)


def _histogram(
    name: str,
    description: str,
    labels: list[str],
    buckets: tuple[float, ...],
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    prometheus_histogram = (
        prometheus.Histogram(name, description, labels, buckets=buckets)
        if prometheus is not None
        else None
    )
    otel_histogram = (
        meter.create_histogram(
            name,
            description=description,
            explicit_bucket_boundaries_advisory=buckets,
        )
        if meter is not None
        else None
    )
    if prometheus_histogram is None and otel_histogram is None:
        return _noop_metric()
    return _DualHistogram(prometheus_histogram, otel_histogram)


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
