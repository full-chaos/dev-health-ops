"""Prometheus and OpenTelemetry instruments for investment LLM telemetry.

The dual-backend plumbing lives in ``dev_health_ops.telemetry_metrics``
(extracted under CHAOS-4112 so the work-item attribution module can register
counters without copying it). This module keeps its own meter, so these
instruments stay attributed to it.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    build_histogram,
    load_otel_meter,
    load_prometheus,
    noop_metric,
)

# Kept under its original private name: this module's existing test surface
# refers to it (tests/work_graph/test_investment_llm_telemetry.py).
_noop_metric = noop_metric

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def _counter(
    name: str,
    description: str,
    labels: list[str],
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    return build_counter(name, description, labels, meter=meter, prometheus=prometheus)


def _histogram(
    name: str,
    description: str,
    labels: list[str],
    buckets: tuple[float, ...],
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    return build_histogram(
        name, description, labels, buckets, meter=meter, prometheus=prometheus
    )


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
