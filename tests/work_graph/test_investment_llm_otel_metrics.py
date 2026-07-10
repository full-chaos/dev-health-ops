from __future__ import annotations

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import Histogram, InMemoryMetricReader

from dev_health_ops.work_graph.investment import llm_telemetry as telemetry
from dev_health_ops.work_graph.investment import llm_telemetry_metrics as metrics


def test_investment_metrics_reach_otel_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
    meter = provider.get_meter("investment-telemetry-test")
    common_labels = ["provider", "model", "stage", "prompt_kind", "prompt_version"]
    terminal_labels = ["provider", "model", "prompt_kind", "prompt_version", "status"]
    replacements = {
        "REQUESTS_TOTAL": metrics._counter(
            "devhealth_investment_llm_requests_total",
            "requests",
            [*common_labels, "outcome"],
            meter=meter,
            prometheus=None,
        ),
        "REQUEST_DURATION_SECONDS": metrics._histogram(
            "devhealth_investment_llm_request_duration_seconds",
            "duration",
            common_labels,
            (0.1, 1.0),
            meter=meter,
            prometheus=None,
        ),
        "REQUEST_ERRORS_TOTAL": metrics._counter(
            "devhealth_investment_llm_request_errors_total",
            "errors",
            [*common_labels, "error_family"],
            meter=meter,
            prometheus=None,
        ),
        "TOKENS_TOTAL": metrics._counter(
            "devhealth_investment_llm_tokens_total",
            "tokens",
            [*common_labels, "direction"],
            meter=meter,
            prometheus=None,
        ),
        "OUTPUT_CHARS": metrics._histogram(
            "devhealth_investment_llm_output_chars",
            "output chars",
            common_labels,
            (1.0, 10.0),
            meter=meter,
            prometheus=None,
        ),
        "VALIDATION_TOTAL": metrics._counter(
            "devhealth_investment_llm_validation_total",
            "validation",
            [*common_labels, "result"],
            meter=meter,
            prometheus=None,
        ),
        "VALIDATION_FAILURES_TOTAL": metrics._counter(
            "devhealth_investment_llm_validation_failures_total",
            "validation failures",
            [*common_labels, "error_family"],
            meter=meter,
            prometheus=None,
        ),
        "CATEGORIZATION_OUTCOMES_TOTAL": metrics._counter(
            "devhealth_investment_llm_categorization_outcomes_total",
            "categorization outcomes",
            terminal_labels,
            meter=meter,
            prometheus=None,
        ),
        "EXPLANATION_PARSE_TOTAL": metrics._counter(
            "devhealth_investment_llm_explanation_parse_total",
            "explanation parse",
            terminal_labels,
            meter=meter,
            prometheus=None,
        ),
    }
    for name, replacement in replacements.items():
        monkeypatch.setattr(metrics, name, replacement)

    with telemetry.llm_call_metrics(
        provider="openai",
        model="gpt-5-nano",
        stage=telemetry.STAGE_INITIAL,
        prompt_kind=telemetry.PROMPT_KIND_CATEGORIZE,
        prompt_version="investment-categorization-v2",
    ) as call:
        call.set_result(model="gpt-5-nano", input_tokens=12, output_tokens=4, text="{}")
    telemetry.record_validation(
        provider="openai",
        model="gpt-5-nano",
        stage=telemetry.STAGE_REPAIR,
        prompt_version="investment-categorization-v2",
        errors=["all_weights_zero"],
    )
    telemetry.record_batch_completion(
        provider="openai",
        model="gpt-5-nano",
        prompt_version="investment-categorization-v2",
        duration_seconds=0.5,
        input_tokens=3,
        output_tokens=2,
        output_chars=20,
        succeeded=False,
    )
    telemetry.record_categorization_outcome(
        provider="openai",
        model="gpt-5-nano",
        prompt_version="investment-categorization-v2",
        status="invalid_llm_output",
    )
    telemetry.record_explanation_parse(
        provider="openai",
        model="gpt-5-nano",
        prompt_version="investment-mix-explain-v2",
        status="fallback",
    )

    data = reader.get_metrics_data()
    assert data is not None
    exported = {
        metric.name: [dict(point.attributes or {}) for point in metric.data.data_points]
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
    }
    duration_metric = next(
        metric
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == "devhealth_investment_llm_request_duration_seconds"
    )
    assert isinstance(duration_metric.data, Histogram)
    assert duration_metric.data.data_points[0].explicit_bounds == (0.1, 1.0)
    assert set(exported) == {
        "devhealth_investment_llm_requests_total",
        "devhealth_investment_llm_request_duration_seconds",
        "devhealth_investment_llm_request_errors_total",
        "devhealth_investment_llm_tokens_total",
        "devhealth_investment_llm_output_chars",
        "devhealth_investment_llm_validation_total",
        "devhealth_investment_llm_validation_failures_total",
        "devhealth_investment_llm_categorization_outcomes_total",
        "devhealth_investment_llm_explanation_parse_total",
    }
    attribute_keys = {
        key for points in exported.values() for point in points for key in point
    }
    assert attribute_keys <= {
        "provider",
        "model",
        "stage",
        "prompt_kind",
        "prompt_version",
        "outcome",
        "error_family",
        "direction",
        "result",
        "status",
    }
    token_points = exported["devhealth_investment_llm_tokens_total"]
    assert {point["direction"] for point in token_points} == {"input", "output"}
    validation_points = exported["devhealth_investment_llm_validation_total"]
    assert validation_points[0]["stage"] == "repair"
    outcome_points = exported["devhealth_investment_llm_categorization_outcomes_total"]
    assert outcome_points[0]["status"] == "invalid_llm_output"
    provider.shutdown()
