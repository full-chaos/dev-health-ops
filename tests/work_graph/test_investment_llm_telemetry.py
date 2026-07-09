from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field

import pytest

from dev_health_ops.llm.errors import LLMServerError
from dev_health_ops.work_graph.investment import llm_telemetry as telemetry
from dev_health_ops.work_graph.investment import llm_telemetry_metrics as metrics


@dataclass
class _FakeMetric:
    calls: list[tuple[dict[str, str], str, float]] = field(default_factory=list)

    def labels(self, **values: str) -> _FakeMetricChild:
        return _FakeMetricChild(self, values)


@dataclass
class _FakeMetricChild:
    parent: _FakeMetric
    labels: dict[str, str]

    def inc(self, amount: float = 1) -> None:
        self.parent.calls.append((self.labels, "inc", amount))

    def observe(self, amount: float) -> None:
        self.parent.calls.append((self.labels, "observe", amount))


@dataclass
class _FakeSpan:
    attributes: dict[str, str] = field(default_factory=dict)
    recorded_exception: BaseException | None = None
    status_set: bool = False

    def set_attribute(self, key: str, value: str | int) -> None:
        self.attributes[key] = str(value)

    def record_exception(self, exc: BaseException) -> None:
        self.recorded_exception = exc

    def set_status(self, status: object) -> None:
        self.status_set = True

    def add_event(self, name: str, attributes: dict[str, str]) -> None:
        return None


@pytest.fixture
def fake_metrics(monkeypatch: pytest.MonkeyPatch) -> dict[str, _FakeMetric]:
    names = (
        "REQUESTS_TOTAL",
        "REQUEST_DURATION_SECONDS",
        "REQUEST_ERRORS_TOTAL",
        "TOKENS_TOTAL",
        "OUTPUT_CHARS",
        "VALIDATION_TOTAL",
        "VALIDATION_FAILURES_TOTAL",
        "CATEGORIZATION_OUTCOMES_TOTAL",
        "EXPLANATION_PARSE_TOTAL",
    )
    fakes = {name: _FakeMetric() for name in names}
    for name, fake in fakes.items():
        monkeypatch.setattr(metrics, name, fake)
    return fakes


@pytest.mark.parametrize(
    "raw_error, expected",
    [
        ("invalid_weight:quality.bugfix", "invalid_weight"),
        ("non_finite_weight:quality.bugfix", "non_finite_weight"),
        ("negative_weight:quality.bugfix", "negative_weight"),
        ("weight_overflow:quality.bugfix", "weight_overflow"),
        ("weight_sum_not_finite", "weight_sum_not_finite"),
        ("all_weights_zero", "all_weights_zero"),
        ("evidence_quote_invalid_type:0", "evidence_quote_invalid_type"),
        ("uncertainty_invalid_type", "uncertainty_invalid_type"),
    ],
)
def test_validation_error_family_covers_current_validator_codes(
    raw_error: str, expected: str
) -> None:
    assert telemetry.validation_error_family(raw_error) == expected


def test_validation_error_family_never_leaks_dynamic_content() -> None:
    raw_error = "unknown_subcategory:secret-customer-value"
    family = telemetry.validation_error_family(raw_error)
    assert family == "unknown_subcategory"
    assert "secret" not in family


@pytest.mark.parametrize(
    "model, expected",
    [
        ("gpt-5-nano-2026-07-01", "gpt-5-nano"),
        ("gpt-5-mini", "gpt-5-mini"),
        ("arbitrary-user-model-a", "other"),
        ("arbitrary-user-model-b", "other"),
        (None, "unknown"),
    ],
)
def test_model_labels_map_to_fixed_buckets(model: str | None, expected: str) -> None:
    assert telemetry._model_bucket(model) == expected


def test_llm_call_metrics_records_success(fake_metrics: dict[str, _FakeMetric]) -> None:
    with telemetry.llm_call_metrics(
        provider="openai",
        model="gpt-5-nano",
        stage=telemetry.STAGE_INITIAL,
        prompt_kind=telemetry.PROMPT_KIND_CATEGORIZE,
        prompt_version="investment-categorization-v2",
    ) as call:
        call.set_result(model="gpt-5-nano", input_tokens=12, output_tokens=4, text="{}")

    request_labels = fake_metrics["REQUESTS_TOTAL"].calls[0][0]
    assert request_labels == {
        "provider": "openai",
        "model": "gpt-5-nano",
        "stage": "initial",
        "prompt_kind": "investment_categorize",
        "prompt_version": "investment-categorization-v2",
        "outcome": "ok",
    }
    assert [call[2] for call in fake_metrics["TOKENS_TOTAL"].calls] == [12, 4]
    assert fake_metrics["OUTPUT_CHARS"].calls[0][2] == 2


def test_llm_call_metrics_records_bounded_error_and_reraises(
    fake_metrics: dict[str, _FakeMetric], monkeypatch: pytest.MonkeyPatch
) -> None:
    span = _FakeSpan()

    @contextmanager
    def span_context():
        yield span

    monkeypatch.setattr(telemetry, "_span_context", lambda name: span_context())
    with pytest.raises(LLMServerError):
        with telemetry.llm_call_metrics(
            provider="openai",
            model="tenant-secret-model-name",
            stage=telemetry.STAGE_REQUEST,
            prompt_kind=telemetry.PROMPT_KIND_MIX_EXPLAIN,
            prompt_version="investment-mix-explain-v2",
        ):
            raise LLMServerError("api_key=secret")

    labels = fake_metrics["REQUEST_ERRORS_TOTAL"].calls[0][0]
    assert labels["error_family"] == "server_error"
    assert labels["model"] == "other"
    assert "secret" not in str(labels)
    assert span.attributes["llm.provider"] == "openai"
    assert span.attributes["llm.model"] == "other"
    assert span.attributes["llm.stage"] == "request"
    assert span.attributes["llm.prompt_kind"] == "investment_mix_explain"
    assert span.attributes["llm.status"] == "error"
    assert span.status_set is True


def test_record_validation_emits_current_error_families(
    fake_metrics: dict[str, _FakeMetric],
) -> None:
    telemetry.record_validation(
        provider="openai",
        model="gpt-5-nano",
        stage=telemetry.STAGE_INITIAL,
        prompt_version="investment-categorization-v2",
        errors=["all_weights_zero", "evidence_quote_invalid_type:0"],
    )
    families = {
        call[0]["error_family"]
        for call in fake_metrics["VALIDATION_FAILURES_TOTAL"].calls
    }
    assert families == {"all_weights_zero", "evidence_quote_invalid_type"}


def test_terminal_and_parse_outcomes_are_bounded(
    fake_metrics: dict[str, _FakeMetric],
) -> None:
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
        status="forbidden_language",
    )
    assert (
        fake_metrics["CATEGORIZATION_OUTCOMES_TOTAL"].calls[0][0]["status"]
        == "invalid_llm_output"
    )
    assert (
        fake_metrics["EXPLANATION_PARSE_TOTAL"].calls[0][0]["status"]
        == "forbidden_language"
    )


def test_noop_metric_accepts_all_operations() -> None:
    metric = metrics._noop_metric()
    metric.labels(provider="openai").inc()
    metric.labels(provider="openai").observe(0.1)
