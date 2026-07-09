"""Bounded Prometheus and OpenTelemetry signals for investment LLM calls."""

from __future__ import annotations

import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any

from . import llm_telemetry_metrics as metrics
from .llm_telemetry_labels import (
    CATEGORIZATION_STATUSES,
    PARSE_STATUSES,
    PROMPT_KIND_CATEGORIZE,
    PROMPT_KIND_MIX_EXPLAIN,
    PROMPT_KINDS,
    PROMPT_VERSIONS,
    STAGE_INITIAL,
    STAGE_REPAIR,
    STAGE_REQUEST,
    STAGES,
    classify_llm_exception_family,
    validation_error_family,
)
from .llm_telemetry_labels import bounded as _bounded
from .llm_telemetry_labels import model_bucket as _model_bucket
from .llm_telemetry_labels import provider_bucket as _provider_bucket

__all__ = [
    "PROMPT_KIND_CATEGORIZE",
    "PROMPT_KIND_MIX_EXPLAIN",
    "STAGE_INITIAL",
    "STAGE_REPAIR",
    "STAGE_REQUEST",
    "classify_llm_exception_family",
    "llm_call_metrics",
    "record_batch_completion",
    "record_categorization_outcome",
    "record_explanation_parse",
    "record_validation",
    "validation_error_family",
]


def _labels(
    *,
    provider: str,
    model: str | None,
    stage: str,
    prompt_kind: str,
    prompt_version: str,
) -> dict[str, str]:
    return {
        "provider": _provider_bucket(provider),
        "model": _model_bucket(model),
        "stage": _bounded(stage, STAGES),
        "prompt_kind": _bounded(prompt_kind, PROMPT_KINDS),
        "prompt_version": _bounded(prompt_version, PROMPT_VERSIONS),
    }


def _current_span() -> Any:
    try:
        from opentelemetry import trace
    except ImportError:
        return None
    return trace.get_current_span()


def _span_context(name: str) -> Any:
    try:
        from opentelemetry import trace
    except ImportError:
        return nullcontext(None)
    return trace.get_tracer(__name__).start_as_current_span(name)


@dataclass(slots=True)
class LLMCallRecording:
    """Mutable request measurements populated inside ``llm_call_metrics``."""

    model: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    output_chars: int = 0

    def set_result(
        self,
        *,
        model: str,
        input_tokens: int | None,
        output_tokens: int | None,
        text: str,
    ) -> None:
        self.model = model
        self.input_tokens = int(input_tokens or 0)
        self.output_tokens = int(output_tokens or 0)
        self.output_chars = len(text)


@contextmanager
def llm_call_metrics(
    *,
    provider: str,
    model: str | None,
    stage: str,
    prompt_kind: str,
    prompt_version: str,
) -> Generator[LLMCallRecording, None, None]:
    recording = LLMCallRecording(model=model)
    started = time.perf_counter()
    with _span_context("llm.complete") as span:
        labels = _labels(
            provider=provider,
            model=model,
            stage=stage,
            prompt_kind=prompt_kind,
            prompt_version=prompt_version,
        )
        if span is not None:
            for key, value in labels.items():
                span.set_attribute(f"llm.{key}", value)
        try:
            yield recording
        except Exception as exc:
            duration = time.perf_counter() - started
            metrics.REQUESTS_TOTAL.labels(**labels, outcome="error").inc()
            metrics.REQUEST_DURATION_SECONDS.labels(**labels).observe(duration)
            metrics.REQUEST_ERRORS_TOTAL.labels(
                **labels, error_family=classify_llm_exception_family(exc)
            ).inc()
            if span is not None:
                span.set_attribute("llm.status", "error")
                span.record_exception(exc)
                try:
                    from opentelemetry.trace import Status, StatusCode

                    span.set_status(Status(StatusCode.ERROR))
                except ImportError:
                    pass
            raise
        else:
            labels["model"] = _model_bucket(recording.model)
            metrics.REQUESTS_TOTAL.labels(**labels, outcome="ok").inc()
            metrics.REQUEST_DURATION_SECONDS.labels(**labels).observe(
                time.perf_counter() - started
            )
            metrics.TOKENS_TOTAL.labels(**labels, direction="input").inc(
                recording.input_tokens
            )
            metrics.TOKENS_TOTAL.labels(**labels, direction="output").inc(
                recording.output_tokens
            )
            metrics.OUTPUT_CHARS.labels(**labels).observe(recording.output_chars)
            if span is not None:
                for key, value in labels.items():
                    span.set_attribute(f"llm.{key}", value)
                span.set_attribute("llm.status", "ok")
                span.set_attribute("llm.input_tokens", recording.input_tokens)
                span.set_attribute("llm.output_tokens", recording.output_tokens)
                span.set_attribute("llm.output_chars", recording.output_chars)


def record_validation(
    *,
    provider: str,
    model: str | None,
    stage: str,
    prompt_version: str,
    errors: Sequence[str],
) -> None:
    labels = _labels(
        provider=provider,
        model=model,
        stage=stage,
        prompt_kind=PROMPT_KIND_CATEGORIZE,
        prompt_version=prompt_version,
    )
    result = "invalid" if errors else "valid"
    metrics.VALIDATION_TOTAL.labels(**labels, result=result).inc()
    for family in {validation_error_family(error) for error in errors}:
        metrics.VALIDATION_FAILURES_TOTAL.labels(**labels, error_family=family).inc()
    span = _current_span()
    if span is not None:
        span.add_event("llm.validation", {**labels, "validation.result": result})


def record_batch_completion(
    *,
    provider: str,
    model: str | None,
    prompt_version: str,
    duration_seconds: float,
    input_tokens: int,
    output_tokens: int,
    output_chars: int,
    succeeded: bool,
) -> None:
    labels = _labels(
        provider=provider,
        model=model,
        stage=STAGE_INITIAL,
        prompt_kind=PROMPT_KIND_CATEGORIZE,
        prompt_version=prompt_version,
    )
    outcome = "ok" if succeeded else "error"
    metrics.REQUESTS_TOTAL.labels(**labels, outcome=outcome).inc()
    metrics.REQUEST_DURATION_SECONDS.labels(**labels).observe(max(0, duration_seconds))
    metrics.TOKENS_TOTAL.labels(**labels, direction="input").inc(max(0, input_tokens))
    metrics.TOKENS_TOTAL.labels(**labels, direction="output").inc(max(0, output_tokens))
    metrics.OUTPUT_CHARS.labels(**labels).observe(max(0, output_chars))
    if not succeeded:
        metrics.REQUEST_ERRORS_TOTAL.labels(
            **labels, error_family="batch_item_error"
        ).inc()
    span = _current_span()
    if span is not None:
        span.add_event("llm.batch.item", {**labels, "llm.status": outcome})


def record_categorization_outcome(
    *, provider: str, model: str | None, prompt_version: str, status: str
) -> None:
    labels = {
        "provider": _provider_bucket(provider),
        "model": _model_bucket(model),
        "prompt_kind": PROMPT_KIND_CATEGORIZE,
        "prompt_version": _bounded(prompt_version, PROMPT_VERSIONS),
        "status": _bounded(status, CATEGORIZATION_STATUSES),
    }
    metrics.CATEGORIZATION_OUTCOMES_TOTAL.labels(**labels).inc()
    span = _current_span()
    if span is not None:
        span.add_event("llm.categorization.outcome", labels)


def record_explanation_parse(
    *, provider: str, model: str | None, prompt_version: str, status: str
) -> None:
    labels = {
        "provider": _provider_bucket(provider),
        "model": _model_bucket(model),
        "prompt_kind": PROMPT_KIND_MIX_EXPLAIN,
        "prompt_version": _bounded(prompt_version, PROMPT_VERSIONS),
        "status": _bounded(status, PARSE_STATUSES),
    }
    metrics.EXPLANATION_PARSE_TOTAL.labels(**labels).inc()
    span = _current_span()
    if span is not None:
        span.add_event("llm.explanation.parse", labels)
