"""Prometheus + OpenTelemetry instrument builders shared across metric modules.

Extracted from ``work_graph/investment/llm_telemetry_metrics.py`` (CHAOS-4112)
so a second module can register counters without copying the dual-backend
plumbing. Behaviour is unchanged: each instrument writes to whichever backends
are importable, and degrades to a no-op when neither is.

Each consumer keeps its OWN meter (``get_meter(__name__)``) and passes it in,
so instruments stay attributed to the module that owns them rather than to
this one.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "build_counter",
    "build_histogram",
    "load_otel_meter",
    "load_prometheus",
    "noop_metric",
]


def noop_metric(*args: Any, **kwargs: Any) -> Any:
    class _Noop:
        def labels(self, **values: str) -> _Noop:
            return self

        def inc(self, amount: float = 1) -> None:
            return None

        def observe(self, amount: float) -> None:
            return None

    return _Noop()


def load_prometheus() -> Any:
    try:
        return import_module("prometheus_client")
    except ImportError:
        return None


def load_otel_meter(module_name: str) -> Any:
    try:
        otel_metrics: Any = import_module("opentelemetry.metrics")
    except ImportError:
        return None
    return otel_metrics.get_meter(module_name)


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


def build_counter(
    name: str,
    description: str,
    labels: list[str],
    *,
    meter: Any,
    prometheus: Any,
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
        return noop_metric()
    return _DualCounter(prometheus_counter, otel_counter)


def build_histogram(
    name: str,
    description: str,
    labels: list[str],
    buckets: tuple[float, ...],
    *,
    meter: Any,
    prometheus: Any,
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
        return noop_metric()
    return _DualHistogram(prometheus_histogram, otel_histogram)
