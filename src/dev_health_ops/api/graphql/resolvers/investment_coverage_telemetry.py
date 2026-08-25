"""Instrument for the investment Sankey coverage query's exception fallback
(CHAOS-4241).

``resolve_analytics``'s coverage branch wraps its ClickHouse query in a
``try/except`` and falls back to ``coverage=None`` on any failure, so the
Team/Repo coverage cards degrade to an honest empty state instead of a 500.
That fallback is exactly how a real, unrelated SQL bug (an ambiguous
``repo_id`` identifier, discovered while fixing CHAOS-4241) went unnoticed:
the coverage query had apparently never executed successfully against real
ClickHouse, and every failure was swallowed silently -- no counter, and the
existing ``logger.error`` line was easy to miss among the rest of the API's
error volume. A raising coverage query is neither a cache hit nor a normal
empty result, so it needs its own signal, the same reasoning as
``core.cache_epoch_telemetry``'s cache-bypass counter: this counter, plus the
structured ``investment_coverage.query_failed`` log line at the fallback
site. A rising series means the coverage cards are silently degrading for
some slice of investment Sankey requests and needs investigation.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "INVESTMENT_COVERAGE_QUERY_FAILED_TOTAL",
    "build_investment_coverage_query_failed_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def build_investment_coverage_query_failed_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of investment Sankey coverage queries that raised and fell
    back to ``coverage=None``, by the requested measure."""
    return build_counter(
        "devhealth_graphql_resolver_fallback_total",
        "GraphQL resolver sub-queries that raised and fell back to a "
        "degraded/empty result instead of propagating the error, by "
        "resolver and reason",
        ["resolver", "reason"],
        meter=meter,
        prometheus=prometheus,
    )


INVESTMENT_COVERAGE_QUERY_FAILED_TOTAL = (
    build_investment_coverage_query_failed_counter()
)
