"""
ClickHouse metrics sink package.

Public API (stable — do not remove):
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

The single `ClickHouseMetricsSink` class is built by composing mixin classes,
each responsible for one table family:

  ClickHouseCore        — connection, schema, shared _insert_rows helper
  CIMixin               — CI/CD, deploy, incident, testops pipeline/test/coverage,
                          release confidence, feature flags, telemetry, release impact
  DoraMixin             — DORA metrics, period comparisons, benchmarks
  WellbeingMixin        — user metrics, quality drag, pipeline stability
  InvestmentMixin       — investment classifications/metrics, work-unit investments
  WorkGraphMixin        — work graph edges, work items, git/repo/file metrics, forecasts
  AIAttributionMixin    — AI attribution records (ai_attribution table)
"""

from __future__ import annotations

from dev_health_ops.metrics.sinks.clickhouse.ai_attribution import AIAttributionMixin
from dev_health_ops.metrics.sinks.clickhouse.ci import CIMixin
from dev_health_ops.metrics.sinks.clickhouse.core import ClickHouseCore
from dev_health_ops.metrics.sinks.clickhouse.dora import DoraMixin
from dev_health_ops.metrics.sinks.clickhouse.investment import InvestmentMixin
from dev_health_ops.metrics.sinks.clickhouse.wellbeing import WellbeingMixin
from dev_health_ops.metrics.sinks.clickhouse.work_graph import WorkGraphMixin


class ClickHouseMetricsSink(
    # Mixins come BEFORE ClickHouseCore so their concrete write_* methods
    # take priority in the MRO over BaseMetricsSink abstract methods.
    AIAttributionMixin,
    CIMixin,
    DoraMixin,
    WellbeingMixin,
    InvestmentMixin,
    WorkGraphMixin,
    ClickHouseCore,
):
    """
    Composite ClickHouse metrics sink.

    Instantiate via: ClickHouseMetricsSink(dsn=...) or ClickHouseMetricsSink(dsn=..., client=...)
    All write_* methods are provided by the mixin classes above.
    """


__all__ = ["ClickHouseMetricsSink"]
