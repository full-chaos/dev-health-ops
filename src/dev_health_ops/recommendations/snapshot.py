"""MetricsSnapshot — the input contract between the engine and rule evaluators.

Rule evaluators (CHAOS-1623) import ``MetricsSnapshot`` from
``dev_health_ops.recommendations.engine`` (re-exported there).  This module
is the single source of truth for the dataclass so neither engine.py nor
loader.py depend on each other.

RecommendationRecord
--------------------
Also lives here as the sink-layer row type written to
``recommendations_daily`` by the ClickHouse sink.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

# ---------------------------------------------------------------------------
# MetricsSnapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """Immutable snapshot of all metric signals for one team + window.

    Produced by a ``MetricsLoader`` and consumed by rule evaluator functions.
    All evaluators receive the same snapshot object; they MUST be pure and
    deterministic given identical inputs.

    Conventions
    -----------
    * List fields are ordered ascending by day (index 0 = oldest).
    * ``None`` scalar fields mean *no data available* for that signal —
      evaluators must guard against ``None`` and return ``None`` (no fire).
    * Empty list fields mean no daily rows matched; evaluators should return
      ``None`` after a ``len(x) < 2`` guard.

    Fields — saturation + thrash (shared)
    --------------------------------------
    wip_by_day:
        ``wip_count_end_of_day`` summed per calendar day from
        ``work_item_metrics_daily``.
    throughput_by_cycle:
        ``items_completed`` summed per calendar day (same table).
        Named "by_cycle" because rule logic treats each point as a
        discrete delivery unit.

    Fields — review-concentration
    ------------------------------
    review_latency_p75_hours:
        Team p75 PR cycle time (hours) from ``repo_metrics_daily``.
        ``None`` when no PR data exists.
    reviewer_gini:
        Gini coefficient of ``reviews_given`` distribution across team
        members over the window.  ``None`` when fewer than 2 reviewers.

    Fields — thrash
    ---------------
    rework_churn_ratio:
        Average ``pr_rework_ratio`` from ``repo_metrics_daily``.
        ``None`` when no PR data.

    Fields — sustainability-risk
    ----------------------------
    after_hours_ratio:
        Average ``after_hours_commit_ratio`` from ``team_metrics_daily``
        over the window.  ``None`` when no commit data.
    cycle_time_by_day:
        ``cycle_time_p50_hours`` averaged per day across scopes.
        Empty list when no work-item data.

    Fields — compounding-risk
    -------------------------
    hotspot_complexity_delta:
        Normalised change in ``cyclomatic_per_kloc``: second-half average
        minus first-half average, divided by max(first-half average, 1.0).
        ``None`` when fewer than 2 data points.
    hotspot_churn_overlap:
        Fraction ∈ [0, 1] of high-risk hotspot files (``risk_score > 0``)
        that also have increasing cyclomatic complexity.
        ``None`` when no hotspot data.
    """

    team_id: str
    org_id: str
    window_start: date
    window_end: date

    # saturation + thrash (shared)
    wip_by_day: list[float]
    throughput_by_cycle: list[float]

    # review-concentration
    review_latency_p75_hours: float | None
    reviewer_gini: float | None

    # thrash
    rework_churn_ratio: float | None

    # sustainability-risk
    after_hours_ratio: float | None
    cycle_time_by_day: list[float]

    # compounding-risk
    hotspot_complexity_delta: float | None
    hotspot_churn_overlap: float | None
    # Persisted Compounding Risk composite (CHAOS-1641). When set, the
    # ``compounding-risk`` rule consumes these instead of the legacy
    # hotspot_complexity_delta / hotspot_churn_overlap proxy.
    compounding_risk_score: float | None = None
    compounding_risk_severity: str | None = None


# ---------------------------------------------------------------------------
# RecommendationRecord — sink row for recommendations_daily
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RecommendationRecord:
    """Row written to the append-only ``recommendations_daily`` ClickHouse table.

    One row per evaluation run per (team_id, rule_id).  Use
    ``argMax(fired, computed_at)`` to read the latest status.

    evidence_json
    -------------
    JSON-encoded ``list[dict]`` matching ``EvidenceRef`` shape::

        [{"team_id": str, "metric_table": str, "window_start": "YYYY-MM-DD",
          "window_end": "YYYY-MM-DD", "field": str, "value": float}, ...]
    """

    team_id: str
    org_id: str
    rule_id: str
    rule_version: str
    window_start: date
    window_end: date
    fired: bool
    severity: str
    title: str
    rationale: str
    success_criterion: str
    evidence_json: str  # serialised list[EvidenceRef]
    computed_at: datetime
