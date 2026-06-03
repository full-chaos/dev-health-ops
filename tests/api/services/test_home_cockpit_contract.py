from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter
from dev_health_ops.api.models.schemas import MetricDelta, SparkPoint
from dev_health_ops.api.services.home import (
    build_data_confidence,
    build_health_state,
    build_limiting_factor,
    build_metric_signals,
)


def _spark(count: int) -> list[SparkPoint]:
    return [
        SparkPoint(ts=datetime(2026, 6, day + 1, tzinfo=timezone.utc), value=float(day))
        for day in range(count)
    ]


def test_cockpit_signals_rank_highest_impact_first() -> None:
    confidence = build_data_confidence(
        coverage={
            "repos_covered_pct": 90.0,
            "prs_linked_to_issues_pct": 85.0,
            "issues_with_cycle_states_pct": 80.0,
        },
        sources={"github": "ok", "jira": "ok", "ci": "ok"},
    )
    filters = MetricFilter(scope=ScopeFilter(level="team", ids=["team-a"]))
    signals = build_metric_signals(
        [
            MetricDelta(
                metric="throughput",
                label="Throughput",
                value=40,
                unit="items",
                delta_pct=-70,
                spark=_spark(10),
            ),
            MetricDelta(
                metric="cycle_time",
                label="Cycle Time",
                value=4,
                unit="days",
                delta_pct=20,
                spark=_spark(10),
            ),
        ],
        filters,
        confidence,
    )

    assert [signal.metric for signal in signals] == ["throughput", "cycle_time"]
    assert signals[0].severity == "critical"
    assert signals[0].direction == "down"
    assert signals[0].current_value == "40 items"
    assert signals[0].delta == "-70%"


def test_cockpit_sparse_data_uses_low_confidence_and_watch_state() -> None:
    confidence = build_data_confidence(
        coverage={
            "repos_covered_pct": 25.0,
            "prs_linked_to_issues_pct": 0.0,
            "issues_with_cycle_states_pct": 10.0,
        },
        sources={"github": "down", "jira": "down", "ci": "down"},
    )
    filters = MetricFilter()
    signals = build_metric_signals(
        [
            MetricDelta(
                metric="review_latency",
                label="Review Latency",
                value=0,
                unit="hours",
                delta_pct=0,
                spark=[],
            )
        ],
        filters,
        confidence,
    )
    state = build_health_state(signals, confidence, None)

    assert confidence.level == "low"
    assert confidence.missing_sources == ["ci", "github", "jira"]
    assert signals[0].confidence == "low"
    assert signals[0].evidence_count == 0
    assert state.status == "watch"


def test_cockpit_confidence_derives_from_coverage_and_evidence() -> None:
    confidence = build_data_confidence(
        coverage={
            "repos_covered_pct": 90.0,
            "prs_linked_to_issues_pct": 90.0,
            "issues_with_cycle_states_pct": 90.0,
        },
        sources={"github": "ok", "jira": "ok", "ci": "ok"},
    )
    signals = build_metric_signals(
        [
            MetricDelta(
                metric="blocked_work",
                label="Blocked Work",
                value=8,
                unit="hours",
                delta_pct=35,
                spark=_spark(7),
            )
        ],
        MetricFilter(),
        confidence,
    )

    assert confidence.level == "high"
    assert signals[0].confidence == "high"
    assert signals[0].severity == "high"


def test_cockpit_additive_fields_are_present_on_limiting_factor() -> None:
    confidence = build_data_confidence(
        coverage={
            "repos_covered_pct": 80.0,
            "prs_linked_to_issues_pct": 80.0,
            "issues_with_cycle_states_pct": 80.0,
        },
        sources={"github": "ok", "jira": "ok", "ci": "ok"},
    )
    signals = build_metric_signals(
        [
            MetricDelta(
                metric="wip_saturation",
                label="WIP Saturation",
                value=88,
                unit="%",
                delta_pct=44,
                spark=_spark(8),
            )
        ],
        MetricFilter(scope=ScopeFilter(level="team", ids=["team-a"])),
        confidence,
    )
    limiting_factor = build_limiting_factor(signals)

    dumped = limiting_factor.model_dump()
    assert set(dumped) == {
        "claim",
        "why_it_matters",
        "recommended_action",
        "confidence",
        "evidence_ref",
    }
    assert signals[0].category == "dynamics"
    assert signals[0].affected_scope == "team-a"
