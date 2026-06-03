from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter
from dev_health_ops.api.models.schemas import MetricDelta, SparkPoint
from dev_health_ops.api.services.home import (
    HomeDataConfidence,
    _risk_signal,
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


# ---------------------------------------------------------------------------
# Risk signal: display name resolution + A8/B7 guards
# ---------------------------------------------------------------------------


def _risk_confidence() -> HomeDataConfidence:
    return build_data_confidence(
        coverage={
            "repos_covered_pct": 80.0,
            "prs_linked_to_issues_pct": 80.0,
            "issues_with_cycle_states_pct": 80.0,
        },
        sources={"github": "ok"},
    )


def test_risk_signal_uses_resolved_display_name() -> None:
    """Title must contain the resolved entity name, never a raw UUID."""
    confidence = _risk_confidence()
    filters = MetricFilter()
    row = {
        "scope_id": "698c1234-0000-0000-0000-000000000000",
        "scope": "repo",
        "score": 0.75,
        "severity": "elevated",
    }
    signal = _risk_signal(row, filters, confidence, scope_display_name="acme/backend")

    assert signal is not None
    assert "acme/backend" in signal.title
    assert "698c" not in signal.title
    assert "698c" not in signal.affected_scope


def test_risk_signal_subject_and_scope_are_distinct() -> None:
    """Signal title subject and affected_scope must be distinct (no duplication)."""
    confidence = _risk_confidence()
    filters = MetricFilter()
    row = {
        "scope_id": "repo-abc",
        "scope": "repo",
        "score": 0.6,
        "severity": "elevated",
    }
    signal = _risk_signal(row, filters, confidence, scope_display_name="org/service-x")

    assert signal is not None
    # affected_scope describes the scope context; must not equal the entity name in title
    assert signal.affected_scope != signal.title
    assert "org/service-x" not in signal.affected_scope


def test_risk_signal_uuid_scope_display_name_returns_controlled_state() -> None:
    """B7: when display name is a bare UUID, signal is suppressed (controlled flat state)."""
    confidence = _risk_confidence()
    filters = MetricFilter()
    uuid_val = "698c1234-abcd-4321-8765-000000000000"
    row = {"scope_id": uuid_val, "scope": "repo", "score": 0.8, "severity": "high"}
    # Passing a UUID as display_name must suppress the signal
    signal = _risk_signal(row, filters, confidence, scope_display_name=uuid_val)
    assert signal is None


def test_risk_signal_no_display_name_returns_controlled_state() -> None:
    """B7: unresolved display name (None) yields None — no silent UUID surfacing."""
    confidence = _risk_confidence()
    filters = MetricFilter()
    row = {"scope_id": "repo-xyz", "scope": "repo", "score": 0.5, "severity": "low"}
    signal = _risk_signal(row, filters, confidence)  # no scope_display_name
    assert signal is None


def test_risk_signal_empty_scope_id_returns_controlled_state() -> None:
    """B7: empty scope_id yields None (missing backing row → controlled empty)."""
    confidence = _risk_confidence()
    filters = MetricFilter()
    row = {"scope_id": "", "scope": "repo", "score": 0.5, "severity": "low"}
    signal = _risk_signal(row, filters, confidence, scope_display_name="some-name")
    assert signal is None


def test_risk_signal_affected_scope_uses_scope_type_plural() -> None:
    """affected_scope must be the scope type label (e.g. 'repos'), never the entity name."""
    confidence = _risk_confidence()
    filters = MetricFilter()
    row = {"scope_id": "repo-1", "scope": "repo", "score": 0.5, "severity": "elevated"}
    signal = _risk_signal(row, filters, confidence, scope_display_name="acme/backend")

    assert signal is not None
    assert signal.affected_scope == "repos"
