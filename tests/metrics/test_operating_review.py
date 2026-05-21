from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.metrics.operating_review import (
    OperatingReviewRows,
    compute_operating_review,
)


def test_operating_review_computes_sections_and_week_over_week_deltas() -> None:
    review = compute_operating_review(
        org_id="org-1",
        team_id="team-a",
        week_start=date(2026, 5, 11),
        current=OperatingReviewRows(
            work_items=[
                {
                    "items_completed": 14,
                    "items_started": 18,
                    "wip_count_end_of_day": 8,
                    "cycle_time_p50_hours": 30.0,
                    "wip_age_p90_hours": 70.0,
                },
                {
                    "items_completed": 6,
                    "items_started": 4,
                    "wip_count_end_of_day": 12,
                    "cycle_time_p50_hours": 42.0,
                    "wip_age_p90_hours": 90.0,
                },
            ],
            state_durations=[{"duration_hours": 30.0, "items_touched": 2}],
            repo_metrics=[
                {
                    "prs_merged": 10,
                    "pr_first_review_p50_hours": 8.0,
                    "single_owner_file_ratio_30d": 0.30,
                    "code_ownership_gini": 0.55,
                    "bus_factor": 2,
                    "change_failure_rate": 0.10,
                    "mttr_hours": 3.0,
                }
            ],
            hotspots=[{"risk_score": 80.0}],
            complexity=[{"cyclomatic_per_kloc": 14.0}],
            deployments=[{"deployments_count": 5, "failed_deployments_count": 1}],
            incidents=[{"incidents_count": 1, "mttr_p50_hours": 4.0}],
            investment=[
                {"investment_area": "ktlo", "delivery_units": 8},
                {"investment_area": "new value", "delivery_units": 12},
                {"investment_area": "security", "delivery_units": 2},
                {"investment_area": "infra", "delivery_units": 3},
            ],
        ),
        prior=OperatingReviewRows(
            work_items=[
                {
                    "items_completed": 10,
                    "items_started": 10,
                    "wip_count_end_of_day": 9,
                    "cycle_time_p50_hours": 48.0,
                    "wip_age_p90_hours": 120.0,
                }
            ],
            state_durations=[{"duration_hours": 44.0, "items_touched": 1}],
            repo_metrics=[
                {
                    "prs_merged": 5,
                    "pr_first_review_p50_hours": 12.0,
                    "single_owner_file_ratio_30d": 0.45,
                    "code_ownership_gini": 0.70,
                    "bus_factor": 1,
                    "change_failure_rate": 0.20,
                    "mttr_hours": 8.0,
                }
            ],
            hotspots=[{"risk_score": 90.0}],
            complexity=[{"cyclomatic_per_kloc": 16.0}],
            deployments=[{"deployments_count": 3, "failed_deployments_count": 1}],
            incidents=[{"incidents_count": 2, "mttr_p50_hours": 6.0}],
            investment=[
                {"investment_area": "ktlo", "delivery_units": 10},
                {"investment_area": "new value", "delivery_units": 5},
                {"investment_area": "security", "delivery_units": 1},
                {"investment_area": "infra", "delivery_units": 4},
            ],
        ),
    )

    assert review.week_start == date(2026, 5, 11)
    assert review.prior_week_start == date(2026, 5, 4)
    assert [section.key for section in review.sections] == [
        "delivery_movement",
        "bottleneck",
        "risk",
        "reliability",
        "investment",
    ]

    delivery = review.section("delivery_movement")
    assert delivery.metric("throughput").value == 20
    assert delivery.metric("throughput").delta.status == "improved"
    assert delivery.metric("cycle_time_p50_hours").value == pytest.approx(36.0)
    assert delivery.metric("cycle_time_p50_hours").delta.status == "improved"
    assert delivery.changed == []
    assert delivery.improved == [
        "Cycle time p50 improved by -12.0 hours",
        "Throughput improved by +10.0 items completed",
    ]

    bottleneck = review.section("bottleneck")
    assert bottleneck.metric("state_duration_hours").value == pytest.approx(30.0)
    assert bottleneck.metric("wip_age_p90_hours").delta.status == "improved"

    risk = review.section("risk")
    assert risk.metric("hotspot_risk_score").delta.status == "improved"
    assert risk.metric("bus_factor").delta.status == "improved"

    reliability = review.section("reliability")
    assert reliability.metric("incidents_count").delta.status == "improved"
    assert reliability.metric("deployments_count").delta.status == "improved"

    investment = review.section("investment")
    assert investment.metric("new_value_units").value == 12
    assert investment.metric("ktlo_units").delta.status == "improved"

    assert review.recommendations == []
    assert (
        review.recommendations_empty_state
        == "No operating review rules are configured."
    )


def test_operating_review_renders_for_empty_team_week() -> None:
    review = compute_operating_review(
        org_id="org-1",
        team_id="missing-team",
        week_start=date(2026, 5, 11),
        current=OperatingReviewRows(),
        prior=OperatingReviewRows(),
    )

    assert review.team_id == "missing-team"
    assert all(
        metric.value == 0 for section in review.sections for metric in section.metrics
    )
    assert all(
        metric.delta.status == "unchanged"
        for section in review.sections
        for metric in section.metrics
    )


def test_build_operating_review_queries_single_team_mode() -> None:
    """Per-team mode binds team_id and filters in the WHERE clause."""
    from dev_health_ops.metrics.operating_review import build_operating_review_queries

    queries = {q.key: q.sql for q in build_operating_review_queries(team_id="team-a")}

    for key in ("work_items", "state_durations", "investment"):
        assert "AND team_id = %(team_id)s" in queries[key], (
            f"single-team query {key!r} must filter by team_id"
        )

    # repo_metrics is repo-scoped (no team_id column in WHERE) — unchanged.
    assert "team_id" not in queries["repo_metrics"], (
        "repo_metrics should never reference team_id"
    )


def test_build_operating_review_queries_all_teams_mode() -> None:
    """All-teams mode drops the team predicate and pushes team_id into the
    inner GROUP BY so per-team rows aren't collapsed by argMax mid-aggregation.
    """
    from dev_health_ops.metrics.operating_review import build_operating_review_queries

    queries = {q.key: q.sql for q in build_operating_review_queries(team_id=None)}

    # work_items / state_durations / investment must NOT filter by team
    # in all-teams mode, and MUST keep team_id in inner GROUP BY so the
    # outer SUM/AVG aggregates correctly across teams.
    for key in ("work_items", "state_durations", "investment"):
        sql = queries[key]
        assert "AND team_id = %(team_id)s" not in sql, (
            f"all-teams query {key!r} must not filter by team_id"
        )
        assert "team_id" in sql, (
            f"all-teams query {key!r} must keep team_id in inner GROUP BY"
        )

    # The argMax-by-computed_at idiom must remain — otherwise we'd
    # combine across recomputes within a single team.
    for key in ("work_items", "state_durations", "investment"):
        assert "argMax(" in queries[key], (
            f"all-teams query {key!r} must keep argMax-by-computed_at"
        )


def test_compute_operating_review_all_teams_mode_emits_null_team_id() -> None:
    """In cross-team aggregate mode the payload carries team_id=None so callers
    can render an explicit "All Teams" label rather than pretend a single
    team was chosen."""
    review = compute_operating_review(
        org_id="org-1",
        team_id=None,
        week_start=date(2026, 5, 11),
        current=OperatingReviewRows(
            work_items=[
                # Two teams worth of rows in the same period.
                {"items_completed": 14, "items_started": 18,
                 "wip_count_end_of_day": 8, "cycle_time_p50_hours": 30.0,
                 "wip_age_p90_hours": 70.0},
                {"items_completed": 11, "items_started": 9,
                 "wip_count_end_of_day": 5, "cycle_time_p50_hours": 24.0,
                 "wip_age_p90_hours": 60.0},
            ],
            state_durations=[{"duration_hours": 30.0, "items_touched": 2}],
            repo_metrics=[],
            hotspots=[],
            complexity=[],
            deployments=[{"deployments_count": 5, "failed_deployments_count": 1}],
            incidents=[{"incidents_count": 1, "mttr_p50_hours": 4.0}],
            investment=[
                {"investment_area": "ktlo", "delivery_units": 8},
                {"investment_area": "new value", "delivery_units": 12},
            ],
        ),
        prior=OperatingReviewRows(),
    )

    assert review.team_id is None, (
        "cross-team aggregate must surface team_id=None, not a sentinel value"
    )
    # Throughput sums across the two team rows (SUM aggregation contract).
    delivery = review.section("delivery_movement")
    assert delivery.metric("throughput").value == 14 + 11
