from __future__ import annotations

import asyncio
from datetime import date

import pytest

from dev_health_ops.api.queries import metrics
from dev_health_ops.investment_taxonomy import THEMES


def test_fetch_rework_theme_allocation_uses_investment_metrics(monkeypatch):
    captured = {}

    async def fake_query_dicts(_client, query, params):
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "theme": "feature_delivery",
                "allocation": 30,
                "prs_merged": 6,
                "churn_loc": 120,
            },
            {
                "theme": "quality",
                "allocation": 10,
                "prs_merged": 2,
                "churn_loc": 40,
            },
        ]

    monkeypatch.setattr(metrics, "query_dicts", fake_query_dicts)

    rows = asyncio.run(
        metrics.fetch_rework_theme_allocation(
            object(),
            start_day=date(2026, 1, 1),
            end_day=date(2026, 2, 1),
            scope_filter="AND team_id IN %(team_ids)s",
            scope_params={"team_ids": ["team-a"]},
            work_category_filter="AND investment_area IN %(work_categories)s",
            work_category_params={"work_categories": ["feature_delivery", "quality"]},
            org_id="org1",
        )
    )

    assert "FROM investment_metrics_daily" in captured["query"]
    assert "argMax(work_items_completed, computed_at)" in captured["query"]
    assert "canonical_theme AS theme" in captured["query"]
    assert "WHERE canonical_theme != ''" in captured["query"]
    assert captured["params"] == {
        "start_day": date(2026, 1, 1),
        "end_day": date(2026, 2, 1),
        "team_ids": ["team-a"],
        "work_categories": ["feature_delivery", "quality"],
        "org_id": "org1",
    }
    assert rows == [
        {
            "theme": "feature_delivery",
            "label": "Feature Delivery",
            "allocation": 30.0,
            "allocation_pct": 75.0,
            "prs_merged": 6,
            "churn_loc": 120,
        },
        {
            "theme": "quality",
            "label": "Quality / Reliability",
            "allocation": 10.0,
            "allocation_pct": 25.0,
            "prs_merged": 2,
            "churn_loc": 40,
        },
    ]


def test_fetch_rework_theme_allocation_maps_legacy_values_to_canonical_only(
    monkeypatch,
):
    captured = {}

    async def fake_query_dicts(_client, query, _params):
        captured["query"] = query
        return [
            {
                "theme": "risk",
                "allocation": 5,
                "prs_merged": 1,
                "churn_loc": 10,
            },
            {
                "theme": "infra",
                "allocation": 100,
                "prs_merged": 20,
                "churn_loc": 200,
            },
        ]

    monkeypatch.setattr(metrics, "query_dicts", fake_query_dicts)

    rows = asyncio.run(
        metrics.fetch_rework_theme_allocation(
            object(),
            start_day=date(2026, 1, 1),
            end_day=date(2026, 2, 1),
            scope_filter="",
            scope_params={},
            org_id="org1",
        )
    )

    assert "lowerUTF8(investment_area) = 'security', 'risk'" in captured["query"]
    assert "lowerUTF8(investment_area) = 'infra'" not in captured["query"]
    assert {row["theme"] for row in rows} <= THEMES
    assert rows == [
        {
            "theme": "risk",
            "label": "Risk / Security",
            "allocation": 5.0,
            "allocation_pct": 100.0,
            "prs_merged": 1,
            "churn_loc": 10,
        }
    ]


@pytest.mark.parametrize(
    ("repo_days", "expected"),
    [
        (
            [
                {"pr_rework_ratio": 1.0, "prs_merged": 1},
                {"pr_rework_ratio": 0.0, "prs_merged": 100},
            ],
            pytest.approx(1 / 101),
        )
    ],
)
def test_pr_rework_ratio_weighting_differs_from_naive_average(repo_days, expected):
    weighted = sum(
        row["pr_rework_ratio"] * row["prs_merged"] for row in repo_days
    ) / sum(row["prs_merged"] for row in repo_days)
    naive = sum(row["pr_rework_ratio"] for row in repo_days) / len(repo_days)

    assert weighted == expected
    assert naive == 0.5
    assert weighted != naive


def test_metric_value_expression_weights_pr_rework_by_merged_pr_volume():
    assert (
        metrics._metric_value_expression(
            table="repo_metrics_daily", column="pr_rework_ratio", aggregator="avg"
        )
        == "SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0)"
    )
