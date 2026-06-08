from __future__ import annotations

import asyncio
from datetime import date

from dev_health_ops.api.queries import metrics


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
