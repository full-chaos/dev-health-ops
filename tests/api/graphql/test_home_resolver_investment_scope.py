from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.api.graphql.resolvers.home import resolve_home


@pytest.mark.asyncio
async def test_home_throughput_uses_scoped_latest_work_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[str] = []

    async def fake_query_dicts(_client: object, query: str, _params: dict[str, Any]):
        captured.append(query)
        if "devhealth_investment_metrics_daily" in query:
            return []
        if "latest_work_unit_investments" in query:
            return [
                {
                    "metric": "throughput",
                    "label": "Throughput",
                    "value": 1,
                    "unit": "units",
                },
                {
                    "metric": "pr_rework_ratio",
                    "label": "PR Rework Ratio",
                    "value": 0,
                    "unit": "%",
                },
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    context = SimpleNamespace(client=object(), org_id="org-1")
    await resolve_home(context)  # type: ignore[arg-type]

    deltas_sql = captured[1]
    assert "latest_complete_membership_run AS" in deltas_sql
    assert "latest_work_unit_investments AS" in deltas_sql
    assert "membership_scoped_work_unit_ids AS" in deltas_sql
    assert "FROM latest_work_unit_investments AS work_unit_investments" in deltas_sql
    assert "FROM work_unit_investments" in deltas_sql
    assert "argMax(pr_rework_ratio, computed_at) AS pr_rework_ratio" in deltas_sql
    assert "argMax(prs_merged, computed_at) AS prs_merged" in deltas_sql
    assert "GROUP BY day, repo_id" in deltas_sql
