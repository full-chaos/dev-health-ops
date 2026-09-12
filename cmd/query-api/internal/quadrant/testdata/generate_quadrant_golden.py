"""Golden-fixture generator for the Go port of GET /api/v1/quadrant
(CHAOS-5550), same convention as
cmd/query-api/internal/investmentexplain/testdata/
generate_explain_investment_mix_golden.py: monkeypatch Python's ClickHouse
readers with fixed fixture rows, call the real
build_quadrant_response(...), and dump its model_dump(mode="json") output
as the golden file the Go test's fixture QueryClient must reproduce
byte-for-byte (field names, ordering, null handling).

Uses unittest.mock.patch.object (not a direct module-attribute
assignment) for the same reason
generate_explain_investment_mix_golden.py does: mypy statically checks a
direct `module.name = replacement` assignment against the ORIGINAL
attribute's declared type, which a narrower fixture-shaped fake always
fails -- patch.object's replacement parameter is typed Any, so it
monkeypatches without tripping the typecheck gate.

Run from the ops repo root:
    uv run python3 cmd/query-api/internal/quadrant/testdata/generate_quadrant_golden.py
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from unittest import mock

from dev_health_ops.api.services import quadrant as quadrant_module

OUT_DIR = Path(__file__).parent


@asynccontextmanager
async def _fake_client(_db_url):
    yield object()


def _dump(response) -> dict:
    return response.model_dump(mode="json")


async def _gen_cycle_throughput_team() -> None:
    """cycle_throughput, team scope: both axes go through
    fetch_work_item_team_quadrant_metric (the attribution quirk). Team
    "team-a" gets two weekly buckets (a real trajectory); team "team-b"
    gets one. Team labels resolve team-a -> "Team Alpha" via the teams
    catalog; team-b's label is left as its own entity_id (catalog lookup
    returns nothing for it), exercising the "or entity_id" fallback.
    """

    async def _fake_attributed_metric(_sink, *, metric, start_day, end_day, bucket, org_id):
        if metric == "throughput":
            return [
                {"bucket": date(2024, 1, 1), "entity_id": "team-a", "entity_label": "team-a", "value": 5.0},
                {"bucket": date(2024, 1, 8), "entity_id": "team-a", "entity_label": "team-a", "value": 9.0},
                {"bucket": date(2024, 1, 1), "entity_id": "team-b", "entity_label": "team-b", "value": 3.0},
            ]
        if metric == "cycle_time":
            return [
                {"bucket": date(2024, 1, 1), "entity_id": "team-a", "entity_label": "team-a", "value": 48.0},
                {"bucket": date(2024, 1, 8), "entity_id": "team-a", "entity_label": "team-a", "value": 24.0},
                {"bucket": date(2024, 1, 1), "entity_id": "team-b", "entity_label": "team-b", "value": 72.0},
            ]
        raise AssertionError(f"unexpected metric {metric}")

    async def _unexpected_rollup_metric(*_args, **_kwargs):
        raise AssertionError("cycle_throughput team axes must use primary attribution")

    async def _fake_query_dicts(_sink, _query, params):
        assert sorted(params["team_ids"]) == ["team-a", "team-b"]
        return [{"team_id": "team-a", "team_name": "Team Alpha"}]

    with (
        mock.patch.object(quadrant_module, "clickhouse_client", _fake_client),
        mock.patch.object(quadrant_module, "fetch_quadrant_metric", _unexpected_rollup_metric),
        mock.patch.object(quadrant_module, "fetch_work_item_team_quadrant_metric", _fake_attributed_metric),
        mock.patch.object(quadrant_module, "query_dicts", _fake_query_dicts),
    ):
        response = await quadrant_module.build_quadrant_response(
            db_url="clickhouse://test",
            org_id="org-1",
            type="cycle_throughput",
            scope_type="team",
            scope_id="",
            range_days=30,
            bucket="week",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 8),
        )
    (OUT_DIR / "cycle_throughput_team.json").write_text(json.dumps(_dump(response), indent=2) + "\n")


async def _gen_churn_throughput_forces_repo() -> None:
    """churn_throughput requested at team scope: CHAOS-2079 forces repo
    grain, so both axes read repo_metrics_daily via fetch_quadrant_metric,
    joined to repos -- team-label resolution never runs."""

    async def _fake_metric(_sink, *, table, value_expr, entity_expr, label_expr, join_clause, where_clause, scope_filter, scope_params, org_id, start_day, end_day, bucket):
        assert table == "repo_metrics_daily AS m"
        if "total_loc_touched" in value_expr:
            return [{"bucket": date(2024, 1, 1), "entity_id": "checkout-service", "entity_label": "checkout-service", "value": 4200.0}]
        if "prs_merged" in value_expr:
            return [{"bucket": date(2024, 1, 1), "entity_id": "checkout-service", "entity_label": "checkout-service", "value": 12.0}]
        raise AssertionError(f"unexpected value_expr {value_expr}")

    async def _unexpected_query_dicts(*_args, **_kwargs):
        raise AssertionError("team-label resolution must not run for repo-grain churn")

    with (
        mock.patch.object(quadrant_module, "clickhouse_client", _fake_client),
        mock.patch.object(quadrant_module, "fetch_quadrant_metric", _fake_metric),
        mock.patch.object(quadrant_module, "query_dicts", _unexpected_query_dicts),
    ):
        response = await quadrant_module.build_quadrant_response(
            db_url="clickhouse://test",
            org_id="org-1",
            type="churn_throughput",
            scope_type="team",
            scope_id="",
            range_days=30,
            bucket="week",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 8),
        )
    (OUT_DIR / "churn_throughput_team_forces_repo.json").write_text(json.dumps(_dump(response), indent=2) + "\n")


async def _gen_review_load_latency_repo() -> None:
    """review_load_latency requested directly at repo scope: plain
    repo-grain path, no attribution quirk, two buckets for one repo (a
    real trajectory)."""

    async def _fake_metric(_sink, *, table, value_expr, entity_expr, label_expr, join_clause, where_clause, scope_filter, scope_params, org_id, start_day, end_day, bucket):
        assert table == "user_metrics_daily AS m"
        if "reviews_given" in value_expr:
            return [
                {"bucket": date(2024, 1, 1), "entity_id": "api-gateway", "entity_label": "api-gateway", "value": 6.0},
                {"bucket": date(2024, 1, 8), "entity_id": "api-gateway", "entity_label": "api-gateway", "value": 10.0},
            ]
        if "pr_first_review_p50_hours" in value_expr:
            return [
                {"bucket": date(2024, 1, 1), "entity_id": "api-gateway", "entity_label": "api-gateway", "value": 5.5},
                {"bucket": date(2024, 1, 8), "entity_id": "api-gateway", "entity_label": "api-gateway", "value": 3.25},
            ]
        raise AssertionError(f"unexpected value_expr {value_expr}")

    async def _unexpected_query_dicts(*_args, **_kwargs):
        raise AssertionError("team-label resolution must not run for repo scope")

    with (
        mock.patch.object(quadrant_module, "clickhouse_client", _fake_client),
        mock.patch.object(quadrant_module, "fetch_quadrant_metric", _fake_metric),
        mock.patch.object(quadrant_module, "query_dicts", _unexpected_query_dicts),
    ):
        response = await quadrant_module.build_quadrant_response(
            db_url="clickhouse://test",
            org_id="org-1",
            type="review_load_latency",
            scope_type="repo",
            scope_id="",
            range_days=30,
            bucket="week",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 8),
        )
    (OUT_DIR / "review_load_latency_repo.json").write_text(json.dumps(_dump(response), indent=2) + "\n")


async def _main() -> None:
    await _gen_cycle_throughput_team()
    await _gen_churn_throughput_forces_repo()
    await _gen_review_load_latency_repo()


if __name__ == "__main__":
    asyncio.run(_main())
