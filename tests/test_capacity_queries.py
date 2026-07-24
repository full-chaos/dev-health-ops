from __future__ import annotations

from datetime import date

import pytest


class FakeClickHouseSink:
    backend_type = "clickhouse"
    org_id: str

    def __init__(self, rows: list[tuple[date, int]]) -> None:
        self.rows = rows
        self.query = ""
        self.parameters: dict[str, str] = {}

    def query_dicts(
        self, query: str, parameters: dict[str, str]
    ) -> list[dict[str, object]]:
        self.query = query
        self.parameters = parameters

        if "items_completed" in query:
            return [
                {"day": day, "items_completed": items_completed}
                for day, items_completed in self.rows
            ]
        if "sum(wip_count_end_of_day)" not in query.lower():
            return [{"wip_count_end_of_day": self.rows[0][1]}]

        latest_day = max(row[0] for row in self.rows)
        return [
            {
                "wip_count_end_of_day": sum(
                    backlog for day, backlog in self.rows if day == latest_day
                )
            }
        ]


@pytest.mark.asyncio
async def test_get_backlog_aggregates_latest_day_for_org_scope() -> None:
    from dev_health_ops.metrics.job_capacity import get_backlog_from_sink

    sink = FakeClickHouseSink(
        rows=[
            (date(2026, 1, 1), 3),
            (date(2026, 1, 2), 0),
            (date(2026, 1, 2), 7),
        ]
    )
    sink.org_id = "org-1"

    backlog = await get_backlog_from_sink(sink)

    assert backlog == 7
    assert "sum(wip_count_end_of_day)" in sink.query.lower()
    assert "max(day)" in sink.query.lower()
    assert sink.parameters == {"org_id": "org-1"}


@pytest.mark.asyncio
async def test_capacity_throughput_reader_uses_rmt_final() -> None:
    from dev_health_ops.metrics.job_capacity import load_throughput_from_sink

    sink = FakeClickHouseSink(rows=[(date(2026, 1, 1), 3)])
    sink.org_id = "org-1"

    await load_throughput_from_sink(sink, history_days=7)

    assert "FROM work_item_metrics_daily FINAL" in sink.query
