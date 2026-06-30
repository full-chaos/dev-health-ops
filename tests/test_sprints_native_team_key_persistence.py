from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.work_items import Sprint
from dev_health_ops.storage.clickhouse import ClickHouseStore


class _QueryResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


def _linear_sprint() -> Sprint:
    return Sprint(
        provider="linear",
        sprint_id="linear:cycle:cycle-1",
        name="Cycle 1",
        state="active",
        started_at=None,
        ended_at=None,
        completed_at=None,
        native_team_key="ENG",
        org_id="org-1",
    )


def _jira_sprint() -> Sprint:
    return Sprint(
        provider="jira",
        sprint_id="7",
        name="Sprint 7",
        state="active",
        started_at=None,
        ended_at=None,
        completed_at=None,
        org_id="org-1",
    )


def test_work_graph_sink_persists_sprint_native_team_key() -> None:
    sink = ClickHouseMetricsSink.__new__(ClickHouseMetricsSink)
    sink.client = MagicMock()
    sink.org_id = ""

    sink.write_sprints([_linear_sprint()])

    args, kwargs = sink.client.insert.call_args
    assert args[0] == "sprints"
    column_names = kwargs["column_names"]
    row = args[1][0]

    assert "native_team_key" in column_names
    assert row[column_names.index("native_team_key")] == "ENG"


def test_work_graph_sink_coerces_non_linear_sprint_native_team_key_to_empty_string() -> (
    None
):
    sink = ClickHouseMetricsSink.__new__(ClickHouseMetricsSink)
    sink.client = MagicMock()
    sink.org_id = ""

    sink.write_sprints([_jira_sprint()])

    args, kwargs = sink.client.insert.call_args
    column_names = kwargs["column_names"]
    row = args[1][0]
    assert row[column_names.index("native_team_key")] == ""


def test_clickhouse_store_get_all_sprints_reads_native_team_key_argmax() -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    store = ClickHouseStore("clickhouse://localhost:8123/stats")
    store.client = MagicMock()
    store.client.query.return_value = _QueryResult(
        [
            (
                "linear",
                "linear:cycle:cycle-1",
                "Cycle 1",
                "active",
                None,
                None,
                None,
                now,
                "ENG",
                "org-1",
            )
        ]
    )

    sprints = asyncio.run(store.get_all_sprints(org_id="org-1"))

    query = store.client.query.call_args.args[0]
    assert "argMax(native_team_key, last_synced) AS native_team_key" in query
    assert "WHERE org_id = {org_id:String}" in query
    assert "GROUP BY provider, sprint_id, org_id" in query
    assert sprints[0].native_team_key == "ENG"
    assert sprints[0].org_id == "org-1"


def test_clickhouse_metrics_sink_get_all_sprints_reads_native_team_key_argmax() -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sink = ClickHouseMetricsSink.__new__(ClickHouseMetricsSink)
    sink.client = MagicMock()
    sink.org_id = "org-1"
    sink.client.query.return_value = _QueryResult(
        [
            (
                "jira",
                "7",
                "Sprint 7",
                "active",
                None,
                None,
                None,
                now,
                "",
                "org-1",
            )
        ]
    )

    sprints = asyncio.run(sink.get_all_sprints())

    query = sink.client.query.call_args.args[0]
    assert "argMax(native_team_key, last_synced) AS native_team_key" in query
    assert "WHERE org_id = {org_id:String}" in query
    assert "GROUP BY provider, sprint_id, org_id" in query
    assert sprints[0].provider == "jira"
    assert sprints[0].native_team_key is None
