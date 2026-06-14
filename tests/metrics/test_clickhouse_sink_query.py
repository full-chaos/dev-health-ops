from __future__ import annotations

from unittest.mock import MagicMock

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


def test_query_delegates_to_owned_clickhouse_client():
    client = MagicMock()
    client.query.return_value.result_rows = [[1]]

    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    result = sink.query("SELECT count() FROM repo_metrics_daily")

    assert result.result_rows == [[1]]
    client.query.assert_called_once_with(
        "SELECT count() FROM repo_metrics_daily", parameters={}
    )


def test_query_passes_parameters_to_owned_clickhouse_client():
    client = MagicMock()
    client.query.return_value.result_rows = [["repo-1"]]

    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    result = sink.query(
        "SELECT id FROM repos WHERE org_id = {org_id:String}",
        parameters={"org_id": "org-abc"},
    )

    assert result.result_rows == [["repo-1"]]
    client.query.assert_called_once_with(
        "SELECT id FROM repos WHERE org_id = {org_id:String}",
        parameters={"org_id": "org-abc"},
    )
