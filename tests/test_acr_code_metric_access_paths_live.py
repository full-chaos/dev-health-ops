from __future__ import annotations

import os
import uuid

import pytest

_CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
_MAX_BYTES_TO_READ = 16 << 20

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(not _CLICKHOUSE_URI, reason="Requires CLICKHOUSE_URI"),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert _CLICKHOUSE_URI is not None
    result = ClickHouseMetricsSink(_CLICKHOUSE_URI)
    result.ensure_schema(force=True)
    yield result
    result.close()


def test_file_complexity_ref_index_bounds_absent_ref_read(sink) -> None:
    # Given
    from clickhouse_connect.driver.exceptions import DatabaseError

    org_id = f"test-chaos-3112-ref-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    parameters = {"org_id": org_id, "repo_id": str(repo_id)}
    sink.client.command(
        "INSERT INTO file_complexity_snapshots "
        "(repo_id, as_of_day, ref, file_path, language, loc, functions_count, "
        "cyclomatic_total, cyclomatic_avg, high_complexity_functions, "
        "very_high_complexity_functions, computed_at, org_id) "
        "SELECT {repo_id:UUID}, toDate('2026-01-14'), 'main', "
        "concat('src/generated/', toString(number), '.py'), 'python', "
        "20, 2, 3, 1.5, 0, 0, toDateTime('2026-01-14 10:00:00'), "
        "{org_id:String} FROM numbers(8192)",
        parameters=parameters,
    )
    query = (
        "SELECT count() FROM file_complexity_snapshots "
        "WHERE org_id = {org_id:String} AND repo_id = {repo_id:UUID} "
        "AND ref = 'release/absent' SETTINGS max_bytes_to_read = 32768"
    )

    try:
        # When
        explain = sink.client.query(
            "EXPLAIN indexes = 1 " + query, parameters=parameters
        )
        result = sink.client.query(query, parameters=parameters)

        # Then
        assert "idx_file_complexity_ref" in "\n".join(
            str(row[0]) for row in explain.result_rows
        )
        assert result.result_rows[0][0] == 0
        with pytest.raises(DatabaseError):
            sink.client.query(
                query + ", use_skip_indexes = 0",
                parameters=parameters,
            )
    finally:
        _delete_test_rows(sink, "file_complexity_snapshots", org_id)


def test_hotspot_run_projection_keeps_latest_replacement_read_bounded(sink) -> None:
    # Given
    from clickhouse_connect.driver.exceptions import DatabaseError

    org_id = f"test-chaos-3112-hotspot-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    parameters = {"org_id": org_id, "repo_id": str(repo_id)}
    sink.client.command(
        "INSERT INTO file_hotspot_daily "
        "(repo_id, day, file_path, churn_loc_30d, churn_commits_30d, "
        "cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, "
        "computed_at, org_id) "
        "SELECT {repo_id:UUID}, toDate('2026-01-14'), "
        "concat('src/archive/very_long_hotspot_file_path_', toString(number), '.typescript'), "
        "10, 1, 2, 1.0, 0.1, 1.0, toDateTime('2026-01-14 10:00:00'), "
        "{org_id:String} FROM numbers(400000)",
        parameters=parameters,
    )
    sink.client.command(
        "INSERT INTO file_hotspot_daily "
        "(repo_id, day, file_path, churn_loc_30d, churn_commits_30d, "
        "cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, "
        "computed_at, org_id) "
        "SELECT {repo_id:UUID}, toDate('2026-01-14'), "
        "concat('src/current/file_', toString(number), '.ts'), "
        "220, 9, 34, 2.8, 0.62, 41.5, toDateTime('2026-01-14 12:00:00'), "
        "{org_id:String} FROM numbers(500)",
        parameters=parameters,
    )
    latest_run = (
        "SELECT day, computed_at FROM file_hotspot_daily "
        "WHERE org_id = {org_id:String} AND repo_id = {repo_id:UUID} "
        "GROUP BY org_id, repo_id, day, computed_at "
        "ORDER BY day DESC, computed_at DESC LIMIT 1 "
        "SETTINGS force_optimize_projection_name='prj_acr_file_hotspot_runs'"
    )
    bounded_query = (
        "SELECT file_path, churn_loc_30d, cyclomatic_total "
        "FROM file_hotspot_daily "
        "WHERE org_id = {org_id:String} AND repo_id = {repo_id:UUID} "
        f"AND (day, computed_at) = ({latest_run}) "
        "ORDER BY file_path LIMIT 100 "
        f"SETTINGS max_bytes_to_read = {_MAX_BYTES_TO_READ}"
    )
    legacy_query = (
        "SELECT file_path, argMax(churn_loc_30d, computed_at), "
        "argMax(cyclomatic_total, computed_at) FROM file_hotspot_daily "
        "WHERE org_id = {org_id:String} AND repo_id = {repo_id:UUID} "
        "GROUP BY file_path "
        f"SETTINGS max_bytes_to_read = {_MAX_BYTES_TO_READ}"
    )

    try:
        # When
        explain = sink.client.query(
            "EXPLAIN projections = 1 " + latest_run,
            parameters=parameters,
        )
        result = sink.client.query(bounded_query, parameters=parameters)

        # Then
        assert "prj_acr_file_hotspot_runs" in "\n".join(
            str(row[0]) for row in explain.result_rows
        )
        assert len(result.result_rows) == 100
        assert all(str(row[0]).startswith("src/current/") for row in result.result_rows)
        with pytest.raises(DatabaseError):
            sink.client.query(legacy_query, parameters=parameters)
    finally:
        _delete_test_rows(sink, "file_hotspot_daily", org_id)


def _delete_test_rows(sink, table: str, org_id: str) -> None:
    sink.client.command(
        f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} "
        "SETTINGS mutations_sync = 2",
        parameters={"org_id": org_id},
    )
