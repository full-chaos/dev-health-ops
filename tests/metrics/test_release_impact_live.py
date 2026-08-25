"""Live ClickHouse readback proof for release_impact_daily (CHAOS-4243).

worker_job_runs reports metrics.remaining.release_impact as 100% succeeded
(20/20 local, 10/10 prod over 7 days), but release_impact_daily has zero rows
locally and is stuck at the 1970 epoch sentinel in prod. The traced root
cause is telemetry_signal_bucket being empty in both environments observed,
which makes _find_release_env_pairs legitimately return [] for every
partition -- a genuinely empty-scope zero, not a broken query.

This test proves the OTHER half, the one the mocked unit tests in
test_compute_release_impact.py cannot: given a REAL deployment row and REAL
telemetry_signal_bucket row for one release/environment/day, against a real
ClickHouse engine (not a fake client), compute_release_impact_daily must
land exactly one row in release_impact_daily. If this goes red, the compute
path itself is broken independent of the upstream table being empty --
distinguishing "nothing to compute" from "compute is broken" is the whole
point of a readback proof (see AGENTS.md's reachability-past-result-struct
discipline: the constructor/mock is not proof, the row landing is).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from uuid import uuid4

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    clickhouse_uri = CLICKHOUSE_URI
    assert clickhouse_uri is not None
    database = (urlparse(clickhouse_uri).path or "").lstrip("/")
    if database in ("", "default"):
        pytest.skip("refusing to run ClickHouse schema setup against default")
    result = ClickHouseMetricsSink(clickhouse_uri)
    result.ensure_schema(force=True)
    yield result
    result.close()


@pytest.mark.asyncio
async def test_release_impact_partition_writes_a_row_for_a_real_release(sink):
    from dev_health_ops.metrics.release_impact import compute_release_impact_daily

    org_id = f"test-chaos-4243-{uuid4()}"
    repo_id = uuid4()
    release_ref = "v1.0.0-chaos4243"
    environment = "production"
    # telemetry_signal_bucket carries a 90-day rolling TTL (bucket_start +
    # INTERVAL 90 DAY DELETE); a fixed historical date would age out of that
    # window and silently vanish, which is exactly the trap that made the
    # first version of this test falsely red for the wrong reason. Anchor on
    # "now" so the fixture stays inside the TTL window whenever this runs.
    now = datetime.now(timezone.utc)
    day = (now - timedelta(days=1)).date()
    deploy_ts = now - timedelta(days=1, hours=1)
    bucket_start = now - timedelta(days=1)
    bucket_end = bucket_start + timedelta(hours=1)

    # One real deployment for this org/release/environment/day.
    sink.client.insert(
        "deployments",
        [
            [
                repo_id,
                "deploy-chaos-4243-1",
                "success",
                environment,
                deploy_ts,
                deploy_ts,
                deploy_ts,
                None,
                None,
                release_ref,
                1.0,
                deploy_ts,
                org_id,
            ]
        ],
        column_names=[
            "repo_id",
            "deployment_id",
            "status",
            "environment",
            "started_at",
            "finished_at",
            "deployed_at",
            "merged_at",
            "pull_request_number",
            "release_ref",
            "release_ref_confidence",
            "last_synced",
            "org_id",
        ],
    )
    # One real telemetry bucket on the day for the same release/environment --
    # this is exactly what _find_release_env_pairs reads, and exactly what is
    # empty (0 rows) in both environments the CHAOS-4243 audit measured.
    sink.client.insert(
        "telemetry_signal_bucket",
        [
            [
                org_id,
                "error",
                1,
                10,
                None,
                "default",
                environment,
                str(repo_id),
                release_ref,
                bucket_start,
                bucket_end,
                bucket_start,
                0,
                "v1",
                f"chaos-4243-{uuid4()}",
            ]
        ],
        column_names=[
            "org_id",
            "signal_type",
            "signal_count",
            "session_count",
            "unique_pseudonymous_count",
            "endpoint_group",
            "environment",
            "repo_id",
            "release_ref",
            "bucket_start",
            "bucket_end",
            "ingested_at",
            "is_sampled",
            "schema_version",
            "dedupe_key",
        ],
    )
    sink.client.command("OPTIMIZE TABLE deployments FINAL")

    written = await compute_release_impact_daily(
        ch_client=sink.client,
        sink=sink,
        org_id=org_id,
        day=day,
        recomputation_window_days=1,
    )
    assert written == 1, f"compute_release_impact_daily wrote {written} rows, want 1"

    result = sink.client.query(
        "SELECT release_ref, environment, org_id FROM release_impact_daily "
        "WHERE org_id = {org_id:String} AND release_ref = {release_ref:String}",
        parameters={"org_id": org_id, "release_ref": release_ref},
    )
    rows = result.result_rows
    assert len(rows) == 1, (
        f"release_impact_daily readback = {rows!r}, want exactly one row"
    )
    assert rows[0][0] == release_ref
    assert rows[0][1] == environment
    assert rows[0][2] == org_id


@pytest.mark.asyncio
async def test_release_impact_partition_reports_zero_rows_for_a_genuinely_empty_scope(
    sink,
):
    """The companion negative case: an org/day with no telemetry at all is a
    legitimate zero, not a bug -- compute_release_impact_daily must still
    return 0 and write nothing, matching the measured production shape.
    Distinct-reporting of this zero is the Go-side compatibility bridge's
    job (internal/jobs/metrics/remaining/compatibility_http.go); this proves
    the Python compute side genuinely produces that 0, not some other count.
    """
    from dev_health_ops.metrics.release_impact import compute_release_impact_daily

    org_id = f"test-chaos-4243-empty-{uuid4()}"
    written = await compute_release_impact_daily(
        ch_client=sink.client,
        sink=sink,
        org_id=org_id,
        day=datetime.now(timezone.utc).date(),
        recomputation_window_days=1,
    )
    assert written == 0
