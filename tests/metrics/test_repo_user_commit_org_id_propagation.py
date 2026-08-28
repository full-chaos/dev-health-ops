"""CHAOS-4341: repo_metrics_daily/user_metrics_daily/commit_metrics must
carry the partition's real org_id, never "".

The native Go executor (internal/jobs/metrics/daily/repouser/clickhouse.go)
used to hard-code org_id="" on all three tables, on the claim -- quoted
verbatim in CHAOS-4341's description -- that "nothing in job_daily.py ever
sets [org_id] on the sink instance these three writers use". That claim was
already false when it was written: run_daily_metrics_job has propagated the
real org_id onto every sink via ``setattr(s, "org_id", org_id)`` since
commit a165ef3c0 ("fix: propagate org_id to metrics sinks for correct data
isolation", 2026-02-23) -- six months before this ticket. The investigation
grepped ``ClickHouseCore.__init__``'s signature and missed the
post-construction setattr, exactly the "grepped and found nothing is not
evidence of absence" trap AGENTS.md warns about.

These tests pin the CORRECT, already-shipped Python behaviour directly
against the real ``ClickHouseMetricsSink``/``_insert_rows`` auto-injection
path (not a hand-rolled fake sink), so a future change to that plumbing
fails loudly here instead of silently reopening the "both languages" gap
CHAOS-4341 was filed against. The Go-side fix for the same ticket lives in
internal/jobs/metrics/daily/repouser/clickhouse_write_org_id_test.go and
internal/jobs/metrics/daily/repo_user_commit_org_scope_integration_test.go.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone

from dev_health_ops.metrics.schemas import (
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

ORG_ID = "c6a38355-dad6-42e4-8cc9-4c712450827d"
REPO_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def insert(
        self, table: str, matrix: list[list[object]], column_names: list[str]
    ) -> None:
        self.inserts.append((table, matrix, column_names))


def _org_id_values(client: FakeClickHouseClient, table: str) -> list[object]:
    values: list[object] = []
    for got_table, matrix, columns in client.inserts:
        if got_table != table:
            continue
        org_id_index = columns.index("org_id")
        values.extend(row[org_id_index] for row in matrix)
    return values


def test_write_repo_metrics_injects_real_org_id_not_empty() -> None:
    client = FakeClickHouseClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    sink.org_id = ORG_ID  # what run_daily_metrics_job does after construction.

    sink.write_repo_metrics(
        [
            RepoMetricsDailyRecord(
                repo_id=REPO_ID,
                day=date(2026, 8, 24),
                commits_count=1,
                total_loc_touched=10,
                avg_commit_size_loc=10.0,
                large_commit_ratio=0.0,
                prs_merged=0,
                median_pr_cycle_hours=0.0,
                computed_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
            )
        ]
    )

    org_ids = _org_id_values(client, "repo_metrics_daily")
    assert org_ids == [ORG_ID]


def test_write_user_metrics_injects_real_org_id_not_empty() -> None:
    client = FakeClickHouseClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    sink.org_id = ORG_ID

    sink.write_user_metrics(
        [
            UserMetricsDailyRecord(
                repo_id=REPO_ID,
                day=date(2026, 8, 24),
                author_email="dev@example.com",
                commits_count=1,
                loc_added=10,
                loc_deleted=0,
                files_changed=1,
                large_commits_count=0,
                avg_commit_size_loc=10.0,
                prs_authored=0,
                prs_merged=0,
                avg_pr_cycle_hours=0.0,
                median_pr_cycle_hours=0.0,
                computed_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
            )
        ]
    )

    org_ids = _org_id_values(client, "user_metrics_daily")
    assert org_ids == [ORG_ID]


def test_write_commit_metrics_injects_real_org_id_not_empty() -> None:
    client = FakeClickHouseClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    sink.org_id = ORG_ID

    sink.write_commit_metrics(
        [
            CommitMetricsRecord(
                repo_id=REPO_ID,
                commit_hash="abc123",
                day=date(2026, 8, 24),
                author_email="dev@example.com",
                total_loc=10,
                files_changed=1,
                size_bucket="small",
                computed_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
            )
        ]
    )

    org_ids = _org_id_values(client, "commit_metrics")
    assert org_ids == [ORG_ID]


def test_write_repo_metrics_without_sink_org_id_keeps_record_default() -> None:
    """Documents the OTHER half of the contract: a caller that never sets
    self.org_id (e.g. a bare CLI run with no --org, per ops/AGENTS.md's
    "Interim (CHAOS-2475)" note) still gets the record's own default -- the
    auto-injection is additive, never a silent override of an explicit
    per-row org_id."""
    client = FakeClickHouseClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    assert getattr(sink, "org_id", "") == ""

    sink.write_repo_metrics(
        [
            RepoMetricsDailyRecord(
                repo_id=REPO_ID,
                day=date(2026, 8, 24),
                commits_count=1,
                total_loc_touched=10,
                avg_commit_size_loc=10.0,
                large_commit_ratio=0.0,
                prs_merged=0,
                median_pr_cycle_hours=0.0,
                computed_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
            )
        ]
    )

    assert _org_id_values(client, "repo_metrics_daily") == [""]
