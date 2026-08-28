from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from dev_health_ops import cli
from dev_health_ops.metrics import job_compounding_risk


@pytest.mark.asyncio
async def test_compounding_risk_accepts_shared_date_backfill_flags(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_run_compounding_risk_job(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(
        job_compounding_risk,
        "run_compounding_risk_job",
        fake_run_compounding_risk_job,
    )
    monkeypatch.setattr(
        job_compounding_risk,
        "resolve_sink_uri",
        lambda ns: ns.analytics_db,
    )

    parser = cli.build_parser()
    ns = parser.parse_args(
        [
            "metrics",
            "compounding-risk",
            "--analytics-db",
            "clickhouse://user:pass@localhost:8123/default",
            "--org",
            "00000000-0000-0000-0000-000000000000",
            "--before",
            "2025-02-02",
            "--backfill",
            "7",
        ]
    )
    cli._resolve_org(ns)

    assert await ns.func(ns) == 0
    assert captured == {
        "db_url": "clickhouse://user:pass@localhost:8123/default",
        "day": date(2025, 2, 1),
        "backfill_days": 7,
        "org_id": "00000000-0000-0000-0000-000000000000",
    }


def test_compounding_risk_backfill_range_is_same_size_as_other_metrics():
    assert job_compounding_risk._date_range(date(2025, 2, 1), 7) == [
        date(2025, 1, 26),
        date(2025, 1, 27),
        date(2025, 1, 28),
        date(2025, 1, 29),
        date(2025, 1, 30),
        date(2025, 1, 31),
        date(2025, 2, 1),
    ]


def test_compounding_risk_rejects_removed_day_alias():
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["metrics", "compounding-risk", "--day", "2025-02-01"])


@pytest.mark.asyncio
async def test_load_repo_to_team_reads_repo_identifier_from_repos_id():
    class FakeSink:
        def __init__(self) -> None:
            self.queries: list[str] = []

        async def get_all_teams(self) -> list[dict[str, Any]]:
            return [
                {
                    "id": "team-1",
                    "name": "Team One",
                    "repo_patterns": ["full-chaos/dev-health-ops"],
                }
            ]

        def query_dicts(
            self, query: str, parameters: dict[str, Any]
        ) -> list[dict[str, Any]]:
            self.queries.append(query)
            # CHAOS-4365: the team_repo_ownership query additionally passes
            # as_of; this fake isn't query-shape-aware (both calls get the
            # same canned response), so only org_id is asserted here.
            assert parameters.get("org_id") == "org-1"
            return [
                {
                    "repo_id": "550e8400-e29b-41d4-a716-446655440000",
                    "full_name": "full-chaos/dev-health-ops",
                }
            ]

    sink = FakeSink()

    result = await job_compounding_risk._load_repo_to_team(
        sink, "org-1", as_of=datetime.now(timezone.utc)
    )

    # CHAOS-4365: two queries now run -- the repos catalog and
    # team_repo_ownership -- so find the repos one specifically rather than
    # asserting on "the last query" (order isn't a contract here).
    repos_query = next(q for q in sink.queries if "FROM repos" in q)
    assert "toString(id) AS repo_id" in repos_query
    assert "argMax(repo, last_synced) AS full_name" in repos_query
    assert "GROUP BY org_id, id" in repos_query
    assert "toString(repo_id)" not in repos_query
    assert "SELECT toString(id) AS repo_id, full_name" not in repos_query
    assert result == {"550e8400-e29b-41d4-a716-446655440000": "team-1"}
