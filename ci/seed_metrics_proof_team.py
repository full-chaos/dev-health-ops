#!/usr/bin/env python3
"""Seed one ClickHouse `teams` row with a repo_patterns match for the
metrics-executed-proof gate's git-seeded repo (CHAOS-4276).

Without any team, every commit team_wellbeing computes resolves to the
"unassigned" bucket (compute_team_wellbeing_metrics_daily's default) --
still a valid, non-empty proof row, but it never exercises repo-pattern
team resolution end to end through the real pipeline. This is a deliberate,
minimal team fixture: ONE repo-pattern team, so team_wellbeing's primary
resolution path (repo pattern checked before membership) is what actually
produces the proof row, not just the fallback default.

Uses ClickHouseMetricsSink's own client (the same one
ci/assert_metrics_executed_proof.py reads back with) rather than hand-rolled
SQL string interpolation, so there is no escaping/injection surface for the
org id or repo name this script is invoked with.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-uri", required=True)
    parser.add_argument("--org-id", required=True)
    parser.add_argument(
        "--repo-name",
        required=True,
        help="Full repo name (org/repo) to match via an exact repo_patterns entry.",
    )
    parser.add_argument("--team-id", default="metrics-proof-repo-team")
    parser.add_argument("--team-name", default="Metrics Proof Repo Team")
    args = parser.parse_args()

    sink = ClickHouseMetricsSink(args.clickhouse_uri)
    try:
        sink.client.insert(
            "teams",
            [
                [
                    args.team_id,
                    str(uuid.uuid4()),
                    args.team_name,
                    [],
                    [args.repo_name.strip().lower()],
                    datetime.now(timezone.utc),
                    args.org_id,
                ]
            ],
            column_names=[
                "id",
                "team_uuid",
                "name",
                "members",
                "repo_patterns",
                "updated_at",
                "org_id",
            ],
        )
    finally:
        sink.close()

    print(f"OK: seeded team {args.team_id!r} with repo_patterns=[{args.repo_name!r}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
