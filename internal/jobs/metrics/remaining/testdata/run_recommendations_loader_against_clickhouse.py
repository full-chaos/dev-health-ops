#!/usr/bin/env python3
"""Run the shipped ClickHouseMetricsLoader against a live ClickHouse and print
the snapshot as JSON, floats as exact bit patterns.

Invoked by the Testcontainers integration test so the Go loader can be compared
against the REAL Python one reading the SAME rows from the SAME database. That
is what makes the comparison cover the SQL text and not merely the
post-processing, which the no-container corpus already pins.

Usage:
    run_recommendations_loader_against_clickhouse.py --dsn URL --team ID --org ID \
        --window-start YYYY-MM-DD --window-end YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT / "src"))

import clickhouse_connect  # noqa: E402

from dev_health_ops.recommendations.loader import (  # noqa: E402
    ClickHouseMetricsLoader,
)


def bits(value):
    return None if value is None else struct.pack(">d", float(value)).hex()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--team", required=True)
    parser.add_argument("--org", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    args = parser.parse_args()

    client = clickhouse_connect.get_client(dsn=args.dsn)
    loader = ClickHouseMetricsLoader(client, org_id=args.org)
    snapshot = loader.load_team_metrics_window(
        args.team,
        args.org,
        date.fromisoformat(args.window_start),
        date.fromisoformat(args.window_end),
    )

    json.dump(
        {
            "wip_by_day": [bits(v) for v in snapshot.wip_by_day],
            "throughput_by_cycle": [bits(v) for v in snapshot.throughput_by_cycle],
            "review_latency_p75_hours": bits(snapshot.review_latency_p75_hours),
            "reviewer_gini": bits(snapshot.reviewer_gini),
            "rework_churn_ratio": bits(snapshot.rework_churn_ratio),
            "after_hours_ratio": bits(snapshot.after_hours_ratio),
            "cycle_time_by_day": [bits(v) for v in snapshot.cycle_time_by_day],
            "hotspot_complexity_delta": bits(snapshot.hotspot_complexity_delta),
            "hotspot_churn_overlap": bits(snapshot.hotspot_churn_overlap),
            "compounding_risk_score": bits(snapshot.compounding_risk_score),
            "compounding_risk_severity": snapshot.compounding_risk_severity,
        },
        sys.stdout,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
