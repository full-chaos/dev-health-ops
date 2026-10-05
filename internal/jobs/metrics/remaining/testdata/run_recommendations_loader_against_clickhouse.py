#!/usr/bin/env python3
"""Run the shipped ClickHouseMetricsLoader against a live ClickHouse and print
the snapshot as JSON, floats as exact bit patterns.

Invoked by the Testcontainers integration test so the Go loader can be compared
against the REAL Python one reading the SAME rows from the SAME database. That
is what makes the comparison cover the SQL text and not merely the
post-processing, which the no-container corpus already pins.

It runs as a Produce producer (CHAOS-8303): the program text is passed to the interpreter with -c, the request is one JSON
object on stdin ({"team", "org", "window_start", "window_end"}), the address of the run's database is the CLICKHOUSE_URI environment
entry, and the checkout's src is on PYTHONPATH (the closed environment). The answer is the one JSON line on stdout. Recorded once on the
pinned build by the record verb and frozen; the Go loader is compared with the frozen answers.
"""

from __future__ import annotations

import json
import os
import struct
import sys
from datetime import date

import clickhouse_connect  # noqa: E402

from dev_health_ops.recommendations.loader import (  # noqa: E402
    ClickHouseMetricsLoader,
)


def bits(value):
    return None if value is None else struct.pack(">d", float(value)).hex()


def main() -> int:
    request = json.loads(sys.stdin.read())
    client = clickhouse_connect.get_client(dsn=os.environ["CLICKHOUSE_URI"])
    loader = ClickHouseMetricsLoader(client, org_id=request["org"])
    snapshot = loader.load_team_metrics_window(
        request["team"],
        request["org"],
        date.fromisoformat(request["window_start"]),
        date.fromisoformat(request["window_end"]),
    )

    json.dump(
        {
            # Identity and window fields are emitted so the Go comparator can
            # check them. They are echoes of the loader's own arguments on both
            # sides, which is precisely why a port that echoed the WRONG one
            # went undetected: nothing compared them.
            "team_id": snapshot.team_id,
            "org_id": snapshot.org_id,
            "window_start": snapshot.window_start.isoformat(),
            "window_end": snapshot.window_end.isoformat(),
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
