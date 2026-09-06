#!/usr/bin/env python3
"""Run the shipped run_complexity_db_job against a live ClickHouse and print
the resulting file_complexity_snapshots/repo_complexity_daily rows as JSON.

Invoked by the Testcontainers integration test so the Go ComplexityExecutor
can be compared against the REAL Python job reading/writing the SAME
database -- this covers the SQL (git_files/git_blame reads, the budget and
blame-fallback control flow, the two table writes), not just the per-file
complexity arithmetic (already proven separately by lizardcc/pycc's own
golden-vs-real-lizard/radon parity tests).

Usage:
    run_complexity_db_job_against_clickhouse.py --dsn URL --repo-id UUID \
        --org ID --day YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT / "src"))

import clickhouse_connect  # noqa: E402

from dev_health_ops.metrics.job_complexity_db import run_complexity_db_job  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--repo-id", required=True)
    parser.add_argument("--org", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--max-files", type=int, default=None)
    args = parser.parse_args()

    exit_code = run_complexity_db_job(
        repo_id=uuid.UUID(args.repo_id),
        db_url=args.dsn,
        date=date.fromisoformat(args.day),
        backfill_days=1,
        language_globs=None,
        max_files=args.max_files,
        org_id=args.org,
    )

    client = clickhouse_connect.get_client(dsn=args.dsn)
    snapshot_rows = client.query(
        """
        SELECT file_path, language, loc, functions_count, cyclomatic_total,
               cyclomatic_avg, high_complexity_functions,
               very_high_complexity_functions
        FROM file_complexity_snapshots
        WHERE repo_id = {repo_id:UUID} AND as_of_day = {day:Date} AND org_id = {org_id:String}
        ORDER BY file_path
        """,
        parameters={"repo_id": args.repo_id, "day": args.day, "org_id": args.org},
    ).result_rows
    daily_rows = client.query(
        """
        SELECT loc_total, cyclomatic_total, cyclomatic_per_kloc,
               high_complexity_functions, very_high_complexity_functions
        FROM repo_complexity_daily
        WHERE repo_id = {repo_id:UUID} AND day = {day:Date} AND org_id = {org_id:String}
        ORDER BY computed_at DESC
        LIMIT 1
        """,
        parameters={"repo_id": args.repo_id, "day": args.day, "org_id": args.org},
    ).result_rows

    json.dump(
        {
            "exit_code": exit_code,
            "snapshots": [
                {
                    "file_path": row[0],
                    "language": row[1],
                    "loc": row[2],
                    "functions_count": row[3],
                    "cyclomatic_total": row[4],
                    "cyclomatic_avg": row[5],
                    "high_complexity_functions": row[6],
                    "very_high_complexity_functions": row[7],
                }
                for row in snapshot_rows
            ],
            "repo_daily": (
                {
                    "loc_total": daily_rows[0][0],
                    "cyclomatic_total": daily_rows[0][1],
                    "cyclomatic_per_kloc": daily_rows[0][2],
                    "high_complexity_functions": daily_rows[0][3],
                    "very_high_complexity_functions": daily_rows[0][4],
                }
                if daily_rows
                else None
            ),
        },
        sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
