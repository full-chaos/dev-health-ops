from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import date
from pathlib import Path

# Allow `python scripts/compute_metrics_daily.py` from repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dev_health_ops.metrics.job_daily import run_daily_metrics_job  # noqa: E402


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def _parse_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid UUID '{value}'") from exc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute and persist daily developer health metrics."
    )
    parser.add_argument(
        "--date",
        required=True,
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help="Database URI (ClickHouse, MongoDB, or SQLite). Defaults to DATABASE_URI.",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Compute N days ending at --date (inclusive). Default: 1.",
    )
    parser.add_argument(
        "--repo-id", type=_parse_uuid, help="Optional repo_id UUID filter."
    )
    parser.add_argument(
        "--sink",
        choices=["auto", "clickhouse", "mongo", "sqlite", "both"],
        default="auto",
        help="Where to write derived metrics (default: auto = same as --db backend).",
    )
    parser.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "none"],
        default="all",
        help="Which work tracking providers to include for WorkItem metrics (default: all).",
    )

    args = parser.parse_args()

    try:
        run_daily_metrics_job(
            db_url=args.db,
            day=args.date,
            backfill_days=max(1, int(args.backfill)),
            repo_id=args.repo_id,
            include_commit_metrics=True,
            sink=args.sink,
            provider=args.provider,
        )
        return 0
    except Exception as exc:
        print(f"Failed to compute daily metrics: {exc.__class__.__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
