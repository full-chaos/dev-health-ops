import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Any

import git
from dev_health_ops.analytics.complexity import ComplexityScanner
from dev_health_ops.metrics.schemas import FileComplexitySnapshot, RepoComplexityDaily
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink
from dev_health_ops.storage import detect_db_type

logger = logging.getLogger(__name__)

DEFAULT_COMPLEXITY_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "complexity.yaml"


def _normalize_sqlite_url(db_url: str) -> str:
    if "sqlite+aiosqlite://" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return db_url


def _normalize_postgres_url(db_url: str) -> str:
    if "postgresql+asyncpg://" in db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url


def run_complexity_scan_job(
    *,
    repo_path: Path,
    repo_id: uuid.UUID,
    db_url: str,
    date: date,
    backfill_days: int = 1,
    ref: str = "HEAD",
    sink: Optional[Any] = None,
) -> None:
    if not db_url and not sink:
        raise ValueError("DB connection string or sink is required.")

    if not repo_path.exists():
        raise FileNotFoundError(f"Repo path {repo_path} does not exist.")

    repo = git.Repo(repo_path)
    scanner = ComplexityScanner(config_path=DEFAULT_COMPLEXITY_CONFIG_PATH)

    # Calculate date range
    start_date = date - timedelta(days=max(1, backfill_days) - 1)
    dates_to_process = [
        start_date + timedelta(days=i) for i in range(max(1, backfill_days))
    ]

    own_sink = False
    if sink is None:
        backend = detect_db_type(db_url)
        own_sink = True
        if backend == "clickhouse":
            sink = ClickHouseMetricsSink(db_url)
        elif backend == "sqlite":
            sink = SQLiteMetricsSink(_normalize_sqlite_url(db_url))
        elif backend == "mongo":
            sink = MongoMetricsSink(db_url)
        elif backend == "postgres":
            sink = SQLiteMetricsSink(_normalize_postgres_url(db_url))
        else:
            raise ValueError(f"Unsupported backend for complexity job: {backend}")

    try:
        if own_sink:
            sink.ensure_tables()

        for d in dates_to_process:
            logger.info(f"Processing date: {d}")

            # Find commit for this day
            # "last commit before end of day"
            # git rev-list -1 --before="YYYY-MM-DD 23:59:59" ref
            end_of_day_ts = (
                datetime.combine(d, datetime.max.time())
                .replace(tzinfo=timezone.utc)
                .isoformat()
            )

            try:
                commit_sha = repo.git.rev_list("-1", f"--before={end_of_day_ts}", ref)
            except git.Exc as e:
                logger.warning(f"Could not find commit for {d}: {e}")
                continue

            if not commit_sha:
                logger.warning(f"No commit found before {d} (ref={ref}). Skipping.")
                continue

            commit_sha = commit_sha.strip()
            logger.info(f"Scanning {repo_path} at commit {commit_sha} (as of {d})...")

            file_results = scanner.scan_git_ref(repo_path, commit_sha)
            logger.info(f"Scanned {len(file_results)} files.")

            if not file_results:
                logger.warning(f"No complexity data found for {d}.")
                continue

            # Aggregations
            computed_at = datetime.now(timezone.utc)

            snapshots = []
            total_loc = 0
            total_cc = 0
            total_high = 0
            total_very_high = 0

            for f in file_results:
                snapshots.append(
                    FileComplexitySnapshot(
                        repo_id=repo_id,
                        as_of_day=d,
                        ref=commit_sha,
                        file_path=f.file_path,
                        language=f.language,
                        loc=f.loc,
                        functions_count=f.functions_count,
                        cyclomatic_total=f.cyclomatic_total,
                        cyclomatic_avg=f.cyclomatic_avg,
                        high_complexity_functions=f.high_complexity_functions,
                        very_high_complexity_functions=f.very_high_complexity_functions,
                        computed_at=computed_at,
                    )
                )

                total_loc += f.loc
                total_cc += f.cyclomatic_total
                total_high += f.high_complexity_functions
                total_very_high += f.very_high_complexity_functions

            cc_per_kloc = (total_cc / (total_loc / 1000.0)) if total_loc > 0 else 0.0

            repo_daily = RepoComplexityDaily(
                repo_id=repo_id,
                day=d,
                loc_total=total_loc,
                cyclomatic_total=total_cc,
                cyclomatic_per_kloc=cc_per_kloc,
                high_complexity_functions=total_high,
                very_high_complexity_functions=total_very_high,
                computed_at=computed_at,
            )

            logger.info(f"Writing {len(snapshots)} file snapshots for {d}...")
            sink.write_file_complexity_snapshots(snapshots)
            logger.info(f"Writing repo daily summary for {d}...")
            sink.write_repo_complexity_daily([repo_daily])

    finally:
        if own_sink:
            sink.close()

    logger.info("Complexity job done.")
