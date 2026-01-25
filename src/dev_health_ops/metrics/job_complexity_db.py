from __future__ import annotations

import argparse
import fnmatch
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

from dev_health_ops.analytics.complexity import ComplexityScanner, FileComplexity
from dev_health_ops.metrics.schemas import FileComplexitySnapshot, RepoComplexityDaily
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.storage import detect_db_type

logger = logging.getLogger(__name__)

DEFAULT_COMPLEXITY_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "complexity.yaml"


def _date_range(end_day: date, backfill_days: int) -> List[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _coerce_uuid(value: Any) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _query_rows(client: Any, query: str, params: Optional[dict] = None) -> list:
    result = client.query(query, parameters=params or {})
    return list(getattr(result, "result_rows", []) or [])


def _load_repos(
    client: Any, repo_id: Optional[uuid.UUID], search_pattern: Optional[str]
) -> List[Tuple[uuid.UUID, Optional[str]]]:
    if repo_id is not None:
        return [(repo_id, None)]

    rows = _query_rows(client, "SELECT id, repo FROM repos")
    repos: List[Tuple[uuid.UUID, Optional[str]]] = []
    for row in rows:
        repo_uuid = _coerce_uuid(row[0])
        repo_name = row[1] if len(row) > 1 else None
        if search_pattern and not fnmatch.fnmatch(repo_name or "", search_pattern):
            continue
        repos.append((repo_uuid, repo_name))
    return repos


def _git_file_counts(client: Any, repo_id: uuid.UUID) -> Tuple[int, int]:
    rows = _query_rows(
        client,
        """
        SELECT
          count() AS total,
          countIf(contents IS NOT NULL AND contents != '') AS non_empty
        FROM git_files
        WHERE repo_id = {repo_id:UUID}
        """,
        {"repo_id": str(repo_id)},
    )
    if not rows:
        return 0, 0
    total, non_empty = rows[0]
    return int(total or 0), int(non_empty or 0)


def _load_git_files(
    client: Any, repo_id: uuid.UUID, limit: Optional[int]
) -> List[Tuple[str, str]]:
    query = """
        SELECT path, contents
        FROM git_files
        WHERE repo_id = {repo_id:UUID}
          AND contents IS NOT NULL
          AND contents != ''
        ORDER BY path
    """
    params: dict = {"repo_id": str(repo_id)}
    if limit is not None:
        query = f"{query} LIMIT {{limit:UInt64}}"
        params["limit"] = int(limit)
    rows = _query_rows(client, query, params)
    return [(row[0], row[1]) for row in rows]


def _load_missing_paths(
    client: Any, repo_id: uuid.UUID, limit: Optional[int]
) -> List[str]:
    query = """
        SELECT path
        FROM git_files
        WHERE repo_id = {repo_id:UUID}
          AND (contents IS NULL OR contents = '')
        ORDER BY path
    """
    params: dict = {"repo_id": str(repo_id)}
    if limit is not None:
        query = f"{query} LIMIT {{limit:UInt64}}"
        params["limit"] = int(limit)
    rows = _query_rows(client, query, params)
    return [row[0] for row in rows]


def _load_blame_contents(
    client: Any,
    repo_id: uuid.UUID,
    paths: Optional[Sequence[str]],
    limit: Optional[int],
) -> List[Tuple[str, str]]:
    query = """
        SELECT
          path,
          arrayStringConcat(
            arrayMap(
              x -> x.2,
              arraySort(groupArray((line_no, ifNull(line, ''))))
            ),
            '\n'
          ) AS contents
        FROM git_blame
        WHERE repo_id = {repo_id:UUID}
    """
    params: dict = {"repo_id": str(repo_id)}
    if paths:
        query += " AND path IN {paths:Array(String)}"
        params["paths"] = list(paths)
    query += " GROUP BY path ORDER BY path"
    if limit is not None:
        query = f"{query} LIMIT {{limit:UInt64}}"
        params["limit"] = int(limit)
    rows = _query_rows(client, query, params)
    return [(row[0], row[1]) for row in rows]


def _max_last_synced(client: Any, table: str, repo_id: uuid.UUID) -> Optional[datetime]:
    rows = _query_rows(
        client,
        f"SELECT max(last_synced) FROM {table} WHERE repo_id = {{repo_id:UUID}}",
        {"repo_id": str(repo_id)},
    )
    if not rows:
        return None
    return rows[0][0]


def _build_ref(client: Any, repo_id: uuid.UUID) -> str:
    last_synced = _max_last_synced(client, "git_files", repo_id)
    blame_synced = _max_last_synced(client, "git_blame", repo_id)
    candidates = [dt for dt in [last_synced, blame_synced] if dt]
    if not candidates:
        return "db_last_synced:unknown"
    latest = max(candidates)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    else:
        latest = latest.astimezone(timezone.utc)
    return f"db_last_synced:{latest.isoformat()}"


def _filter_files(
    scanner: ComplexityScanner,
    files: List[Tuple[str, str]],
    max_files: Optional[int],
) -> List[Tuple[str, str]]:
    filtered: List[Tuple[str, str]] = []
    for path, contents in files:
        if not scanner.should_process(path):
            continue
        filtered.append((path, contents))
        if max_files is not None and len(filtered) >= max_files:
            break
    return filtered


def run_complexity_db_job(
    *,
    repo_id: Optional[uuid.UUID],
    db_url: str,
    date: date,
    backfill_days: int,
    language_globs: Optional[List[str]],
    max_files: Optional[int],
    search_pattern: Optional[str] = None,
    exclude_globs: Optional[List[str]] = None,
) -> int:
    """
    Compute complexity metrics from ClickHouse git_files/git_blame contents.

    Note: For backfill_days > 1, this job reuses the current file snapshot
    across the requested days because historical file snapshots are not stored.
    """
    db_url = db_url or os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    backend = detect_db_type(db_url)
    if backend != "clickhouse":
        logger.error("complexity_db_job supports clickhouse only")
        return 2

    sink = ClickHouseMetricsSink(db_url)
    try:
        sink.ensure_tables()

        scanner = ComplexityScanner(config_path=DEFAULT_COMPLEXITY_CONFIG_PATH)
        if language_globs:
            scanner.include_globs = list(language_globs)
        if exclude_globs:
            scanner.exclude_globs = list(exclude_globs)

        repos = _load_repos(sink.client, repo_id, search_pattern)
        if not repos:
            logger.warning("No repositories found in database.")
            return 0

        if backfill_days > 1:
            logger.info(
                "Backfill requested; reusing current file snapshot for each day."
            )

        days = _date_range(date, max(1, int(backfill_days)))
        for repo_uuid, repo_name in repos:
            repo_label = repo_name or str(repo_uuid)
            total_files, non_empty = _git_file_counts(sink.client, repo_uuid)

            files: List[Tuple[str, str]] = []
            remaining = max_files

            if non_empty > 0:
                git_files = _load_git_files(sink.client, repo_uuid, remaining)
                files.extend(git_files)
                if remaining is not None:
                    remaining = max(remaining - len(git_files), 0)

                missing = max(total_files - non_empty, 0)
                if missing > 0 and (remaining is None or remaining > 0):
                    logger.info(
                        "Repo %s missing %s/%s git_files contents; filling with blame.",
                        repo_label,
                        missing,
                        total_files,
                    )
                    missing_paths = _load_missing_paths(
                        sink.client, repo_uuid, remaining
                    )
                    missing_paths = [
                        path for path in missing_paths if scanner.should_process(path)
                    ]
                    if missing_paths:
                        blame_files = _load_blame_contents(
                            sink.client,
                            repo_uuid,
                            missing_paths,
                            remaining,
                        )
                        files.extend(blame_files)
            else:
                if total_files > 0:
                    logger.warning(
                        "Repo %s has no git_files contents; falling back to git_blame.",
                        repo_label,
                    )
                files = _load_blame_contents(sink.client, repo_uuid, None, remaining)

            files = _filter_files(scanner, files, max_files)
            if not files:
                logger.warning("No scannable contents found for repo %s.", repo_label)
                continue

            ref_value = _build_ref(sink.client, repo_uuid)
            file_results = scanner.scan_file_contents(files)
            if not file_results:
                logger.warning("No complexity data found for repo %s.", repo_label)
                continue

            for d in days:
                computed_at = datetime.now(timezone.utc)
                snapshots, repo_daily = _build_snapshots(
                    repo_uuid, d, ref_value, file_results, computed_at
                )

                if not snapshots:
                    logger.warning("No complexity snapshots generated for %s.", d)
                    continue

                sink.write_file_complexity_snapshots(snapshots)
                sink.write_repo_complexity_daily([repo_daily])

    finally:
        sink.close()

    logger.info("Complexity DB job done.")
    return 0


def _build_snapshots(
    repo_id: uuid.UUID,
    day: date,
    ref_value: str,
    file_results: List[FileComplexity],
    computed_at: datetime,
) -> Tuple[List[FileComplexitySnapshot], RepoComplexityDaily]:
    snapshots: List[FileComplexitySnapshot] = []
    total_loc = 0
    total_cc = 0
    total_high = 0
    total_very_high = 0

    for f in file_results:
        snapshots.append(
            FileComplexitySnapshot(
                repo_id=repo_id,
                as_of_day=day,
                ref=ref_value,
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
        day=day,
        loc_total=total_loc,
        cyclomatic_total=total_cc,
        cyclomatic_per_kloc=cc_per_kloc,
        high_complexity_functions=total_high,
        very_high_complexity_functions=total_very_high,
        computed_at=computed_at,
    )

    return snapshots, repo_daily


def register_commands(metrics_subparsers: argparse._SubParsersAction) -> None:
    complexity = metrics_subparsers.add_parser(
        "complexity",
        help="Compute file complexity metrics from DB (git_files/git_blame).",
    )
    complexity.add_argument("--db", help="Database connection string.")
    complexity.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today().isoformat(),
        help="Target day (YYYY-MM-DD).",
    )
    complexity.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Compute for N days ending at --date.",
    )
    complexity.add_argument(
        "--repo-id", type=uuid.UUID, help="Filter to specific repo."
    )
    complexity.add_argument("-s", "--search", help="Repo name search pattern (glob).")
    complexity.add_argument(
        "--lang", action="append", help="Include language globs (e.g. *.py)."
    )
    complexity.add_argument(
        "--exclude", action="append", help="Exclude language globs (e.g. */tests/*)."
    )
    complexity.add_argument(
        "--max-files", type=int, help="Limit number of files scanned per repo."
    )
    complexity.set_defaults(func=_cmd_metrics_complexity)


def _cmd_metrics_complexity(ns: argparse.Namespace) -> int:
    return run_complexity_db_job(
        repo_id=ns.repo_id,
        db_url=ns.db,
        date=ns.date,
        backfill_days=ns.backfill,
        language_globs=ns.lang,
        max_files=ns.max_files,
        search_pattern=ns.search,
        exclude_globs=ns.exclude,
    )
