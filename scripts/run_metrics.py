from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

# Allow `python scripts/run_metrics.py` from repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402

from dev_health_ops.analytics.metrics import (  # noqa: E402
    compute_commit_metrics,
    compute_pr_metrics,
    compute_repo_metrics,
    compute_user_metrics,
)
from dev_health_ops.models.git import GitCommit, GitCommitStat, GitPullRequest  # noqa: E402


def _is_async_db_url(db_url: str) -> bool:
    url = db_url.lower()
    return "+asyncpg" in url or "+aiosqlite" in url


def _detect_backend(db_url: str) -> str:
    url = db_url.lower()
    if url.startswith(("mongodb://", "mongodb+srv://")):
        return "mongo"
    if url.startswith(
        (
            "clickhouse://",
            "clickhouse+http://",
            "clickhouse+https://",
            "clickhouse+native://",
        )
    ):
        return "clickhouse"
    return "sqlalchemy"


def _fmt_dt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _fmt_float(value: Any, digits: int = 2) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def _print_table(
    title: str, headers: Sequence[str], rows: Iterable[Sequence[Any]]
) -> None:
    rows_list: List[List[str]] = [[str(cell) for cell in row] for row in rows]
    print(f"\n{title}")
    if not rows_list:
        print("(no rows)")
        return

    try:
        from tabulate import tabulate  # type: ignore

        print(tabulate(rows_list, headers=headers, tablefmt="github"))
        return
    except Exception:
        pass

    all_rows = [list(headers), *rows_list]
    widths = [max(len(str(r[i])) for r in all_rows) for i in range(len(headers))]

    def fmt_row(r: Sequence[Any]) -> str:
        return " | ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers)))

    print(fmt_row(headers))
    print("-+-".join("-" * w for w in widths))
    for r in rows_list:
        print(fmt_row(r))


def _parse_uuid(value: Any) -> Optional[uuid.UUID]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _parse_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return None


def _commit_from_row(row: Any) -> Optional[GitCommit]:
    if not isinstance(row, dict):
        return None

    repo_id = _parse_uuid(row.get("repo_id"))
    commit_hash = row.get("hash") or row.get("commit_hash")
    if repo_id is None or not commit_hash:
        return None

    author_when = _parse_datetime(row.get("author_when")) or datetime.min
    committer_when = _parse_datetime(row.get("committer_when")) or author_when

    return GitCommit(
        repo_id=repo_id,
        hash=str(commit_hash),
        author_name=row.get("author_name"),
        author_when=author_when,
        committer_when=committer_when,
        parents=_parse_int(row.get("parents"), default=0),
    )


def _commit_stat_from_row(row: Any) -> Optional[GitCommitStat]:
    if not isinstance(row, dict):
        return None

    repo_id = _parse_uuid(row.get("repo_id"))
    commit_hash = row.get("commit_hash")
    file_path = row.get("file_path")
    if repo_id is None or not commit_hash or file_path is None:
        return None

    return GitCommitStat(
        repo_id=repo_id,
        commit_hash=str(commit_hash),
        file_path=str(file_path),
        additions=_parse_int(row.get("additions"), default=0),
        deletions=_parse_int(row.get("deletions"), default=0),
        old_file_mode=row.get("old_file_mode") or "unknown",
        new_file_mode=row.get("new_file_mode") or "unknown",
    )


def _pr_from_row(row: Any) -> Optional[GitPullRequest]:
    if not isinstance(row, dict):
        return None

    repo_id = _parse_uuid(row.get("repo_id"))
    number = row.get("number")
    if repo_id is None or number is None:
        return None

    created_at = _parse_datetime(row.get("created_at")) or datetime.min
    merged_at = _parse_datetime(row.get("merged_at"))

    return GitPullRequest(
        repo_id=repo_id,
        number=_parse_int(number, default=0),
        author_name=row.get("author_name"),
        created_at=created_at,
        merged_at=merged_at,
    )


async def _run_mongo(db_url: str, limit_commits: int) -> int:
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except Exception as exc:
        print(f"MongoDB support unavailable: {exc}")
        return 1

    client = AsyncIOMotorClient(db_url)
    try:
        # Get db name from URI or default to 'mergestat'
        try:
            default_db = client.get_default_database()
            db = default_db if default_db is not None else client["mergestat"]
        except Exception:
            db = client["mergestat"]

        commits: List[GitCommit] = []
        stats: List[GitCommitStat] = []
        prs: List[GitPullRequest] = []

        commit_projection = {
            "repo_id": 1,
            "hash": 1,
            "author_name": 1,
            "author_when": 1,
            "committer_when": 1,
            "parents": 1,
        }
        stat_projection = {
            "repo_id": 1,
            "commit_hash": 1,
            "file_path": 1,
            "additions": 1,
            "deletions": 1,
        }
        pr_projection = {
            "repo_id": 1,
            "number": 1,
            "author_name": 1,
            "created_at": 1,
            "merged_at": 1,
        }

        async for doc in db["git_commits"].find({}, commit_projection):
            obj = _commit_from_row(doc)
            if obj is not None:
                commits.append(obj)

        async for doc in db["git_commit_stats"].find({}, stat_projection):
            obj = _commit_stat_from_row(doc)
            if obj is not None:
                stats.append(obj)

        async for doc in db["git_pull_requests"].find({}, pr_projection):
            obj = _pr_from_row(doc)
            if obj is not None:
                prs.append(obj)

    finally:
        client.close()

    commit_metrics = compute_commit_metrics(commits, stats)
    pr_metrics = compute_pr_metrics(prs)
    user_metrics = compute_user_metrics(commits, stats, pr_metrics)
    repo_metrics = compute_repo_metrics(commits, stats, pr_metrics)

    _print_reports(
        commit_metrics, user_metrics, repo_metrics, limit_commits=limit_commits
    )
    return 0


def _clickhouse_query_rows(client: Any, query: str) -> List[dict]:
    result = client.query(query)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _run_clickhouse(db_url: str, limit_commits: int) -> int:
    try:
        import clickhouse_connect
    except Exception as exc:
        print(f"ClickHouse support unavailable: {exc}")
        return 1

    client = clickhouse_connect.get_client(dsn=db_url)
    try:
        commits_rows = _clickhouse_query_rows(
            client,
            "SELECT repo_id, hash, author_name, author_when, committer_when, parents FROM git_commits",
        )
        stats_rows = _clickhouse_query_rows(
            client,
            "SELECT repo_id, commit_hash, file_path, additions, deletions FROM git_commit_stats",
        )
        prs_rows = _clickhouse_query_rows(
            client,
            "SELECT repo_id, number, author_name, created_at, merged_at FROM git_pull_requests",
        )
    finally:
        try:
            client.close()
        except Exception:
            logging.error("Error closing ClickHouse client", exc_info=True)

    commits = [c for c in (_commit_from_row(r) for r in commits_rows) if c is not None]
    stats = [s for s in (_commit_stat_from_row(r) for r in stats_rows) if s is not None]
    prs = [p for p in (_pr_from_row(r) for r in prs_rows) if p is not None]

    commit_metrics = compute_commit_metrics(commits, stats)
    pr_metrics = compute_pr_metrics(prs)
    user_metrics = compute_user_metrics(commits, stats, pr_metrics)
    repo_metrics = compute_repo_metrics(commits, stats, pr_metrics)

    _print_reports(
        commit_metrics, user_metrics, repo_metrics, limit_commits=limit_commits
    )
    return 0


async def _run_async(db_url: str, limit_commits: int) -> int:
    engine = create_async_engine(db_url, echo=False)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with session_factory() as session:
            commits = (await session.execute(select(GitCommit))).scalars().all()
            stats = (await session.execute(select(GitCommitStat))).scalars().all()
            prs = (await session.execute(select(GitPullRequest))).scalars().all()
    finally:
        await engine.dispose()

    commit_metrics = compute_commit_metrics(commits, stats)
    pr_metrics = compute_pr_metrics(prs)
    user_metrics = compute_user_metrics(commits, stats, pr_metrics)
    repo_metrics = compute_repo_metrics(commits, stats, pr_metrics)

    _print_reports(
        commit_metrics, user_metrics, repo_metrics, limit_commits=limit_commits
    )
    return 0


def _run_sync(db_url: str, limit_commits: int) -> int:
    from sqlalchemy import create_engine

    engine = create_engine(db_url, echo=False)
    try:
        with Session(engine) as session:
            commits = session.query(GitCommit).all()
            stats = session.query(GitCommitStat).all()
            prs = session.query(GitPullRequest).all()
    finally:
        engine.dispose()

    commit_metrics = compute_commit_metrics(commits, stats)
    pr_metrics = compute_pr_metrics(prs)
    user_metrics = compute_user_metrics(commits, stats, pr_metrics)
    repo_metrics = compute_repo_metrics(commits, stats, pr_metrics)

    _print_reports(
        commit_metrics, user_metrics, repo_metrics, limit_commits=limit_commits
    )
    return 0


def _print_reports(
    commit_metrics,
    user_metrics,
    repo_metrics,
    limit_commits: int,
) -> None:
    commit_sample = sorted(
        commit_metrics, key=lambda m: m.committed_at or datetime.min, reverse=True
    )[: max(0, limit_commits)]

    _print_table(
        title=f"Commit Metrics (sample, n={len(commit_sample)} of {len(commit_metrics)})",
        headers=[
            "repo_id",
            "hash",
            "author",
            "committed_at",
            "total_loc",
            "files_changed",
            "bucket",
            "is_large",
        ],
        rows=[
            [
                str(m.repo_id),
                (m.commit_hash or "")[:7],
                m.author_name,
                _fmt_dt(m.committed_at),
                str(m.total_loc),
                str(m.files_changed),
                m.size_bucket,
                str(m.is_large_commit),
            ]
            for m in commit_sample
        ],
    )

    users_sorted = sorted(
        user_metrics.values(),
        key=lambda u: (
            u.commits_count,
            u.prs_authored,
            u.total_loc_added + u.total_loc_deleted,
        ),
        reverse=True,
    )
    _print_table(
        title=f"User Metrics (n={len(users_sorted)})",
        headers=[
            "user",
            "commits",
            "loc_added",
            "loc_deleted",
            "files_changed",
            "large_commits",
            "avg_commit_size",
            "prs_authored",
            "avg_pr_cycle_h",
        ],
        rows=[
            [
                u.user_name,
                str(u.commits_count),
                str(u.total_loc_added),
                str(u.total_loc_deleted),
                str(u.total_files_changed),
                str(u.large_commits_count),
                _fmt_float(u.avg_commit_size),
                str(u.prs_authored),
                _fmt_float(u.avg_pr_cycle_time),
            ]
            for u in users_sorted
        ],
    )

    repos_sorted = sorted(
        repo_metrics.values(),
        key=lambda r: (r.commits_count, r.total_loc_touched),
        reverse=True,
    )
    _print_table(
        title=f"Repo Metrics (n={len(repos_sorted)})",
        headers=[
            "repo_id",
            "commits",
            "loc_touched",
            "avg_commit_size",
            "large_commit_ratio",
            "prs_merged",
            "median_pr_cycle_h",
        ],
        rows=[
            [
                str(r.repo_id),
                str(r.commits_count),
                str(r.total_loc_touched),
                _fmt_float(r.avg_commit_size),
                _fmt_float(r.large_commit_ratio, digits=3),
                str(r.prs_merged),
                _fmt_float(r.median_pr_cycle_time),
            ]
            for r in repos_sorted
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute developer-oriented commit/PR/user/repo metrics from the MergeStat database."
    )
    parser.add_argument(
        "--db",
        default=os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL"),
        help=(
            "Database URI (SQLAlchemy: postgresql+asyncpg://..., sqlite+aiosqlite:///...; "
            "MongoDB: mongodb://...; ClickHouse: clickhouse://...)."
        ),
    )
    parser.add_argument(
        "--limit-commits",
        type=int,
        default=10,
        help="Number of commit metric rows to print.",
    )
    args = parser.parse_args()

    if not args.db:
        print("No database connection string provided.")
        print("Set `DATABASE_URI` or pass `--db`.")
        return 0

    try:
        backend = _detect_backend(args.db)
        if backend == "mongo":
            return asyncio.run(_run_mongo(args.db, limit_commits=args.limit_commits))
        if backend == "clickhouse":
            return _run_clickhouse(args.db, limit_commits=args.limit_commits)

        if _is_async_db_url(args.db):
            return asyncio.run(_run_async(args.db, limit_commits=args.limit_commits))
        return _run_sync(args.db, limit_commits=args.limit_commits)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"Failed to run metrics: {exc.__class__.__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
