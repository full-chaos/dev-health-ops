from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Sequence

from .client import query_dicts
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


async def fetch_review_wait_density(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            toDayOfWeek(created_at) AS weekday,
            toHour(created_at) AS hour,
            sum(dateDiff('minute', created_at, first_review_at)) / 60.0 AS value
        FROM git_pull_requests
        WHERE created_at >= %(start_ts)s
          AND created_at < %(end_ts)s
          AND first_review_at IS NOT NULL
        {scope_filter}
        GROUP BY weekday, hour
    """
    params = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    return await query_dicts(sink, query, params)


async def fetch_review_wait_evidence(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    weekday: int,
    hour: int,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            repo_id,
            number,
            title,
            created_at,
            first_review_at
        FROM git_pull_requests
        WHERE created_at >= %(start_ts)s
          AND created_at < %(end_ts)s
          AND first_review_at IS NOT NULL
          AND toDayOfWeek(created_at) = %(weekday)s
          AND toHour(created_at) = %(hour)s
        {scope_filter}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "weekday": weekday,
        "hour": hour,
        "limit": limit,
    }
    params.update(scope_params)
    return await query_dicts(sink, query, params)


async def fetch_repo_touchpoints(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    top_query = f"""
        SELECT
            repos.repo AS repo,
            count() AS total
        FROM git_commits
        INNER JOIN repos ON toString(repos.id) = toString(git_commits.repo_id)
        WHERE author_when >= %(start_ts)s
          AND author_when < %(end_ts)s
        {scope_filter}
        GROUP BY repos.repo
        ORDER BY total DESC
        LIMIT %(limit)s
    """
    params = {"start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    params.update(scope_params)
    top_rows = await query_dicts(sink, top_query, params)
    repos = [str(row.get("repo")) for row in top_rows if row.get("repo")]
    if not repos:
        return []

    query = f"""
        SELECT
            toDate(author_when) AS day,
            repos.repo AS repo,
            count() AS value
        FROM git_commits
        INNER JOIN repos ON toString(repos.id) = toString(git_commits.repo_id)
        WHERE author_when >= %(start_ts)s
          AND author_when < %(end_ts)s
          AND repos.repo IN %(repos)s
        {scope_filter}
        GROUP BY day, repo
        ORDER BY day
    """
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "repos": repos,
    }
    params.update(scope_params)
    return await query_dicts(sink, query, params)


async def fetch_hotspot_risk(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    top_query = f"""
        SELECT
            concat(repos.repo, ':', path) AS file_key,
            sum(hotspot_score) AS total
        FROM file_metrics_daily
        INNER JOIN repos ON toString(repos.id) = toString(file_metrics_daily.repo_id)
        WHERE day >= %(start_day)s
          AND day < %(end_day)s
        {scope_filter}
        GROUP BY file_key
        ORDER BY total DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    top_rows = await query_dicts(sink, top_query, params)
    files = [str(row.get("file_key")) for row in top_rows if row.get("file_key")]
    if not files:
        return []

    query = f"""
        SELECT
            toStartOfWeek(day) AS week,
            concat(repos.repo, ':', path) AS file_key,
            sum(hotspot_score) AS value
        FROM file_metrics_daily
        INNER JOIN repos ON toString(repos.id) = toString(file_metrics_daily.repo_id)
        WHERE day >= %(start_day)s
          AND day < %(end_day)s
          AND concat(repos.repo, ':', path) IN %(files)s
        {scope_filter}
        GROUP BY week, file_key
        ORDER BY week
    """
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "files": files,
    }
    params.update(scope_params)
    return await query_dicts(sink, query, params)


async def fetch_hotspot_evidence(
    sink: BaseMetricsSink,
    *,
    week_start: date,
    week_end: date,
    file_key: str,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            day,
            repos.repo AS repo,
            path,
            churn,
            contributors,
            commits_count,
            hotspot_score
        FROM file_metrics_daily
        INNER JOIN repos ON toString(repos.id) = toString(file_metrics_daily.repo_id)
        WHERE day >= %(week_start)s
          AND day < %(week_end)s
          AND concat(repos.repo, ':', path) = %(file_key)s
        {scope_filter}
        ORDER BY day
        LIMIT %(limit)s
    """
    params = {
        "week_start": week_start,
        "week_end": week_end,
        "file_key": file_key,
        "limit": limit,
    }
    params.update(scope_params)
    return await query_dicts(sink, query, params)


async def fetch_individual_active_hours(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    identities: Sequence[str],
) -> List[Dict[str, Any]]:
    if not identities:
        return []
    query = """
        SELECT
            toDayOfWeek(author_when) AS weekday,
            toHour(author_when) AS hour,
            count() AS value
        FROM git_commits
        WHERE author_when >= %(start_ts)s
          AND author_when < %(end_ts)s
          AND (author_email IN %(identities)s OR author_name IN %(identities)s)
        GROUP BY weekday, hour
    """
    params = {"start_ts": start_ts, "end_ts": end_ts, "identities": list(identities)}
    return await query_dicts(sink, query, params)


async def fetch_individual_active_evidence(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    weekday: int,
    hour: int,
    identities: Sequence[str],
    limit: int,
) -> List[Dict[str, Any]]:
    if not identities:
        return []
    query = """
        SELECT
            repos.repo AS repo,
            git_commits.hash AS commit_hash,
            git_commits.message AS message,
            git_commits.author_name AS author_name,
            git_commits.author_email AS author_email,
            git_commits.author_when AS author_when
        FROM git_commits
        INNER JOIN repos ON toString(repos.id) = toString(git_commits.repo_id)
        WHERE author_when >= %(start_ts)s
          AND author_when < %(end_ts)s
          AND toDayOfWeek(author_when) = %(weekday)s
          AND toHour(author_when) = %(hour)s
          AND (author_email IN %(identities)s OR author_name IN %(identities)s)
        ORDER BY author_when DESC
        LIMIT %(limit)s
    """
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "weekday": weekday,
        "hour": hour,
        "identities": list(identities),
        "limit": limit,
    }
    return await query_dicts(sink, query, params)
