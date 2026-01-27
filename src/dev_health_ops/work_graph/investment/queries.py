"""ClickHouse query helpers for investment materialization."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def query_dicts(
    sink: BaseMetricsSink, query: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    return sink.query_dicts(query, params)


def fetch_work_graph_edges(
    sink: BaseMetricsSink,
    *,
    repo_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    where_sql = ""
    if repo_ids:
        params["repo_ids"] = repo_ids
        where_sql = "WHERE repo_id IN %(repo_ids)s"
    query = f"""
        SELECT
            edge_id,
            source_type,
            source_id,
            target_type,
            target_id,
            edge_type,
            toString(repo_id) AS repo_id,
            provider,
            provenance,
            confidence,
            evidence
        FROM work_graph_edges
        {where_sql}
    """
    return query_dicts(sink, query, params)


def fetch_work_items(
    sink: BaseMetricsSink,
    *,
    work_item_ids: Iterable[str],
) -> List[Dict[str, Any]]:
    ids = list(dict.fromkeys(work_item_ids))
    if not ids:
        return []
    params = {"work_item_ids": ids}
    query = """
        SELECT
            work_item_id,
            provider,
            toString(repo_id) AS repo_id,
            title,
            description,
            type,
            labels,
            parent_id,
            epic_id,
            created_at,
            updated_at,
            completed_at
        FROM work_items
        WHERE work_item_id IN %(work_item_ids)s
    """
    return query_dicts(sink, query, params)


def fetch_parent_titles(
    sink: BaseMetricsSink,
    *,
    work_item_ids: Iterable[str],
) -> Dict[str, str]:
    ids = list(dict.fromkeys(work_item_ids))
    if not ids:
        return {}
    params = {"work_item_ids": ids}
    query = """
        SELECT
            work_item_id,
            title
        FROM work_items
        WHERE work_item_id IN %(work_item_ids)s
    """
    rows = query_dicts(sink, query, params)
    return {
        str(row.get("work_item_id")): str(row.get("title") or "")
        for row in rows
        if row.get("work_item_id") and row.get("title")
    }


def fetch_work_item_active_hours(
    sink: BaseMetricsSink,
    *,
    work_item_ids: Iterable[str],
) -> Dict[str, float]:
    ids = list(dict.fromkeys(work_item_ids))
    if not ids:
        return {}
    params = {"work_item_ids": ids}
    query = """
        SELECT
            work_item_id,
            argMax(active_time_hours, computed_at) AS active_time_hours
        FROM work_item_cycle_times
        WHERE work_item_id IN %(work_item_ids)s
        GROUP BY work_item_id
    """
    rows = query_dicts(sink, query, params)
    return {
        str(row.get("work_item_id")): float(row.get("active_time_hours") or 0.0)
        for row in rows
    }


def fetch_pull_requests(
    sink: BaseMetricsSink,
    *,
    repo_numbers: Dict[str, List[int]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for repo_id, numbers in repo_numbers.items():
        if not numbers:
            continue
        params = {"repo_id": repo_id, "numbers": numbers}
        query = """
            SELECT
                toString(repo_id) AS repo_id,
                number,
                title,
                body,
                created_at,
                merged_at,
                closed_at,
                additions,
                deletions
            FROM git_pull_requests
            WHERE repo_id = %(repo_id)s
              AND number IN %(numbers)s
        """
        rows.extend(query_dicts(sink, query, params))
    return rows


def fetch_commits(
    sink: BaseMetricsSink,
    *,
    repo_commits: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for repo_id, hashes in repo_commits.items():
        if not hashes:
            continue
        params = {"repo_id": repo_id, "hashes": hashes}
        query = """
            SELECT
                toString(repo_id) AS repo_id,
                hash,
                message,
                author_when,
                committer_when
            FROM git_commits
            WHERE repo_id = %(repo_id)s
              AND hash IN %(hashes)s
        """
        rows.extend(query_dicts(sink, query, params))
    return rows


def fetch_commit_churn(
    sink: BaseMetricsSink,
    *,
    repo_commits: Dict[str, List[str]],
) -> Dict[str, float]:
    churn: Dict[str, float] = {}
    for repo_id, hashes in repo_commits.items():
        if not hashes:
            continue
        params = {"repo_id": repo_id, "hashes": hashes}
        query = """
            SELECT
                commit_hash,
                sum(additions) + sum(deletions) AS churn_loc
            FROM git_commit_stats
            WHERE repo_id = %(repo_id)s
              AND commit_hash IN %(hashes)s
            GROUP BY commit_hash
        """
        rows = query_dicts(sink, query, params)
        for row in rows:
            commit_hash = str(row.get("commit_hash") or "")
            churn_key = f"{repo_id}@{commit_hash}"
            churn[churn_key] = float(row.get("churn_loc") or 0.0)
    return churn


def resolve_repo_ids_for_teams(
    sink: BaseMetricsSink,
    *,
    team_ids: Iterable[str],
) -> List[str]:
    team_list = [team_id for team_id in team_ids if team_id]
    if not team_list:
        return []
    query = """
        SELECT distinct repo_id AS id
        FROM user_metrics_daily
        WHERE team_id IN %(team_ids)s
    """
    rows = query_dicts(sink, query, {"team_ids": team_list})
    return [str(row.get("id")) for row in rows if row.get("id")]
