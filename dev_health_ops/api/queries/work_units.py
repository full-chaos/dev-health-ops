from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .client import query_dicts


async def fetch_work_graph_edges(
    client: Any,
    *,
    repo_id: Optional[str] = None,
    repo_ids: Optional[List[str]] = None,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": int(limit)}
    filters = []
    if repo_ids:
        filters.append("repo_id IN %(repo_ids)s")
        params["repo_ids"] = repo_ids
    elif repo_id:
        filters.append("repo_id = %(repo_id)s")
        params["repo_id"] = repo_id
    where_clause = " AND ".join(filters)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
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
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_work_items(
    client: Any,
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
            created_at,
            updated_at,
            completed_at
        FROM work_items
        WHERE work_item_id IN %(work_item_ids)s
    """
    return await query_dicts(client, query, params)


async def fetch_work_item_active_hours(
    client: Any,
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
    rows = await query_dicts(client, query, params)
    return {
        str(row.get("work_item_id")): float(row.get("active_time_hours") or 0.0)
        for row in rows
    }


async def fetch_pull_requests(
    client: Any,
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
        rows.extend(await query_dicts(client, query, params))
    return rows


async def fetch_commits(
    client: Any,
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
        rows.extend(await query_dicts(client, query, params))
    return rows


async def fetch_commit_churn(
    client: Any,
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
        rows = await query_dicts(client, query, params)
        for row in rows:
            commit_hash = str(row.get("commit_hash") or "")
            churn_key = f"{repo_id}@{commit_hash}"
            churn[churn_key] = float(row.get("churn_loc") or 0.0)
    return churn
