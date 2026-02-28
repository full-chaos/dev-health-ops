from __future__ import annotations

from typing import Any

from .client import query_dicts


async def fetch_pull_request(
    client: Any,
    *,
    repo_id: str,
    number: int,
    org_id: str = "",
) -> dict[str, Any] | None:
    query = """
        SELECT
            repo_id,
            number,
            title,
            state,
            created_at,
            first_review_at,
            merged_at,
            closed_at
        FROM git_pull_requests
        INNER JOIN repos ON toString(repos.id) = toString(git_pull_requests.repo_id)
        WHERE repo_id = %(repo_id)s
          AND number = %(number)s
          AND repos.org_id = %(org_id)s
        LIMIT 1
    """
    rows = await query_dicts(
        client,
        query,
        {"repo_id": repo_id, "number": number, "org_id": org_id},
    )
    return rows[0] if rows else None


async def fetch_pull_request_reviews(
    client: Any,
    *,
    repo_id: str,
    number: int,
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = """
        SELECT
            review_id,
            reviewer,
            state,
            submitted_at
        FROM git_pull_request_reviews
        INNER JOIN repos ON toString(repos.id) = toString(git_pull_request_reviews.repo_id)
        WHERE repo_id = %(repo_id)s
          AND number = %(number)s
          AND submitted_at IS NOT NULL
          AND repos.org_id = %(org_id)s
        ORDER BY submitted_at
    """
    return await query_dicts(
        client,
        query,
        {"repo_id": repo_id, "number": number, "org_id": org_id},
    )


async def fetch_issue(
    client: Any,
    *,
    work_item_id: str,
    org_id: str = "",
) -> dict[str, Any] | None:
    query = """
        SELECT
            work_item_id,
            provider,
            type,
            status,
            created_at,
            started_at,
            completed_at,
            team_id,
            work_scope_id
        FROM work_item_cycle_times
        WHERE work_item_id = %(work_item_id)s
          AND org_id = %(org_id)s
        LIMIT 1
    """
    params = {"work_item_id": work_item_id}
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    return rows[0] if rows else None


async def fetch_deployment(
    client: Any,
    *,
    repo_id: str,
    deployment_id: str,
    org_id: str = "",
) -> dict[str, Any] | None:
    query = """
        SELECT
            repo_id,
            deployment_id,
            status,
            environment,
            started_at,
            finished_at,
            deployed_at,
            merged_at
        FROM deployments
        INNER JOIN repos ON toString(repos.id) = toString(deployments.repo_id)
        WHERE repo_id = %(repo_id)s
          AND deployment_id = %(deployment_id)s
          AND repos.org_id = %(org_id)s
        LIMIT 1
    """
    rows = await query_dicts(
        client,
        query,
        {"repo_id": repo_id, "deployment_id": deployment_id, "org_id": org_id},
    )
    return rows[0] if rows else None
