from __future__ import annotations

from typing import Any

from .client import query_dicts
from .investment import PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE


async def fetch_pull_request(
    client: Any,
    *,
    repo_id: str,
    number: int,
    org_id: str = "",
) -> dict[str, Any] | None:
    # CHAOS-2397: scope on git_pull_requests.org_id directly. The previous
    # `INNER JOIN repos ... WHERE repos.org_id` leaked across tenants because
    # repos.id is duplicated across orgs, so a shared repo_id crossed the
    # requester's repos row with another tenant's PR sharing (repo_id, number).
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
        WHERE org_id = %(org_id)s
          AND repo_id = %(repo_id)s
          AND number = %(number)s
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
    # CHAOS-2397: scope on git_pull_request_reviews.org_id directly (see
    # fetch_pull_request) — the repos join leaked across tenants for shared
    # repo_ids.
    query = """
        SELECT
            review_id,
            reviewer,
            state,
            submitted_at
        FROM git_pull_request_reviews
        WHERE org_id = %(org_id)s
          AND repo_id = %(repo_id)s
          AND number = %(number)s
          AND submitted_at IS NOT NULL
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
    query = f"""
        SELECT
            wct.work_item_id,
            wct.provider,
            wct.type,
            wct.status,
            wct.created_at,
            wct.started_at,
            wct.completed_at,
            nullIf(t.team_id, '') AS team_id,
            wct.work_scope_id
        FROM work_item_cycle_times AS wct FINAL
        LEFT JOIN {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE} AS t
          ON t.work_item_id = wct.work_item_id
        WHERE wct.work_item_id = %(work_item_id)s
          AND wct.org_id = %(org_id)s
        ORDER BY wct.day DESC
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
    # Scope directly on deployments.org_id (added to the table + sort key in
    # migration 027). The previous `INNER JOIN repos ON repos.id =
    # deployments.repo_id ... WHERE repos.org_id = %(org_id)s` leaked across
    # tenants: repos.id is duplicated across orgs, so for a shared repo_id the
    # join crossed the requester's repos row with ANOTHER org's deployment row
    # and `LIMIT 1` (no ORDER BY) could return that other tenant's deployment
    # (CHAOS-2397). ORDER BY last_synced DESC also makes the ReplacingMergeTree
    # read pick the latest version deterministically.
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
        WHERE org_id = %(org_id)s
          AND repo_id = %(repo_id)s
          AND deployment_id = %(deployment_id)s
        ORDER BY last_synced DESC
        LIMIT 1
    """
    rows = await query_dicts(
        client,
        query,
        {"repo_id": repo_id, "deployment_id": deployment_id, "org_id": org_id},
    )
    return rows[0] if rows else None
