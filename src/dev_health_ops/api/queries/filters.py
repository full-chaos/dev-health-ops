from __future__ import annotations

import asyncio

from typing import Any

from dev_health_ops.investment_taxonomy import SUBCATEGORIES, THEMES

from .client import query_dicts


async def fetch_filter_options(
    client: Any, *, org_id: str = ""
) -> dict[str, list[str]]:
    options: dict[str, list[str]] = {
        "teams": [],
        "repos": [],
        "services": [],
        "developers": [],
        "work_category": [],
        "issue_type": [],
        "flow_stage": [],
    }

    # -- build query coroutines ------------------------------------------------
    team_coro = query_dicts(
        client,
        """
        SELECT DISTINCT value
        FROM (
            SELECT id AS value
            FROM teams FINAL
            WHERE id != '' AND is_active = 1
              AND org_id = %(org_id)s

            UNION ALL

            SELECT team_id AS value
            FROM user_metrics_daily
            WHERE team_id != ''
              AND org_id = %(org_id)s

            UNION ALL

            SELECT team_id AS value
            FROM work_item_user_metrics_daily
            WHERE team_id != ''
              AND org_id = %(org_id)s
        )
        WHERE value != ''
        ORDER BY value
        """,
        {"org_id": org_id},
    )

    repo_coro = query_dicts(
        client,
        "SELECT distinct repo AS value FROM repos WHERE repo != '' AND org_id = %(org_id)s ORDER BY repo",
        {"org_id": org_id},
    )

    dev_coro = query_dicts(
        client,
        """
        SELECT distinct author_email AS value
        FROM user_metrics_daily
        WHERE author_email != ''
          AND org_id = %(org_id)s
        ORDER BY author_email
        """,
        {"org_id": org_id},
    )

    issue_coro = query_dicts(
        client,
        """
        SELECT distinct issue_type_norm AS value
        FROM issue_type_metrics_daily
        WHERE issue_type_norm != ''
          AND org_id = %(org_id)s
        ORDER BY issue_type_norm
        """,
        {"org_id": org_id},
    )

    stage_coro = query_dicts(
        client,
        """
        SELECT distinct status AS value
        FROM work_item_state_durations_daily
        WHERE status != ''
          AND org_id = %(org_id)s
        ORDER BY status
        """,
        {"org_id": org_id},
    )

    # -- run all five queries in parallel ---------------------------------------
    team_rows, repo_rows, dev_rows, issue_rows, stage_rows = await asyncio.gather(
        team_coro, repo_coro, dev_coro, issue_coro, stage_coro,
    )

    options["teams"] = [row["value"] for row in team_rows if row.get("value")]
    options["repos"] = [row["value"] for row in repo_rows if row.get("value")]
    options["developers"] = [row["value"] for row in dev_rows if row.get("value")]
    options["work_category"] = sorted(THEMES) + sorted(SUBCATEGORIES)
    options["issue_type"] = [row["value"] for row in issue_rows if row.get("value")]
    options["flow_stage"] = [row["value"] for row in stage_rows if row.get("value")]

    return options
