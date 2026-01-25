from __future__ import annotations

from typing import Any, Dict, List

from .client import query_dicts
from dev_health_ops.investment_taxonomy import SUBCATEGORIES, THEMES


async def fetch_filter_options(client: Any) -> Dict[str, List[str]]:
    options: Dict[str, List[str]] = {
        "teams": [],
        "repos": [],
        "services": [],
        "developers": [],
        "work_category": [],
        "issue_type": [],
        "flow_stage": [],
    }

    team_rows = await query_dicts(
        client,
        """
        SELECT DISTINCT value
        FROM (
            SELECT id AS value
            FROM teams
            WHERE id != ''

            UNION ALL

            SELECT team_id AS value
            FROM user_metrics_daily
            WHERE team_id != ''

            UNION ALL

            SELECT team_id AS value
            FROM work_item_user_metrics_daily
            WHERE team_id != ''
        )
        WHERE value != ''
        ORDER BY value
        """,
        {},
    )
    options["teams"] = [row["value"] for row in team_rows if row.get("value")]

    repo_rows = await query_dicts(
        client,
        "SELECT distinct repo AS value FROM repos WHERE repo != '' ORDER BY repo",
        {},
    )
    options["repos"] = [row["value"] for row in repo_rows if row.get("value")]

    dev_rows = await query_dicts(
        client,
        """
        SELECT distinct author_email AS value
        FROM user_metrics_daily
        WHERE author_email != ''
        ORDER BY author_email
        """,
        {},
    )
    options["developers"] = [row["value"] for row in dev_rows if row.get("value")]

    options["work_category"] = sorted(THEMES) + sorted(SUBCATEGORIES)

    issue_rows = await query_dicts(
        client,
        """
        SELECT distinct issue_type_norm AS value
        FROM issue_type_metrics_daily
        WHERE issue_type_norm != ''
        ORDER BY issue_type_norm
        """,
        {},
    )
    options["issue_type"] = [row["value"] for row in issue_rows if row.get("value")]

    stage_rows = await query_dicts(
        client,
        """
        SELECT distinct status AS value
        FROM work_item_state_durations_daily
        WHERE status != ''
        ORDER BY status
        """,
        {},
    )
    options["flow_stage"] = [row["value"] for row in stage_rows if row.get("value")]

    return options
