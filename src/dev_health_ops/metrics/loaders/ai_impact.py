from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from dev_health_ops.metrics.loaders.base import parse_uuid
from dev_health_ops.metrics.query_builder import OrgScopedQuery
from dev_health_ops.metrics.schemas import (
    AIImpactMetricsDailyRecord,
    AIOperatingLeverageComponents,
    AIPullRequestAttributionRow,
)


class AIImpactClickHouseLoader:
    def __init__(self, client: Any, org_id: str = "") -> None:
        self.client = client
        self.org_id = org_id
        self._scope = OrgScopedQuery(org_id)

    async def load_ai_pr_attributions(
        self,
        *,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None = None,
    ) -> list[AIPullRequestAttributionRow]:
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
        }
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = "AND pr.repo_id = {repo_id:UUID}"
        params = self._scope.inject(params)
        org_filter_attr = self._scope.filter(alias="attr")
        org_filter_pr = self._scope.filter(alias="pr")
        query = f"""
        SELECT
            pr.repo_id AS repo_id,
            pr.number AS number,
            attr.kind AS kind,
            coalesce(nullIf(wi.type, ''), 'pull_request') AS work_type,
            CAST(NULL, 'Nullable(String)') AS team_id
        FROM git_pull_requests AS pr
        INNER JOIN work_graph_issue_pr AS link
            ON link.repo_id = pr.repo_id AND link.pr_number = pr.number
        INNER JOIN ai_attribution_resolved AS attr
            ON attr.subject_type = 'pull_request'
            AND attr.subject_id = link.work_item_id
        LEFT JOIN work_items AS wi
            ON wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
        WHERE ((pr.created_at >= {{start:DateTime}} AND pr.created_at < {{end:DateTime}})
            OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {{start:DateTime}} AND pr.merged_at < {{end:DateTime}}))
          {repo_filter}
          {org_filter_pr}
          {org_filter_attr}
        UNION ALL
        SELECT
            pr.repo_id AS repo_id,
            pr.number AS number,
            attr.kind AS kind,
            'pull_request' AS work_type,
            CAST(NULL, 'Nullable(String)') AS team_id
        FROM git_pull_requests AS pr
        INNER JOIN ai_attribution_resolved AS attr
            ON attr.subject_type = 'pull_request'
            AND attr.repo_id = pr.repo_id
            AND attr.subject_id IN (toString(pr.number), concat(toString(pr.repo_id), '#', toString(pr.number)))
        WHERE ((pr.created_at >= {{start:DateTime}} AND pr.created_at < {{end:DateTime}})
            OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {{start:DateTime}} AND pr.merged_at < {{end:DateTime}}))
          {repo_filter}
          {org_filter_pr}
          {org_filter_attr}
        """
        raw_rows = await query_dicts(self.client, query, params)
        rows: list[AIPullRequestAttributionRow] = []
        for raw in raw_rows:
            parsed_repo_id = parse_uuid(raw.get("repo_id"))
            if parsed_repo_id is None:
                continue
            rows.append(
                {
                    "repo_id": parsed_repo_id,
                    "number": int(raw.get("number") or 0),
                    "kind": raw.get("kind"),
                    "work_type": raw.get("work_type"),
                    "team_id": raw.get("team_id"),
                }
            )
        return rows

    async def load_ai_impact_metrics(
        self,
        *,
        start_day: date,
        end_day: date,
        repo_id: uuid.UUID | None = None,
        team_id: str | None = None,
        work_type: str | None = None,
    ) -> list[AIImpactMetricsDailyRecord]:
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
        filters = ["day >= {start_day:Date}", "day <= {end_day:Date}"]
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            filters.append("repo_id = {repo_id:UUID}")
        if team_id is not None:
            params["team_id"] = team_id
            filters.append("team_id = {team_id:String}")
        if work_type is not None:
            params["work_type"] = work_type
            filters.append("work_type = {work_type:String}")
        params = self._scope.inject(params)
        org_filter = self._scope.filter()
        if org_filter:
            filters.append(org_filter.removeprefix("AND "))
        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            org_id,
            team_id,
            repo_id,
            work_type,
            day,
            attribution_bucket,
            argMax(prs_total, computed_at) AS prs_total,
            argMax(prs_merged, computed_at) AS prs_merged,
            argMax(ai_assisted_prs, computed_at) AS ai_assisted_prs,
            argMax(agent_created_prs, computed_at) AS agent_created_prs,
            argMax(human_prs, computed_at) AS human_prs,
            argMax(unknown_prs, computed_at) AS unknown_prs,
            argMax(ai_assisted_pr_ratio, computed_at) AS ai_assisted_pr_ratio,
            argMax(agent_created_pr_count, computed_at) AS agent_created_pr_count,
            argMax(cycle_time_avg_hours, computed_at) AS cycle_time_avg_hours,
            argMax(baseline_cycle_time_avg_hours, computed_at) AS baseline_cycle_time_avg_hours,
            argMax(ai_cycle_time_delta_hours, computed_at) AS ai_cycle_time_delta_hours,
            argMax(reviews_per_pr, computed_at) AS reviews_per_pr,
            argMax(baseline_reviews_per_pr, computed_at) AS baseline_reviews_per_pr,
            argMax(ai_review_amplification, computed_at) AS ai_review_amplification,
            argMax(changes_requested_per_pr, computed_at) AS changes_requested_per_pr,
            argMax(rework_prs, computed_at) AS rework_prs,
            argMax(rework_drag_rate, computed_at) AS rework_drag_rate,
            argMax(followup_commits_count, computed_at) AS followup_commits_count,
            argMax(revert_prs, computed_at) AS revert_prs,
            argMax(revert_rate, computed_at) AS revert_rate,
            argMax(incidents_count, computed_at) AS incidents_count,
            argMax(incident_drag_rate, computed_at) AS incident_drag_rate,
            argMax(test_gap_prs, computed_at) AS test_gap_prs,
            argMax(test_gap_rate, computed_at) AS test_gap_rate,
            argMax(leverage_prs_component, computed_at) AS leverage_prs_component,
            argMax(leverage_cycle_time_component, computed_at) AS leverage_cycle_time_component,
            argMax(leverage_review_component, computed_at) AS leverage_review_component,
            argMax(leverage_rework_component, computed_at) AS leverage_rework_component,
            argMax(leverage_test_component, computed_at) AS leverage_test_component,
            argMax(leverage_incident_component, computed_at) AS leverage_incident_component,
            max(computed_at) AS computed_at
        FROM ai_impact_metrics_daily
        WHERE {where_clause}
        GROUP BY org_id, team_id, repo_id, work_type, day, attribution_bucket
        ORDER BY day, repo_id, work_type, attribution_bucket
        """
        raw_rows = await query_dicts(self.client, query, params)
        return [_to_record(raw) for raw in raw_rows]


def _to_record(raw: dict[str, Any]) -> AIImpactMetricsDailyRecord:
    repo_id = parse_uuid(raw.get("repo_id"))
    if repo_id is None:
        raise ValueError("ai_impact_metrics_daily row has invalid repo_id")
    return AIImpactMetricsDailyRecord(
        org_id=str(raw.get("org_id") or ""),
        team_id=raw.get("team_id"),
        repo_id=repo_id,
        work_type=str(raw.get("work_type") or "pull_request"),
        day=raw["day"],
        attribution_bucket=str(raw.get("attribution_bucket") or "unknown"),
        prs_total=int(raw.get("prs_total") or 0),
        prs_merged=int(raw.get("prs_merged") or 0),
        ai_assisted_prs=int(raw.get("ai_assisted_prs") or 0),
        agent_created_prs=int(raw.get("agent_created_prs") or 0),
        human_prs=int(raw.get("human_prs") or 0),
        unknown_prs=int(raw.get("unknown_prs") or 0),
        ai_assisted_pr_ratio=raw.get("ai_assisted_pr_ratio"),
        agent_created_pr_count=int(raw.get("agent_created_pr_count") or 0),
        cycle_time_avg_hours=raw.get("cycle_time_avg_hours"),
        baseline_cycle_time_avg_hours=raw.get("baseline_cycle_time_avg_hours"),
        ai_cycle_time_delta_hours=raw.get("ai_cycle_time_delta_hours"),
        reviews_per_pr=raw.get("reviews_per_pr"),
        baseline_reviews_per_pr=raw.get("baseline_reviews_per_pr"),
        ai_review_amplification=raw.get("ai_review_amplification"),
        changes_requested_per_pr=raw.get("changes_requested_per_pr"),
        rework_prs=int(raw.get("rework_prs") or 0),
        rework_drag_rate=raw.get("rework_drag_rate"),
        followup_commits_count=int(raw.get("followup_commits_count") or 0),
        revert_prs=int(raw.get("revert_prs") or 0),
        revert_rate=raw.get("revert_rate"),
        incidents_count=int(raw.get("incidents_count") or 0),
        incident_drag_rate=raw.get("incident_drag_rate"),
        test_gap_prs=int(raw.get("test_gap_prs") or 0),
        test_gap_rate=raw.get("test_gap_rate"),
        leverage=AIOperatingLeverageComponents(
            prs_component=float(raw.get("leverage_prs_component") or 0.0),
            cycle_time_component=raw.get("leverage_cycle_time_component"),
            review_component=raw.get("leverage_review_component"),
            rework_component=raw.get("leverage_rework_component"),
            test_component=raw.get("leverage_test_component"),
            incident_component=raw.get("leverage_incident_component"),
        ),
        computed_at=raw["computed_at"],
    )
