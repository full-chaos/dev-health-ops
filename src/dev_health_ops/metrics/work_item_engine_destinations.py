"""Config-engine-derived daily work-item destinations.

This is the production computation used by ``job_work_items`` and by the
Python-Go differential oracle. Keeping it here prevents the oracle from
reimplementing the former inline block and agreeing with a transcription that
the worker itself never executes.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from dev_health_ops.analytics.investment import InvestmentClassifier
from dev_health_ops.metrics.compute_work_items import (
    TeamAttributionContext,
    resolve_team_attribution,
)
from dev_health_ops.metrics.schemas import (
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemType
from dev_health_ops.providers.status_mapping import StatusMapping
from dev_health_ops.providers.teams import (
    LinkedIssueTeamResolver,
    ProjectKeyTeamResolver,
    TeamResolver,
    normalize_team_id,
)
from dev_health_ops.utils.datetime import to_utc


def compute_work_item_engine_destinations_daily(
    *,
    day: date,
    work_items: Sequence[WorkItem],
    computed_at: datetime,
    org_id: str,
    status_mapping: StatusMapping,
    investment_classifier: InvestmentClassifier,
    team_resolver: TeamResolver | None = None,
    project_key_resolver: ProjectKeyTeamResolver | None = None,
    linked_issue_resolver: LinkedIssueTeamResolver | None = None,
    attribution_context: TeamAttributionContext | None = None,
) -> tuple[
    list[IssueTypeMetricsRecord],
    list[InvestmentClassificationRecord],
    list[InvestmentMetricsRecord],
]:
    """Compute the three config-engine surfaces for exactly one UTC day."""

    issue_type_stats: dict[
        tuple[uuid.UUID, Any, str, WorkItemType], dict[str, Any]
    ] = {}

    def _get_team(work_item: WorkItem) -> str:
        team_id, _, _ = resolve_team_attribution(
            work_item,
            team_resolver,
            project_key_resolver,
            linked_issue_resolver=linked_issue_resolver,
            attribution_context=attribution_context,
        )
        return normalize_team_id(team_id)

    def _normalize_investment_team_id(team_id: str | None) -> str | None:
        if not team_id or team_id == "unassigned":
            return None
        return team_id

    start_dt = to_utc(datetime.combine(day, time.min, tzinfo=timezone.utc))
    end_dt = start_dt + timedelta(days=1)
    for item in work_items:
        repo_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
        provider = item.provider
        team_id = _get_team(item)
        normalized_type = status_mapping.normalize_type(
            provider=provider,
            type_raw=item.type,
            labels=getattr(item, "labels", []),
        )

        key = (repo_id, provider, team_id, normalized_type)
        if key not in issue_type_stats:
            issue_type_stats[key] = {
                "created": 0,
                "completed": 0,
                "active": 0,
                "cycle_hours": [],
            }

        stats = issue_type_stats[key]
        created = to_utc(item.created_at)
        if start_dt <= created < end_dt:
            stats["created"] += 1

        if item.completed_at:
            completed = to_utc(item.completed_at)
            if start_dt <= completed < end_dt:
                stats["completed"] += 1
                if item.started_at:
                    started = to_utc(item.started_at)
                    hours = (completed - started).total_seconds() / 3600.0
                    if hours >= 0:
                        stats["cycle_hours"].append(hours)

        if created < end_dt and (
            not item.completed_at or to_utc(item.completed_at) >= start_dt
        ):
            stats["active"] += 1

    issue_type_metrics_rows: list[IssueTypeMetricsRecord] = []
    for (
        repo_id,
        provider,
        team_id,
        normalized_type,
    ), stats in issue_type_stats.items():
        cycles = sorted(stats["cycle_hours"])
        p50 = cycles[len(cycles) // 2] if cycles else 0.0
        p90 = cycles[int(len(cycles) * 0.9)] if cycles else 0.0
        issue_type_metrics_rows.append(
            IssueTypeMetricsRecord(
                repo_id=repo_id if repo_id.int != 0 else None,
                day=day,
                provider=provider,
                team_id=team_id,
                issue_type_norm=normalized_type,
                created_count=stats["created"],
                completed_count=stats["completed"],
                active_count=stats["active"],
                cycle_p50_hours=p50,
                cycle_p90_hours=p90,
                lead_p50_hours=0.0,
                computed_at=computed_at,
                org_id=org_id,
            )
        )

    investment_classifications: list[InvestmentClassificationRecord] = []
    investment_metrics: dict[tuple[Any, str, str, str], dict[str, Any]] = {}

    for item in work_items:
        repo_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
        created = to_utc(item.created_at)
        if not (
            created < end_dt
            and (not item.completed_at or to_utc(item.completed_at) >= start_dt)
        ):
            continue

        classification = investment_classifier.classify(
            {
                "labels": getattr(item, "labels", []),
                "component": getattr(item, "component", ""),
                "title": item.title,
                "provider": item.provider,
            }
        )

        investment_classifications.append(
            InvestmentClassificationRecord(
                repo_id=repo_id if repo_id.int != 0 else None,
                day=day,
                artifact_type="work_item",
                artifact_id=item.work_item_id,
                provider=item.provider,
                investment_area=classification.investment_area,
                project_stream=classification.project_stream or "",
                confidence=classification.confidence,
                rule_id=classification.rule_id,
                computed_at=computed_at,
                org_id=org_id,
            )
        )

        if item.completed_at:
            completed = to_utc(item.completed_at)
            if not (start_dt <= completed < end_dt):
                continue
            team_id_value = _normalize_investment_team_id(_get_team(item)) or ""
            investment_key = (
                repo_id,
                team_id_value,
                classification.investment_area,
                classification.project_stream or "",
            )
            if investment_key not in investment_metrics:
                investment_metrics[investment_key] = {
                    "units": 0,
                    "completed": 0,
                    "churn": 0,
                    "cycles": [],
                }
            investment_metrics[investment_key]["completed"] += 1
            points = getattr(item, "story_points", 1) or 1
            investment_metrics[investment_key]["units"] += int(points)
            if item.started_at:
                started = to_utc(item.started_at)
                hours = (completed - started).total_seconds() / 3600.0
                if hours >= 0:
                    investment_metrics[investment_key]["cycles"].append(hours)

    investment_metrics_rows: list[InvestmentMetricsRecord] = []
    for (repo_id, team_id, area, stream), data in investment_metrics.items():
        cycles = sorted(data["cycles"])
        p50 = cycles[len(cycles) // 2] if cycles else 0.0
        investment_metrics_rows.append(
            InvestmentMetricsRecord(
                repo_id=repo_id if repo_id.int != 0 else None,
                day=day,
                team_id=team_id,
                investment_area=area,
                project_stream=stream,
                delivery_units=data["units"],
                work_items_completed=data["completed"],
                prs_merged=0,
                churn_loc=data["churn"],
                cycle_p50_hours=p50,
                computed_at=computed_at,
                org_id=org_id,
            )
        )

    return (
        issue_type_metrics_rows,
        investment_classifications,
        investment_metrics_rows,
    )
