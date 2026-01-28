"""MongoDB data loader implementation."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, date, timedelta, time
from typing import Any, Dict, List, Optional, Tuple

from dev_health_ops.metrics.loaders.base import (
    DataLoader,
    naive_utc,
    parse_uuid,
    to_dataclass,
)
from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    PullRequestRow,
    PullRequestReviewRow,
    PipelineRunRow,
    DeploymentRow,
    IncidentRow,
)
from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsIncident,
    AtlassianOpsAlert,
    AtlassianOpsSchedule,
)
from dev_health_ops.models.teams import JiraProjectOpsTeamLink


class MongoDataLoader(DataLoader):
    """DataLoader implementation for MongoDB backend."""

    def __init__(self, db: Any) -> None:
        self.db = db

    async def load_git_rows(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[CommitStatRow], List[PullRequestRow], List[PullRequestReviewRow]]:
        start_naive = naive_utc(start)
        end_naive = naive_utc(end)

        repo_filter: Dict[str, Any] = {}
        if repo_id is not None:
            repo_filter["repo_id"] = str(repo_id)
        elif repo_name is not None:
            repo_doc = self.db["repos"].find_one({"repo": repo_name}, {"id": 1})
            if repo_doc:
                repo_filter["repo_id"] = str(repo_doc["id"])

        commit_query = {
            "committer_when": {"$gte": start_naive, "$lt": end_naive},
            **repo_filter,
        }
        commits = list(self.db["git_commits"].find(commit_query))
        commit_hashes = [c["hash"] for c in commits]

        stats_query = {
            "commit_hash": {"$in": commit_hashes},
            **repo_filter,
        }
        stats = list(self.db["git_commit_stats"].find(stats_query))

        stats_by_hash: Dict[str, List[Dict[str, Any]]] = {}
        for s in stats:
            h = s["commit_hash"]
            if h not in stats_by_hash:
                stats_by_hash[h] = []
            stats_by_hash[h].append(s)

        commit_rows: List[CommitStatRow] = []
        for c in commits:
            h = c["hash"]
            repo_uuid = parse_uuid(c.get("repo_id"))
            if repo_uuid is None:
                continue

            h_stats = stats_by_hash.get(h, [{}])
            for s in h_stats:
                commit_rows.append(
                    {
                        "repo_id": repo_uuid,
                        "commit_hash": h,
                        "author_email": c.get("author_email"),
                        "author_name": c.get("author_name"),
                        "committer_when": c.get("committer_when"),
                        "file_path": s.get("file_path"),
                        "additions": int(s.get("additions") or 0),
                        "deletions": int(s.get("deletions") or 0),
                    }
                )

        pr_query = {
            "$or": [
                {"created_at": {"$gte": start_naive, "$lt": end_naive}},
                {"merged_at": {"$ne": None, "$gte": start_naive, "$lt": end_naive}},
            ],
            **repo_filter,
        }
        prs = list(self.db["git_pull_requests"].find(pr_query))

        pr_rows: List[PullRequestRow] = []
        for p in prs:
            u = parse_uuid(p.get("repo_id"))
            if u:
                pr_rows.append(
                    {
                        "repo_id": u,
                        "number": int(p.get("number") or 0),
                        "author_email": p.get("author_email"),
                        "author_name": p.get("author_name"),
                        "created_at": p.get("created_at"),
                        "merged_at": p.get("merged_at"),
                        "first_review_at": p.get("first_review_at"),
                        "first_comment_at": p.get("first_comment_at"),
                        "changes_requested_count": int(
                            p.get("changes_requested_count") or 0
                        ),
                        "reviews_count": int(p.get("reviews_count") or 0),
                        "comments_count": int(p.get("comments_count") or 0),
                        "additions": int(p.get("additions") or 0),
                        "deletions": int(p.get("deletions") or 0),
                        "changed_files": int(p.get("changed_files") or 0),
                    }
                )

        review_query = {
            "submitted_at": {"$gte": start_naive, "$lt": end_naive},
            **repo_filter,
        }
        reviews = list(self.db["git_pull_request_reviews"].find(review_query))
        review_rows: List[PullRequestReviewRow] = []
        for r in reviews:
            u = parse_uuid(r.get("repo_id"))
            if u:
                review_rows.append(
                    {
                        "repo_id": u,
                        "number": int(r.get("number") or 0),
                        "reviewer": r.get("reviewer"),
                        "submitted_at": r.get("submitted_at"),
                        "state": r.get("state"),
                    }
                )

        return commit_rows, pr_rows, review_rows

    async def load_work_items(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[Any], List[Any]]:
        from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition

        end_naive = naive_utc(end)
        start_naive = naive_utc(start)

        repo_filter: Dict[str, Any] = {}
        if repo_id is not None:
            repo_filter["repo_id"] = str(repo_id)

        item_query = {
            "created_at": {"$lt": end_naive},
            "$or": [
                {"status": {"$ne": "done"}},
                {"completed_at": {"$gte": start_naive}},
            ],
            **repo_filter,
        }
        items_raw = list(self.db["work_items"].find(item_query))

        trans_query = {
            "occurred_at": {"$lt": end_naive},
            **repo_filter,
        }
        trans_raw = list(self.db["work_item_transitions"].find(trans_query))

        items = [to_dataclass(WorkItem, i) for i in items_raw]
        transitions = [to_dataclass(WorkItemStatusTransition, t) for t in trans_raw]

        return items, transitions

    async def load_cicd_data(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[PipelineRunRow], List[DeploymentRow]]:
        sn = naive_utc(start)
        en = naive_utc(end)
        repo_filter: Dict[str, Any] = {}
        if repo_id is not None:
            repo_filter["repo_id"] = str(repo_id)

        pipe_query = {"finished_at": {"$gte": sn, "$lt": en}, **repo_filter}
        deploy_query = {"deployed_at": {"$gte": sn, "$lt": en}, **repo_filter}

        pipes_raw = list(self.db["ci_pipeline_runs"].find(pipe_query))
        deploys_raw = list(self.db["deployments"].find(deploy_query))

        pipes: List[PipelineRunRow] = [dict(p) for p in pipes_raw]  # type: ignore
        deploys: List[DeploymentRow] = [dict(d) for d in deploys_raw]  # type: ignore

        return pipes, deploys

    async def load_incidents(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> List[IncidentRow]:
        sn = naive_utc(start)
        en = naive_utc(end)
        repo_filter: Dict[str, Any] = {}
        if repo_id is not None:
            repo_filter["repo_id"] = str(repo_id)

        query = {"started_at": {"$gte": sn, "$lt": en}, **repo_filter}
        items_raw = list(self.db["incidents"].find(query))
        return [dict(i) for i in items_raw]  # type: ignore

    async def load_blame_concentration(
        self,
        repo_id: uuid.UUID,
        as_of: datetime,
    ) -> Dict[uuid.UUID, float]:
        pipeline = [
            {"$match": {"repo_id": str(repo_id)}},
            {
                "$group": {
                    "_id": "$repo_id",
                    "total_lines": {"$sum": "$lines_count"},
                    "sum_sq": {"$sum": {"$multiply": ["$lines_count", "$lines_count"]}},
                }
            },
            {
                "$project": {
                    "concentration": {
                        "$divide": [
                            "$sum_sq",
                            {"$multiply": ["$total_lines", "$total_lines"]},
                        ]
                    }
                }
            },
        ]
        results = list(self.db["git_file_blame"].aggregate(pipeline))
        if not results:
            return {}
        return {repo_id: float(results[0]["concentration"])}

    async def load_atlassian_ops_incidents(
        self,
        start: datetime,
        end: datetime,
    ) -> List[AtlassianOpsIncident]:
        sn = naive_utc(start)
        en = naive_utc(end)

        query = {"created_at": {"$gte": sn, "$lt": en}}
        items_raw = list(self.db["atlassian_ops_incidents"].find(query))

        incidents: List[AtlassianOpsIncident] = []
        for item in items_raw:
            incidents.append(
                AtlassianOpsIncident(
                    id=item.get("id", ""),
                    url=item.get("url"),
                    summary=item.get("summary", ""),
                    description=item.get("description"),
                    status=item.get("status", ""),
                    severity=item.get("severity", ""),
                    created_at=item.get("created_at") or datetime.now(timezone.utc),
                    provider_id=item.get("provider_id"),
                    last_synced=item.get("last_synced") or datetime.now(timezone.utc),
                )
            )
        return incidents

    async def load_atlassian_ops_alerts(
        self,
        start: datetime,
        end: datetime,
    ) -> List[AtlassianOpsAlert]:
        sn = naive_utc(start)
        en = naive_utc(end)

        query = {"created_at": {"$gte": sn, "$lt": en}}
        items_raw = list(self.db["atlassian_ops_alerts"].find(query))

        alerts: List[AtlassianOpsAlert] = []
        for item in items_raw:
            alerts.append(
                AtlassianOpsAlert(
                    id=item.get("id", ""),
                    status=item.get("status", ""),
                    priority=item.get("priority", ""),
                    created_at=item.get("created_at") or datetime.now(timezone.utc),
                    acknowledged_at=item.get("acknowledged_at"),
                    snoozed_at=item.get("snoozed_at"),
                    closed_at=item.get("closed_at"),
                    last_synced=item.get("last_synced") or datetime.now(timezone.utc),
                )
            )
        return alerts

    async def load_atlassian_ops_schedules(
        self,
    ) -> List[AtlassianOpsSchedule]:
        items_raw = list(self.db["atlassian_ops_schedules"].find({}))

        schedules: List[AtlassianOpsSchedule] = []
        for item in items_raw:
            schedules.append(
                AtlassianOpsSchedule(
                    id=item.get("id", ""),
                    name=item.get("name", ""),
                    timezone=item.get("timezone"),
                    last_synced=item.get("last_synced") or datetime.now(timezone.utc),
                )
            )
        return schedules

    async def load_jira_project_ops_team_links(
        self,
    ) -> List[JiraProjectOpsTeamLink]:
        items_raw = list(self.db["jira_project_ops_team_links"].find({}))

        links: List[JiraProjectOpsTeamLink] = []
        for item in items_raw:
            links.append(
                JiraProjectOpsTeamLink(
                    project_key=item.get("project_key", ""),
                    ops_team_id=item.get("ops_team_id", ""),
                    project_name=item.get("project_name", ""),
                    ops_team_name=item.get("ops_team_name", ""),
                    updated_at=item.get("updated_at") or datetime.now(timezone.utc),
                )
            )
        return links

    async def load_user_metrics_rolling_30d(
        self,
        as_of: date,
    ) -> List[Dict[str, Any]]:
        start = datetime.combine(
            as_of - timedelta(days=29), time.min, tzinfo=timezone.utc
        )
        end = datetime.combine(as_of, time.max, tzinfo=timezone.utc)

        pipeline = [
            {"$match": {"day": {"$gte": start, "$lte": end}}},
            {
                "$group": {
                    "_id": "$identity_id",
                    "team_id": {"$first": "$team_id"},
                    "churn_loc_30d": {"$sum": "$loc_touched"},
                    "delivery_units_30d": {"$sum": "$delivery_units"},
                    "cycle_p50_30d_hours": {"$avg": "$cycle_p50_hours"},
                    "wip_max_30d": {"$max": "$work_items_active"},
                }
            },
            {
                "$project": {
                    "identity_id": "$_id",
                    "team_id": 1,
                    "churn_loc_30d": 1,
                    "delivery_units_30d": 1,
                    "cycle_p50_30d_hours": 1,
                    "wip_max_30d": 1,
                }
            },
        ]
        return list(self.db["user_metrics_daily"].aggregate(pipeline))
