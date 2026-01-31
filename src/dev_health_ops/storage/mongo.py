from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import ConfigurationError

from dev_health_ops.models.git import (
    CiPipelineRun,
    Deployment,
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
    GitPullRequestReview,
    Incident,
    Repo,
)
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemDependency,
    WorkItemStatusTransition,
)  # noqa: F401

from dev_health_ops.metrics.schemas import FileComplexitySnapshot
from dev_health_ops.metrics.schemas import WorkItemUserMetricsDailyRecord

from .utils import (
    _parse_date_value,
    _parse_datetime_value,
    _serialize_value,
    model_to_dict,
)

if TYPE_CHECKING:
    from dev_health_ops.models.atlassian_ops import (
        AtlassianOpsAlert,
        AtlassianOpsIncident,
        AtlassianOpsSchedule,
    )
    from dev_health_ops.models.teams import JiraProjectOpsTeamLink, Team


class MongoStore:
    """Async storage implementation backed by MongoDB (via Motor)."""

    if TYPE_CHECKING:
        _normalize_uuid: Callable[[Any], Any]
        _normalize_datetime: Callable[[Any], Any]

    def __init__(self, conn_string: str, db_name: Optional[str] = None) -> None:
        if not conn_string:
            raise ValueError("MongoDB connection string is required")
        self.client = AsyncIOMotorClient(conn_string)
        self.db_name = db_name
        self.db = None

    async def __aenter__(self) -> "MongoStore":
        if self.db_name:
            self.db = self.client[self.db_name]
        else:
            try:
                default_db = self.client.get_default_database()
                self.db = (
                    default_db if default_db is not None else self.client["mergestat"]
                )
            except ConfigurationError:
                raise ValueError(
                    "No default database specified. Please provide a database name "
                    "either via the MONGO_DB_NAME environment variable or include it "
                    "in your MongoDB connection string (e.g., 'mongodb://localhost:27017/mydb')"
                )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.client.close()

    async def insert_repo(self, repo: Repo) -> None:
        assert self.db is not None
        doc = model_to_dict(repo)
        doc["_id"] = doc["id"]
        await self.db["repos"].update_one(
            {"_id": doc["_id"]}, {"$set": doc}, upsert=True
        )

    async def get_all_repos(self) -> List[Repo]:
        assert self.db is not None
        cursor = self.db["repos"].find({})
        repos = []
        async for doc in cursor:
            # Basic reconstruction
            r_id = uuid.UUID(doc["_id"]) if isinstance(doc["_id"], str) else doc["_id"]
            repos.append(Repo(id=r_id, repo=doc.get("repo", "")))
        return repos

    async def get_complexity_snapshots(
        self,
        *,
        as_of_day: date,
        repo_id: Optional[uuid.UUID] = None,
        repo_name: Optional[str] = None,
    ) -> List["FileComplexitySnapshot"]:
        assert self.db is not None
        from dev_health_ops.metrics.schemas import FileComplexitySnapshot

        as_of_dt = datetime(
            as_of_day.year, as_of_day.month, as_of_day.day, tzinfo=timezone.utc
        )
        query: Dict[str, Any] = {"as_of_day": {"$lte": as_of_dt}}

        resolved_repo_id = repo_id
        if resolved_repo_id is None and repo_name:
            repo_doc = await self.db["repos"].find_one(
                {"repo": repo_name}, {"id": 1, "_id": 1}
            )
            if not repo_doc:
                return []
            resolved_repo_id = uuid.UUID(str(repo_doc.get("id") or repo_doc.get("_id")))

        if resolved_repo_id is not None:
            query["repo_id"] = str(resolved_repo_id)

        if resolved_repo_id is not None:
            max_doc = await self.db["file_complexity_snapshots"].find_one(
                query,
                sort=[("as_of_day", -1)],
                projection={"as_of_day": 1},
            )
            if not max_doc:
                return []
            query["as_of_day"] = max_doc["as_of_day"]
            cursor = self.db["file_complexity_snapshots"].find(query)
        else:
            pipeline = [
                {"$match": {"as_of_day": {"$lte": as_of_dt}}},
                {"$group": {"_id": "$repo_id", "max_day": {"$max": "$as_of_day"}}},
            ]
            latest_days = [
                d
                async for d in self.db["file_complexity_snapshots"].aggregate(pipeline)
            ]
            if not latest_days:
                return []
            or_clauses = [
                {"repo_id": d["_id"], "as_of_day": d["max_day"]} for d in latest_days
            ]
            cursor = self.db["file_complexity_snapshots"].find({"$or": or_clauses})

        docs = [doc async for doc in cursor]
        snapshots: List[FileComplexitySnapshot] = []
        for doc in docs:
            file_path = doc.get("file_path")
            if not file_path:
                continue
            repo_id_raw = doc.get("repo_id")
            if not repo_id_raw:
                continue
            r_id = uuid.UUID(str(repo_id_raw))
            as_of_day_val = _parse_date_value(doc.get("as_of_day"))
            if as_of_day_val is None:
                continue
            computed_at_val = _parse_datetime_value(
                doc.get("computed_at")
            ) or datetime.now(timezone.utc)
            snapshots.append(
                FileComplexitySnapshot(
                    repo_id=r_id,
                    as_of_day=as_of_day_val,
                    ref=str(doc.get("ref") or ""),
                    file_path=str(file_path),
                    language=str(doc.get("language") or ""),
                    loc=int(doc.get("loc") or 0),
                    functions_count=int(doc.get("functions_count") or 0),
                    cyclomatic_total=int(doc.get("cyclomatic_total") or 0),
                    cyclomatic_avg=float(doc.get("cyclomatic_avg") or 0.0),
                    high_complexity_functions=int(
                        doc.get("high_complexity_functions") or 0
                    ),
                    very_high_complexity_functions=int(
                        doc.get("very_high_complexity_functions") or 0
                    ),
                    computed_at=computed_at_val,
                )
            )
        return snapshots

    async def get_work_item_user_metrics_daily(
        self,
        *,
        day: date,
        provider: Optional[str] = None,
    ) -> List["WorkItemUserMetricsDailyRecord"]:
        assert self.db is not None
        from dev_health_ops.metrics.schemas import WorkItemUserMetricsDailyRecord

        # Mongo sink stores day as naive UTC datetime at midnight.
        day_dt = datetime(day.year, day.month, day.day)
        query: Dict[str, Any] = {"day": day_dt}
        if provider:
            query["provider"] = provider

        cursor = self.db["work_item_user_metrics_daily"].find(query)

        out: List[WorkItemUserMetricsDailyRecord] = []
        async for doc in cursor:
            day_val = _parse_date_value(doc.get("day"))
            if day_val is None:
                continue
            user_identity = str(doc.get("user_identity") or "")
            if not user_identity:
                continue
            computed_at_val = _parse_datetime_value(
                doc.get("computed_at")
            ) or datetime.now(timezone.utc)
            out.append(
                WorkItemUserMetricsDailyRecord(
                    day=day_val,
                    provider=str(doc.get("provider") or ""),
                    work_scope_id=str(doc.get("work_scope_id") or ""),
                    user_identity=user_identity,
                    team_id=str(doc.get("team_id"))
                    if doc.get("team_id") is not None
                    else None,
                    team_name=str(doc.get("team_name"))
                    if doc.get("team_name") is not None
                    else None,
                    items_started=int(doc.get("items_started") or 0),
                    items_completed=int(doc.get("items_completed") or 0),
                    wip_count_end_of_day=int(doc.get("wip_count_end_of_day") or 0),
                    cycle_time_p50_hours=float(doc.get("cycle_time_p50_hours"))
                    if doc.get("cycle_time_p50_hours") is not None
                    else None,
                    cycle_time_p90_hours=float(doc.get("cycle_time_p90_hours"))
                    if doc.get("cycle_time_p90_hours") is not None
                    else None,
                    computed_at=computed_at_val,
                )
            )
        return out

    async def has_any_git_files(self, repo_id) -> bool:
        assert self.db is not None
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_files"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        assert self.db is not None
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_commit_stats"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def has_any_git_blame(self, repo_id) -> bool:
        assert self.db is not None
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_blame"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        await self._upsert_many(
            "git_files",
            file_data,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'path')}",
        )

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        await self._upsert_many(
            "git_commits",
            commit_data,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'hash')}",
        )

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        await self._upsert_many(
            "git_commit_stats",
            commit_stats,
            lambda obj: (
                f"{getattr(obj, 'repo_id')}:"
                f"{getattr(obj, 'commit_hash')}:"
                f"{getattr(obj, 'file_path')}"
            ),
        )

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        await self._upsert_many(
            "git_blame",
            data_batch,
            lambda obj: (
                f"{getattr(obj, 'repo_id')}:"
                f"{getattr(obj, 'path')}:"
                f"{getattr(obj, 'line_no')}"
            ),
        )

    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        await self._upsert_many(
            "git_pull_requests",
            pr_data,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'number')}",
        )

    async def insert_git_pull_request_reviews(
        self, review_data: List[GitPullRequestReview]
    ) -> None:
        await self._upsert_many(
            "git_pull_request_reviews",
            review_data,
            lambda obj: (
                f"{getattr(obj, 'repo_id')}:{getattr(obj, 'number')}:{getattr(obj, 'review_id')}"
            ),
        )

    async def insert_ci_pipeline_runs(self, runs: List[CiPipelineRun]) -> None:
        if not runs:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in runs:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "run_id": item.get("run_id"),
                        "status": item.get("status"),
                        "queued_at": self._normalize_datetime(item.get("queued_at")),
                        "started_at": self._normalize_datetime(item.get("started_at")),
                        "finished_at": self._normalize_datetime(
                            item.get("finished_at")
                        ),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "run_id": getattr(item, "run_id"),
                        "status": getattr(item, "status"),
                        "queued_at": self._normalize_datetime(
                            getattr(item, "queued_at", None)
                        ),
                        "started_at": self._normalize_datetime(
                            getattr(item, "started_at")
                        ),
                        "finished_at": self._normalize_datetime(
                            getattr(item, "finished_at", None)
                        ),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "ci_pipeline_runs",
            [
                "repo_id",
                "run_id",
                "status",
                "queued_at",
                "started_at",
                "finished_at",
                "last_synced",
            ],
            rows,
        )

    async def insert_deployments(self, deployments: List[Deployment]) -> None:
        if not deployments:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in deployments:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "deployment_id": item.get("deployment_id"),
                        "status": item.get("status"),
                        "environment": item.get("environment"),
                        "started_at": self._normalize_datetime(item.get("started_at")),
                        "finished_at": self._normalize_datetime(
                            item.get("finished_at")
                        ),
                        "deployed_at": self._normalize_datetime(
                            item.get("deployed_at")
                        ),
                        "merged_at": self._normalize_datetime(item.get("merged_at")),
                        "pull_request_number": item.get("pull_request_number"),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "deployment_id": getattr(item, "deployment_id"),
                        "status": getattr(item, "status"),
                        "environment": getattr(item, "environment", None),
                        "started_at": self._normalize_datetime(
                            getattr(item, "started_at", None)
                        ),
                        "finished_at": self._normalize_datetime(
                            getattr(item, "finished_at", None)
                        ),
                        "deployed_at": self._normalize_datetime(
                            getattr(item, "deployed_at", None)
                        ),
                        "merged_at": self._normalize_datetime(
                            getattr(item, "merged_at", None)
                        ),
                        "pull_request_number": getattr(
                            item, "pull_request_number", None
                        ),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "deployments",
            [
                "repo_id",
                "deployment_id",
                "status",
                "environment",
                "started_at",
                "finished_at",
                "deployed_at",
                "merged_at",
                "pull_request_number",
                "last_synced",
            ],
            rows,
        )

    async def insert_incidents(self, incidents: List[Incident]) -> None:
        await self._upsert_many(
            "incidents",
            incidents,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'incident_id')}",
        )

    async def insert_teams(self, teams: List["Team"]) -> None:
        await self._upsert_many(
            "teams",
            teams,
            lambda obj: str(getattr(obj, "id")),
        )

    async def insert_jira_project_ops_team_links(
        self, links: List[JiraProjectOpsTeamLink]
    ) -> None:
        await self._upsert_many(
            "jira_project_ops_team_links",
            links,
            lambda obj: f"{getattr(obj, 'project_key')}:{getattr(obj, 'ops_team_id')}",
        )

    async def insert_atlassian_ops_incidents(
        self, incidents: List[AtlassianOpsIncident]
    ) -> None:
        await self._upsert_many(
            "atlassian_ops_incidents",
            incidents,
            lambda obj: str(getattr(obj, "id")),
        )

    async def insert_atlassian_ops_alerts(
        self, alerts: List[AtlassianOpsAlert]
    ) -> None:
        await self._upsert_many(
            "atlassian_ops_alerts",
            alerts,
            lambda obj: str(getattr(obj, "id")),
        )

    async def insert_atlassian_ops_schedules(
        self, schedules: List[AtlassianOpsSchedule]
    ) -> None:
        await self._upsert_many(
            "atlassian_ops_schedules",
            schedules,
            lambda obj: str(getattr(obj, "id")),
        )

    async def insert_work_item_dependencies(
        self, dependencies: List[WorkItemDependency]
    ) -> None:
        if not dependencies:
            return

        # We use _upsert_many if possible, but ClickHouseStore._upsert_many is for Mongo?
        # No, ClickHouseStore in storage.py does NOT have _upsert_many. MongoStore has.
        # ClickHouseStore has _insert_rows.

        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in dependencies:
            if isinstance(item, dict):
                rows.append(
                    {
                        "source_work_item_id": item.get("source_work_item_id"),
                        "target_work_item_id": item.get("target_work_item_id"),
                        "relationship_type": item.get("relationship_type"),
                        "relationship_type_raw": item.get("relationship_type_raw"),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "source_work_item_id": getattr(item, "source_work_item_id"),
                        "target_work_item_id": getattr(item, "target_work_item_id"),
                        "relationship_type": getattr(item, "relationship_type"),
                        "relationship_type_raw": getattr(item, "relationship_type_raw"),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "work_item_dependencies",
            [
                "source_work_item_id",
                "target_work_item_id",
                "relationship_type",
                "relationship_type_raw",
                "last_synced",
            ],
            rows,
        )

    async def insert_work_graph_pr_commit(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        # work_graph_pr_commit schema:
        # repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced

        columns = [
            "repo_id",
            "pr_number",
            "commit_hash",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
        ]

        rows: List[Dict[str, Any]] = []
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))

        for item in records:
            # item is expected to be a dict
            rows.append(
                {
                    "repo_id": self._normalize_uuid(item.get("repo_id")),
                    "pr_number": int(item.get("pr_number") or 0),
                    "commit_hash": item.get("commit_hash"),
                    "confidence": float(item.get("confidence") or 1.0),
                    "provenance": item.get("provenance"),
                    "evidence": item.get("evidence"),
                    "last_synced": self._normalize_datetime(
                        item.get("last_synced") or synced_at_default
                    ),
                }
            )

        await self._insert_rows("work_graph_pr_commit", columns, rows)

    async def get_all_teams(self) -> List["Team"]:
        from dev_health_ops.models.teams import Team

        assert self.db is not None
        cursor = self.db["teams"].find({})
        teams = []
        async for doc in cursor:
            teams.append(
                Team(
                    id=doc["id"],
                    team_uuid=doc.get("team_uuid"),
                    name=doc["name"],
                    description=doc.get("description"),
                    members=doc.get("members", []),
                    updated_at=doc["updated_at"],
                )
            )
        return teams

    async def get_jira_project_ops_team_links(self) -> List["JiraProjectOpsTeamLink"]:
        from dev_health_ops.models.teams import JiraProjectOpsTeamLink

        assert self.db is not None
        cursor = self.db["jira_project_ops_team_links"].find({})
        links = []
        async for doc in cursor:
            links.append(
                JiraProjectOpsTeamLink(
                    project_key=doc["project_key"],
                    ops_team_id=doc["ops_team_id"],
                    project_name=doc["project_name"],
                    ops_team_name=doc["ops_team_name"],
                    updated_at=doc["updated_at"],
                )
            )
        return links

    async def _upsert_many(
        self,
        collection: str,
        payload: Iterable[Any],
        id_builder: Callable[[Any], str],
    ) -> None:
        assert self.db is not None
        docs = []
        for item in payload:
            doc = model_to_dict(item) if not isinstance(item, dict) else dict(item)
            doc["_id"] = id_builder(item)
            docs.append(doc)

        if not docs:
            return

        operations = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) for doc in docs
        ]
        await self.db[collection].bulk_write(operations, ordered=False)

    async def _insert_rows(
        self, collection: str, columns: List[str], rows: List[Dict[str, Any]]
    ) -> None:
        assert self.db is not None
        if not rows:
            return

        docs = []
        for row in rows:
            doc = {col: row.get(col) for col in columns}
            key_string = ":".join(
                str(row.get(col)) for col in columns if row.get(col) is not None
            )
            doc["_id"] = hashlib.sha256(key_string.encode()).hexdigest()
            docs.append(doc)

        await self.db[collection].insert_many(docs, ordered=False)
