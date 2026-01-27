from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Sequence, Dict, Any

from pymongo import MongoClient, ReplaceOne

from dev_health_ops.metrics.schemas import (
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
    FileMetricsRecord,
    ReviewEdgeDailyRecord,
    CICDMetricsDailyRecord,
    DeployMetricsDailyRecord,
    DORAMetricsRecord,
    IncidentMetricsDailyRecord,
    ICLandscapeRollingRecord,
    FileComplexitySnapshot,
    RepoComplexityDaily,
    FileHotspotDaily,
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    InvestmentExplanationRecord,
    WorkGraphEdgeRecord,
    WorkGraphIssuePRRecord,
    WorkGraphPRCommitRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
    Worklog,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.loaders.base import (
    to_dataclass,
)
import logging


def _day_to_mongo_datetime(day: date) -> datetime:
    # BSON stores datetimes as UTC; naive values are treated as UTC by convention.
    return datetime(day.year, day.month, day.day)


def _dt_to_mongo_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


class MongoMetricsSink(BaseMetricsSink):
    """MongoDB sink for derived daily metrics (idempotent upserts by stable _id)."""

    def query_dicts(
        self, query: str, parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        q = query.lower().strip()
        params = parameters or {}

        if "from work_graph_edges" in q:
            f: Dict[str, Any] = {}
            if "repo_id in" in q:
                f["repo_id"] = {"$in": params.get("repo_ids", [])}
            docs = list(self.db["work_graph_edges"].find(f))
            for d in docs:
                d["repo_id"] = str(d.get("repo_id")) if d.get("repo_id") else None
            return docs

        if "from work_items" in q:
            f = {}
            if "work_item_id in" in q:
                f["work_item_id"] = {"$in": params.get("work_item_ids", [])}
            return list(self.db["work_items"].find(f))

        if "from work_item_cycle_times" in q:
            f = {}
            if "work_item_id in" in q:
                f["work_item_id"] = {"$in": params.get("work_item_ids", [])}
            return list(self.db["work_item_cycle_times"].find(f))

        if "from git_pull_requests" in q:
            f = {
                "repo_id": str(params.get("repo_id")),
                "number": {"$in": params.get("numbers", [])},
            }
            docs = list(self.db["git_pull_requests"].find(f))
            for d in docs:
                d["repo_id"] = str(d.get("repo_id"))
            return docs

        if "from git_commits" in q:
            f = {
                "repo_id": str(params.get("repo_id")),
                "hash": {"$in": params.get("hashes", [])},
            }
            docs = list(self.db["git_commits"].find(f))
            for d in docs:
                d["repo_id"] = str(d.get("repo_id"))
            return docs

        if "from git_commit_stats" in q:
            pipeline = [
                {
                    "$match": {
                        "repo_id": str(params.get("repo_id")),
                        "commit_hash": {"$in": params.get("hashes", [])},
                    }
                },
                {
                    "$group": {
                        "_id": "$commit_hash",
                        "churn_loc": {"$sum": {"$add": ["$additions", "$deletions"]}},
                    }
                },
                {"$project": {"commit_hash": "$_id", "churn_loc": 1, "_id": 0}},
            ]
            return list(self.db["git_commit_stats"].aggregate(pipeline))

        if "from user_metrics_daily" in q:
            f = {}
            if "team_id in" in q:
                f["team_id"] = {"$in": params.get("team_ids", [])}
            docs = list(self.db["user_metrics_daily"].find(f, {"repo_id": 1}))
            return [{"id": str(d["repo_id"])} for d in docs if "repo_id" in d]

        raise NotImplementedError(
            f"MongoMetricsSink.query_dicts does not support: {query[:100]}..."
        )

    @property
    def backend_type(self) -> str:
        return "mongo"

    def __init__(self, uri: str, db_name: Optional[str] = None) -> None:
        if not uri:
            raise ValueError("MongoDB URI is required")
        self.client = MongoClient(uri)
        if db_name:
            self.db = self.client[db_name]
        else:
            try:
                self.db = self.client.get_default_database() or self.client["mergestat"]
            except Exception:
                self.db = self.client["mergestat"]

    def close(self) -> None:
        try:
            self.client.close()
        except Exception as e:
            logging.warning("Failed to close MongoDB client: %s", e)

    def ensure_indexes(self) -> None:
        self.db["teams"].create_index([("id", 1)], unique=True)
        self.db["repo_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index(
            [
                ("repo_id", 1),
                ("author_email", 1),
                ("day", 1),
            ]
        )
        self.db["commit_metrics"].create_index([("repo_id", 1), ("day", 1)])
        self.db["commit_metrics"].create_index(
            [
                ("repo_id", 1),
                ("author_email", 1),
                ("day", 1),
            ]
        )
        self.db["team_metrics_daily"].create_index([("team_id", 1), ("day", 1)])
        self.db["work_item_metrics_daily"].create_index([("provider", 1), ("day", 1)])
        self.db["work_item_metrics_daily"].create_index(
            [
                ("provider", 1),
                ("work_scope_id", 1),
                ("day", 1),
            ]
        )
        self.db["work_item_metrics_daily"].create_index(
            [
                ("provider", 1),
                ("work_scope_id", 1),
                ("team_id", 1),
                ("day", 1),
            ]
        )
        self.db["work_item_user_metrics_daily"].create_index(
            [
                ("provider", 1),
                ("work_scope_id", 1),
                ("user_identity", 1),
                ("day", 1),
            ]
        )
        self.db["work_item_cycle_times"].create_index([("provider", 1), ("day", 1)])
        self.db["work_item_state_durations_daily"].create_index(
            [
                ("provider", 1),
                ("day", 1),
            ]
        )
        self.db["work_item_state_durations_daily"].create_index(
            [
                ("provider", 1),
                ("work_scope_id", 1),
                ("day", 1),
            ]
        )
        self.db["work_item_state_durations_daily"].create_index(
            [
                ("provider", 1),
                ("work_scope_id", 1),
                ("team_id", 1),
                ("day", 1),
            ]
        )
        self.db["work_item_state_durations_daily"].create_index(
            [
                ("provider", 1),
                ("work_scope_id", 1),
                ("team_id", 1),
                ("status", 1),
                ("day", 1),
            ]
        )
        self.db["review_edges_daily"].create_index(
            [
                ("repo_id", 1),
                ("day", 1),
                ("reviewer", 1),
                ("author", 1),
            ]
        )
        self.db["cicd_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["deploy_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["incident_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["dora_metrics_daily"].create_index(
            [("repo_id", 1), ("day", 1), ("metric_name", 1)]
        )
        self.db["ic_landscape_rolling_30d"].create_index(
            [("repo_id", 1), ("map_name", 1), ("as_of_day", 1), ("identity_id", 1)],
            unique=True,
        )
        self.db["file_complexity_snapshots"].create_index(
            [
                ("repo_id", 1),
                ("as_of_day", 1),
            ]
        )
        self.db["repo_complexity_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["file_hotspot_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["investment_classifications_daily"].create_index(
            [
                ("repo_id", 1),
                ("day", 1),
            ]
        )
        self.db["investment_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["issue_type_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        # Work unit investment indexes
        self.db["work_unit_investments"].create_index(
            [
                ("work_unit_id", 1),
                ("categorization_run_id", 1),
            ]
        )
        self.db["work_unit_investments"].create_index(
            [
                ("repo_id", 1),
                ("computed_at", -1),
            ]
        )
        self.db["work_unit_investment_quotes"].create_index(
            [
                ("work_unit_id", 1),
            ]
        )
        self.db["investment_explanations"].create_index([("cache_key", 1)], unique=True)
        # Work graph indexes
        self.db["work_graph_edges"].create_index([("edge_id", 1)], unique=True)
        self.db["work_graph_edges"].create_index(
            [("source_id", 1), ("target_id", 1), ("edge_type", 1)]
        )
        self.db["work_graph_issue_pr"].create_index(
            [("repo_id", 1), ("work_item_id", 1), ("pr_number", 1)], unique=True
        )
        self.db["work_graph_pr_commit"].create_index(
            [("repo_id", 1), ("pr_number", 1), ("commit_hash", 1)], unique=True
        )

    async def get_all_teams(self) -> List[Dict[str, Any]]:
        """Fetch all teams from MongoDB for identity resolution."""
        rows = list(self.db["teams"].find({}, {"_id": 0}))
        teams: List[Dict[str, Any]] = []
        for row in rows:
            teams.append(
                {
                    "id": row.get("id") or row.get("team_id"),
                    "name": row.get("name") or row.get("team_name"),
                    "members": row.get("members") or [],
                }
            )
        return teams

    async def insert_teams(self, teams: List[Any]) -> None:
        """Insert or update teams in MongoDB."""
        if not teams:
            return
        ops: List[ReplaceOne] = []
        for team in teams:
            if isinstance(team, dict):
                team_id = team.get("id") or team.get("team_id")
                doc = {
                    "id": team_id,
                    "name": team.get("name") or team.get("team_name"),
                    "members": team.get("members") or [],
                    "description": team.get("description"),
                    "updated_at": _dt_to_mongo_datetime(
                        team.get("updated_at") or datetime.now(timezone.utc)
                    ),
                }
            else:
                team_id = getattr(team, "id", None)
                doc = {
                    "id": team_id,
                    "name": getattr(team, "name", None),
                    "members": getattr(team, "members", []) or [],
                    "description": getattr(team, "description", None),
                    "updated_at": _dt_to_mongo_datetime(
                        getattr(team, "updated_at", None) or datetime.now(timezone.utc)
                    ),
                }
            ops.append(ReplaceOne({"id": team_id}, doc, upsert=True))
        if ops:
            self.db["teams"].bulk_write(ops, ordered=False)

    def ensure_schema(self) -> None:
        """Create MongoDB indexes for efficient querying."""
        self.ensure_indexes()

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["repo_metrics_daily"].bulk_write(ops, ordered=False)

    def write_user_metrics(self, rows: Sequence[UserMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.author_email}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["user_metrics_daily"].bulk_write(ops, ordered=False)

    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.commit_hash}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["commit_metrics"].bulk_write(ops, ordered=False)

    def write_file_metrics(self, rows: Sequence[FileMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.path}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["file_metrics_daily"].bulk_write(ops, ordered=False)

    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.day.isoformat()}:{row.team_id}"
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["team_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_item_metrics(
        self, rows: Sequence[WorkItemMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            team_key = row.team_id or ""
            scope_key = row.work_scope_id or ""
            doc["_id"] = f"{row.day.isoformat()}:{row.provider}:{scope_key}:{team_key}"
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_item_user_metrics(
        self, rows: Sequence[WorkItemUserMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            scope_key = row.work_scope_id or ""
            doc["_id"] = (
                f"{row.day.isoformat()}:{row.provider}:{scope_key}:{row.user_identity}"
            )
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_user_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_item_cycle_times(
        self, rows: Sequence[WorkItemCycleTimeRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = str(row.work_item_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["created_at"] = _dt_to_mongo_datetime(row.created_at)
            if row.started_at is not None:
                doc["started_at"] = _dt_to_mongo_datetime(row.started_at)
            if row.completed_at is not None:
                doc["completed_at"] = _dt_to_mongo_datetime(row.completed_at)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_cycle_times"].bulk_write(ops, ordered=False)

    def write_work_item_state_durations(
        self, rows: Sequence[WorkItemStateDurationDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            scope_key = row.work_scope_id or ""
            team_key = row.team_id or ""
            doc["_id"] = (
                f"{row.day.isoformat()}:{row.provider}:{scope_key}:{team_key}:{row.status}"
            )
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_state_durations_daily"].bulk_write(ops, ordered=False)

    def write_review_edges(self, rows: Sequence[ReviewEdgeDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = (
                f"{row.repo_id}:{row.day.isoformat()}:{row.reviewer}:{row.author}"
            )
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["review_edges_daily"].bulk_write(ops, ordered=False)

    def write_cicd_metrics(self, rows: Sequence[CICDMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["cicd_metrics_daily"].bulk_write(ops, ordered=False)

    def write_deploy_metrics(self, rows: Sequence[DeployMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["deploy_metrics_daily"].bulk_write(ops, ordered=False)

    def write_incident_metrics(
        self, rows: Sequence[IncidentMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["incident_metrics_daily"].bulk_write(ops, ordered=False)

    def write_dora_metrics(self, rows: Sequence[DORAMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.metric_name}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["metric_name"] = str(row.metric_name)
            doc["value"] = float(row.value)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["dora_metrics_daily"].bulk_write(ops, ordered=False)

    def write_ic_landscape_rolling(
        self, rows: Sequence[ICLandscapeRollingRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            # key: repo_id:map_name:as_of_day:identity_id
            doc["_id"] = (
                f"{row.repo_id}:{row.map_name}:{row.as_of_day.isoformat()}:{row.identity_id}"
            )
            doc["repo_id"] = str(row.repo_id)
            doc["as_of_day"] = _day_to_mongo_datetime(row.as_of_day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["ic_landscape_rolling_30d"].bulk_write(ops, ordered=False)

    def get_rolling_30d_user_stats(
        self,
        as_of_day: date,
        repo_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Compute rolling 30d stats by aggregating daily docs in Python.
        """
        start_day = as_of_day - timedelta(days=29)
        # Mongo stores dates as datetime objects (midnight UTC for days)
        start_dt = _day_to_mongo_datetime(start_day)
        end_dt = _day_to_mongo_datetime(as_of_day)

        query: Dict[str, Any] = {"day": {"$gte": start_dt, "$lte": end_dt}}
        if repo_id:
            query["repo_id"] = str(repo_id)

        projection = {
            "identity_id": 1,
            "author_email": 1,
            "team_id": 1,
            "loc_touched": 1,
            "delivery_units": 1,
            "cycle_p50_hours": 1,
            "work_items_active": 1,
        }

        docs = list(self.db["user_metrics_daily"].find(query, projection))

        # Aggregate in Python
        aggs: Dict[str, Dict[str, Any]] = {}

        for doc in docs:
            # Fallback for identity_id
            identity_id = doc.get("identity_id") or doc.get("author_email")
            if not identity_id:
                continue

            team_id = doc.get("team_id")
            loc_touched = doc.get("loc_touched") or 0
            delivery_units = doc.get("delivery_units") or 0
            cycle_p50 = doc.get("cycle_p50_hours") or 0.0
            wip = doc.get("work_items_active") or 0

            if identity_id not in aggs:
                aggs[identity_id] = {
                    "identity_id": identity_id,
                    "team_id": team_id,
                    "churn_loc_30d": 0,
                    "delivery_units_30d": 0,
                    "wip_max_30d": 0,
                    "cycle_p50_values": [],
                }

            entry = aggs[identity_id]
            entry["churn_loc_30d"] += loc_touched
            entry["delivery_units_30d"] += delivery_units
            entry["wip_max_30d"] = max(entry["wip_max_30d"], wip)
            if cycle_p50 > 0:
                entry["cycle_p50_values"].append(cycle_p50)

            if not entry["team_id"] and team_id:
                entry["team_id"] = team_id

        # Finalize
        results = []
        for identity, data in aggs.items():
            cycle_vals = data.pop("cycle_p50_values")
            median_cycle = 0.0
            if cycle_vals:
                cycle_vals.sort()
                mid = len(cycle_vals) // 2
                if len(cycle_vals) % 2 == 1:
                    median_cycle = cycle_vals[mid]
                else:
                    median_cycle = (cycle_vals[mid - 1] + cycle_vals[mid]) / 2.0

            data["cycle_p50_30d_hours"] = median_cycle
            results.append(data)

        return results

    def write_file_complexity_snapshots(
        self, rows: Sequence[FileComplexitySnapshot]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.as_of_day.isoformat()}:{row.file_path}"
            doc["repo_id"] = str(row.repo_id)
            doc["as_of_day"] = _day_to_mongo_datetime(row.as_of_day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["file_complexity_snapshots"].bulk_write(ops, ordered=False)

    def write_repo_complexity_daily(self, rows: Sequence[RepoComplexityDaily]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["repo_complexity_daily"].bulk_write(ops, ordered=False)

    def write_file_hotspot_daily(self, rows: Sequence[FileHotspotDaily]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.file_path}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["file_hotspot_daily"].bulk_write(ops, ordered=False)

    def write_investment_classifications(
        self, rows: Sequence[InvestmentClassificationRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = (
                f"{row.provider}:{row.artifact_type}:{row.artifact_id}:{row.day.isoformat()}"
            )
            if row.repo_id:
                doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["investment_classifications_daily"].bulk_write(ops, ordered=False)

    def write_investment_metrics(self, rows: Sequence[InvestmentMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            repo_part = str(row.repo_id) if row.repo_id else "global"
            doc["_id"] = (
                f"{repo_part}:{row.team_id}:{row.investment_area}:{row.project_stream}:{row.day.isoformat()}"
            )
            if row.repo_id:
                doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["investment_metrics_daily"].bulk_write(ops, ordered=False)

    def write_issue_type_metrics(self, rows: Sequence[IssueTypeMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            repo_part = str(row.repo_id) if row.repo_id else "global"
            doc["_id"] = (
                f"{repo_part}:{row.provider}:{row.team_id}:{row.issue_type_norm}:{row.day.isoformat()}"
            )
            if row.repo_id:
                doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["issue_type_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_unit_investments(
        self, rows: Sequence[WorkUnitInvestmentRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            # Composite key: work_unit_id + categorization_run_id for versioning
            doc["_id"] = f"{row.work_unit_id}:{row.categorization_run_id}"
            if row.repo_id:
                doc["repo_id"] = str(row.repo_id)
            doc["from_ts"] = _dt_to_mongo_datetime(row.from_ts)
            doc["to_ts"] = _dt_to_mongo_datetime(row.to_ts)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_unit_investments"].bulk_write(ops, ordered=False)

    def write_work_unit_investment_quotes(
        self, rows: Sequence[WorkUnitInvestmentEvidenceQuoteRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            # Composite key: work_unit_id + source_type + source_id + quote hash
            quote_hash = hash(row.quote) & 0xFFFFFFFF  # Truncate to 32-bit
            doc["_id"] = (
                f"{row.work_unit_id}:{row.source_type}:{row.source_id}:{quote_hash}"
            )
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_unit_investment_quotes"].bulk_write(ops, ordered=False)

    # -------------------------------------------------------------------------
    # Investment explanation caching
    # -------------------------------------------------------------------------

    def write_investment_explanation(self, record: InvestmentExplanationRecord) -> None:
        """Write or replace an investment explanation to the cache."""
        doc = asdict(record)
        doc["_id"] = record.cache_key
        doc["computed_at"] = _dt_to_mongo_datetime(record.computed_at)
        self.db["investment_explanations"].replace_one(
            {"_id": doc["_id"]}, doc, upsert=True
        )

    def read_investment_explanation(
        self, cache_key: str
    ) -> Optional[InvestmentExplanationRecord]:
        """Read a cached investment explanation by cache_key."""
        doc = self.db["investment_explanations"].find_one(
            {"_id": cache_key}, {"_id": 0}
        )
        if not doc:
            return None
        return to_dataclass(InvestmentExplanationRecord, doc)

    # -------------------------------------------------------------------------
    # Work graph (derived relationships)
    # -------------------------------------------------------------------------

    def write_work_graph_edges(self, rows: Sequence[WorkGraphEdgeRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = row.edge_id
            doc["repo_id"] = str(row.repo_id) if row.repo_id else None
            doc["discovered_at"] = _dt_to_mongo_datetime(row.discovered_at)
            doc["last_synced"] = _dt_to_mongo_datetime(row.last_synced)
            doc["event_ts"] = _dt_to_mongo_datetime(row.event_ts)
            doc["day"] = _day_to_mongo_datetime(row.day)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_graph_edges"].bulk_write(ops, ordered=False)

    def write_work_graph_issue_pr(self, rows: Sequence[WorkGraphIssuePRRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.work_item_id}:{row.pr_number}"
            doc["repo_id"] = str(row.repo_id)
            doc["last_synced"] = _dt_to_mongo_datetime(row.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_graph_issue_pr"].bulk_write(ops, ordered=False)

    def write_work_graph_pr_commit(
        self, rows: Sequence[WorkGraphPRCommitRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.pr_number}:{row.commit_hash}"
            doc["repo_id"] = str(row.repo_id)
            doc["last_synced"] = _dt_to_mongo_datetime(row.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_graph_pr_commit"].bulk_write(ops, ordered=False)

    # -------------------------------------------------------------------------
    # Raw collection write methods
    # -------------------------------------------------------------------------

    def write_work_items(self, work_items: Sequence[WorkItem]) -> None:
        if not work_items:
            return
        ops: List[ReplaceOne] = []
        for item in work_items:
            doc = asdict(item)
            doc["_id"] = item.work_item_id
            if item.repo_id:
                doc["repo_id"] = str(item.repo_id)
            doc["created_at"] = _dt_to_mongo_datetime(item.created_at)
            doc["updated_at"] = _dt_to_mongo_datetime(item.updated_at)
            if item.started_at:
                doc["started_at"] = _dt_to_mongo_datetime(item.started_at)
            if item.completed_at:
                doc["completed_at"] = _dt_to_mongo_datetime(item.completed_at)
            if item.closed_at:
                doc["closed_at"] = _dt_to_mongo_datetime(item.closed_at)
            if item.due_at:
                doc["due_at"] = _dt_to_mongo_datetime(item.due_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_items"].bulk_write(ops, ordered=False)

    def write_work_item_transitions(
        self, transitions: Sequence[WorkItemStatusTransition]
    ) -> None:
        if not transitions:
            return
        ops: List[ReplaceOne] = []
        for tr in transitions:
            doc = asdict(tr)
            doc["_id"] = (
                f"{tr.work_item_id}:{tr.occurred_at.isoformat()}:{tr.to_status}"
            )
            doc["occurred_at"] = _dt_to_mongo_datetime(tr.occurred_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_transitions"].bulk_write(ops, ordered=False)

    def write_work_item_dependencies(self, rows: Sequence[WorkItemDependency]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for r in rows:
            doc = asdict(r)
            doc["_id"] = (
                f"{r.source_work_item_id}:{r.target_work_item_id}:{r.relationship_type}"
            )
            doc["last_synced"] = _dt_to_mongo_datetime(r.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_dependencies"].bulk_write(ops, ordered=False)

    def write_work_item_reopen_events(
        self, rows: Sequence[WorkItemReopenEvent]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for r in rows:
            doc = asdict(r)
            doc["_id"] = f"{r.work_item_id}:{r.occurred_at.isoformat()}"
            doc["occurred_at"] = _dt_to_mongo_datetime(r.occurred_at)
            doc["last_synced"] = _dt_to_mongo_datetime(r.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_reopen_events"].bulk_write(ops, ordered=False)

    def write_work_item_interactions(
        self, rows: Sequence[WorkItemInteractionEvent]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for r in rows:
            doc = asdict(r)
            doc["_id"] = (
                f"{r.work_item_id}:{r.occurred_at.isoformat()}:{r.interaction_type}"
            )
            doc["occurred_at"] = _dt_to_mongo_datetime(r.occurred_at)
            doc["last_synced"] = _dt_to_mongo_datetime(r.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_interactions"].bulk_write(ops, ordered=False)

    def write_sprints(self, rows: Sequence[Sprint]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for r in rows:
            doc = asdict(r)
            doc["_id"] = f"{r.provider}:{r.sprint_id}"
            if r.started_at:
                doc["started_at"] = _dt_to_mongo_datetime(r.started_at)
            if r.ended_at:
                doc["ended_at"] = _dt_to_mongo_datetime(r.ended_at)
            if r.completed_at:
                doc["completed_at"] = _dt_to_mongo_datetime(r.completed_at)
            doc["last_synced"] = _dt_to_mongo_datetime(r.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["sprints"].bulk_write(ops, ordered=False)

    def write_worklogs(self, rows: Sequence[Worklog]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for r in rows:
            doc = asdict(r)
            doc["_id"] = f"{r.provider}:{r.worklog_id}"
            doc["started_at"] = _dt_to_mongo_datetime(r.started_at)
            doc["created_at"] = _dt_to_mongo_datetime(r.created_at)
            doc["updated_at"] = _dt_to_mongo_datetime(r.updated_at)
            doc["last_synced"] = _dt_to_mongo_datetime(r.last_synced)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["worklogs"].bulk_write(ops, ordered=False)
