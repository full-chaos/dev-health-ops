from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Sequence, Dict, Any

from pymongo import MongoClient, ReplaceOne

from metrics.schemas import (
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
    IncidentMetricsDailyRecord,
    ICLandscapeRollingRecord,
    FileComplexitySnapshot,
    RepoComplexityDaily,
    FileHotspotDaily,
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from metrics.sinks.base import BaseMetricsSink
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
        self.db["repo_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([
            ("repo_id", 1),
            ("author_email", 1),
            ("day", 1),
        ])
        self.db["commit_metrics"].create_index([("repo_id", 1), ("day", 1)])
        self.db["commit_metrics"].create_index([
            ("repo_id", 1),
            ("author_email", 1),
            ("day", 1),
        ])
        self.db["team_metrics_daily"].create_index([("team_id", 1), ("day", 1)])
        self.db["work_item_metrics_daily"].create_index([("provider", 1), ("day", 1)])
        self.db["work_item_metrics_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("day", 1),
        ])
        self.db["work_item_metrics_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("team_id", 1),
            ("day", 1),
        ])
        self.db["work_item_user_metrics_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("user_identity", 1),
            ("day", 1),
        ])
        self.db["work_item_cycle_times"].create_index([("provider", 1), ("day", 1)])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("day", 1),
        ])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("day", 1),
        ])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("team_id", 1),
            ("day", 1),
        ])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("team_id", 1),
            ("status", 1),
            ("day", 1),
        ])
        self.db["review_edges_daily"].create_index([
            ("repo_id", 1),
            ("day", 1),
            ("reviewer", 1),
            ("author", 1),
        ])
        self.db["cicd_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["deploy_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["incident_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["ic_landscape_rolling_30d"].create_index(
            [("repo_id", 1), ("map_name", 1), ("as_of_day", 1), ("identity_id", 1)],
            unique=True,
        )
        self.db["file_complexity_snapshots"].create_index([
            ("repo_id", 1),
            ("as_of_day", 1),
        ])
        self.db["repo_complexity_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["file_hotspot_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["investment_classifications_daily"].create_index([
            ("repo_id", 1),
            ("day", 1),
        ])
        self.db["investment_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["issue_type_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])

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

        query = {"day": {"$gte": start_dt, "$lte": end_dt}}
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
        raise NotImplementedError(
            "Work unit investment materialization is not supported for MongoDB"
        )

    def write_work_unit_investment_quotes(
        self, rows: Sequence[WorkUnitInvestmentEvidenceQuoteRecord]
    ) -> None:
        raise NotImplementedError(
            "Work unit investment evidence quotes are not supported for MongoDB"
        )
