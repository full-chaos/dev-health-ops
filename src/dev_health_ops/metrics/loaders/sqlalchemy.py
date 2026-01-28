"""SQLAlchemy data loader implementation (SQLite/Postgres)."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dev_health_ops.metrics.loaders.base import (
    DataLoader,
    parse_uuid,
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


def _to_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


class SqlAlchemyDataLoader(DataLoader):
    """DataLoader implementation for SQLAlchemy-compatible backends (SQLite, Postgres)."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    async def load_git_rows(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> Tuple[List[CommitStatRow], List[PullRequestRow], List[PullRequestReviewRow]]:
        start_str = start.isoformat()
        end_str = end.isoformat()
        params: Dict[str, Any] = {"start": start_str, "end": end_str}

        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND c.repo_id = :repo_id"
        elif repo_name is not None:
            repo_filter = (
                " AND c.repo_id IN (SELECT id FROM repos WHERE repo = :repo_name)"
            )
            params["repo_name"] = repo_name

        commit_query = f"""
        SELECT
          c.repo_id,
          c.hash AS commit_hash,
          c.author_email,
          c.author_name,
          c.committer_when,
          s.file_path,
          s.additions,
          s.deletions
        FROM git_commits AS c
        LEFT JOIN git_commit_stats AS s
          ON (s.repo_id = c.repo_id) AND (s.commit_hash = c.hash)
        WHERE c.committer_when >= :start AND c.committer_when < :end
        {repo_filter}
        """

        pr_query = f"""
        SELECT * FROM git_pull_requests
        WHERE (created_at >= :start AND created_at < :end)
        OR (merged_at IS NOT NULL AND merged_at >= :start AND merged_at < :end)
        {repo_filter.replace("c.repo_id", "repo_id") if repo_id or repo_name else ""}
        """

        review_query = f"""
        SELECT * FROM git_pull_request_reviews
        WHERE submitted_at >= :start AND submitted_at < :end
        {repo_filter.replace("c.repo_id", "repo_id") if repo_id or repo_name else ""}
        """

        commit_rows: List[CommitStatRow] = []
        pr_rows: List[PullRequestRow] = []
        review_rows: List[PullRequestReviewRow] = []

        with self.engine.connect() as conn:
            c_result = conn.execute(text(commit_query), params).mappings().all()
            for r in c_result:
                u = parse_uuid(r.get("repo_id"))
                cw = _to_dt(r.get("committer_when"))
                if u and cw:
                    commit_rows.append(
                        {
                            "repo_id": u,
                            "commit_hash": str(r.get("commit_hash")),
                            "author_email": r.get("author_email"),
                            "author_name": r.get("author_name"),
                            "committer_when": cw,
                            "file_path": r.get("file_path"),
                            "additions": int(r.get("additions") or 0),
                            "deletions": int(r.get("deletions") or 0),
                        }
                    )

            p_result = conn.execute(text(pr_query), params).mappings().all()
            for r in p_result:
                u = parse_uuid(r.get("repo_id"))
                ca = _to_dt(r.get("created_at"))
                if u and ca:
                    pr_rows.append(
                        {
                            "repo_id": u,
                            "number": int(r.get("number") or 0),
                            "author_email": r.get("author_email"),
                            "author_name": r.get("author_name"),
                            "created_at": ca,
                            "merged_at": _to_dt(r.get("merged_at")),
                            "first_review_at": _to_dt(r.get("first_review_at")),
                            "first_comment_at": _to_dt(r.get("first_comment_at")),
                            "changes_requested_count": int(
                                r.get("changes_requested_count") or 0
                            ),
                            "reviews_count": int(r.get("reviews_count") or 0),
                            "comments_count": int(r.get("comments_count") or 0),
                            "additions": int(r.get("additions") or 0),
                            "deletions": int(r.get("deletions") or 0),
                            "changed_files": int(r.get("changed_files") or 0),
                        }
                    )

            rv_result = conn.execute(text(review_query), params).mappings().all()
            for r in rv_result:
                u = parse_uuid(r.get("repo_id"))
                sa = _to_dt(r.get("submitted_at"))
                if u and sa:
                    review_rows.append(
                        {
                            "repo_id": u,
                            "number": int(r.get("number") or 0),
                            "reviewer": r.get("reviewer") or "unknown",
                            "submitted_at": sa,
                            "state": r.get("state") or "unknown",
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

        params: Dict[str, Any] = {"start": start.isoformat(), "end": end.isoformat()}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = :repo_id"

        item_query = f"""
        SELECT * FROM work_items
        WHERE (created_at < :end)
        AND (status != 'done' OR completed_at >= :start)
        {repo_filter}
        """
        trans_query = f"""
        SELECT * FROM work_item_transitions
        WHERE (occurred_at < :end)
        {repo_filter}
        """

        import dataclasses

        def to_dataclass(cls, row_map):
            field_names = {f.name for f in dataclasses.fields(cls)}
            filtered = {}
            for k, v in row_map.items():
                if k in field_names:
                    if isinstance(v, str) and (
                        k.endswith("_at") or k.endswith("_when") or k == "day"
                    ):
                        v = _to_dt(v)
                    filtered[k] = v
            return cls(**filtered)

        with self.engine.connect() as conn:
            items_raw = conn.execute(text(item_query), params).mappings().all()
            trans_raw = conn.execute(text(trans_query), params).mappings().all()

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
        params = {"start": start.isoformat(), "end": end.isoformat()}
        repo_filter = ""
        if repo_id:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = :repo_id"

        pipe_query = f"SELECT * FROM ci_pipeline_runs WHERE finished_at >= :start AND finished_at < :end {repo_filter}"
        deploy_query = f"SELECT * FROM deployments WHERE deployed_at >= :start AND deployed_at < :end {repo_filter}"

        pipes: List[PipelineRunRow] = []
        deploys: List[DeploymentRow] = []
        with self.engine.connect() as conn:
            p_rows = conn.execute(text(pipe_query), params).mappings().all()
            d_rows = conn.execute(text(deploy_query), params).mappings().all()

            for pr in p_rows:
                pd = dict(pr)
                pd["finished_at"] = _to_dt(pd.get("finished_at"))
                pd["started_at"] = _to_dt(pd.get("started_at"))
                pd["queued_at"] = _to_dt(pd.get("queued_at"))
                pipes.append(pd)  # type: ignore

            for dr in d_rows:
                dd = dict(dr)
                dd["deployed_at"] = _to_dt(dd.get("deployed_at"))
                dd["merged_at"] = _to_dt(dd.get("merged_at"))
                dd["started_at"] = _to_dt(dd.get("started_at"))
                dd["finished_at"] = _to_dt(dd.get("finished_at"))
                deploys.append(dd)  # type: ignore

        return pipes, deploys

    async def load_incidents(
        self,
        start: datetime,
        end: datetime,
        repo_id: Optional[uuid.UUID],
        repo_name: Optional[str] = None,
    ) -> List[IncidentRow]:
        params = {"start": start.isoformat(), "end": end.isoformat()}
        repo_filter = ""
        if repo_id:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = :repo_id"

        query = f"SELECT * FROM incidents WHERE started_at >= :start AND started_at < :end {repo_filter}"
        incidents: List[IncidentRow] = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
            for r in rows:
                rd = dict(r)
                rd["started_at"] = _to_dt(rd.get("started_at"))
                rd["resolved_at"] = _to_dt(rd.get("resolved_at"))
                incidents.append(rd)  # type: ignore
        return incidents

    async def load_blame_concentration(
        self,
        repo_id: uuid.UUID,
        as_of: datetime,
    ) -> Dict[uuid.UUID, float]:
        params = {"repo_id": str(repo_id)}
        query = """
        SELECT
            repo_id,
            CAST(sum(lines_count * lines_count) AS FLOAT) / (sum(lines_count) * sum(lines_count)) as concentration
        FROM git_file_blame
        WHERE repo_id = :repo_id
        GROUP BY repo_id
        """
        with self.engine.connect() as conn:
            row = conn.execute(text(query), params).first()
            if row and row[0]:
                u = parse_uuid(row[0])
                if u:
                    return {u: float(row[1])}
        return {}

    async def load_atlassian_ops_incidents(
        self,
        start: datetime,
        end: datetime,
    ) -> List[AtlassianOpsIncident]:
        params = {"start": start, "end": end}
        query = "SELECT * FROM atlassian_ops_incidents WHERE created_at >= :start AND created_at < :end"

        incidents: List[AtlassianOpsIncident] = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
            for r in rows:
                incidents.append(
                    AtlassianOpsIncident(
                        id=r.get("id", ""),
                        url=r.get("url"),
                        summary=r.get("summary", ""),
                        description=r.get("description"),
                        status=r.get("status", ""),
                        severity=r.get("severity", ""),
                        created_at=_to_dt(r.get("created_at"))
                        or datetime.now(timezone.utc),
                        provider_id=r.get("provider_id"),
                        last_synced=_to_dt(r.get("last_synced"))
                        or datetime.now(timezone.utc),
                    )
                )
        return incidents

    async def load_atlassian_ops_alerts(
        self,
        start: datetime,
        end: datetime,
    ) -> List[AtlassianOpsAlert]:
        params = {"start": start, "end": end}
        query = "SELECT * FROM atlassian_ops_alerts WHERE created_at >= :start AND created_at < :end"

        alerts: List[AtlassianOpsAlert] = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
            for r in rows:
                alerts.append(
                    AtlassianOpsAlert(
                        id=r.get("id", ""),
                        status=r.get("status", ""),
                        priority=r.get("priority", ""),
                        created_at=_to_dt(r.get("created_at"))
                        or datetime.now(timezone.utc),
                        acknowledged_at=_to_dt(r.get("acknowledged_at")),
                        snoozed_at=_to_dt(r.get("snoozed_at")),
                        closed_at=_to_dt(r.get("closed_at")),
                        last_synced=_to_dt(r.get("last_synced"))
                        or datetime.now(timezone.utc),
                    )
                )
        return alerts

    async def load_atlassian_ops_schedules(
        self,
    ) -> List[AtlassianOpsSchedule]:
        query = "SELECT * FROM atlassian_ops_schedules"

        schedules: List[AtlassianOpsSchedule] = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(query)).mappings().all()
            for r in rows:
                schedules.append(
                    AtlassianOpsSchedule(
                        id=r.get("id", ""),
                        name=r.get("name", ""),
                        timezone=r.get("timezone"),
                        last_synced=_to_dt(r.get("last_synced"))
                        or datetime.now(timezone.utc),
                    )
                )
        return schedules

    async def load_jira_project_ops_team_links(
        self,
    ) -> List[JiraProjectOpsTeamLink]:
        query = "SELECT * FROM jira_project_ops_team_links"

        links: List[JiraProjectOpsTeamLink] = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(query)).mappings().all()
            for r in rows:
                links.append(
                    JiraProjectOpsTeamLink(
                        project_key=r.get("project_key", ""),
                        ops_team_id=r.get("ops_team_id", ""),
                        project_name=r.get("project_name", ""),
                        ops_team_name=r.get("ops_team_name", ""),
                        updated_at=_to_dt(r.get("updated_at"))
                        or datetime.now(timezone.utc),
                    )
                )
        return links

    async def load_user_metrics_rolling_30d(
        self,
        as_of: date,
    ) -> List[Dict[str, Any]]:
        start = as_of - timedelta(days=29)

        query = """
        SELECT
            identity_id,
            MAX(team_id) as team_id,
            SUM(loc_touched) as churn_loc_30d,
            SUM(delivery_units) as delivery_units_30d,
            AVG(cycle_p50_hours) as cycle_p50_30d_hours,
            MAX(work_items_active) as wip_max_30d
        FROM user_metrics_daily
        WHERE day >= :start AND day <= :end
        GROUP BY identity_id
        """
        params = {"start": start.isoformat(), "end": as_of.isoformat()}

        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(text(query), params).mappings().all()]
