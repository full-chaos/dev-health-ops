"""ClickHouse data loader implementation."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

from dev_health_ops.metrics.loaders.base import (
    DataLoader,
    naive_utc,
    parse_uuid,
    to_dataclass,
)
from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    DeploymentRow,
    IncidentRow,
    PipelineRunRow,
    PullRequestReviewRow,
    PullRequestRow,
)
from dev_health_ops.metrics.testops_schemas import (
    CoverageSnapshotRow,
    JobRunRow,
    PipelineRunExtendedRow,
    TestCaseResultRow,
    TestSuiteResultRow,
)
from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsAlert,
    AtlassianOpsIncident,
    AtlassianOpsSchedule,
)
from dev_health_ops.models.teams import JiraProjectOpsTeamLink


async def _clickhouse_query_dicts(
    client: Any, query: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    from dev_health_ops.api.queries.client import query_dicts

    return await query_dicts(client, query, params)


class ClickHouseDataLoader(DataLoader):
    """DataLoader implementation for ClickHouse backend.

    Args:
        client: ClickHouse client instance.
        org_id: Optional organisation ID.  When set, every read query is
                scoped to this org preventing cross-org data leakage.
    """

    def __init__(self, client: Any, org_id: str = "") -> None:
        self.client = client
        self.org_id = org_id

    def _org_filter(self, *, alias: str = "") -> str:
        """Return an ``AND org_id = …`` clause when *org_id* is set."""
        if not self.org_id:
            return ""
        col = f"{alias}.org_id" if alias else "org_id"
        return f" AND {col} = {{org_id:String}}"

    def _inject_org_id(self, params: dict[str, Any]) -> dict[str, Any]:
        """Inject *org_id* into query parameters when set."""
        if self.org_id:
            params = dict(params)
            params["org_id"] = self.org_id
        return params

    async def load_git_rows(
        self,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None,
        repo_name: str | None = None,
    ) -> tuple[list[CommitStatRow], list[PullRequestRow], list[PullRequestReviewRow]]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND c.repo_id = {repo_id:UUID}"
        elif repo_name is not None:
            params["repo_name"] = repo_name
            repo_filter = (
                " AND c.repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String}"
                + (" AND org_id = {org_id:String}" if self.org_id else "")
                + ")"
            )

        org_filter_c = self._org_filter(alias="c")
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        commit_query = f"""
        SELECT
          c.repo_id AS repo_id,
          c.hash AS commit_hash,
          c.author_email AS author_email,
          c.author_name AS author_name,
          c.committer_when AS committer_when,
          s.file_path AS file_path,
          s.additions AS additions,
          s.deletions AS deletions
        FROM git_commits AS c
        LEFT JOIN git_commit_stats AS s
          ON (s.repo_id = c.repo_id) AND (s.commit_hash = c.hash)
        WHERE c.committer_when >= {{start:DateTime}} AND c.committer_when < {{end:DateTime}}
        {repo_filter}
        {org_filter_c}
        """

        pr_query = f"""
        SELECT
          repo_id,
          number,
          author_email,
          author_name,
          created_at,
          merged_at,
          first_review_at,
          first_comment_at,
          changes_requested_count,
          reviews_count,
          comments_count,
          additions,
          deletions,
          changed_files
        FROM git_pull_requests
        WHERE
          (created_at >= {{start:DateTime}} AND created_at < {{end:DateTime}})
          OR (merged_at IS NOT NULL AND merged_at >= {{start:DateTime}} AND merged_at < {{end:DateTime}})
          {repo_filter.replace("c.repo_id", "repo_id") if repo_id or repo_name else ""}
        {org_filter}
        """

        review_query = f"""
        SELECT
          repo_id,
          number,
          reviewer,
          submitted_at,
          state
        FROM git_pull_request_reviews
        WHERE submitted_at >= {{start:DateTime}} AND submitted_at < {{end:DateTime}}
        {repo_filter.replace("c.repo_id", "repo_id") if repo_id or repo_name else ""}
        {org_filter}
        """

        commit_dicts = await _clickhouse_query_dicts(self.client, commit_query, params)
        pr_dicts = await _clickhouse_query_dicts(self.client, pr_query, params)
        review_dicts = await _clickhouse_query_dicts(self.client, review_query, params)

        commit_rows: list[CommitStatRow] = []
        for r in commit_dicts:
            u = parse_uuid(r.get("repo_id"))
            cw = r.get("committer_when")
            if u and cw:
                commit_rows.append(
                    {
                        "repo_id": u,
                        "commit_hash": str(r.get("commit_hash") or ""),
                        "author_email": r.get("author_email") or "",
                        "author_name": r.get("author_name") or "",
                        "committer_when": cw,
                        "file_path": r.get("file_path"),
                        "additions": int(r.get("additions") or 0),
                        "deletions": int(r.get("deletions") or 0),
                    }
                )

        pr_rows: list[PullRequestRow] = []
        for r in pr_dicts:
            u = parse_uuid(r.get("repo_id"))
            ca = r.get("created_at")
            if u and ca:
                pr_rows.append(
                    {
                        "repo_id": u,
                        "number": int(r.get("number") or 0),
                        "author_email": r.get("author_email") or "",
                        "author_name": r.get("author_name") or "",
                        "created_at": ca,
                        "merged_at": r.get("merged_at"),
                        "first_review_at": r.get("first_review_at"),
                        "first_comment_at": r.get("first_comment_at"),
                        "changes_requested_count": int(
                            r.get("changes_requested_count", 0)
                        ),
                        "reviews_count": int(r.get("reviews_count", 0)),
                        "comments_count": int(r.get("comments_count", 0)),
                        "additions": int(r.get("additions", 0)),
                        "deletions": int(r.get("deletions", 0)),
                        "changed_files": int(r.get("changed_files", 0)),
                    }
                )

        review_rows: list[PullRequestReviewRow] = []
        for r in review_dicts:
            u = parse_uuid(r.get("repo_id"))
            sa = r.get("submitted_at")
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
        repo_id: uuid.UUID | None,
        repo_name: str | None = None,
    ) -> tuple[list[Any], list[Any]]:
        from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition

        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = {repo_id:UUID}"

        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        item_query = f"""
        SELECT * FROM work_items
        WHERE (created_at < {{end:DateTime}})
        AND (status != 'done' OR completed_at >= {{start:DateTime}})
        {repo_filter}
        {org_filter}
        """

        trans_query = f"""
        SELECT * FROM work_item_transitions
        WHERE (occurred_at < {{end:DateTime}})
        {repo_filter}
        {org_filter}
        """

        item_dicts = await _clickhouse_query_dicts(self.client, item_query, params)
        trans_dicts = await _clickhouse_query_dicts(self.client, trans_query, params)

        items = [to_dataclass(WorkItem, d) for d in item_dicts]
        transitions = [to_dataclass(WorkItemStatusTransition, t) for t in trans_dicts]

        return items, transitions

    async def load_cicd_data(
        self,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None,
        repo_name: str | None = None,
    ) -> tuple[list[PipelineRunRow], list[DeploymentRow]]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = {repo_id:UUID}"

        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        pipe_query = f"""
        SELECT * FROM ci_pipeline_runs
        WHERE finished_at >= {{start:DateTime}} AND finished_at < {{end:DateTime}}
        {repo_filter}
        {org_filter}
        """
        deploy_query = f"""
        SELECT * FROM deployments
        WHERE deployed_at >= {{start:DateTime}} AND deployed_at < {{end:DateTime}}
        {repo_filter}
        {org_filter}
        """

        pipes_dicts = await _clickhouse_query_dicts(self.client, pipe_query, params)
        deploys_dicts = await _clickhouse_query_dicts(self.client, deploy_query, params)

        # ClickHouse dicts can be directly cast if they match keys
        pipes: list[PipelineRunRow] = [dict(p) for p in pipes_dicts]  # type: ignore
        deploys: list[DeploymentRow] = [dict(d) for d in deploys_dicts]  # type: ignore

        return pipes, deploys

    async def load_incidents(
        self,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None,
        repo_name: str | None = None,
    ) -> list[IncidentRow]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = {repo_id:UUID}"

        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        query = f"""
        SELECT * FROM incidents
        WHERE started_at >= {{start:DateTime}} AND started_at < {{end:DateTime}}
        {repo_filter}
        {org_filter}
        """
        dicts = await _clickhouse_query_dicts(self.client, query, params)
        return [dict(d) for d in dicts]  # type: ignore

    async def load_testops_pipeline_data(
        self,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None,
    ) -> tuple[list[PipelineRunExtendedRow], list[JobRunRow]]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = {repo_id:UUID}"

        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        pipeline_query = f"""
        SELECT
          repo_id,
          run_id,
          pipeline_name,
          provider,
          status,
          queued_at,
          started_at,
          finished_at,
          duration_seconds,
          queue_seconds,
          retry_count,
          cancel_reason,
          trigger_source,
          commit_hash,
          branch,
          pr_number,
          team_id,
          service_id,
          org_id
        FROM ci_pipeline_runs
        WHERE started_at >= {{start:DateTime}} AND started_at < {{end:DateTime}}
        {repo_filter}
        {org_filter}
        """
        job_query = f"""
        SELECT
          j.repo_id,
          j.run_id,
          j.job_id,
          j.job_name,
          j.stage,
          j.status,
          j.started_at,
          j.finished_at,
          j.duration_seconds,
          j.runner_type,
          j.retry_attempt,
          j.org_id
        FROM ci_job_runs AS j
        INNER JOIN ci_pipeline_runs AS p
          ON (p.repo_id = j.repo_id) AND (p.run_id = j.run_id)
        WHERE p.started_at >= {{start:DateTime}} AND p.started_at < {{end:DateTime}}
        {repo_filter.replace("repo_id", "p.repo_id") if repo_id is not None else ""}
        {self._org_filter(alias="p")}
        """

        pipeline_dicts = await _clickhouse_query_dicts(
            self.client, pipeline_query, params
        )
        job_dicts = await _clickhouse_query_dicts(self.client, job_query, params)
        return (
            [cast(PipelineRunExtendedRow, dict(row)) for row in pipeline_dicts],
            [cast(JobRunRow, dict(row)) for row in job_dicts],
        )

    async def load_testops_test_data(
        self,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None,
    ) -> tuple[list[TestSuiteResultRow], list[TestCaseResultRow]]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = {repo_id:UUID}"

        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        suite_query = f"""
        SELECT
          repo_id,
          run_id,
          suite_id,
          suite_name,
          framework,
          environment,
          total_count,
          passed_count,
          failed_count,
          skipped_count,
          error_count,
          quarantined_count,
          retried_count,
          duration_seconds,
          started_at,
          finished_at,
          team_id,
          service_id,
          org_id
        FROM test_suite_results
        WHERE coalesce(started_at, finished_at) >= {{start:DateTime}}
          AND coalesce(started_at, finished_at) < {{end:DateTime}}
        {repo_filter}
        {org_filter}
        """
        case_query = f"""
        SELECT
          c.repo_id,
          c.run_id,
          c.suite_id,
          c.case_id,
          c.case_name,
          c.class_name,
          c.status,
          c.duration_seconds,
          c.retry_attempt,
          c.failure_message,
          c.failure_type,
          c.stack_trace,
          c.is_quarantined,
          c.org_id
        FROM test_case_results AS c
        INNER JOIN test_suite_results AS s
          ON (s.repo_id = c.repo_id)
         AND (s.run_id = c.run_id)
         AND (s.suite_id = c.suite_id)
        WHERE coalesce(s.started_at, s.finished_at) >= {{start:DateTime}}
          AND coalesce(s.started_at, s.finished_at) < {{end:DateTime}}
        {repo_filter.replace("repo_id", "s.repo_id") if repo_id is not None else ""}
        {self._org_filter(alias="s")}
        """

        suite_dicts = await _clickhouse_query_dicts(self.client, suite_query, params)
        case_dicts = await _clickhouse_query_dicts(self.client, case_query, params)
        return (
            [cast(TestSuiteResultRow, dict(row)) for row in suite_dicts],
            [cast(TestCaseResultRow, dict(row)) for row in case_dicts],
        )

    async def load_testops_coverage_data(
        self,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None,
    ) -> list[CoverageSnapshotRow]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND p.repo_id = {repo_id:UUID}"

        params = self._inject_org_id(params)
        query = f"""
        SELECT
          c.repo_id,
          c.run_id,
          c.snapshot_id,
          c.report_format,
          c.lines_total,
          c.lines_covered,
          c.line_coverage_pct,
          c.branches_total,
          c.branches_covered,
          c.branch_coverage_pct,
          c.functions_total,
          c.functions_covered,
          c.commit_hash,
          c.branch,
          c.pr_number,
          c.team_id,
          c.service_id,
          c.org_id
        FROM coverage_snapshots AS c
        INNER JOIN ci_pipeline_runs AS p
          ON (p.repo_id = c.repo_id) AND (p.run_id = c.run_id)
        WHERE p.started_at >= {{start:DateTime}} AND p.started_at < {{end:DateTime}}
        {repo_filter}
        {self._org_filter(alias="p")}
        """
        dicts = await _clickhouse_query_dicts(self.client, query, params)
        return [cast(CoverageSnapshotRow, dict(row)) for row in dicts]

    async def load_blame_concentration(
        self,
        repo_id: uuid.UUID,
        as_of: datetime,
    ) -> dict[uuid.UUID, float]:
        params: dict[str, Any] = {"repo_id": str(repo_id), "as_of": naive_utc(as_of)}
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        query = f"""
        SELECT
            repo_id,
            sum(lines_count * lines_count) / (sum(lines_count) * sum(lines_count)) as concentration
        FROM git_file_blame
        WHERE repo_id = {{repo_id:UUID}}
        {org_filter}
        GROUP BY repo_id
        """
        rows = await _clickhouse_query_dicts(self.client, query, params)
        res = {}
        for r in rows:
            u = parse_uuid(r.get("repo_id"))
            if u:
                res[u] = float(r["concentration"])
        return res

    async def load_atlassian_ops_incidents(
        self,
        start: datetime,
        end: datetime,
    ) -> list[AtlassianOpsIncident]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        query = f"""
        SELECT * FROM atlassian_ops_incidents
        WHERE created_at >= {{start:DateTime}} AND created_at < {{end:DateTime}}
        {org_filter}
        """
        dicts = await _clickhouse_query_dicts(self.client, query, params)

        incidents: list[AtlassianOpsIncident] = []
        for r in dicts:
            incidents.append(
                AtlassianOpsIncident(
                    id=r.get("id", ""),
                    url=r.get("url"),
                    summary=r.get("summary", ""),
                    description=r.get("description"),
                    status=r.get("status", ""),
                    severity=r.get("severity", ""),
                    created_at=r.get("created_at") or datetime.now(timezone.utc),
                    provider_id=r.get("provider_id"),
                    last_synced=r.get("last_synced") or datetime.now(timezone.utc),
                )
            )
        return incidents

    async def load_atlassian_ops_alerts(
        self,
        start: datetime,
        end: datetime,
    ) -> list[AtlassianOpsAlert]:
        params: dict[str, Any] = {"start": naive_utc(start), "end": naive_utc(end)}
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        query = f"""
        SELECT * FROM atlassian_ops_alerts
        WHERE created_at >= {{start:DateTime}} AND created_at < {{end:DateTime}}
        {org_filter}
        """
        dicts = await _clickhouse_query_dicts(self.client, query, params)

        alerts: list[AtlassianOpsAlert] = []
        for r in dicts:
            alerts.append(
                AtlassianOpsAlert(
                    id=r.get("id", ""),
                    status=r.get("status", ""),
                    priority=r.get("priority", ""),
                    created_at=r.get("created_at") or datetime.now(timezone.utc),
                    acknowledged_at=r.get("acknowledged_at"),
                    snoozed_at=r.get("snoozed_at"),
                    closed_at=r.get("closed_at"),
                    last_synced=r.get("last_synced") or datetime.now(timezone.utc),
                )
            )
        return alerts

    async def load_atlassian_ops_schedules(
        self,
    ) -> list[AtlassianOpsSchedule]:
        params: dict[str, Any] = {}
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        if org_filter:
            query = f"SELECT * FROM atlassian_ops_schedules WHERE 1=1 {org_filter}"
        else:
            query = "SELECT * FROM atlassian_ops_schedules"
        dicts = await _clickhouse_query_dicts(self.client, query, params)

        schedules: list[AtlassianOpsSchedule] = []
        for r in dicts:
            schedules.append(
                AtlassianOpsSchedule(
                    id=r.get("id", ""),
                    name=r.get("name", ""),
                    timezone=r.get("timezone"),
                    last_synced=r.get("last_synced") or datetime.now(timezone.utc),
                )
            )
        return schedules

    async def load_jira_project_ops_team_links(
        self,
    ) -> list[JiraProjectOpsTeamLink]:
        params: dict[str, Any] = {}
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        if org_filter:
            query = f"SELECT * FROM jira_project_ops_team_links WHERE 1=1 {org_filter}"
        else:
            query = "SELECT * FROM jira_project_ops_team_links"
        dicts = await _clickhouse_query_dicts(self.client, query, params)

        links: list[JiraProjectOpsTeamLink] = []
        for r in dicts:
            links.append(
                JiraProjectOpsTeamLink(
                    project_key=r.get("project_key", ""),
                    ops_team_id=r.get("ops_team_id", ""),
                    project_name=r.get("project_name", ""),
                    ops_team_name=r.get("ops_team_name", ""),
                    updated_at=r.get("updated_at") or datetime.now(timezone.utc),
                )
            )
        return links

    async def load_user_metrics_rolling_30d(
        self,
        as_of: date,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"end": as_of, "start": as_of - timedelta(days=29)}
        org_filter = self._org_filter()
        params = self._inject_org_id(params)

        query = f"""
        SELECT
            identity_id,
            any(team_id) as team_id,
            sum(loc_touched) as churn_loc_30d,
            sum(delivery_units) as delivery_units_30d,
            median(cycle_p50_hours) as cycle_p50_30d_hours,
            max(work_items_active) as wip_max_30d
        FROM user_metrics_daily
        WHERE day >= {{start:Date}} AND day <= {{end:Date}}
        {org_filter}
        GROUP BY identity_id
        """
        return await _clickhouse_query_dicts(self.client, query, params)
