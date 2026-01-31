from __future__ import annotations

import asyncio
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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
)

from .utils import (
    _parse_date_value,
    _parse_datetime_value,
    model_to_dict,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.schemas import FileComplexitySnapshot
    from dev_health_ops.models.atlassian_ops import (
        AtlassianOpsAlert,
        AtlassianOpsIncident,
        AtlassianOpsSchedule,
    )
    from dev_health_ops.models.teams import JiraProjectOpsTeamLink, Team


class ClickHouseStore:
    """Async storage implementation backed by ClickHouse (via clickhouse-connect)."""

    def __init__(self, conn_string: str) -> None:
        if not conn_string:
            raise ValueError("ClickHouse connection string is required")
        self.conn_string = conn_string
        self.client = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "ClickHouseStore":
        import clickhouse_connect

        self.client = await asyncio.to_thread(
            clickhouse_connect.get_client, dsn=self.conn_string
        )
        await self._ensure_tables()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client is not None:
            await asyncio.to_thread(self.client.close)

    @staticmethod
    def _normalize_uuid(value: Any) -> uuid.UUID:
        if value is None:
            raise ValueError("UUID value is required")
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))

    @staticmethod
    def _normalize_datetime(value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, datetime):
            return value
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _json_or_none(value: Any) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value, default=str)

    async def _ensure_tables(self) -> None:
        assert self.client is not None

        # Locate migrations directory
        migrations_dir = Path(__file__).resolve().parent / "migrations" / "clickhouse"
        if not migrations_dir.exists():
            return

        async with self._lock:
            # Ensure schema_migrations table exists
            await asyncio.to_thread(
                self.client.command,
                "CREATE TABLE IF NOT EXISTS schema_migrations (version String, applied_at DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY version",
            )

            # Get applied migrations
            applied_result = await asyncio.to_thread(
                self.client.query, "SELECT version FROM schema_migrations"
            )
            applied_versions = set(
                row[0] for row in (getattr(applied_result, "result_rows", []) or [])
            )

            # Collect all migration files
            migration_files = sorted(
                list(migrations_dir.glob("*.sql")) + list(migrations_dir.glob("*.py"))
            )

            for path in migration_files:
                version = path.name
                if version in applied_versions:
                    continue

                if path.suffix == ".sql":
                    try:
                        sql = await asyncio.to_thread(path.read_text, encoding="utf-8")
                        for stmt in sql.split(";"):
                            stmt = stmt.strip()
                            if not stmt:
                                continue
                            await asyncio.to_thread(self.client.command, stmt)
                    except Exception as e:
                        print(f"CRITICAL: Migration failed: {path.name}\nError: {e}")
                        raise
                elif path.suffix == ".py":
                    # Dynamic import and execution for Python migrations
                    import importlib.util

                    spec = importlib.util.spec_from_file_location(
                        f"migrations.clickhouse.{path.stem}", path
                    )
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        if hasattr(module, "upgrade"):
                            await asyncio.to_thread(module.upgrade, self.client)

                # Record migration
                await asyncio.to_thread(
                    self.client.command,
                    "INSERT INTO schema_migrations (version, applied_at) VALUES ({version:String}, now())",
                    parameters={"version": version},
                )

    async def _insert_rows(
        self, table: str, columns: List[str], rows: List[Dict[str, Any]]
    ) -> None:
        if not rows:
            return
        assert self.client is not None
        matrix = [[row.get(col) for col in columns] for row in rows]
        async with self._lock:
            await asyncio.to_thread(
                self.client.insert, table, matrix, column_names=columns
            )

    async def _has_any(self, table: str, repo_id: uuid.UUID) -> bool:
        assert self.client is not None
        query = f"SELECT 1 FROM {table} WHERE repo_id = {{repo_id:UUID}} LIMIT 1"
        async with self._lock:
            result = await asyncio.to_thread(
                self.client.query, query, parameters={"repo_id": str(repo_id)}
            )
        return bool(getattr(result, "result_rows", None))

    async def insert_repo(self, repo: Repo) -> None:
        assert self.client is not None
        repo_id = self._normalize_uuid(getattr(repo, "id"))
        async with self._lock:
            existing = await asyncio.to_thread(
                self.client.query,
                "SELECT 1 FROM repos WHERE id = {id:UUID} LIMIT 1",
                parameters={"id": str(repo_id)},
            )
        if getattr(existing, "result_rows", None):
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        created_at = (
            self._normalize_datetime(getattr(repo, "created_at", None)) or synced_at
        )

        row = {
            "id": repo_id,
            "repo": getattr(repo, "repo"),
            "ref": getattr(repo, "ref", None),
            "created_at": created_at,
            "settings": self._json_or_none(getattr(repo, "settings", None)),
            "tags": self._json_or_none(getattr(repo, "tags", None)),
            "last_synced": synced_at,
        }
        await self._insert_rows(
            "repos",
            [
                "id",
                "repo",
                "ref",
                "created_at",
                "settings",
                "tags",
                "last_synced",
            ],
            [row],
        )

    async def get_all_repos(self) -> List[Repo]:
        assert self.client is not None
        query = "SELECT id, repo FROM repos"
        async with self._lock:
            result = await asyncio.to_thread(self.client.query, query)

        repos = []
        if result.result_rows:
            for row in result.result_rows:
                r_id = uuid.UUID(str(row[0]))
                r_name = row[1]
                # We return minimal Repo objects
                repos.append(Repo(id=r_id, repo=r_name))
        return repos

    async def get_complexity_snapshots(
        self,
        *,
        as_of_day: date,
        repo_id: Optional[uuid.UUID] = None,
        repo_name: Optional[str] = None,
    ) -> List["FileComplexitySnapshot"]:
        assert self.client is not None
        from dev_health_ops.metrics.schemas import FileComplexitySnapshot

        params: Dict[str, Any] = {"day": as_of_day}
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = " AND repo_id = {repo_id:UUID}"
        elif repo_name is not None:
            params["repo_name"] = repo_name
            repo_filter = (
                " AND repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String})"
            )

        query = f"""
        SELECT
          f.repo_id, f.as_of_day, f.ref, f.file_path, f.language, f.loc,
          f.functions_count, f.cyclomatic_total, f.cyclomatic_avg,
          f.high_complexity_functions, f.very_high_complexity_functions, f.computed_at
        FROM file_complexity_snapshots AS f
        INNER JOIN (
          SELECT repo_id, max(as_of_day) AS max_day
          FROM file_complexity_snapshots
          WHERE as_of_day <= {{day:Date}} {repo_filter}
          GROUP BY repo_id
        ) AS l
          ON (f.repo_id = l.repo_id) AND (f.as_of_day = l.max_day)
        """

        async with self._lock:
            result = await asyncio.to_thread(
                self.client.query, query, parameters=params
            )

        col_names = list(getattr(result, "column_names", []) or [])
        rows = list(getattr(result, "result_rows", []) or [])
        if not col_names or not rows:
            return []

        snapshots: List[FileComplexitySnapshot] = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            r_id = self._normalize_uuid(row_dict.get("repo_id"))
            file_path = row_dict.get("file_path")
            if not file_path:
                continue
            as_of_day_val = _parse_date_value(row_dict.get("as_of_day"))
            if as_of_day_val is None:
                continue
            computed_at_val = _parse_datetime_value(
                row_dict.get("computed_at")
            ) or datetime.now(timezone.utc)
            snapshots.append(
                FileComplexitySnapshot(
                    repo_id=r_id,
                    as_of_day=as_of_day_val,
                    ref=str(row_dict.get("ref") or ""),
                    file_path=str(file_path),
                    language=str(row_dict.get("language") or ""),
                    loc=int(row_dict.get("loc") or 0),
                    functions_count=int(row_dict.get("functions_count") or 0),
                    cyclomatic_total=int(row_dict.get("cyclomatic_total") or 0),
                    cyclomatic_avg=float(row_dict.get("cyclomatic_avg") or 0.0),
                    high_complexity_functions=int(
                        row_dict.get("high_complexity_functions") or 0
                    ),
                    very_high_complexity_functions=int(
                        row_dict.get("very_high_complexity_functions") or 0
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
        assert self.client is not None
        from dev_health_ops.metrics.schemas import WorkItemUserMetricsDailyRecord

        params: Dict[str, Any] = {"day": day}
        where = "WHERE day = {day:Date}"
        if provider:
            params["provider"] = provider
            where += " AND provider = {provider:String}"

        query = f"""
        SELECT
          day, provider, work_scope_id, user_identity, team_id, team_name,
          items_started, items_completed, wip_count_end_of_day,
          cycle_time_p50_hours, cycle_time_p90_hours, computed_at
        FROM work_item_user_metrics_daily
        {where}
        """

        async with self._lock:
            result = await asyncio.to_thread(
                self.client.query, query, parameters=params
            )

        col_names = list(getattr(result, "column_names", []) or [])
        rows = list(getattr(result, "result_rows", []) or [])
        if not col_names or not rows:
            return []

        out: List[WorkItemUserMetricsDailyRecord] = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            day_val = _parse_date_value(row_dict.get("day"))
            if day_val is None:
                continue
            user_identity = str(row_dict.get("user_identity") or "")
            if not user_identity:
                continue
            computed_at_val = _parse_datetime_value(
                row_dict.get("computed_at")
            ) or datetime.now(timezone.utc)
            out.append(
                WorkItemUserMetricsDailyRecord(
                    day=day_val,
                    provider=str(row_dict.get("provider") or ""),
                    work_scope_id=str(row_dict.get("work_scope_id") or ""),
                    user_identity=user_identity,
                    team_id=str(row_dict.get("team_id"))
                    if row_dict.get("team_id") is not None
                    else None,
                    team_name=str(row_dict.get("team_name"))
                    if row_dict.get("team_name") is not None
                    else None,
                    items_started=int(row_dict.get("items_started") or 0),
                    items_completed=int(row_dict.get("items_completed") or 0),
                    wip_count_end_of_day=int(row_dict.get("wip_count_end_of_day") or 0),
                    cycle_time_p50_hours=float(row_dict.get("cycle_time_p50_hours"))
                    if row_dict.get("cycle_time_p50_hours") is not None
                    else None,
                    cycle_time_p90_hours=float(row_dict.get("cycle_time_p90_hours"))
                    if row_dict.get("cycle_time_p90_hours") is not None
                    else None,
                    computed_at=computed_at_val,
                )
            )
        return out

    async def has_any_git_files(self, repo_id) -> bool:
        return await self._has_any("git_files", self._normalize_uuid(repo_id))

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        return await self._has_any("git_commit_stats", self._normalize_uuid(repo_id))

    async def has_any_git_blame(self, repo_id) -> bool:
        return await self._has_any("git_blame", self._normalize_uuid(repo_id))

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        if not file_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in file_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "path": item.get("path"),
                        "executable": 1 if item.get("executable") else 0,
                        "contents": item.get("contents"),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "path": getattr(item, "path"),
                        "executable": 1 if getattr(item, "executable") else 0,
                        "contents": getattr(item, "contents"),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_files",
            ["repo_id", "path", "executable", "contents", "last_synced"],
            rows,
        )

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        if not commit_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in commit_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "hash": item.get("hash"),
                        "message": item.get("message"),
                        "author_name": item.get("author_name"),
                        "author_email": item.get("author_email"),
                        "author_when": self._normalize_datetime(
                            item.get("author_when")
                        ),
                        "committer_name": item.get("committer_name"),
                        "committer_email": item.get("committer_email"),
                        "committer_when": self._normalize_datetime(
                            item.get("committer_when")
                        ),
                        "parents": int(item.get("parents") or 0),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "hash": getattr(item, "hash"),
                        "message": getattr(item, "message"),
                        "author_name": getattr(item, "author_name"),
                        "author_email": getattr(item, "author_email"),
                        "author_when": self._normalize_datetime(
                            getattr(item, "author_when")
                        ),
                        "committer_name": getattr(item, "committer_name"),
                        "committer_email": getattr(item, "committer_email"),
                        "committer_when": self._normalize_datetime(
                            getattr(item, "committer_when")
                        ),
                        "parents": int(getattr(item, "parents") or 0),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_commits",
            [
                "repo_id",
                "hash",
                "message",
                "author_name",
                "author_email",
                "author_when",
                "committer_name",
                "committer_email",
                "committer_when",
                "parents",
                "last_synced",
            ],
            rows,
        )

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        if not commit_stats:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in commit_stats:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "commit_hash": item.get("commit_hash"),
                        "file_path": item.get("file_path"),
                        "additions": int(item.get("additions") or 0),
                        "deletions": int(item.get("deletions") or 0),
                        "old_file_mode": item.get("old_file_mode") or "unknown",
                        "new_file_mode": item.get("new_file_mode") or "unknown",
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "commit_hash": getattr(item, "commit_hash"),
                        "file_path": getattr(item, "file_path"),
                        "additions": int(getattr(item, "additions") or 0),
                        "deletions": int(getattr(item, "deletions") or 0),
                        "old_file_mode": getattr(item, "old_file_mode", None)
                        or "unknown",
                        "new_file_mode": getattr(item, "new_file_mode", None)
                        or "unknown",
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_commit_stats",
            [
                "repo_id",
                "commit_hash",
                "file_path",
                "additions",
                "deletions",
                "old_file_mode",
                "new_file_mode",
                "last_synced",
            ],
            rows,
        )

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        if not data_batch:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in data_batch:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "path": item.get("path"),
                        "line_no": int(item.get("line_no") or 0),
                        "author_email": item.get("author_email"),
                        "author_name": item.get("author_name"),
                        "author_when": self._normalize_datetime(
                            item.get("author_when")
                        ),
                        "commit_hash": item.get("commit_hash"),
                        "line": item.get("line"),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "path": getattr(item, "path"),
                        "line_no": int(getattr(item, "line_no") or 0),
                        "author_email": getattr(item, "author_email"),
                        "author_name": getattr(item, "author_name"),
                        "author_when": self._normalize_datetime(
                            getattr(item, "author_when")
                        ),
                        "commit_hash": getattr(item, "commit_hash"),
                        "line": getattr(item, "line"),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_blame",
            [
                "repo_id",
                "path",
                "line_no",
                "author_email",
                "author_name",
                "author_when",
                "commit_hash",
                "line",
                "last_synced",
            ],
            rows,
        )

    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        if not pr_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in pr_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "number": int(item.get("number") or 0),
                        "title": item.get("title"),
                        "body": item.get("body"),
                        "state": item.get("state"),
                        "author_name": item.get("author_name"),
                        "author_email": item.get("author_email"),
                        "created_at": self._normalize_datetime(item.get("created_at")),
                        "merged_at": self._normalize_datetime(item.get("merged_at")),
                        "closed_at": self._normalize_datetime(item.get("closed_at")),
                        "head_branch": item.get("head_branch"),
                        "base_branch": item.get("base_branch"),
                        "additions": item.get("additions"),
                        "deletions": item.get("deletions"),
                        "changed_files": item.get("changed_files"),
                        "first_review_at": self._normalize_datetime(
                            item.get("first_review_at")
                        ),
                        "first_comment_at": self._normalize_datetime(
                            item.get("first_comment_at")
                        ),
                        "changes_requested_count": int(
                            item.get("changes_requested_count", 0) or 0
                        ),
                        "reviews_count": int(item.get("reviews_count", 0) or 0),
                        "comments_count": int(item.get("comments_count", 0) or 0),
                        "last_synced": self._normalize_datetime(
                            item.get("last_synced") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "number": int(getattr(item, "number") or 0),
                        "title": getattr(item, "title"),
                        "body": getattr(item, "body", None),
                        "state": getattr(item, "state"),
                        "author_name": getattr(item, "author_name"),
                        "author_email": getattr(item, "author_email"),
                        "created_at": self._normalize_datetime(
                            getattr(item, "created_at")
                        ),
                        "merged_at": self._normalize_datetime(
                            getattr(item, "merged_at")
                        ),
                        "closed_at": self._normalize_datetime(
                            getattr(item, "closed_at")
                        ),
                        "head_branch": getattr(item, "head_branch"),
                        "base_branch": getattr(item, "base_branch"),
                        "additions": getattr(item, "additions", None),
                        "deletions": getattr(item, "deletions", None),
                        "changed_files": getattr(item, "changed_files", None),
                        "first_review_at": self._normalize_datetime(
                            getattr(item, "first_review_at", None)
                        ),
                        "first_comment_at": self._normalize_datetime(
                            getattr(item, "first_comment_at", None)
                        ),
                        "changes_requested_count": int(
                            getattr(item, "changes_requested_count", 0) or 0
                        ),
                        "reviews_count": int(getattr(item, "reviews_count", 0) or 0),
                        "comments_count": int(getattr(item, "comments_count", 0) or 0),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_pull_requests",
            [
                "repo_id",
                "number",
                "title",
                "body",
                "state",
                "author_name",
                "author_email",
                "created_at",
                "merged_at",
                "closed_at",
                "head_branch",
                "base_branch",
                "additions",
                "deletions",
                "changed_files",
                "first_review_at",
                "first_comment_at",
                "changes_requested_count",
                "reviews_count",
                "comments_count",
                "last_synced",
            ],
            rows,
        )

    async def insert_git_pull_request_reviews(
        self, review_data: List[GitPullRequestReview]
    ) -> None:
        if not review_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in review_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "number": int(item.get("number") or 0),
                        "review_id": str(item.get("review_id")),
                        "reviewer": str(item.get("reviewer")),
                        "state": str(item.get("state")),
                        "submitted_at": self._normalize_datetime(
                            item.get("submitted_at")
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
                        "number": int(getattr(item, "number") or 0),
                        "review_id": str(getattr(item, "review_id")),
                        "reviewer": str(getattr(item, "reviewer")),
                        "state": str(getattr(item, "state")),
                        "submitted_at": self._normalize_datetime(
                            getattr(item, "submitted_at")
                        ),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_pull_request_reviews",
            [
                "repo_id",
                "number",
                "review_id",
                "reviewer",
                "state",
                "submitted_at",
                "last_synced",
            ],
            rows,
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
                        "run_id": str(item.get("run_id")),
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
                        "run_id": str(getattr(item, "run_id")),
                        "status": getattr(item, "status", None),
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
                        "deployment_id": str(item.get("deployment_id")),
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
                        "deployment_id": str(getattr(item, "deployment_id")),
                        "status": getattr(item, "status", None),
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
        if not incidents:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in incidents:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "incident_id": str(item.get("incident_id")),
                        "status": item.get("status"),
                        "started_at": self._normalize_datetime(item.get("started_at")),
                        "resolved_at": self._normalize_datetime(
                            item.get("resolved_at")
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
                        "incident_id": str(getattr(item, "incident_id")),
                        "status": getattr(item, "status", None),
                        "started_at": self._normalize_datetime(
                            getattr(item, "started_at")
                        ),
                        "resolved_at": self._normalize_datetime(
                            getattr(item, "resolved_at", None)
                        ),
                        "last_synced": self._normalize_datetime(
                            getattr(item, "last_synced", None) or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "incidents",
            [
                "repo_id",
                "incident_id",
                "status",
                "started_at",
                "resolved_at",
                "last_synced",
            ],
            rows,
        )

    async def insert_teams(self, teams: List["Team"]) -> None:
        if not teams:
            return
        # Note: Imports inside method to avoid circular deps if models imports storage

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in teams:
            if isinstance(item, dict):
                rows.append(
                    {
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "description": item.get("description"),
                        "members": item.get("members") or [],
                        "updated_at": self._normalize_datetime(item.get("updated_at")),
                        "last_synced": synced_at,
                    }
                )
            else:
                rows.append(
                    {
                        "id": getattr(item, "id"),
                        "team_uuid": self._normalize_uuid(getattr(item, "team_uuid")),
                        "name": getattr(item, "name"),
                        "description": getattr(item, "description"),
                        "members": getattr(item, "members", []) or [],
                        "updated_at": self._normalize_datetime(
                            getattr(item, "updated_at")
                        ),
                        "last_synced": synced_at,
                    }
                )

        await self._insert_rows(
            "teams",
            [
                "id",
                "team_uuid",
                "name",
                "description",
                "members",
                "updated_at",
                "last_synced",
            ],
            rows,
        )

    async def insert_jira_project_ops_team_links(
        self, links: List[JiraProjectOpsTeamLink]
    ) -> None:
        if not links:
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in links:
            if isinstance(item, dict):
                rows.append(
                    {
                        "project_key": item.get("project_key"),
                        "ops_team_id": item.get("ops_team_id"),
                        "project_name": item.get("project_name"),
                        "ops_team_name": item.get("ops_team_name"),
                        "updated_at": self._normalize_datetime(item.get("updated_at")),
                        "last_synced": synced_at,
                    }
                )
            else:
                rows.append(
                    {
                        "project_key": getattr(item, "project_key"),
                        "ops_team_id": getattr(item, "ops_team_id"),
                        "project_name": getattr(item, "project_name"),
                        "ops_team_name": getattr(item, "ops_team_name"),
                        "updated_at": self._normalize_datetime(
                            getattr(item, "updated_at")
                        ),
                        "last_synced": synced_at,
                    }
                )

        await self._insert_rows(
            "jira_project_ops_team_links",
            [
                "project_key",
                "ops_team_id",
                "project_name",
                "ops_team_name",
                "updated_at",
                "last_synced",
            ],
            rows,
        )

    async def insert_atlassian_ops_incidents(
        self, incidents: List[AtlassianOpsIncident]
    ) -> None:
        if not incidents:
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in incidents:
            rows.append(
                {
                    "id": item.id,
                    "url": item.url,
                    "summary": item.summary,
                    "description": item.description,
                    "status": item.status,
                    "severity": item.severity,
                    "created_at": self._normalize_datetime(item.created_at),
                    "provider_id": item.provider_id,
                    "last_synced": synced_at,
                }
            )

        await self._insert_rows(
            "atlassian_ops_incidents",
            [
                "id",
                "url",
                "summary",
                "description",
                "status",
                "severity",
                "created_at",
                "provider_id",
                "last_synced",
            ],
            rows,
        )

    async def insert_atlassian_ops_alerts(
        self, alerts: List[AtlassianOpsAlert]
    ) -> None:
        if not alerts:
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in alerts:
            rows.append(
                {
                    "id": item.id,
                    "status": item.status,
                    "priority": item.priority,
                    "created_at": self._normalize_datetime(item.created_at),
                    "acknowledged_at": self._normalize_datetime(item.acknowledged_at),
                    "snoozed_at": self._normalize_datetime(item.snoozed_at),
                    "closed_at": self._normalize_datetime(item.closed_at),
                    "last_synced": synced_at,
                }
            )

        await self._insert_rows(
            "atlassian_ops_alerts",
            [
                "id",
                "status",
                "priority",
                "created_at",
                "acknowledged_at",
                "snoozed_at",
                "closed_at",
                "last_synced",
            ],
            rows,
        )

    async def insert_atlassian_ops_schedules(
        self, schedules: List[AtlassianOpsSchedule]
    ) -> None:
        if not schedules:
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in schedules:
            rows.append(
                {
                    "id": item.id,
                    "name": item.name,
                    "timezone": item.timezone,
                    "last_synced": synced_at,
                }
            )

        await self._insert_rows(
            "atlassian_ops_schedules",
            [
                "id",
                "name",
                "timezone",
                "last_synced",
            ],
            rows,
        )

    async def get_all_teams(self) -> List["Team"]:
        from dev_health_ops.models.teams import Team

        assert self.client is not None
        # Using FINAL to get the latest version of each team
        query = "SELECT id, team_uuid, name, description, members, updated_at FROM teams FINAL"
        async with self._lock:
            result = await asyncio.to_thread(self.client.query, query)

        teams = []
        if result.result_rows:
            for row in result.result_rows:
                teams.append(
                    Team(
                        id=row[0],
                        team_uuid=row[1],
                        name=row[2],
                        description=row[3],
                        members=row[4],
                        updated_at=_parse_datetime_value(row[5]),
                    )
                )
        return teams

    async def get_jira_project_ops_team_links(self) -> List["JiraProjectOpsTeamLink"]:
        from dev_health_ops.models.teams import JiraProjectOpsTeamLink

        assert self.client is not None
        query = "SELECT project_key, ops_team_id, project_name, ops_team_name, updated_at FROM jira_project_ops_team_links FINAL"
        async with self._lock:
            result = await asyncio.to_thread(self.client.query, query)

        links = []
        if result.result_rows:
            for row in result.result_rows:
                links.append(
                    JiraProjectOpsTeamLink(
                        project_key=row[0],
                        ops_team_id=row[1],
                        project_name=row[2],
                        ops_team_name=row[3],
                        updated_at=_parse_datetime_value(row[4]),
                    )
                )
        return links

    async def insert_work_items(self, work_items: List["WorkItem"]) -> None:
        if not work_items:
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []

        for item in work_items:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None: getattr(item, k, default)
            )

            repo_id_val = get("repo_id")
            if repo_id_val:
                repo_id_val = self._normalize_uuid(repo_id_val)
            else:
                repo_id_val = uuid.UUID(int=0)

            rows.append(
                {
                    "repo_id": repo_id_val,
                    "work_item_id": str(get("work_item_id")),
                    "provider": str(get("provider")),
                    "title": str(get("title")),
                    "description": get("description"),
                    "type": str(get("type")),
                    "status": str(get("status")),
                    "status_raw": str(get("status_raw") or ""),
                    "project_key": str(get("project_key") or ""),
                    "project_id": str(get("project_id") or ""),
                    "assignees": get("assignees") or [],
                    "reporter": str(get("reporter") or ""),
                    "created_at": self._normalize_datetime(get("created_at")),
                    "updated_at": self._normalize_datetime(get("updated_at")),
                    "started_at": self._normalize_datetime(get("started_at")),
                    "completed_at": self._normalize_datetime(get("completed_at")),
                    "closed_at": self._normalize_datetime(get("closed_at")),
                    "labels": get("labels") or [],
                    "story_points": float(get("story_points"))
                    if get("story_points") is not None
                    else None,
                    "sprint_id": str(get("sprint_id") or ""),
                    "sprint_name": str(get("sprint_name") or ""),
                    "parent_id": str(get("parent_id") or ""),
                    "epic_id": str(get("epic_id") or ""),
                    "url": str(get("url") or ""),
                    "priority_raw": str(get("priority_raw") or ""),
                    "service_class": str(get("service_class") or ""),
                    "due_at": self._normalize_datetime(get("due_at")),
                    "last_synced": synced_at,
                }
            )

        await self._insert_rows(
            "work_items",
            [
                "repo_id",
                "work_item_id",
                "provider",
                "title",
                "description",
                "type",
                "status",
                "status_raw",
                "project_key",
                "project_id",
                "assignees",
                "reporter",
                "created_at",
                "updated_at",
                "started_at",
                "completed_at",
                "closed_at",
                "labels",
                "story_points",
                "sprint_id",
                "sprint_name",
                "parent_id",
                "epic_id",
                "url",
                "priority_raw",
                "service_class",
                "due_at",
                "last_synced",
            ],
            rows,
        )

    async def insert_work_item_transitions(
        self, transitions: List["WorkItemStatusTransition"]
    ) -> None:
        if not transitions:
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []

        for item in transitions:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None: getattr(item, k, default)
            )

            repo_id_val = get("repo_id")
            if repo_id_val:
                repo_id_val = self._normalize_uuid(repo_id_val)
            else:
                repo_id_val = uuid.UUID(int=0)

            rows.append(
                {
                    "repo_id": repo_id_val,
                    "work_item_id": str(get("work_item_id")),
                    "occurred_at": self._normalize_datetime(get("occurred_at")),
                    "provider": str(get("provider")),
                    "from_status": str(get("from_status")),
                    "to_status": str(get("to_status")),
                    "from_status_raw": str(get("from_status_raw") or ""),
                    "to_status_raw": str(get("to_status_raw") or ""),
                    "actor": str(get("actor") or ""),
                    "last_synced": synced_at,
                }
            )

        await self._insert_rows(
            "work_item_transitions",
            [
                "repo_id",
                "work_item_id",
                "occurred_at",
                "provider",
                "from_status",
                "to_status",
                "from_status_raw",
                "to_status_raw",
                "actor",
                "last_synced",
            ],
            rows,
        )

    async def insert_work_item_dependencies(
        self, dependencies: List["WorkItemDependency"]
    ) -> None:
        if not dependencies:
            return

        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []

        for item in dependencies:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None: getattr(item, k, default)
            )

            rows.append(
                {
                    "source_work_item_id": str(get("source_work_item_id")),
                    "target_work_item_id": str(get("target_work_item_id")),
                    "relationship_type": str(get("relationship_type") or ""),
                    "relationship_type_raw": str(get("relationship_type_raw") or ""),
                    "last_synced": self._normalize_datetime(
                        get("last_synced") or synced_at_default
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

    async def insert_work_graph_issue_pr(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        columns = [
            "repo_id",
            "work_item_id",
            "pr_number",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
        ]
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in records:
            rows.append(
                {
                    "repo_id": self._normalize_uuid(item.get("repo_id")),
                    "work_item_id": str(item.get("work_item_id") or ""),
                    "pr_number": int(item.get("pr_number") or 0),
                    "confidence": float(item.get("confidence") or 1.0),
                    "provenance": str(item.get("provenance") or ""),
                    "evidence": str(item.get("evidence") or ""),
                    "last_synced": self._normalize_datetime(
                        item.get("last_synced") or synced_at_default
                    ),
                }
            )

        await self._insert_rows("work_graph_issue_pr", columns, rows)

    async def insert_work_graph_pr_commit(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        columns = [
            "repo_id",
            "pr_number",
            "commit_hash",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
        ]
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in records:
            rows.append(
                {
                    "repo_id": self._normalize_uuid(item.get("repo_id")),
                    "pr_number": int(item.get("pr_number") or 0),
                    "commit_hash": str(item.get("commit_hash") or ""),
                    "confidence": float(item.get("confidence") or 1.0),
                    "provenance": str(item.get("provenance") or ""),
                    "evidence": str(item.get("evidence") or ""),
                    "last_synced": self._normalize_datetime(
                        item.get("last_synced") or synced_at_default
                    ),
                }
            )

        await self._insert_rows("work_graph_pr_commit", columns, rows)
