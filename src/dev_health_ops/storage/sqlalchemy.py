from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional  # noqa: F401

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    and_,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

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

from dev_health_ops.metrics.schemas import FileComplexitySnapshot
from dev_health_ops.metrics.schemas import WorkItemUserMetricsDailyRecord

from .utils import _parse_date_value, _parse_datetime_value

ColumnGetter = Callable[[Any, str], Any]

if TYPE_CHECKING:
    from dev_health_ops.models.atlassian_ops import (
        AtlassianOpsAlert,
        AtlassianOpsIncident,
        AtlassianOpsSchedule,
    )
    from dev_health_ops.models.teams import JiraProjectOpsTeamLink, Team


class SQLAlchemyStore:
    """Async storage implementation backed by SQLAlchemy."""

    def __init__(self, conn_string: str, echo: bool = False) -> None:
        # Configure connection pool for better performance (PostgreSQL/MySQL only)
        engine_kwargs: Dict[str, Any] = {"echo": echo}

        # Only add pooling parameters for databases that support them
        if "sqlite" not in conn_string.lower():
            engine_kwargs.update(
                {
                    "pool_size": 20,  # Increased from default 5
                    "max_overflow": 30,  # Increased from default 10
                    "pool_pre_ping": True,  # Verify connections before using
                    "pool_recycle": 3600,  # Recycle connections after 1 hour
                }
            )

        self.engine = create_async_engine(conn_string, **engine_kwargs)
        self.session_factory = async_sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )
        self.session: Optional[AsyncSession] = None
        self._work_item_metadata = MetaData()
        self._work_items_table = Table(
            "work_items",
            self._work_item_metadata,
            Column("work_item_id", String, primary_key=True),
            Column("repo_id", String),
            Column("provider", String),
            Column("title", String),
            Column("description", String),
            Column("type", String),
            Column("status", String),
            Column("status_raw", String),
            Column("project_key", String),
            Column("project_id", String),
            Column("assignees", JSON),
            Column("reporter", String),
            Column("created_at", DateTime(timezone=True)),
            Column("updated_at", DateTime(timezone=True)),
            Column("started_at", DateTime(timezone=True)),
            Column("completed_at", DateTime(timezone=True)),
            Column("closed_at", DateTime(timezone=True)),
            Column("labels", JSON),
            Column("story_points", Float),
            Column("sprint_id", String),
            Column("sprint_name", String),
            Column("parent_id", String),
            Column("epic_id", String),
            Column("url", String),
            Column("priority_raw", String),
            Column("service_class", String),
            Column("due_at", DateTime(timezone=True)),
            Column("last_synced", DateTime(timezone=True)),
        )
        self._work_item_transitions_table = Table(
            "work_item_transitions",
            self._work_item_metadata,
            Column("work_item_id", String, primary_key=True),
            Column("occurred_at", DateTime(timezone=True), primary_key=True),
            Column("repo_id", String),
            Column("provider", String),
            Column("from_status", String),
            Column("to_status", String),
            Column("from_status_raw", String),
            Column("to_status_raw", String),
            Column("actor", String),
            Column("last_synced", DateTime(timezone=True)),
        )
        self._work_item_dependencies_table = Table(
            "work_item_dependencies",
            self._work_item_metadata,
            Column("source_work_item_id", String, primary_key=True),
            Column("target_work_item_id", String, primary_key=True),
            Column("relationship_type", String, primary_key=True),
            Column("relationship_type_raw", String),
            Column("last_synced", DateTime(timezone=True)),
        )
        self._work_graph_issue_pr_table = Table(
            "work_graph_issue_pr",
            self._work_item_metadata,
            Column("repo_id", String, primary_key=True),
            Column("work_item_id", String, primary_key=True),
            Column("pr_number", Integer, primary_key=True),
            Column("confidence", Float),
            Column("provenance", String),
            Column("evidence", String),
            Column("last_synced", DateTime(timezone=True)),
        )
        self._work_graph_pr_commit_table = Table(
            "work_graph_pr_commit",
            self._work_item_metadata,
            Column("repo_id", String, primary_key=True),
            Column("pr_number", Integer, primary_key=True),
            Column("commit_hash", String, primary_key=True),
            Column("confidence", Float),
            Column("provenance", String),
            Column("evidence", String),
            Column("last_synced", DateTime(timezone=True)),
        )

    def _insert_for_dialect(self, model: Any):
        dialect = self.engine.dialect.name
        if dialect == "sqlite":
            return sqlite_insert(model)
        if dialect in ("postgres", "postgresql"):
            return pg_insert(model)
        raise ValueError(f"Unsupported SQL dialect for upserts: {dialect}")

    async def _upsert_many(
        self,
        model: Any,
        rows: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: List[str],
    ) -> None:
        if not rows:
            return
        assert self.session is not None

        def _column(obj: Any, name: str) -> Any:
            if hasattr(obj, "c"):
                return obj.c[name]
            return getattr(obj, name)

        column_getter: ColumnGetter = _column

        stmt = self._insert_for_dialect(model)
        stmt = stmt.on_conflict_do_update(
            index_elements=[column_getter(model, col) for col in conflict_columns],
            set_={col: getattr(stmt.excluded, col) for col in update_columns},
        )
        await self.session.execute(stmt, rows)
        await self.session.commit()

    async def __aenter__(self) -> "SQLAlchemyStore":
        self.session = self.session_factory()

        # Create tables for SQLite automatically
        if "sqlite" in str(self.engine.url):
            from dev_health_ops.models.git import Base

            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                await conn.run_sync(self._work_item_metadata.create_all)

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None
        await self.engine.dispose()

    async def ensure_tables(self) -> None:
        from dev_health_ops.models.git import Base
        import dev_health_ops.models.teams  # noqa: F401

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(self._work_item_metadata.create_all)

    async def insert_repo(self, repo: Repo) -> None:
        assert self.session is not None
        existing_repo = await self.session.get(Repo, repo.id)
        if not existing_repo:
            self.session.add(repo)
            await self.session.commit()

    async def get_all_repos(self) -> List[Repo]:
        assert self.session is not None
        result = await self.session.execute(select(Repo))
        return list(result.scalars().all())

    async def get_complexity_snapshots(
        self,
        *,
        as_of_day: date,
        repo_id: Optional[uuid.UUID] = None,
        repo_name: Optional[str] = None,
    ) -> List["FileComplexitySnapshot"]:
        """
        Return the latest file complexity snapshot rows per repo <= as_of_day.

        When repo_id/repo_name is provided, returns snapshots for that single repo.
        """
        assert self.session is not None
        from dev_health_ops.metrics.schemas import FileComplexitySnapshot

        resolved_repo_id = repo_id
        if resolved_repo_id is None and repo_name:
            repo_res = await self.session.execute(
                select(Repo.id).where(Repo.repo == repo_name).limit(1)
            )
            repo_row = repo_res.first()
            if not repo_row or not repo_row[0]:
                return []
            resolved_repo_id = uuid.UUID(str(repo_row[0]))

        # Table is created by metrics sinks; we define a lightweight Core table for queries.
        snapshots_table = Table(
            "file_complexity_snapshots",
            MetaData(),
            Column("repo_id", String),
            Column("as_of_day", String),
            Column("ref", String),
            Column("file_path", String),
            Column("language", String),
            Column("loc", Integer),
            Column("functions_count", Integer),
            Column("cyclomatic_total", Integer),
            Column("cyclomatic_avg", Float),
            Column("high_complexity_functions", Integer),
            Column("very_high_complexity_functions", Integer),
            Column("computed_at", String),
        )

        day_value = as_of_day.isoformat()
        where_clause = snapshots_table.c.as_of_day <= day_value
        if resolved_repo_id is not None:
            where_clause = and_(
                where_clause, snapshots_table.c.repo_id == str(resolved_repo_id)
            )

        latest = (
            select(
                snapshots_table.c.repo_id,
                func.max(snapshots_table.c.as_of_day).label("max_day"),
            )
            .where(where_clause)
            .group_by(snapshots_table.c.repo_id)
            .subquery("latest")
        )

        query = select(
            snapshots_table.c.repo_id,
            snapshots_table.c.as_of_day,
            snapshots_table.c.ref,
            snapshots_table.c.file_path,
            snapshots_table.c.language,
            snapshots_table.c.loc,
            snapshots_table.c.functions_count,
            snapshots_table.c.cyclomatic_total,
            snapshots_table.c.cyclomatic_avg,
            snapshots_table.c.high_complexity_functions,
            snapshots_table.c.very_high_complexity_functions,
            snapshots_table.c.computed_at,
        ).select_from(
            snapshots_table.join(
                latest,
                and_(
                    snapshots_table.c.repo_id == latest.c.repo_id,
                    snapshots_table.c.as_of_day == latest.c.max_day,
                ),
            )
        )

        res = await self.session.execute(query)
        rows = res.fetchall()

        snapshots: List[FileComplexitySnapshot] = []
        for r in rows:
            r_id = uuid.UUID(str(r[0]))
            as_of_day_val = _parse_date_value(r[1])
            if as_of_day_val is None:
                continue
            file_path = str(r[3] or "")
            if not file_path:
                continue
            computed_at_val = _parse_datetime_value(r[11]) or datetime.now(timezone.utc)
            snapshots.append(
                FileComplexitySnapshot(
                    repo_id=r_id,
                    as_of_day=as_of_day_val,
                    ref=str(r[2] or ""),
                    file_path=file_path,
                    language=str(r[4] or ""),
                    loc=int(r[5] or 0),
                    functions_count=int(r[6] or 0),
                    cyclomatic_total=int(r[7] or 0),
                    cyclomatic_avg=float(r[8] or 0.0),
                    high_complexity_functions=int(r[9] or 0),
                    very_high_complexity_functions=int(r[10] or 0),
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
        assert self.session is not None
        from dev_health_ops.metrics.schemas import WorkItemUserMetricsDailyRecord

        table = Table(
            "work_item_user_metrics_daily",
            MetaData(),
            Column("day", String),
            Column("provider", String),
            Column("work_scope_id", String),
            Column("user_identity", String),
            Column("team_id", String),
            Column("team_name", String),
            Column("items_started", Integer),
            Column("items_completed", Integer),
            Column("wip_count_end_of_day", Integer),
            Column("cycle_time_p50_hours", Float),
            Column("cycle_time_p90_hours", Float),
            Column("computed_at", String),
        )

        where_clause = table.c.day == day.isoformat()
        if provider:
            where_clause = and_(where_clause, table.c.provider == provider)

        query = select(
            table.c.day,
            table.c.provider,
            table.c.work_scope_id,
            table.c.user_identity,
            table.c.team_id,
            table.c.team_name,
            table.c.items_started,
            table.c.items_completed,
            table.c.wip_count_end_of_day,
            table.c.cycle_time_p50_hours,
            table.c.cycle_time_p90_hours,
            table.c.computed_at,
        ).where(where_clause)

        res = await self.session.execute(query)
        rows = res.fetchall()

        out: List[WorkItemUserMetricsDailyRecord] = []
        for r in rows:
            day_val = _parse_date_value(r[0])
            if day_val is None:
                continue
            user_identity = str(r[3] or "")
            if not user_identity:
                continue
            computed_at_val = _parse_datetime_value(r[11]) or datetime.now(timezone.utc)
            out.append(
                WorkItemUserMetricsDailyRecord(
                    day=day_val,
                    provider=str(r[1] or ""),
                    work_scope_id=str(r[2] or ""),
                    user_identity=user_identity,
                    team_id=str(r[4]) if r[4] is not None else None,
                    team_name=str(r[5]) if r[5] is not None else None,
                    items_started=int(r[6] or 0),
                    items_completed=int(r[7] or 0),
                    wip_count_end_of_day=int(r[8] or 0),
                    cycle_time_p50_hours=float(r[9]) if r[9] is not None else None,
                    cycle_time_p90_hours=float(r[10]) if r[10] is not None else None,
                    computed_at=computed_at_val,
                )
            )
        return out

    async def has_any_git_files(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count()).select_from(GitFile).where(GitFile.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitCommitStat)
            .where(GitCommitStat.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_blame(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitBlame)
            .where(GitBlame.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        if not file_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in file_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "path": item.get("path"),
                    "executable": item.get("executable"),
                    "contents": item.get("contents"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "path": getattr(item, "path"),
                    "executable": getattr(item, "executable"),
                    "contents": getattr(item, "contents"),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitFile,
            rows,
            conflict_columns=["repo_id", "path"],
            update_columns=["executable", "contents", "last_synced"],
        )

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        if not commit_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in commit_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "hash": item.get("hash"),
                    "message": item.get("message"),
                    "author_name": item.get("author_name"),
                    "author_email": item.get("author_email"),
                    "author_when": item.get("author_when"),
                    "committer_name": item.get("committer_name"),
                    "committer_email": item.get("committer_email"),
                    "committer_when": item.get("committer_when"),
                    "parents": item.get("parents"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "hash": getattr(item, "hash"),
                    "message": getattr(item, "message"),
                    "author_name": getattr(item, "author_name"),
                    "author_email": getattr(item, "author_email"),
                    "author_when": getattr(item, "author_when"),
                    "committer_name": getattr(item, "committer_name"),
                    "committer_email": getattr(item, "committer_email"),
                    "committer_when": getattr(item, "committer_when"),
                    "parents": getattr(item, "parents"),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitCommit,
            rows,
            conflict_columns=["repo_id", "hash"],
            update_columns=[
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
        )

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        if not commit_stats:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in commit_stats:
            if isinstance(item, dict):
                old_mode = item.get("old_file_mode") or "unknown"
                new_mode = item.get("new_file_mode") or "unknown"
                row = {
                    "repo_id": item.get("repo_id"),
                    "commit_hash": item.get("commit_hash"),
                    "file_path": item.get("file_path"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                    "old_file_mode": old_mode,
                    "new_file_mode": new_mode,
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                old_mode = getattr(item, "old_file_mode", None) or "unknown"
                new_mode = getattr(item, "new_file_mode", None) or "unknown"
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "commit_hash": getattr(item, "commit_hash"),
                    "file_path": getattr(item, "file_path"),
                    "additions": getattr(item, "additions"),
                    "deletions": getattr(item, "deletions"),
                    "old_file_mode": old_mode,
                    "new_file_mode": new_mode,
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitCommitStat,
            rows,
            conflict_columns=["repo_id", "commit_hash", "file_path"],
            update_columns=[
                "additions",
                "deletions",
                "old_file_mode",
                "new_file_mode",
                "last_synced",
            ],
        )

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        if not data_batch:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in data_batch:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "path": item.get("path"),
                    "line_no": item.get("line_no"),
                    "author_email": item.get("author_email"),
                    "author_name": item.get("author_name"),
                    "author_when": item.get("author_when"),
                    "commit_hash": item.get("commit_hash"),
                    "line": item.get("line"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "path": getattr(item, "path"),
                    "line_no": getattr(item, "line_no"),
                    "author_email": getattr(item, "author_email"),
                    "author_name": getattr(item, "author_name"),
                    "author_when": getattr(item, "author_when"),
                    "commit_hash": getattr(item, "commit_hash"),
                    "line": getattr(item, "line"),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitBlame,
            rows,
            conflict_columns=["repo_id", "path", "line_no"],
            update_columns=[
                "author_email",
                "author_name",
                "author_when",
                "commit_hash",
                "line",
                "last_synced",
            ],
        )

    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        if not pr_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in pr_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "number": item.get("number"),
                    "title": item.get("title"),
                    "body": item.get("body"),
                    "state": item.get("state"),
                    "author_name": item.get("author_name"),
                    "author_email": item.get("author_email"),
                    "created_at": item.get("created_at"),
                    "merged_at": item.get("merged_at"),
                    "closed_at": item.get("closed_at"),
                    "head_branch": item.get("head_branch"),
                    "base_branch": item.get("base_branch"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                    "changed_files": item.get("changed_files"),
                    "first_review_at": item.get("first_review_at"),
                    "first_comment_at": item.get("first_comment_at"),
                    "changes_requested_count": item.get("changes_requested_count", 0),
                    "reviews_count": item.get("reviews_count", 0),
                    "comments_count": item.get("comments_count", 0),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "number": getattr(item, "number"),
                    "title": getattr(item, "title"),
                    "body": getattr(item, "body", None),
                    "state": getattr(item, "state"),
                    "author_name": getattr(item, "author_name"),
                    "author_email": getattr(item, "author_email"),
                    "created_at": getattr(item, "created_at"),
                    "merged_at": getattr(item, "merged_at"),
                    "closed_at": getattr(item, "closed_at"),
                    "head_branch": getattr(item, "head_branch"),
                    "base_branch": getattr(item, "base_branch"),
                    "additions": getattr(item, "additions", None),
                    "deletions": getattr(item, "deletions", None),
                    "changed_files": getattr(item, "changed_files", None),
                    "first_review_at": getattr(item, "first_review_at", None),
                    "first_comment_at": getattr(item, "first_comment_at", None),
                    "changes_requested_count": getattr(
                        item, "changes_requested_count", 0
                    ),
                    "reviews_count": getattr(item, "reviews_count", 0),
                    "comments_count": getattr(item, "comments_count", 0),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitPullRequest,
            rows,
            conflict_columns=["repo_id", "number"],
            update_columns=[
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
        )

    async def insert_git_pull_request_reviews(
        self, review_data: List[GitPullRequestReview]
    ) -> None:
        if not review_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in review_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "number": item.get("number"),
                    "review_id": item.get("review_id"),
                    "reviewer": item.get("reviewer"),
                    "state": item.get("state"),
                    "submitted_at": item.get("submitted_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "number": getattr(item, "number"),
                    "review_id": getattr(item, "review_id"),
                    "reviewer": getattr(item, "reviewer"),
                    "state": getattr(item, "state"),
                    "submitted_at": getattr(item, "submitted_at"),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitPullRequestReview,
            rows,
            conflict_columns=["repo_id", "number", "review_id"],
            update_columns=[
                "reviewer",
                "state",
                "submitted_at",
                "last_synced",
            ],
        )

    async def insert_ci_pipeline_runs(self, runs: List[CiPipelineRun]) -> None:
        if not runs:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in runs:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "run_id": item.get("run_id"),
                    "status": item.get("status"),
                    "queued_at": item.get("queued_at"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "run_id": getattr(item, "run_id"),
                    "status": getattr(item, "status"),
                    "queued_at": getattr(item, "queued_at", None),
                    "started_at": getattr(item, "started_at"),
                    "finished_at": getattr(item, "finished_at", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            CiPipelineRun,
            rows,
            conflict_columns=["repo_id", "run_id"],
            update_columns=[
                "status",
                "queued_at",
                "started_at",
                "finished_at",
                "last_synced",
            ],
        )

    async def insert_deployments(self, deployments: List[Deployment]) -> None:
        if not deployments:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in deployments:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "deployment_id": item.get("deployment_id"),
                    "status": item.get("status"),
                    "environment": item.get("environment"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "deployed_at": item.get("deployed_at"),
                    "merged_at": item.get("merged_at"),
                    "pull_request_number": item.get("pull_request_number"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "deployment_id": getattr(item, "deployment_id"),
                    "status": getattr(item, "status"),
                    "environment": getattr(item, "environment", None),
                    "started_at": getattr(item, "started_at", None),
                    "finished_at": getattr(item, "finished_at", None),
                    "deployed_at": getattr(item, "deployed_at", None),
                    "merged_at": getattr(item, "merged_at", None),
                    "pull_request_number": getattr(item, "pull_request_number", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            Deployment,
            rows,
            conflict_columns=["repo_id", "deployment_id"],
            update_columns=[
                "status",
                "environment",
                "started_at",
                "finished_at",
                "deployed_at",
                "merged_at",
                "pull_request_number",
                "last_synced",
            ],
        )

    async def insert_incidents(self, incidents: List[Incident]) -> None:
        if not incidents:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in incidents:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "incident_id": item.get("incident_id"),
                    "status": item.get("status"),
                    "started_at": item.get("started_at"),
                    "resolved_at": item.get("resolved_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "incident_id": getattr(item, "incident_id"),
                    "status": getattr(item, "status"),
                    "started_at": getattr(item, "started_at"),
                    "resolved_at": getattr(item, "resolved_at", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            Incident,
            rows,
            conflict_columns=["repo_id", "incident_id"],
            update_columns=[
                "status",
                "started_at",
                "resolved_at",
                "last_synced",
            ],
        )

    async def insert_work_item_dependencies(
        self, dependencies: List[WorkItemDependency]
    ) -> None:
        if not dependencies:
            return
        rows: List[Dict[str, Any]] = []
        synced_at_default = datetime.now(timezone.utc)
        for item in dependencies:
            if isinstance(item, dict):
                rows.append(
                    {
                        "source_work_item_id": item.get("source_work_item_id"),
                        "target_work_item_id": item.get("target_work_item_id"),
                        "relationship_type": item.get("relationship_type"),
                        "relationship_type_raw": item.get("relationship_type_raw"),
                        "last_synced": item.get("last_synced") or synced_at_default,
                    }
                )
            else:
                rows.append(
                    {
                        "source_work_item_id": getattr(item, "source_work_item_id"),
                        "target_work_item_id": getattr(item, "target_work_item_id"),
                        "relationship_type": getattr(item, "relationship_type"),
                        "relationship_type_raw": getattr(item, "relationship_type_raw"),
                        "last_synced": getattr(item, "last_synced", None)
                        or synced_at_default,
                    }
                )

        await self._upsert_many(
            self._work_item_dependencies_table,
            rows,
            conflict_columns=[
                "source_work_item_id",
                "target_work_item_id",
                "relationship_type",
            ],
            update_columns=["relationship_type_raw", "last_synced"],
        )

    async def insert_work_graph_issue_pr(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        synced_at_default = datetime.now(timezone.utc)
        payload = []
        for r in records:
            payload.append(
                {
                    **r,
                    "repo_id": str(r["repo_id"]),
                    "last_synced": r.get("last_synced") or synced_at_default,
                }
            )
        await self._upsert_many(
            self._work_graph_issue_pr_table,
            payload,
            conflict_columns=["repo_id", "work_item_id", "pr_number"],
            update_columns=["confidence", "provenance", "evidence", "last_synced"],
        )

    async def insert_work_graph_pr_commit(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        synced_at_default = datetime.now(timezone.utc)
        payload = []
        for r in records:
            payload.append(
                {
                    **r,
                    "repo_id": str(r["repo_id"]),
                    "last_synced": r.get("last_synced") or synced_at_default,
                }
            )
        await self._upsert_many(
            self._work_graph_pr_commit_table,
            payload,
            conflict_columns=["repo_id", "pr_number", "commit_hash"],
            update_columns=["confidence", "provenance", "evidence", "last_synced"],
        )

    async def insert_work_items(self, work_items: List["WorkItem"]) -> None:
        if not work_items:
            return

        synced_at_default = datetime.now(timezone.utc)
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
                repo_id_val = str(repo_id_val)

            rows.append(
                {
                    "work_item_id": str(get("work_item_id")),
                    "repo_id": repo_id_val,
                    "provider": str(get("provider") or ""),
                    "title": str(get("title") or ""),
                    "description": get("description"),
                    "type": str(get("type") or ""),
                    "status": str(get("status") or ""),
                    "status_raw": str(get("status_raw") or ""),
                    "project_key": str(get("project_key") or ""),
                    "project_id": str(get("project_id") or ""),
                    "assignees": get("assignees") or [],
                    "reporter": str(get("reporter") or ""),
                    "created_at": get("created_at"),
                    "updated_at": get("updated_at"),
                    "started_at": get("started_at"),
                    "completed_at": get("completed_at"),
                    "closed_at": get("closed_at"),
                    "labels": get("labels") or [],
                    "story_points": float(get("story_points"))  # type: ignore[arg-type]
                    if get("story_points") is not None
                    else None,
                    "sprint_id": str(get("sprint_id") or ""),
                    "sprint_name": str(get("sprint_name") or ""),
                    "parent_id": str(get("parent_id") or ""),
                    "epic_id": str(get("epic_id") or ""),
                    "url": str(get("url") or ""),
                    "priority_raw": str(get("priority_raw") or ""),
                    "service_class": str(get("service_class") or ""),
                    "due_at": get("due_at"),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._work_items_table,
            rows,
            conflict_columns=["work_item_id"],
            update_columns=[
                "repo_id",
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
        )

    async def insert_work_item_transitions(
        self, transitions: List["WorkItemStatusTransition"]
    ) -> None:
        if not transitions:
            return

        synced_at_default = datetime.now(timezone.utc)
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
                repo_id_val = str(repo_id_val)

            rows.append(
                {
                    "work_item_id": str(get("work_item_id")),
                    "occurred_at": get("occurred_at"),
                    "repo_id": repo_id_val,
                    "provider": str(get("provider") or ""),
                    "from_status": str(get("from_status") or ""),
                    "to_status": str(get("to_status") or ""),
                    "from_status_raw": str(get("from_status_raw") or ""),
                    "to_status_raw": str(get("to_status_raw") or ""),
                    "actor": str(get("actor") or ""),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._work_item_transitions_table,
            rows,
            conflict_columns=["work_item_id", "occurred_at"],
            update_columns=[
                "repo_id",
                "provider",
                "from_status",
                "to_status",
                "from_status_raw",
                "to_status_raw",
                "actor",
                "last_synced",
            ],
        )

    async def insert_teams(self, teams: List["Team"]) -> None:
        from dev_health_ops.models.teams import Team

        if not teams:
            return

        # Convert objects to dicts for upsert
        rows: List[Dict[str, Any]] = []
        for item in teams:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(
                    {
                        "id": item.id,
                        "team_uuid": item.team_uuid,
                        "name": item.name,
                        "description": item.description,
                        "members": item.members,
                        "updated_at": item.updated_at,
                    }
                )

        await self._upsert_many(
            Team,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "team_uuid",
                "name",
                "description",
                "members",
                "updated_at",
            ],
        )

    async def insert_jira_project_ops_team_links(
        self, links: List[JiraProjectOpsTeamLink]
    ) -> None:
        from dev_health_ops.models.teams import JiraProjectOpsTeamLink

        if not links:
            return

        rows: List[Dict[str, Any]] = []
        for item in links:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(
                    {
                        "project_key": item.project_key,
                        "ops_team_id": item.ops_team_id,
                        "project_name": item.project_name,
                        "ops_team_name": item.ops_team_name,
                        "updated_at": item.updated_at,
                    }
                )

        await self._upsert_many(
            JiraProjectOpsTeamLink,
            rows,
            conflict_columns=["project_key", "ops_team_id"],
            update_columns=[
                "project_name",
                "ops_team_name",
                "updated_at",
            ],
        )

    async def insert_atlassian_ops_incidents(
        self, incidents: List[AtlassianOpsIncident]
    ) -> None:
        from dev_health_ops.models.atlassian_ops import AtlassianOpsIncidentModel

        if not incidents:
            return

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
                    "created_at": item.created_at,
                    "provider_id": item.provider_id,
                    "last_synced": item.last_synced,
                }
            )

        await self._upsert_many(
            AtlassianOpsIncidentModel,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "url",
                "summary",
                "description",
                "status",
                "severity",
                "created_at",
                "provider_id",
                "last_synced",
            ],
        )

    async def insert_atlassian_ops_alerts(
        self, alerts: List[AtlassianOpsAlert]
    ) -> None:
        from dev_health_ops.models.atlassian_ops import AtlassianOpsAlertModel

        if not alerts:
            return

        rows: List[Dict[str, Any]] = []
        for item in alerts:
            rows.append(
                {
                    "id": item.id,
                    "status": item.status,
                    "priority": item.priority,
                    "created_at": item.created_at,
                    "acknowledged_at": item.acknowledged_at,
                    "snoozed_at": item.snoozed_at,
                    "closed_at": item.closed_at,
                    "last_synced": item.last_synced,
                }
            )

        await self._upsert_many(
            AtlassianOpsAlertModel,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "status",
                "priority",
                "created_at",
                "acknowledged_at",
                "snoozed_at",
                "closed_at",
                "last_synced",
            ],
        )

    async def insert_atlassian_ops_schedules(
        self, schedules: List[AtlassianOpsSchedule]
    ) -> None:
        from dev_health_ops.models.atlassian_ops import AtlassianOpsScheduleModel

        if not schedules:
            return

        rows: List[Dict[str, Any]] = []
        for item in schedules:
            rows.append(
                {
                    "id": item.id,
                    "name": item.name,
                    "timezone": item.timezone,
                    "last_synced": item.last_synced,
                }
            )

        await self._upsert_many(
            AtlassianOpsScheduleModel,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "name",
                "timezone",
                "last_synced",
            ],
        )

    async def get_all_teams(self) -> List["Team"]:
        from dev_health_ops.models.teams import Team

        assert self.session is not None
        result = await self.session.execute(select(Team))
        return list(result.scalars().all())

    async def get_jira_project_ops_team_links(self) -> List["JiraProjectOpsTeamLink"]:
        from dev_health_ops.models.teams import JiraProjectOpsTeamLink

        assert self.session is not None
        result = await self.session.execute(select(JiraProjectOpsTeamLink))
        return list(result.scalars().all())
