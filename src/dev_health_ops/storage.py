from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import ConfigurationError
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
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker

from dev_health_ops.models.git import (
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
    GitPullRequestReview,
    CiPipelineRun,
    Deployment,
    Incident,
    Repo,
)

if TYPE_CHECKING:
    from dev_health_ops.models.atlassian_ops import (
        AtlassianOpsAlert,
        AtlassianOpsIncident,
        AtlassianOpsSchedule,
    )
    from dev_health_ops.models.teams import JiraProjectOpsTeamLink, Team
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemDependency,
    WorkItemStatusTransition,
)

from dev_health_ops.metrics.schemas import FileComplexitySnapshot
from dev_health_ops.metrics.schemas import WorkItemUserMetricsDailyRecord


def _register_sqlite_datetime_adapters() -> None:
    try:
        import sqlite3
    except Exception:
        return
    sqlite3.register_adapter(date, lambda value: value.isoformat())
    sqlite3.register_adapter(datetime, lambda value: value.isoformat(" "))


_register_sqlite_datetime_adapters()


def _parse_date_value(value: Any) -> Optional[date]:
    if value:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except Exception:
            pass
    return None


def _parse_datetime_value(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def detect_db_type(conn_string: str) -> str:
    """
    Detect database type from connection string.

    :param conn_string: Database connection string.
    :return: Database type ('postgres', 'sqlite', 'mongo', or 'clickhouse').
    :raises ValueError: If database type cannot be determined.
    """
    if not conn_string:
        raise ValueError("Connection string is required")

    conn_lower = conn_string.lower()

    # ClickHouse connection strings
    if conn_lower.startswith("clickhouse://") or conn_lower.startswith(
        (
            "clickhouse+http://",
            "clickhouse+https://",
            "clickhouse+native://",
        )
    ):
        return "clickhouse"

    # MongoDB connection strings
    if conn_lower.startswith("mongodb://") or conn_lower.startswith("mongodb+srv://"):
        return "mongo"

    # PostgreSQL connection strings
    if conn_lower.startswith("postgresql://") or conn_lower.startswith("postgres://"):
        return "postgres"
    if conn_lower.startswith("postgresql+asyncpg://"):
        return "postgres"

    # SQLite connection strings
    if conn_lower.startswith("sqlite://") or conn_lower.startswith(
        "sqlite+aiosqlite://"
    ):
        return "sqlite"

    # Extract scheme for better error reporting
    scheme = conn_string.split("://", 1)[0] if "://" in conn_string else "unknown"
    raise ValueError(
        f"Could not detect database type from connection string. "
        f"Supported: mongodb://, postgresql://, postgres://, sqlite://, "
        f"clickhouse://, or variations with async drivers. Got scheme: '{scheme}', "
        f"connection string (first 100 chars): {conn_string[:100]}..."
    )


def create_store(
    conn_string: str,
    db_type: Optional[str] = None,
    db_name: Optional[str] = None,
    echo: bool = False,
) -> Union["SQLAlchemyStore", "MongoStore", "ClickHouseStore"]:
    """
    Create a storage backend based on the connection string.

    This factory function automatically detects the database type from the
    connection string and returns the appropriate store implementation.

    :param conn_string: Database connection string.
    :param db_type: Optional explicit database type ('postgres', 'sqlite', 'mongo', 'clickhouse').
                   If not provided, it will be auto-detected from conn_string.
    :param db_name: Optional database name (for MongoDB).
    :param echo: Whether to echo SQL statements (for SQLAlchemy).
    :return: Appropriate store instance (SQLAlchemyStore, MongoStore, or ClickHouseStore).
    """
    if db_type is None:
        db_type = detect_db_type(conn_string)

    # Normalize connection string for async drivers if using SQLAlchemyStore
    if db_type in ("postgres", "postgresql", "sqlite"):
        if conn_string.startswith("sqlite://") and not conn_string.startswith(
            "sqlite+aiosqlite://"
        ):
            conn_string = conn_string.replace("sqlite://", "sqlite+aiosqlite://", 1)
        elif conn_string.startswith("postgresql://") and not conn_string.startswith(
            "postgresql+asyncpg://"
        ):
            conn_string = conn_string.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        elif conn_string.startswith("postgres://"):
            conn_string = conn_string.replace("postgres://", "postgresql+asyncpg://", 1)

    db_type = db_type.lower()

    if db_type == "mongo":
        return MongoStore(conn_string, db_name=db_name)
    elif db_type == "clickhouse":
        return ClickHouseStore(conn_string)
    elif db_type in ("postgres", "postgresql", "sqlite"):
        return SQLAlchemyStore(conn_string, echo=echo)
    else:
        raise ValueError(
            f"Unsupported database type: {db_type}. "
            f"Supported types: postgres, sqlite, mongo, clickhouse"
        )


def resolve_db_type(db_url: str, db_type: Optional[str]) -> str:
    """
    Resolve database type from URL or explicit type.
    """
    if db_type:
        resolved = db_type.lower()
    else:
        try:
            resolved = detect_db_type(db_url)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if resolved not in {"postgres", "mongo", "sqlite", "clickhouse"}:
        raise SystemExit(
            "DB_TYPE must be 'postgres', 'mongo', 'sqlite', or 'clickhouse'"
        )
    return resolved


async def run_with_store(db_url: str, db_type: str, handler: Callable) -> None:
    """
    Helper to create a store and run a handler within its context.
    """
    store = create_store(db_url, db_type)
    async with store:
        await handler(store)


def _serialize_value(value: Any) -> Any:
    """Convert values so they are safe to store in MongoDB."""
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def model_to_dict(model: Any) -> Dict[str, Any]:
    """Convert a SQLAlchemy model instance to a plain dict."""
    mapper = inspect(model.__class__)
    data: Dict[str, Any] = {}
    for column in mapper.columns:
        data[column.key] = _serialize_value(getattr(model, column.key))
    return data


class SQLAlchemyStore:
    """Async storage implementation backed by SQLAlchemy."""

    def __init__(self, conn_string: str, echo: bool = False) -> None:
        # Configure connection pool for better performance (PostgreSQL/MySQL only)
        engine_kwargs = {"echo": echo}

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
        self.session_factory = sessionmaker(
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

        stmt = self._insert_for_dialect(model)
        stmt = stmt.on_conflict_do_update(
            index_elements=[_column(model, col) for col in conflict_columns],
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


class MongoStore:
    """Async storage implementation backed by MongoDB (via Motor)."""

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
        doc = model_to_dict(repo)
        doc["_id"] = doc["id"]
        await self.db["repos"].update_one(
            {"_id": doc["_id"]}, {"$set": doc}, upsert=True
        )

    async def get_all_repos(self) -> List[Repo]:
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
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_files"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_commit_stats"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def has_any_git_blame(self, repo_id) -> bool:
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
