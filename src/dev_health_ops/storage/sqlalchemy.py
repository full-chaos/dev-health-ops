from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.git import Base, Repo

from .mixins import (
    AtlassianOpsMixin,
    CicdMixin,
    GitDataMixin,
    MetricsMixin,
    PullRequestMixin,
    TeamMixin,
    TestOpsCICDMixin,
    WorkItemMixin,
)

if TYPE_CHECKING:
    pass


class SQLAlchemyStore(
    GitDataMixin,
    PullRequestMixin,
    CicdMixin,
    TestOpsCICDMixin,
    WorkItemMixin,
    TeamMixin,
    AtlassianOpsMixin,
    MetricsMixin,
):
    """Async storage implementation backed by SQLAlchemy."""

    def __init__(self, conn_string: str, echo: bool = False) -> None:
        engine_kwargs: dict[str, Any] = {"echo": echo}

        if "sqlite" not in conn_string.lower():
            engine_kwargs.update(
                {
                    "pool_size": 20,
                    "max_overflow": 30,
                    "pool_pre_ping": True,
                    "pool_recycle": 3600,
                }
            )

        self.engine = create_async_engine(conn_string, **engine_kwargs)
        self.session_factory = async_sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )
        self.session: AsyncSession | None = None
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
            Column("org_id", String, nullable=False, server_default=""),
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
            Column("org_id", String, nullable=False, server_default=""),
            Column("last_synced", DateTime(timezone=True)),
        )
        self._work_item_dependencies_table = Table(
            "work_item_dependencies",
            self._work_item_metadata,
            Column("source_work_item_id", String, primary_key=True),
            Column("target_work_item_id", String, primary_key=True),
            Column("relationship_type", String, primary_key=True),
            Column("relationship_type_raw", String),
            Column("org_id", String, nullable=False, server_default=""),
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
            Column("org_id", String, nullable=False, server_default=""),
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
            Column("org_id", String, nullable=False, server_default=""),
            Column("last_synced", DateTime(timezone=True)),
        )
        self._ci_pipeline_runs_table = Base.metadata.tables["ci_pipeline_runs"]
        for column in (
            Column("pipeline_name", String),
            Column("provider", String, nullable=False, server_default=""),
            Column("duration_seconds", Float),
            Column("queue_seconds", Float),
            Column("retry_count", Integer, nullable=False, server_default="0"),
            Column("cancel_reason", String),
            Column("trigger_source", String),
            Column("commit_hash", String),
            Column("branch", String),
            Column("pr_number", Integer),
            Column("team_id", String),
            Column("service_id", String),
            Column("org_id", String, nullable=False, server_default=""),
        ):
            if column.name not in self._ci_pipeline_runs_table.c:
                self._ci_pipeline_runs_table.append_column(column)

        self._testops_metadata = MetaData()
        self._ci_job_runs_table = Table(
            "ci_job_runs",
            self._testops_metadata,
            Column("repo_id", String, primary_key=True),
            Column("run_id", String, primary_key=True),
            Column("job_id", String, primary_key=True),
            Column("job_name", String, nullable=False),
            Column("stage", String),
            Column("status", String),
            Column("started_at", DateTime(timezone=True)),
            Column("finished_at", DateTime(timezone=True)),
            Column("duration_seconds", Float),
            Column("runner_type", String),
            Column("retry_attempt", Integer, nullable=False, server_default="0"),
            Column("org_id", String, nullable=False, server_default=""),
            Column("last_synced", DateTime(timezone=True), nullable=False),
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
        rows: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: list[str],
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

    async def __aenter__(self) -> SQLAlchemyStore:
        self.session = self.session_factory()

        if "sqlite" in str(self.engine.url):
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                await conn.run_sync(self._work_item_metadata.create_all)
                await conn.run_sync(self._testops_metadata.create_all)

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None
        await self.engine.dispose()

    async def ensure_tables(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(self._work_item_metadata.create_all)
            await conn.run_sync(self._testops_metadata.create_all)

    async def insert_repo(self, repo: Repo) -> None:
        assert self.session is not None
        existing_repo = await self.session.get(Repo, repo.id)
        if not existing_repo:
            self.session.add(repo)
        await self.session.commit()

    async def get_all_repos(self) -> list[Repo]:
        from sqlalchemy import select

        assert self.session is not None
        result = await self.session.execute(select(Repo))
        return list(result.scalars().all())
