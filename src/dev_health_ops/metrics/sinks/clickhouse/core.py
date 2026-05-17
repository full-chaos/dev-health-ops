"""
ClickHouseCore — connection, schema management, and shared insert helper.

Contains: __init__, close, query_dicts, backend_type, get_all_teams,
          insert_teams, _apply_sql_migrations, ensure_schema, _insert_rows,
          and query-helper methods.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)

logger = logging.getLogger(__name__)


class ClickHouseCore(BaseMetricsSink):
    """
    ClickHouse sink for derived daily metrics.

    This sink is append-only: re-computations insert new rows with a newer
    `computed_at`. Queries can select the latest version via `argMax`.
    """

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Execute a ClickHouse query and return results as list of dicts."""
        result = self.client.query(query, parameters=parameters)
        col_names = list(getattr(result, "column_names", []) or [])
        rows = list(getattr(result, "result_rows", []) or [])
        if not col_names or not rows:
            return []
        return [dict(zip(col_names, row)) for row in rows]

    @property
    def backend_type(self) -> str:
        return "clickhouse"

    def __init__(self, dsn: str, client: Any | None = None) -> None:
        if not dsn:
            raise ValueError("ClickHouse DSN is required")
        self.dsn = dsn
        if client:
            self.client = client
        else:
            settings = {
                "max_query_size": 1 * 1024 * 1024,  # 1MB
            }
            self.client = clickhouse_connect.get_client(dsn=dsn, settings=settings)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception as e:
            logger.warning(
                "Exception occurred when closing ClickHouse client: %s",
                e,
                exc_info=True,
            )

    async def get_all_teams(self) -> list[dict[str, Any]]:
        """Fetch all teams from ClickHouse for identity resolution."""
        _org_id = getattr(self, "org_id", None) or ""
        query = "SELECT id, name, members, project_keys, repo_patterns FROM teams FINAL"
        params: dict[str, str] = {}
        if _org_id:
            query += " WHERE org_id = {org_id:String}"
            params["org_id"] = _org_id
        result = await asyncio.to_thread(self.client.query, query, parameters=params)
        teams: list[dict[str, Any]] = []
        for row in result.result_rows or []:
            teams.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "members": row[2] or [],
                    "project_keys": row[3] or [],
                    "repo_patterns": row[4] or [],
                }
            )
        return teams

    async def insert_teams(self, teams: list[Any]) -> None:
        if not teams:
            return
        column_names = [
            "id",
            "team_uuid",
            "name",
            "description",
            "members",
            "project_keys",
            "repo_patterns",
            "is_active",
            "updated_at",
            "org_id",
        ]
        matrix = []
        for team in teams:
            if isinstance(team, dict):
                team_id = str(team.get("id", ""))
                matrix.append(
                    [
                        team_id,
                        team.get("team_uuid")
                        or uuid.uuid5(uuid.NAMESPACE_URL, f"team:{team_id}"),
                        team.get("name", ""),
                        team.get("description"),
                        team.get("members", []),
                        team.get("project_keys", []),
                        team.get("repo_patterns", []),
                        1 if team.get("is_active", True) else 0,
                        _dt_to_clickhouse_datetime(
                            team.get("updated_at", datetime.now(timezone.utc))
                        ),
                        team["org_id"],
                    ]
                )
            else:
                team_id = str(getattr(team, "id", ""))
                matrix.append(
                    [
                        team_id,
                        getattr(team, "team_uuid", None)
                        or uuid.uuid5(uuid.NAMESPACE_URL, f"team:{team_id}"),
                        getattr(team, "name", ""),
                        getattr(team, "description", None),
                        getattr(team, "members", []),
                        getattr(team, "project_keys", []),
                        getattr(team, "repo_patterns", []),
                        1 if getattr(team, "is_active", True) else 0,
                        _dt_to_clickhouse_datetime(
                            getattr(team, "updated_at", datetime.now(timezone.utc))
                        ),
                        team.org_id,
                    ]
                )
        await asyncio.to_thread(
            self.client.insert, "teams", matrix, column_names=column_names
        )

    def _apply_sql_migrations(self) -> None:
        # NOTE: parents[3] because core.py is one level deeper than the old
        # clickhouse.py was (sinks/clickhouse/core.py vs sinks/clickhouse.py).
        migrations_dir = (
            Path(__file__).resolve().parents[3] / "migrations" / "clickhouse"
        )
        if not migrations_dir.exists():
            return

        # Ensure schema_migrations table exists
        self.client.command(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version String, applied_at DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY version"
        )

        # Get applied migrations
        applied_result = self.client.query("SELECT version FROM schema_migrations")
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
                logger.info(f"Skipping already applied migration: {version}")
                continue

            logger.info(f"Applying migration: {version}")

            if path.suffix == ".sql":
                try:
                    sql = path.read_text(encoding="utf-8")
                    # Very small splitter: migrations are expected to contain only DDL.
                    for stmt in sql.split(";"):
                        stmt = stmt.strip()
                        if not stmt:
                            continue
                        self.client.command(stmt)
                except Exception as e:
                    logger.error(f"CRITICAL: Migration failed: {path.name}\nError: {e}")
                    raise
            elif path.suffix == ".py":
                # Execute python migration script
                try:
                    import importlib.util

                    spec = importlib.util.spec_from_file_location(path.stem, path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        if hasattr(module, "upgrade"):
                            logger.info(f"Executing Python migration: {path.name}")
                            module.upgrade(self.client)
                except Exception as e:
                    logger.error(f"Failed to apply python migration {path.name}: {e}")
                    raise

            # Record migration as applied
            self.client.command(
                "INSERT INTO schema_migrations (version, applied_at) VALUES ({version:String}, now())",
                parameters={"version": version},
            )

    def ensure_schema(self) -> None:
        """Create ClickHouse tables via SQL migrations."""
        self._apply_sql_migrations()

    # Alias for backward compatibility
    ensure_tables = ensure_schema

    def _insert_rows(
        self,
        table: str,
        columns: list[str],
        rows: Any,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if not rows:
            return
        # Auto-inject org_id from sink context when record has default empty value.
        _org_id = getattr(self, "org_id", None) or ""
        for chunk in _chunked(rows, batch_size):
            matrix = []
            for row in chunk:
                data = asdict(row)
                if _org_id and "org_id" in columns and not data.get("org_id"):
                    data["org_id"] = _org_id
                values = []
                for col in columns:
                    value = data.get(col)
                    if isinstance(value, datetime):
                        value = _dt_to_clickhouse_datetime(value)
                    values.append(value)
                matrix.append(values)
            self.client.insert(table, matrix, column_names=columns)

    # -------------------------------------------------------------------------
    # Query helpers (useful for Grafana and validation)
    # -------------------------------------------------------------------------

    def get_rolling_30d_user_stats(
        self,
        as_of_day: date,
        repo_id: uuid.UUID | None = None,
    ) -> list[dict[str, Any]]:
        """
        Compute rolling 30d stats for all users as of the given day.

        Aggregation logic:
        - churn_loc_30d: sum(loc_touched)
        - delivery_units_30d: sum(delivery_units)
        - cycle_p50_30d_hours: median of daily cycle_p50_hours (approx) where cycle_p50_hours > 0
        - wip_max_30d: max(work_items_active)
        """
        # We look at [as_of_day - 29 days, as_of_day] inclusive.
        # Note: 'day' in user_metrics_daily is the date of the metrics.

        start_day = as_of_day - timedelta(days=29)

        params = {
            "start": (
                start_day.strftime("%Y-%m-%d")
                if hasattr(start_day, "strftime")
                else str(start_day)
            ),
            "end": (
                as_of_day.strftime("%Y-%m-%d")
                if hasattr(as_of_day, "strftime")
                else str(as_of_day)
            ),
        }
        where = ["day >= toDate(%(start)s)", "day <= toDate(%(end)s)"]
        if repo_id:
            where.append("repo_id = toUUID(%(repo_id)s)")
            params["repo_id"] = str(repo_id)

        where_clause = " AND ".join(where)

        # We use argMax(..., computed_at) to get the latest version of the row for each day
        # before aggregating over days.
        # However, user_metrics_daily is MergeTree, not ReplacingMergeTree in the original schema (001).
        # Wait, 001 says ENGINE = MergeTree. So we might have duplicates if we re-ran.
        # But commonly we just insert.
        # If we assume we might have multiple rows per day/user/repo, we should take the latest.
        # The PK is (repo_id, author_email, day).
        # We'll aggregate over (identity_id, team_id, repo_id)

        # Note: identity_id was added in 005. For older rows it might be null/empty.
        # We fallback to author_email if identity_id is empty.

        sql = f"""
        SELECT
            if(empty(identity_id), author_email, identity_id) as identity_id,
            anyLast(team_id) as team_id,
            sum(loc_touched) as churn_loc_30d,
            sum(delivery_units) as delivery_units_30d,
            quantile(0.5)(if(cycle_p50_hours > 0, cycle_p50_hours, null)) as cycle_p50_30d_hours,
            max(work_items_active) as wip_max_30d
        FROM user_metrics_daily
        WHERE {where_clause}
        GROUP BY identity_id
        HAVING identity_id != ''
        """

        # Note: ClickHouse's quantile(0.5) is approximate but fast.

        try:
            result = self.client.query(sql, parameters=params)
            rows = []
            for r in result.named_results():
                rows.append(r)
            return rows
        except Exception as e:
            logger.warning("Failed to fetch rolling stats: %s", e)
            return []

    def latest_repo_metrics_query(
        self,
        *,
        repo_id: str | None = None,
        start_day: date | None = None,
        end_day: date | None = None,
    ) -> str:
        where = []
        if repo_id:
            where.append(f"repo_id = toUUID('{repo_id}')")
        if start_day:
            where.append(f"day >= toDate('{start_day.isoformat()}')")
        if end_day:
            where.append(f"day < toDate('{end_day.isoformat()}')")
        where_clause = ("WHERE " + " AND ".join(where)) if where else ""
        return f"""
        SELECT
          repo_id,
          day,
          argMax(commits_count, computed_at) AS commits_count,
          argMax(total_loc_touched, computed_at) AS total_loc_touched,
          argMax(avg_commit_size_loc, computed_at) AS avg_commit_size_loc,
          argMax(large_commit_ratio, computed_at) AS large_commit_ratio,
          argMax(prs_merged, computed_at) AS prs_merged,
          argMax(median_pr_cycle_hours, computed_at) AS median_pr_cycle_hours,
          max(computed_at) AS computed_at
        FROM repo_metrics_daily
        {where_clause}
        GROUP BY repo_id, day
        ORDER BY repo_id, day
        """

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
