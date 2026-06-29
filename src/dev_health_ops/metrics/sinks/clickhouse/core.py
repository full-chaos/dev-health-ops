"""
ClickHouseCore — connection, schema management, and shared insert helper.

Contains: __init__, close, query_dicts, backend_type, get_all_teams,
          insert_teams, _apply_sql_migrations, ensure_schema, _insert_rows,
          and query-helper methods.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect

from dev_health_ops.metrics.schemas import (
    ManualAttributionFallbackRecord,
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
    TeamRepoOwnershipRecord,
    WorkItemTeamAttributionRecord,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)
from dev_health_ops.metrics.sinks.factory import detect_backend
from dev_health_ops.models.work_items import Sprint

logger = logging.getLogger(__name__)


def _auto_run_migrations_enabled() -> bool:
    return os.getenv("AUTO_RUN_MIGRATIONS", "true").strip().lower() in {
        "1",
        "true",
        "yes",
    }


class ClickHouseCore(BaseMetricsSink):
    """
    ClickHouse sink for derived daily metrics.

    This sink is append-only: re-computations insert new rows with a newer
    `computed_at`. Queries can select the latest version via `argMax`.
    """

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        return self.client.query(query, parameters=parameters or {})

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
        detect_backend(dsn)
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

    async def get_all_sprints(self, org_id: str | None = None) -> list[Sprint]:
        _org_id = str(org_id or getattr(self, "org_id", None) or "")
        query = (
            "SELECT provider, sprint_id, argMax(name, last_synced) AS name, "
            "argMax(state, last_synced) AS state, "
            "argMax(started_at, last_synced) AS started_at, "
            "argMax(ended_at, last_synced) AS ended_at, "
            "argMax(completed_at, last_synced) AS completed_at, "
            "max(last_synced) AS last_synced, org_id FROM sprints"
        )
        params: dict[str, str] = {}
        if _org_id:
            query += " WHERE org_id = {org_id:String}"
            params["org_id"] = _org_id
        query += " GROUP BY provider, sprint_id, org_id"
        result = await asyncio.to_thread(self.client.query, query, parameters=params)
        return [
            Sprint(
                provider=row[0],
                sprint_id=row[1],
                name=row[2],
                state=row[3],
                started_at=row[4],
                ended_at=row[5],
                completed_at=row[6],
                last_synced=row[7],
                org_id=row[8],
            )
            for row in result.result_rows or []
        ]

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
            "provider",
            "native_team_key",
            "parent_team_id",
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
                        str(team.get("provider") or ""),
                        team.get("native_team_key"),
                        team.get("parent_team_id"),
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
                        str(getattr(team, "provider", "") or ""),
                        getattr(team, "native_team_key", None),
                        getattr(team, "parent_team_id", None),
                    ]
                )
        await asyncio.to_thread(
            self.client.insert, "teams", matrix, column_names=column_names
        )

    def write_projects(self, rows: Sequence[ProjectRecord]) -> None:
        self._insert_rows(
            "projects",
            [
                "id",
                "org_id",
                "provider",
                "project_key",
                "name",
                "is_active",
                "updated_at",
                "last_synced",
            ],
            rows,
        )

    def write_members(self, rows: Sequence[MemberRecord]) -> None:
        self._insert_rows(
            "members",
            [
                "org_id",
                "member_id",
                "name",
                "email",
                "provider_identities",
                "is_active",
                "updated_at",
            ],
            rows,
        )

    def write_team_memberships(self, rows: Sequence[TeamMembershipRecord]) -> None:
        self._insert_rows(
            "team_memberships",
            [
                "org_id",
                "provider",
                "team_id",
                "member_id",
                "raw_provider_user_id",
                "raw_email",
                "identity_facets",
                "source",
                "is_primary",
                "specificity",
                "priority",
                "valid_from",
                "valid_to",
                "updated_at",
            ],
            rows,
        )

    def write_team_project_ownership(
        self, rows: Sequence[TeamProjectOwnershipRecord]
    ) -> None:
        self._insert_rows(
            "team_project_ownership",
            [
                "org_id",
                "provider",
                "team_id",
                "project_id",
                "project_key",
                "source",
                "is_primary",
                "specificity",
                "priority",
                "valid_from",
                "valid_to",
                "updated_at",
            ],
            rows,
        )

    def write_team_repo_ownership(
        self, rows: Sequence[TeamRepoOwnershipRecord]
    ) -> None:
        self._insert_rows(
            "team_repo_ownership",
            [
                "org_id",
                "provider",
                "team_id",
                "repo_id",
                "repo_full_name",
                "match_type",
                "source",
                "is_primary",
                "specificity",
                "priority",
                "valid_from",
                "valid_to",
                "updated_at",
            ],
            rows,
        )

    def write_work_item_team_attributions(
        self, rows: Sequence[WorkItemTeamAttributionRecord]
    ) -> None:
        if not rows:
            return
        org_id = getattr(self, "org_id", None) or ""
        column_names = [
            "org_id",
            "repo_id",
            "work_item_id",
            "provider",
            "team_id",
            "team_name",
            "source",
            "is_primary",
            "confidence",
            "evidence",
            "computed_at",
        ]
        data = []
        for row in rows:
            data.append(
                {
                    "org_id": row.org_id or org_id,
                    "repo_id": row.repo_id or uuid.UUID(int=0),
                    "work_item_id": row.work_item_id,
                    "provider": row.provider,
                    "team_id": row.team_id,
                    "team_name": row.team_name,
                    "source": row.source,
                    "is_primary": row.is_primary,
                    "confidence": row.confidence,
                    "evidence": row.evidence,
                    "computed_at": _dt_to_clickhouse_datetime(row.computed_at),
                }
            )
        for chunk in _chunked(data, DEFAULT_BATCH_SIZE):
            matrix = [[row[col] for col in column_names] for row in chunk]
            self.client.insert(
                "work_item_team_attributions", matrix, column_names=column_names
            )

    def write_manual_attribution_fallbacks(
        self, rows: Sequence[ManualAttributionFallbackRecord]
    ) -> None:
        if not rows:
            return
        org_id = getattr(self, "org_id", None) or ""
        column_names = [
            "org_id",
            "provider",
            "scope_type",
            "scope_id",
            "team_id",
            "team_name",
            "reason",
            "priority",
            "valid_from",
            "valid_to",
            "created_by",
            "created_at",
            "updated_at",
        ]
        data = []
        for row in rows:
            updated_at = _dt_to_clickhouse_datetime(row.updated_at)
            data.append(
                {
                    "org_id": row.org_id or org_id,
                    "provider": row.provider,
                    "scope_type": row.scope_type,
                    "scope_id": row.scope_id,
                    "team_id": row.team_id,
                    "team_name": row.team_name,
                    "reason": row.reason,
                    "priority": row.priority,
                    # non-nullable DB columns fall back to updated_at when unset
                    "valid_from": _dt_to_clickhouse_datetime(row.valid_from)
                    if row.valid_from
                    else updated_at,
                    "valid_to": _dt_to_clickhouse_datetime(row.valid_to)
                    if row.valid_to
                    else None,
                    "created_by": row.created_by,
                    "created_at": _dt_to_clickhouse_datetime(row.created_at)
                    if row.created_at
                    else updated_at,
                    "updated_at": updated_at,
                }
            )
        for chunk in _chunked(data, DEFAULT_BATCH_SIZE):
            matrix = [[row[col] for col in column_names] for row in chunk]
            self.client.insert(
                "manual_attribution_fallbacks", matrix, column_names=column_names
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

        # Collect all migration files in apply order.
        migration_files = sorted(
            list(migrations_dir.glob("*.sql")) + list(migrations_dir.glob("*.py"))
        )

        # Fast-path: short-circuit only when EVERY on-disk migration is already
        # applied (full-set completeness check via all_migrations_applied —
        # CHAOS-2440).  A latest-filename-only check is unsound here because of
        # inserted / mixed-ordering migrations (023b_ between 023_ and 024_,
        # duplicate numeric prefixes): the DB could hold the latest row yet be
        # missing an intermediate migration.  The full-set check stays O(n) in
        # memory over the SELECT we already ran — no per-file DB query, no log
        # loop — so it remains fast and quiet while being correct.
        from dev_health_ops.migrations.clickhouse import all_migrations_applied

        if migration_files and all_migrations_applied(
            (p.name for p in migration_files), applied_versions
        ):
            logger.debug(
                "ClickHouse schema is current (%d migrations applied) — "
                "skipping migration loop",
                len(migration_files),
            )
            return

        for path in migration_files:
            version = path.name
            if version in applied_versions:
                logger.debug("Skipping already applied migration: %s", version)
                continue

            logger.info("Applying migration: %s", version)

            if path.suffix == ".sql":
                try:
                    from dev_health_ops.migrations.clickhouse import (
                        split_sql_statements,
                    )

                    sql = path.read_text(encoding="utf-8")
                    # split_sql_statements strips '-- ...' line comments BEFORE
                    # splitting on ';', so a stray ';' inside a comment can never
                    # orphan bare text into its own statement (CHAOS-2430).
                    for stmt in split_sql_statements(sql):
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

    def ensure_schema(self, *, force: bool = False) -> None:
        """Create ClickHouse tables via SQL migrations.

        Schema management belongs to the dedicated migration entrypoint
        (``dev-hops migrate clickhouse`` / the one-shot ``migrate`` compose
        service), not to task execution. Services that are guaranteed to start
        after migrations (worker, beat) set ``AUTO_RUN_MIGRATIONS=false`` to
        turn the ambient ``ensure_tables()`` calls into no-ops; ``force=True``
        bypasses the flag for the CLI runner.
        """
        if not force and not _auto_run_migrations_enabled():
            logger.debug(
                "Skipping ClickHouse auto-migration (AUTO_RUN_MIGRATIONS=false)"
            )
            return
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
          argMax(commits_count, metrics.computed_at) AS commits_count,
          argMax(total_loc_touched, metrics.computed_at) AS total_loc_touched,
          argMax(avg_commit_size_loc, metrics.computed_at) AS avg_commit_size_loc,
          argMax(large_commit_ratio, metrics.computed_at) AS large_commit_ratio,
          argMax(prs_merged, metrics.computed_at) AS prs_merged,
          argMax(median_pr_cycle_hours, metrics.computed_at) AS median_pr_cycle_hours,
          max(metrics.computed_at) AS computed_at
        FROM repo_metrics_daily AS metrics
        {where_clause}
        GROUP BY repo_id, day
        ORDER BY repo_id, day
        """

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
