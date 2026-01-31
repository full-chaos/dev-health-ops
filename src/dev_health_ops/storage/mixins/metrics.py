from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, List, Optional

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    func,
    select,
)

from dev_health_ops.metrics.schemas import (
    FileComplexitySnapshot,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.models.git import Repo
from dev_health_ops.storage.utils import _parse_date_value, _parse_datetime_value


class MetricsMixin:
    async def get_complexity_snapshots(
        self,
        *,
        as_of_day: date,
        repo_id: Optional[uuid.UUID] = None,
        repo_name: Optional[str] = None,
    ) -> List[Any]:
        assert self.session is not None
        resolved_repo_id = repo_id
        if resolved_repo_id is None and repo_name:
            repo_res = await self.session.execute(
                select(Repo.id).where(Repo.repo == repo_name).limit(1)
            )
            repo_row = repo_res.first()
            if not repo_row or not repo_row[0]:
                return []
            resolved_repo_id = uuid.UUID(str(repo_row[0]))

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
    ) -> List[Any]:
        assert self.session is not None
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
