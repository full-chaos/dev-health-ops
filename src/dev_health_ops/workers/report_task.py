from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

DATE_RANGE_DAYS = {
    "last_7_days": 7,
    "last_24_hours": 1,
    "last_30_days": 30,
    "last_90_days": 90,
}

DEFAULT_SECTIONS = ["summary", "delivery", "quality", "wellbeing"]


def _json_object(value: object | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): raw_value for key, raw_value in value.items()}


def _datetime_or_none(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    return None


def _string_value(value: object | None) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def _build_default_plan(
    report_id: str,
    org_id: str,
    parameters: dict,
) -> dict:
    from dev_health_ops.utils.datetime import utc_today

    days = DATE_RANGE_DAYS.get(parameters.get("dateRange", "last_7_days"), 7)
    end = utc_today()
    start = end - timedelta(days=days)

    scope = parameters.get("scope", "org")
    metrics = parameters.get("metrics", [])

    plan = {
        "plan_id": f"auto-{report_id}",
        "report_type": "weekly_health" if days <= 7 else "monthly_review",
        "audience": "team_lead",
        "org_id": org_id,
        "time_range_start": start,
        "time_range_end": end,
        "comparison_period": "prior_week" if days <= 7 else "prior_month",
        "sections": DEFAULT_SECTIONS,
        "requested_metrics": metrics,
        "include_insights": True,
        "include_anomalies": True,
        "confidence_threshold": "direct_fact",
        "scope_teams": [],
        "scope_repos": [],
        "scope_services": [],
    }

    if scope == "team":
        plan["scope_teams"] = parameters.get("team_ids", [])
    elif scope == "repo":
        plan["scope_repos"] = parameters.get("repo_ids", [])

    return plan


@celery_app.task(bind=True, name="dev_health_ops.workers.tasks.execute_saved_report")
def execute_saved_report(self, report_id: str, run_id: str) -> dict:
    from dev_health_ops.db import get_postgres_session_sync, require_clickhouse_uri
    from dev_health_ops.models.reports import ReportRun, ReportRunStatus, SavedReport
    from dev_health_ops.reports.export import persist_report_run

    report_uuid = uuid.UUID(report_id)
    run_uuid = uuid.UUID(run_id)

    with get_postgres_session_sync() as session:
        report = session.execute(
            select(SavedReport).where(SavedReport.id == report_uuid)
        ).scalar_one_or_none()

        if report is None:
            logger.error("SavedReport %s not found", report_id)
            return {"status": "error", "reason": "report_not_found"}

        run = session.execute(
            select(ReportRun).where(ReportRun.id == run_uuid)
        ).scalar_one_or_none()

        if run is None:
            logger.error("ReportRun %s not found", run_id)
            return {"status": "error", "reason": "run_not_found"}

        setattr(run, "status", ReportRunStatus.RUNNING.value)
        setattr(run, "started_at", datetime.now(timezone.utc))
        session.commit()

    try:
        from dev_health_ops.db import reset_async_engines
        from dev_health_ops.metrics.testops_schemas import ChartSpec, ReportPlan
        from dev_health_ops.reports.engine import execute_report

        reset_async_engines()

        clickhouse_dsn = require_clickhouse_uri()

        with get_postgres_session_sync() as session:
            report_row = session.execute(
                select(
                    SavedReport.report_plan,
                    SavedReport.parameters,
                    SavedReport.org_id,
                ).where(SavedReport.id == report_uuid)
            ).one()
            plan_data: dict[str, Any] = _json_object(report_row.report_plan)
            params: dict[str, Any] = _json_object(report_row.parameters)
            report_org_id = _string_value(report_row.org_id)

        if not plan_data:
            plan_data = _build_default_plan(report_id, report_org_id, params)
            logger.info(
                "Generated default plan for report %s from parameters",
                report_id,
            )

        plan = ReportPlan(**plan_data)

        chart_specs = [ChartSpec(**spec) for spec in plan_data.get("chart_specs", [])]

        result = asyncio.run(execute_report(plan, chart_specs, clickhouse_dsn))

        with get_postgres_session_sync() as session:
            persist_report_run(
                session=session,
                run_id=run_id,
                report_id=report_id,
                rendered_markdown=result.rendered_markdown,
                provenance=[
                    {
                        "provenance_id": p.provenance_id,
                        "artifact_type": p.artifact_type,
                        "artifact_id": p.artifact_id,
                    }
                    for p in result.provenance
                ],
            )

        return {"status": "success", "run_id": run_id}

    except Exception as exc:
        logger.exception("Report execution failed for run %s", run_id)
        with get_postgres_session_sync() as session:
            run = session.execute(
                select(ReportRun).where(ReportRun.id == run_uuid)
            ).scalar_one_or_none()
            if run:
                completed_at = datetime.now(timezone.utc)
                setattr(run, "status", ReportRunStatus.FAILED.value)
                setattr(run, "completed_at", completed_at)
                started_at = _datetime_or_none(run.started_at)
                if started_at is not None:
                    setattr(
                        run,
                        "duration_seconds",
                        (completed_at - started_at).total_seconds(),
                    )
                setattr(run, "error", str(exc))
                setattr(run, "error_traceback", traceback.format_exc())
                session.commit()

            report_obj = session.execute(
                select(SavedReport).where(SavedReport.id == report_uuid)
            ).scalar_one_or_none()
            if report_obj:
                setattr(report_obj, "last_run_at", datetime.now(timezone.utc))
                setattr(report_obj, "last_run_status", ReportRunStatus.FAILED.value)
                session.commit()

        return {"status": "failed", "run_id": run_id, "error": str(exc)}
