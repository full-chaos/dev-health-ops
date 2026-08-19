from __future__ import annotations

import asyncio
import logging
import math
import traceback
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import select

from dev_health_ops.sync.error_sanitize import sanitize_error_text
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


def _string_value(value: object | None) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


async def _execute_with_report_run_lease(
    execute_report: Any,
    plan: Any,
    chart_specs: list[Any],
    clickhouse_dsn: str,
    run_id: str,
    claim: Any,
) -> Any:
    """Renew the durable execution fence while the async report work runs."""

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.reports.export import renew_report_run

    work = asyncio.create_task(execute_report(plan, chart_specs, clickhouse_dsn))
    interval = max(0.001, float(claim.lease_seconds) / 3.0)
    try:
        while True:
            done, _ = await asyncio.wait({work}, timeout=interval)
            if done:
                return await work
            with get_postgres_session_sync() as session:
                if not renew_report_run(session, run_id, claim.token):
                    raise RuntimeError("report run execution lease was lost")
                session.commit()
    finally:
        if not work.done():
            work.cancel()
            try:
                await work
            except asyncio.CancelledError:
                pass


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


@celery_app.task(
    bind=True,
    name="dev_health_ops.workers.tasks.execute_saved_report",
    acks_late=True,
    reject_on_worker_lost=True,
)
def execute_saved_report(self, report_id: str, run_id: str) -> dict:
    from dev_health_ops.db import get_postgres_session_sync, require_clickhouse_uri
    from dev_health_ops.metrics.prometheus import REPORT_RUN_LEASE_EXPIRED_TOTAL
    from dev_health_ops.models.reports import ReportRun, SavedReport
    from dev_health_ops.reports.export import (
        ReportRunLeaseActive,
        ReportRunReclaimExhausted,
        fail_report_run,
        persist_report_run,
        start_report_run,
    )

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

        try:
            claim = start_report_run(session, run_id)
        except ReportRunLeaseActive as exc:
            raise self.retry(
                exc=exc,
                countdown=max(1, math.ceil(exc.retry_after_seconds)),
            ) from exc
        except ReportRunReclaimExhausted as exc:
            session.commit()
            if exc.terminalized:
                REPORT_RUN_LEASE_EXPIRED_TOTAL.labels(result="failed").inc()
            raise
        if claim is None:
            return {"status": "ignored", "reason": "run_not_pending", "run_id": run_id}
        session.commit()
    if claim.reclaimed:
        REPORT_RUN_LEASE_EXPIRED_TOTAL.labels(result="retrying").inc()

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

        result = asyncio.run(
            _execute_with_report_run_lease(
                execute_report,
                plan,
                chart_specs,
                clickhouse_dsn,
                run_id,
                claim,
            )
        )

        with get_postgres_session_sync() as session:
            persisted = persist_report_run(
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
                claim_token=claim.token,
            )

        return {"status": "success" if persisted else "ignored", "run_id": run_id}

    except Exception as exc:
        logger.exception("Report execution failed for run %s", run_id)
        # CHAOS-2784: report_runs.error / error_traceback are free-form Text
        # columns populated from str(exc) / traceback.format_exc() -- neither
        # controls what a downstream client library or provider response body
        # embeds in an exception message, so redact credential-shaped
        # substrings before persisting (mirrors sync/dispatch_outbox.py and
        # workers/sync_units.py, CHAOS-2766).
        sanitized_error = sanitize_error_text(exc)
        sanitized_traceback = sanitize_error_text(traceback.format_exc())
        assert sanitized_error is not None
        assert sanitized_traceback is not None
        with get_postgres_session_sync() as session:
            persisted_failure = fail_report_run(
                session,
                run_id,
                claim.token,
                sanitized_error,
                sanitized_traceback,
            )
            session.commit()

        if not persisted_failure:
            return {
                "status": "ignored",
                "reason": "execution_lease_lost",
                "run_id": run_id,
            }
        return {"status": "failed", "run_id": run_id, "error": sanitized_error}
