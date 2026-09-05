from __future__ import annotations

import logging
import uuid
from datetime import date

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
from dev_health_ops.workers.task_utils import (
    _get_db_url,
    _invalidate_metrics_cache,
)

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_daily_metrics",
)
def run_daily_metrics(
    self,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    repo_id: str | None = None,
    repo_name: str | None = None,
    sink: str = "auto",
    provider: str = "auto",
    org_id: str | None = None,
) -> dict:
    """
    Compute and persist daily metrics asynchronously.

    Args:
        db_url: Database connection string (defaults to DATABASE_URI env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        repo_id: Optional repository UUID to filter
        repo_name: Optional repository name to filter
        sink: Sink type (auto|clickhouse|mongo|sqlite|postgres|both)
        provider: Work item provider (auto|all|jira|github|gitlab|none)
        org_id: Organization scope

    Returns:
        dict with job status and summary
    """
    from dev_health_ops.metrics.job_daily import (
        _date_range,
        run_daily_metrics_finalize,
        run_daily_metrics_job,
    )

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else utc_today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting daily metrics task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        repo_name or str(parsed_repo_id) or "all",
    )

    try:
        if org_id:
            from dev_health_ops.db import get_postgres_session_sync

            with get_postgres_session_sync() as session:
                if not organization_exists_sync(session, org_id):
                    logger.info(
                        "Skipping daily metrics task for deleted org_id=%s", org_id
                    )
                    return {
                        "status": "skipped",
                        "reason": "organization_not_found",
                        "day": target_day.isoformat(),
                    }

        # Run the async job in a new event loop
        run_async(
            run_daily_metrics_job(
                db_url=db_url,
                day=target_day,
                backfill_days=backfill_days,
                repo_id=parsed_repo_id,
                repo_name=repo_name,
                sink=sink,
                provider=provider,
                org_id=org_id or "",
                # CHAOS-5194 codex r1 (P1, #2277): this Celery task is a
                # THIRD invocation surface for run_daily_metrics_job --
                # neither of the two the finalize-scope migration had
                # already accounted for (the Go-orchestrated worker path,
                # which triggers a separate finalize job once every
                # partition lands; and the CLI paths in job_daily.py's own
                # _cmd_metrics_daily/_cmd_metrics_rebuild, fixed for the
                # identical gap by CHAOS-4365 codex R2). Recompute.py
                # dispatches THIS task for repository-scoped and fallback
                # recomputes, and it never called the standalone
                # run_daily_metrics_finalize -- so team-scope
                # compounding_risk_daily/team_cognitive_load_daily (once
                # those land) and now benchmarking (this PR) would silently
                # never compute for a recompute-triggered run, with no task
                # failure to signal it. skip_finalize=True here avoids
                # running run_daily_metrics_job's OWN inline ic_metrics/
                # landscape logic (skip_finalize=False's default path)
                # TWICE per day against the explicit run_daily_metrics_finalize
                # call below, matching _cmd_metrics_daily's exact fix for
                # the same defect class.
                skip_finalize=True,
            )
        )
        # CHAOS-5194 codex r1 (P1, #2277): see the skip_finalize comment
        # above -- this call is what that gate exists to route to instead.
        #
        # CHAOS-5194 codex r2 (P1, confirmed): recompute.py's dispatch_recompute
        # fans OUT one independent Celery chain PER repo_id for a multi-repo
        # recompute plan (external_ingest/recompute.py:355, "fan out N
        # independent chains, one per repo") -- there is no fan-IN/join
        # afterward, so N sibling invocations of THIS task, one per repo, each
        # independently reach this call for the SAME org/day. That is fine for
        # a family that is idempotent to duplicate (_cmd_metrics_daily's own
        # docstring already establishes ic_finalize/compounding_risk_team/
        # team_cognitive_load are safe to call more than once for the same
        # day -- their output tables are read with a dedup pattern, e.g.
        # user_metrics_daily's "ORDER BY computed_at DESC LIMIT 1 BY (...)",
        # so the LATER write simply wins). It is NOT fine for benchmarking:
        # its six output tables are plain MergeTree with no version column
        # and no established read-time dedup (see benchmarking/types.go's own
        # package doc comment) -- every one of the N sibling task's finalize
        # calls would independently compute and APPEND a full row set,
        # duplicating N times instead of the single org-wide write this
        # family is supposed to produce. This is the exact
        # once-per-org-not-once-per-repo problem CHAOS-5194's own barrier
        # exists to prevent, reintroduced through THIS invocation surface,
        # which has no cross-repo join and so cannot itself guarantee
        # "exactly one caller" the way ClaimFinalize's transactional barrier
        # does for the Go-orchestrated path.
        #
        # Fix: always exclude "benchmarking" from this call. The corrected
        # claim (the ABOVE PARAGRAPH corrects the WRONG "idempotent... same
        # idempotency _cmd_metrics_daily's docstring already establishes for
        # this exact function" comment that used to be here -- that claim is
        # true for ic_finalize/compounding_risk_team/team_cognitive_load, but
        # was wrongly generalized to benchmarking, which is not idempotent to
        # duplicate) means benchmarking is left to the Go-orchestrated native
        # path (BenchmarkingFinalizeExecutor, gated by ClaimFinalize's real
        # barrier) or a future single, explicitly-synchronized caller for the
        # recompute path specifically -- neither of which this task can be,
        # since it is inherently one-task-per-repo by construction and has no
        # way to know whether a sibling repo's recompute is still in flight
        # for the same org/day.
        for finalize_day in _date_range(target_day, backfill_days):
            run_async(
                run_daily_metrics_finalize(
                    db_url=db_url,
                    day=finalize_day,
                    org_id=org_id or "",
                    sink=sink,
                    skip_families={"benchmarking"},
                )
            )
        # Invalidate GraphQL cache after successful metrics update
        _invalidate_metrics_cache(target_day.isoformat(), "")

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
        }
    except Exception as exc:
        logger.exception("Daily metrics task failed: %s", exc)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
