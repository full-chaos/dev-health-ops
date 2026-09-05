"""CHAOS-5194 codex r1 (P1, #2277, F1): `workers/metrics_daily.py`'s Celery
task `run_daily_metrics` -- dispatched by `external_ingest/recompute.py` for
repository-scoped and fallback recomputes -- must call BOTH
`run_daily_metrics_job(..., skip_finalize=True)` AND
`run_daily_metrics_finalize(...)` once per day in the backfill range, exactly
like `_cmd_metrics_daily`'s established CLI-path fix for the identical defect
class (CHAOS-4365 codex R2 P1).

Before this fix, the task called only `run_daily_metrics_job` with
`skip_finalize` left at its default (False) -- so a family living exclusively
in `run_daily_metrics_finalize` (team-scope compounding_risk_daily /
team_cognitive_load_daily, and now benchmarking, CHAOS-5194's own change)
would never compute for a recompute-triggered run, and the run would report
success regardless. Calling `run_daily_metrics_job` with `skip_finalize`
still False (its default) alongside a *separate* finalize call would instead
double-run job_daily.py's OWN inline ic_metrics/landscape logic -- the second
symptom this fix also guards against.
"""

from __future__ import annotations

from datetime import date

import dev_health_ops.metrics.job_daily as job_daily_mod
from dev_health_ops.workers import metrics_daily as metrics_daily_mod


def _install_recording_fakes(monkeypatch):
    """Patch the two compute entry points at their SOURCE module
    (job_daily), matching how `run_daily_metrics` actually resolves them: the
    import lives INSIDE the task's function body, so it re-binds these names
    from `job_daily` fresh on every call -- patching job_daily's own
    attributes is what a call-time import will see.
    """
    job_job_calls: list[dict] = []
    finalize_calls: list[dict] = []

    async def _fake_run_daily_metrics_job(**kwargs):
        job_job_calls.append(kwargs)
        return {}

    async def _fake_run_daily_metrics_finalize(**kwargs):
        finalize_calls.append(kwargs)

    monkeypatch.setattr(
        job_daily_mod, "run_daily_metrics_job", _fake_run_daily_metrics_job
    )
    monkeypatch.setattr(
        job_daily_mod, "run_daily_metrics_finalize", _fake_run_daily_metrics_finalize
    )
    # `run_async` for real would call `asyncio.run` -- fine here since the
    # coroutines above do no real I/O -- but it also resets async DB engines
    # via `db.reset_async_engines()`, an unrelated side effect this test does
    # not need and should not depend on being safe to call repeatedly.
    import asyncio

    monkeypatch.setattr(metrics_daily_mod, "run_async", asyncio.run)
    monkeypatch.setattr(
        metrics_daily_mod, "_invalidate_metrics_cache", lambda *a, **k: None
    )
    return job_job_calls, finalize_calls


def test_run_daily_metrics_task_skips_inline_finalize_and_calls_it_explicitly(
    monkeypatch,
) -> None:
    job_job_calls, finalize_calls = _install_recording_fakes(monkeypatch)

    result = metrics_daily_mod.run_daily_metrics.run(
        db_url="postgres://fake",
        day="2026-08-24",
        backfill_days=1,
        sink="clickhouse",
        org_id=None,
    )

    assert result["status"] == "success"
    assert len(job_job_calls) == 1
    assert job_job_calls[0]["skip_finalize"] is True, (
        "run_daily_metrics_job must be called with skip_finalize=True -- "
        "otherwise its own inline ic_metrics/landscape logic double-computes "
        "against the explicit run_daily_metrics_finalize call below"
    )
    assert len(finalize_calls) == 1
    assert finalize_calls[0]["day"] == date(2026, 8, 24)
    assert finalize_calls[0]["org_id"] == ""
    assert finalize_calls[0]["sink"] == "clickhouse"


def test_run_daily_metrics_task_calls_finalize_once_per_backfill_day(
    monkeypatch,
) -> None:
    """A multi-day backfill must finalize EVERY day in range, not just the
    target day -- mirrors _cmd_metrics_daily's own for-loop over
    _date_range(end_day, backfill_days)."""
    job_job_calls, finalize_calls = _install_recording_fakes(monkeypatch)

    # org_id deliberately None -- a truthy org_id routes through
    # get_postgres_session_sync's real deleted-org guard, which needs a live
    # Postgres connection this unit test has no business standing up. That
    # guard is orthogonal to what this test proves (the finalize fan-out over
    # backfill_days) and is exercised by test_org_guard.py separately.
    metrics_daily_mod.run_daily_metrics.run(
        db_url="postgres://fake",
        day="2026-08-24",
        backfill_days=3,
        sink="clickhouse",
        org_id=None,
    )

    assert len(job_job_calls) == 1
    assert job_job_calls[0]["skip_finalize"] is True

    finalized_days = sorted(call["day"] for call in finalize_calls)
    assert finalized_days == [
        date(2026, 8, 22),
        date(2026, 8, 23),
        date(2026, 8, 24),
    ], "must finalize every day _date_range(2026-08-24, backfill_days=3) yields"
    for call in finalize_calls:
        assert call["org_id"] == ""
