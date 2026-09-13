"""Unit tests for ``dispatch_recompute()`` (CHAOS-2699, brief D5/D6/D13/D14).

CHAOS-5700: ``dispatch_recompute`` no longer talks to Celery at all -- it
only classifies a bounded plan into an outcome. The actual hand-off to the
existing native external-ingest recompute drain happens one layer up, in
``recompute_status.record_recompute_dispatch`` (see
``tests/test_external_ingest_recompute_status_sql.py`` for that half).
"""

from __future__ import annotations

import pathlib
from typing import Any

# Import connectors first to defuse the providers._base <-> connectors
# circular import that otherwise ERRORs isolated collection (mirrors
# CHAOS-2370, same guard as test_post_sync_investment_dispatch.py).
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.external_ingest.recompute as recompute_mod
from dev_health_ops.external_ingest.recompute import RecomputePlan, dispatch_recompute


def _plan(**overrides: Any) -> RecomputePlan:
    defaults: dict[str, Any] = dict(
        org_id="org-1",
        trigger=True,
        dispatch_daily=True,
        repo_ids=("repo-a", "repo-b"),
        team_ids=(),
        day="2026-06-26",
        backfill_days=2,
        from_date="2026-06-25T00:00:00+00:00",
        to_date="2026-06-26T00:00:00+00:00",
        capped_days=False,
        capped_repos=False,
        fallback_org_wide_daily=False,
        skip_investment_no_scope=False,
    )
    defaults.update(overrides)
    return RecomputePlan(**defaults)


def test_per_repo_daily_and_investment_scope_dispatches() -> None:
    """A plan with a repo scope (daily) and a non-empty investment scope
    (D4) is a "dispatched" outcome -- no per-job records anymore, since the
    actual fan-out now happens natively, keyed off the persisted scope
    rather than a list of Celery task ids."""
    plan = _plan(team_ids=("team-a",))
    result = dispatch_recompute(plan)

    assert result.status == "dispatched"
    assert result.jobs == ()
    assert result.capped_days is False
    assert result.capped_repos is False


def test_fallback_org_wide_daily_dispatches() -> None:
    plan = _plan(
        repo_ids=(), fallback_org_wide_daily=True, skip_investment_no_scope=True
    )
    result = dispatch_recompute(plan)

    assert result.status == "dispatched"
    assert result.jobs == ()


def test_nothing_dispatchable_returns_skipped_no_scope() -> None:
    plan = _plan(
        dispatch_daily=True,
        repo_ids=(),
        fallback_org_wide_daily=False,
        skip_investment_no_scope=True,
        team_ids=(),
    )
    result = dispatch_recompute(plan)

    assert result.status == "skipped_no_scope"
    assert result.jobs == ()


def test_not_trigger_returns_not_applicable() -> None:
    plan = _plan(
        trigger=False,
        dispatch_daily=False,
        repo_ids=(),
        team_ids=(),
        day=None,
        backfill_days=None,
        from_date=None,
        to_date=None,
        fallback_org_wide_daily=False,
        skip_investment_no_scope=False,
    )
    result = dispatch_recompute(plan)

    assert result.status == "not_applicable"
    assert result.jobs == ()


def test_capped_flags_propagate_from_plan_to_result() -> None:
    plan = _plan(
        repo_ids=(), fallback_org_wide_daily=True, capped_days=True, capped_repos=True
    )
    result = dispatch_recompute(plan)

    assert result.status == "dispatched"
    assert result.capped_days is True
    assert result.capped_repos is True


def test_never_references_celery_or_disqualified_tasks() -> None:
    """D6 negative-space: run_dora_metrics/run_complexity_job (deferred-v1
    kinds) and dispatch_daily_metrics_partitioned/run_daily_metrics_batch
    (disqualified per D6's two findings) must never appear in this
    module's source. CHAOS-5700: neither must Celery -- the module no
    longer imports or calls it at all."""
    source = pathlib.Path(recompute_mod.__file__).read_text()
    for forbidden in (
        "run_dora_metrics",
        "run_complexity_job",
        "dispatch_daily_metrics_partitioned",
        "run_daily_metrics_batch",
        "celery",
    ):
        assert forbidden not in source, f"{forbidden} must never be referenced (D6)"
