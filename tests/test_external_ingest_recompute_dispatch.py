"""Unit tests for ``dispatch_recompute()`` (CHAOS-2699, brief D5/D6/D13/D14).

Patches Celery ``chain``/``signature``/``send_task`` exactly like
``tests/test_post_sync_investment_dispatch.py`` -- no live broker, no
ClickHouse.
"""

from __future__ import annotations

import inspect
import pathlib
from typing import Any
from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors
# circular import that otherwise ERRORs isolated collection (mirrors
# CHAOS-2370, same guard as test_post_sync_investment_dispatch.py).
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.external_ingest.recompute as recompute_mod
from dev_health_ops.external_ingest.recompute import RecomputePlan, dispatch_recompute

_DAILY_TASK = "dev_health_ops.workers.tasks.run_daily_metrics"
_INVESTMENT_TASK = (
    "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned"
)


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


def _make_sig(name, **kwargs):
    sig = MagicMock(name=f"sig:{name}")
    sig.task_name = name
    sig.sig_kwargs = kwargs
    return sig


def _async_result(task_id, parent=None):
    r = MagicMock(name=f"result:{task_id}")
    r.id = task_id
    r.parent = parent
    return r


def test_per_repo_daily_dispatched_with_correct_kwargs() -> None:
    """CHAOS-4924: run_work_graph_build was chained after run_daily_metrics
    per repo; the task is deleted outright (its compute was already a
    0-stats no-op, and the Go worker's own post-sync writer creates
    workgraph.build requests independently). Per-repo dispatch is now a
    plain send_task, no chain."""
    with (
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):

        def _send_side_effect(task_name, **kwargs):
            if task_name == _DAILY_TASK:
                repo_id = kwargs["kwargs"]["repo_id"]
                return _async_result(f"daily-{repo_id}")
            return _async_result("investment-1")

        mock_send.side_effect = _send_side_effect

        plan = _plan(team_ids=("team-a",))
        result = dispatch_recompute(plan)

    assert result.status == "dispatched"

    daily_calls = [c for c in mock_send.call_args_list if c.args[0] == _DAILY_TASK]
    assert len(daily_calls) == 2  # one per repo

    for call in daily_calls:
        assert call.kwargs["queue"] == "metrics"
        assert call.kwargs["kwargs"]["org_id"] == "org-1"
        assert call.kwargs["kwargs"]["day"] == "2026-06-26"
        assert call.kwargs["kwargs"]["backfill_days"] == 2

    dispatched_repo_ids = {c.kwargs["kwargs"]["repo_id"] for c in daily_calls}
    assert dispatched_repo_ids == {"repo-a", "repo-b"}

    # Investment materialize fires exactly ONCE, with the full repo list
    # (not per-repo) plus the team_ids scope (D5).
    investment_calls = [
        c for c in mock_send.call_args_list if c.args[0] == _INVESTMENT_TASK
    ]
    assert len(investment_calls) == 1
    inv_kwargs = investment_calls[0].kwargs["kwargs"]
    assert inv_kwargs["repo_ids"] == ["repo-a", "repo-b"]
    assert inv_kwargs["team_ids"] == ["team-a"]
    assert inv_kwargs["force"] is False
    assert investment_calls[0].kwargs["queue"] == "default"

    assert len(result.jobs) == 3  # 2x daily + 1 investment


def test_fallback_org_wide_daily_uses_send_task_no_repo_id_no_work_graph() -> None:
    plan = _plan(
        repo_ids=(), fallback_org_wide_daily=True, skip_investment_no_scope=True
    )
    with (
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.signature"
        ) as mock_sig,
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        mock_send.return_value = _async_result("daily-fallback")
        result = dispatch_recompute(plan)

    mock_sig.assert_not_called()
    mock_send.assert_called_once()
    call = mock_send.call_args
    assert call.args[0] == _DAILY_TASK
    assert "repo_id" not in call.kwargs["kwargs"]
    assert call.kwargs["queue"] == "metrics"

    assert result.status == "dispatched"
    assert len(result.jobs) == 1
    assert result.jobs[0].task == _DAILY_TASK
    assert result.jobs[0].repo_id is None


def test_nothing_dispatchable_returns_skipped_no_scope() -> None:
    plan = _plan(
        dispatch_daily=True,
        repo_ids=(),
        fallback_org_wide_daily=False,
        skip_investment_no_scope=True,
        team_ids=(),
    )
    with (
        patch("dev_health_ops.external_ingest.recompute.celery_app.signature"),
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        result = dispatch_recompute(plan)

    mock_send.assert_not_called()
    assert result.status == "skipped_no_scope"
    assert result.jobs == ()


def test_not_trigger_returns_not_applicable_without_touching_celery() -> None:
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
    with (
        patch("dev_health_ops.external_ingest.recompute.celery_app.signature"),
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        result = dispatch_recompute(plan)

    mock_send.assert_not_called()
    assert result.status == "not_applicable"
    assert result.jobs == ()


def test_dispatch_exception_returns_failed_never_raises() -> None:
    # CHAOS-4924: dispatch_recompute's per-repo daily dispatch no longer
    # calls celery_app.signature at all (the chain it used to feed,
    # daily_sig -> build_sig, is gone) -- inject the failure into send_task,
    # the call every remaining dispatch path in this function actually
    # makes, to keep exercising the real D13 contract.
    plan = _plan()
    with patch(
        "dev_health_ops.external_ingest.recompute.celery_app.send_task",
        side_effect=RuntimeError("send_task boom"),
    ):
        result = dispatch_recompute(plan)

    assert result.status == "failed"
    assert result.jobs == ()
    assert result.error is not None
    assert "send_task boom" in result.error


def test_capped_flags_propagate_from_plan_to_result() -> None:
    plan = _plan(
        repo_ids=(), fallback_org_wide_daily=True, capped_days=True, capped_repos=True
    )
    with (
        patch("dev_health_ops.external_ingest.recompute.celery_app.signature"),
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        mock_send.return_value = _async_result("daily-fallback")
        result = dispatch_recompute(plan)

    assert result.capped_days is True
    assert result.capped_repos is True


def test_never_references_disqualified_tasks() -> None:
    """D6 negative-space: run_dora_metrics/run_complexity_job (deferred-v1
    kinds) and dispatch_daily_metrics_partitioned/run_daily_metrics_batch
    (disqualified per D6's two findings) must never appear in this
    module's source."""
    source = pathlib.Path(recompute_mod.__file__).read_text()
    for forbidden in (
        "run_dora_metrics",
        "run_complexity_job",
        "dispatch_daily_metrics_partitioned",
        "run_daily_metrics_batch",
    ):
        assert forbidden not in source, f"{forbidden} must never be referenced (D6)"


def test_daily_metrics_kwargs_subset_of_task_signature() -> None:
    """D14: kwargs built for run_daily_metrics must be a strict subset of
    the real task's ``.run`` signature -- catches kwarg drift that a
    mocked ``.delay()``/``.apply_async()`` call would hide."""
    from dev_health_ops.workers.metrics_daily import run_daily_metrics

    plan = _plan()
    kwargs = recompute_mod._daily_metrics_kwargs(plan, repo_id="repo-a")
    params = set(inspect.signature(run_daily_metrics.run).parameters)
    assert set(kwargs) <= params

    fallback_kwargs = recompute_mod._daily_metrics_kwargs(plan, repo_id=None)
    assert set(fallback_kwargs) <= params
    assert "repo_id" not in fallback_kwargs


def test_investment_kwargs_subset_of_task_signature() -> None:
    from dev_health_ops.workers.work_graph_tasks import (
        dispatch_investment_materialize_partitioned,
    )

    plan = _plan(team_ids=("team-a",))
    kwargs = recompute_mod._investment_kwargs(plan)
    params = set(
        inspect.signature(dispatch_investment_materialize_partitioned.run).parameters
    )
    assert set(kwargs) <= params
    assert kwargs["force"] is False
