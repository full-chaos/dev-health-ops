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
_BUILD_TASK = "dev_health_ops.workers.tasks.run_work_graph_build"
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


def test_per_repo_chains_dispatched_with_correct_kwargs() -> None:
    with (
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.signature"
        ) as mock_sig,
        patch("dev_health_ops.external_ingest.recompute.chain") as mock_chain,
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        mock_sig.side_effect = _make_sig

        def _chain_side_effect(daily_sig, build_sig):
            repo_id = daily_sig.sig_kwargs["kwargs"]["repo_id"]
            chain_instance = MagicMock(name=f"chain:{repo_id}")
            daily_result = _async_result(f"daily-{repo_id}")
            build_result = _async_result(f"build-{repo_id}", parent=daily_result)
            chain_instance.apply_async.return_value = build_result
            return chain_instance

        mock_chain.side_effect = _chain_side_effect
        mock_send.return_value = _async_result("investment-1")

        plan = _plan(team_ids=("team-a",))
        result = dispatch_recompute(plan)

    assert result.status == "dispatched"
    assert mock_chain.call_count == 2  # one per repo

    for call in mock_chain.call_args_list:
        daily_sig, build_sig = call.args
        assert daily_sig.task_name == _DAILY_TASK
        assert daily_sig.sig_kwargs["queue"] == "metrics"
        assert daily_sig.sig_kwargs.get("immutable") is True
        assert daily_sig.sig_kwargs["kwargs"]["org_id"] == "org-1"
        assert daily_sig.sig_kwargs["kwargs"]["day"] == "2026-06-26"
        assert daily_sig.sig_kwargs["kwargs"]["backfill_days"] == 2

        assert build_sig.task_name == _BUILD_TASK
        assert build_sig.sig_kwargs["queue"] == "metrics"
        assert build_sig.sig_kwargs.get("immutable") is True
        assert build_sig.sig_kwargs["kwargs"]["from_date"] == plan.from_date
        assert build_sig.sig_kwargs["kwargs"]["to_date"] == plan.to_date

    dispatched_repo_ids = {
        call.args[0].sig_kwargs["kwargs"]["repo_id"]
        for call in mock_chain.call_args_list
    }
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

    assert len(result.jobs) == 5  # 2x(daily+build) + 1 investment


def test_per_repo_chain_with_no_parent_result_still_dispatches_with_none_task_id() -> (
    None
):
    """Adversarial-review finding: a chain's AsyncResult can legitimately
    have no ``.parent`` (chain result-metadata unavailable) -- the daily
    job's captured task_id is then ``None``, but the Celery dispatch itself
    already succeeded, so this must still surface as ``status="dispatched"``
    with a job record carrying ``task_id=None`` rather than losing the
    daily job entirely."""
    plan = _plan(repo_ids=("repo-a",), team_ids=())
    with (
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.signature"
        ) as mock_sig,
        patch("dev_health_ops.external_ingest.recompute.chain") as mock_chain,
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        mock_sig.side_effect = _make_sig

        def _chain_side_effect(daily_sig, build_sig):
            chain_instance = MagicMock()
            build_result = _async_result("build-repo-a", parent=None)
            chain_instance.apply_async.return_value = build_result
            return chain_instance

        mock_chain.side_effect = _chain_side_effect
        mock_send.return_value = _async_result("investment-1")

        result = dispatch_recompute(plan)

    assert result.status == "dispatched"
    daily_jobs = [j for j in result.jobs if j.task == _DAILY_TASK]
    assert len(daily_jobs) == 1
    assert daily_jobs[0].task_id is None
    build_jobs = [j for j in result.jobs if j.task == _BUILD_TASK]
    assert build_jobs[0].task_id == "build-repo-a"


def test_fallback_org_wide_daily_uses_send_task_no_repo_id_no_work_graph() -> None:
    plan = _plan(
        repo_ids=(), fallback_org_wide_daily=True, skip_investment_no_scope=True
    )
    with (
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.signature"
        ) as mock_sig,
        patch("dev_health_ops.external_ingest.recompute.chain") as mock_chain,
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        mock_send.return_value = _async_result("daily-fallback")
        result = dispatch_recompute(plan)

    mock_chain.assert_not_called()
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
        patch("dev_health_ops.external_ingest.recompute.chain") as mock_chain,
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        result = dispatch_recompute(plan)

    mock_chain.assert_not_called()
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
        patch("dev_health_ops.external_ingest.recompute.chain") as mock_chain,
        patch(
            "dev_health_ops.external_ingest.recompute.celery_app.send_task"
        ) as mock_send,
    ):
        result = dispatch_recompute(plan)

    mock_chain.assert_not_called()
    mock_send.assert_not_called()
    assert result.status == "not_applicable"
    assert result.jobs == ()


def test_dispatch_exception_returns_failed_never_raises() -> None:
    plan = _plan()
    with patch(
        "dev_health_ops.external_ingest.recompute.celery_app.signature",
        side_effect=RuntimeError("signature boom"),
    ):
        result = dispatch_recompute(plan)

    assert result.status == "failed"
    assert result.jobs == ()
    assert result.error is not None
    assert "signature boom" in result.error


def test_capped_flags_propagate_from_plan_to_result() -> None:
    plan = _plan(
        repo_ids=(), fallback_org_wide_daily=True, capped_days=True, capped_repos=True
    )
    with (
        patch("dev_health_ops.external_ingest.recompute.celery_app.signature"),
        patch("dev_health_ops.external_ingest.recompute.chain"),
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


def test_work_graph_build_kwargs_subset_of_task_signature() -> None:
    from dev_health_ops.workers.work_graph_tasks import run_work_graph_build

    plan = _plan()
    kwargs = recompute_mod._work_graph_build_kwargs(plan, repo_id="repo-a")
    params = set(inspect.signature(run_work_graph_build.run).parameters)
    assert set(kwargs) <= params


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
