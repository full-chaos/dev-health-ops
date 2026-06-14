"""Unit tests for the post-sync investment-materialize dispatch (CHAOS-2374).

work_unit_investments / work_unit_investment_quotes are written only by
``materialize_investments`` via the ``run_investment_materialize`` Celery task.
That task was never dispatched on the live sync path, so real orgs saw an empty
``/investment`` view. ``_dispatch_post_sync_tasks`` now enqueues it right after
``run_work_graph_build`` (investment depends on the work-graph build).

These tests prove the seam without a live ClickHouse: they mock
``celery_app.send_task`` and assert the dispatch contract.
"""

from __future__ import annotations

from unittest.mock import patch

from dev_health_ops.workers.sync_runtime import _dispatch_post_sync_tasks

_INVESTMENT_TASK = "dev_health_ops.workers.tasks.run_investment_materialize"
_WORK_GRAPH_TASK = "dev_health_ops.workers.tasks.run_work_graph_build"


def _sent_task_names(mock_send_task) -> list[str]:
    return [call.args[0] for call in mock_send_task.call_args_list]


def test_investment_materialize_dispatched_with_git_and_work_items() -> None:
    """git + work-items => run_investment_materialize enqueued, org-scoped."""
    with patch(
        "dev_health_ops.workers.sync_runtime.celery_app.send_task"
    ) as mock_send_task:
        _dispatch_post_sync_tasks(
            provider="github",
            sync_targets=["git", "prs", "work-items"],
            org_id="org-123",
        )

    names = _sent_task_names(mock_send_task)
    assert _INVESTMENT_TASK in names
    # Investment must be dispatched AFTER the work-graph build it depends on.
    assert names.index(_INVESTMENT_TASK) > names.index(_WORK_GRAPH_TASK)

    investment_call = next(
        call
        for call in mock_send_task.call_args_list
        if call.args[0] == _INVESTMENT_TASK
    )
    assert investment_call.kwargs["kwargs"] == {"org_id": "org-123"}
    assert investment_call.kwargs["queue"] == "metrics"


def test_investment_materialize_not_dispatched_without_work_items() -> None:
    """git only (no work-items) => investment materialize NOT enqueued."""
    with patch(
        "dev_health_ops.workers.sync_runtime.celery_app.send_task"
    ) as mock_send_task:
        _dispatch_post_sync_tasks(
            provider="github",
            sync_targets=["git", "prs"],
            org_id="org-123",
        )

    names = _sent_task_names(mock_send_task)
    assert _INVESTMENT_TASK not in names
    # The work-graph build (its dependency) is also gated on work-items.
    assert _WORK_GRAPH_TASK not in names


def test_investment_materialize_not_dispatched_with_work_items_only() -> None:
    """work-items only (no git) => investment materialize NOT enqueued."""
    with patch(
        "dev_health_ops.workers.sync_runtime.celery_app.send_task"
    ) as mock_send_task:
        _dispatch_post_sync_tasks(
            provider="jira",
            sync_targets=["work-items"],
            org_id="org-123",
        )

    assert _INVESTMENT_TASK not in _sent_task_names(mock_send_task)


def test_run_investment_materialize_forwards_org_id_to_config() -> None:
    """The task forwards org_id into MaterializeConfig so queries stay scoped."""
    from typing import Any, cast

    from dev_health_ops.workers.work_graph_tasks import run_investment_materialize

    captured: dict[str, Any] = {}

    class _FakeConfig:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    with (
        patch(
            "dev_health_ops.work_graph.investment.materialize.MaterializeConfig",
            _FakeConfig,
        ),
        patch(
            "dev_health_ops.work_graph.investment.materialize.materialize_investments",
            return_value=None,
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.run_async",
            return_value={"components": 0, "records": 0, "quotes": 0},
        ),
    ):
        task = cast(Any, run_investment_materialize)
        result = task.run(db_url="clickhouse://x", org_id="org-123")

    assert result["status"] == "success"
    assert captured["org_id"] == "org-123"


def test_run_investment_materialize_empty_org_id_becomes_none() -> None:
    """An empty org_id collapses to None (no accidental cross-org scan)."""
    from typing import Any, cast

    from dev_health_ops.workers.work_graph_tasks import run_investment_materialize

    captured: dict[str, Any] = {}

    class _FakeConfig:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    with (
        patch(
            "dev_health_ops.work_graph.investment.materialize.MaterializeConfig",
            _FakeConfig,
        ),
        patch(
            "dev_health_ops.work_graph.investment.materialize.materialize_investments",
            return_value=None,
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.run_async",
            return_value={"components": 0, "records": 0, "quotes": 0},
        ),
    ):
        task = cast(Any, run_investment_materialize)
        task.run(db_url="clickhouse://x")

    assert captured["org_id"] is None
