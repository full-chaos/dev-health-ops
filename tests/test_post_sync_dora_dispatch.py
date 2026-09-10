"""Unit tests for the post-sync DORA dispatch (CHAOS-2399).

DORA metrics (deployment frequency, lead time, change-fail rate, MTTR) are
computed by the ``run_dora_metrics`` Celery task from
deployments/cicd/incidents rows persisted in ClickHouse. Originally
``_dispatch_post_sync_tasks`` only enqueued ``run_dora_metrics`` after a *git*
sync (CHAOS-2382). But the DORA inputs — ``deployments``, ``cicd`` and
``incidents`` — can be carried by sync configs that do **not** also sync git
(e.g. a deployments-only config). Gating DORA on git alone left those orgs with
stale DORA until the next daily beat after such a sync.

``_dispatch_post_sync_tasks`` now computes
``has_dora = target_set & _DORA_TARGETS`` ({"deployments", "cicd",
"incidents"}) and dispatches ``run_dora_metrics`` when ``has_git or has_dora``.

CHAOS-3093: the sibling investment-dispatch chain (``chain``/``signature``)
this file used to patch alongside ``send_task`` was deleted outright
(dead-into-the-void Celery machinery, no consumer since 2026-08-19) --
``run_dora_metrics`` was never part of that chain, it has always been a
standalone ``celery_app.send_task(...)`` call, so these tests only ever
needed to patch that one seam.
"""

from __future__ import annotations

from unittest.mock import patch

from dev_health_ops.workers.post_sync_dispatch import _dispatch_post_sync_tasks

_DORA_TASK = "dev_health_ops.workers.tasks.run_dora_metrics"


def _run_dispatch(provider: str, sync_targets: list[str], org_id: str):
    """Drive _dispatch_post_sync_tasks with send_task patched.

    Returns the send_task mock.
    """
    with patch(
        "dev_health_ops.workers.post_sync_dispatch.celery_app.send_task"
    ) as mock_send_task:
        _dispatch_post_sync_tasks(
            provider=provider,
            sync_targets=sync_targets,
            org_id=org_id,
        )
    return mock_send_task


def _dora_calls(mock_send_task):
    """Return the send_task call objects that dispatched run_dora_metrics."""
    return [
        call
        for call in mock_send_task.call_args_list
        if call.args and call.args[0] == _DORA_TASK
    ]


def test_dora_dispatched_for_deployments_only_no_git() -> None:
    """deployments-only (no git) => run_dora_metrics sent, org-scoped."""
    mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["deployments"],
        org_id="org-123",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    call = dora_calls[0]
    assert call.kwargs["kwargs"] == {"org_id": "org-123"}
    assert call.kwargs["queue"] == "metrics"


def test_dora_dispatched_for_cicd_only_no_git() -> None:
    """cicd-only (no git) => run_dora_metrics sent, org-scoped."""
    mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["cicd"],
        org_id="org-456",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    assert dora_calls[0].kwargs["kwargs"] == {"org_id": "org-456"}


def test_dora_dispatched_for_incidents_only_no_git() -> None:
    """incidents-only (no git) => run_dora_metrics sent, org-scoped."""
    mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["incidents"],
        org_id="org-789",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    assert dora_calls[0].kwargs["kwargs"] == {"org_id": "org-789"}


def test_dora_not_dispatched_for_work_items_only() -> None:
    """work-items-only (no git, no DORA targets) => run_dora_metrics NOT sent.

    A Jira/Linear work-items sync has no DORA-relevant inputs, so it must not
    be enqueued.
    """
    mock_send_task = _run_dispatch(
        provider="jira",
        sync_targets=["work-items"],
        org_id="org-123",
    )

    assert _dora_calls(mock_send_task) == []


def test_dora_dispatched_for_git_only() -> None:
    """git-only still dispatches run_dora_metrics (CHAOS-2382 unchanged)."""
    mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs"],
        org_id="org-123",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    assert dora_calls[0].kwargs["kwargs"] == {"org_id": "org-123"}
