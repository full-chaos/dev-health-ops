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

These tests prove the seam without a live ClickHouse: they patch the same
Celery ``send_task`` / ``chain`` / ``signature`` factories the sibling
investment-dispatch test patches and assert the dispatch contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dev_health_ops.workers.sync_runtime import _dispatch_post_sync_tasks

_DORA_TASK = "dev_health_ops.workers.tasks.run_dora_metrics"


def _run_dispatch(provider: str, sync_targets: list[str], org_id: str):
    """Drive _dispatch_post_sync_tasks with chain/signature/send_task patched.

    Returns (signature_mock, chain_mock, chain_instance_mock, send_task_mock).
    """
    with (
        patch(
            "dev_health_ops.workers.sync_runtime.celery_app.signature"
        ) as mock_signature,
        patch("dev_health_ops.workers.sync_runtime.chain") as mock_chain,
        patch(
            "dev_health_ops.workers.sync_runtime.celery_app.send_task"
        ) as mock_send_task,
    ):

        def _make_sig(name, **kwargs):
            sig = MagicMock(name=f"sig:{name}")
            sig.task_name = name
            sig.sig_kwargs = kwargs
            return sig

        mock_signature.side_effect = _make_sig
        chain_instance = MagicMock(name="chain_instance")
        mock_chain.return_value = chain_instance
        _dispatch_post_sync_tasks(
            provider=provider,
            sync_targets=sync_targets,
            org_id=org_id,
        )
    return mock_signature, mock_chain, chain_instance, mock_send_task


def _dora_calls(mock_send_task):
    """Return the send_task call objects that dispatched run_dora_metrics."""
    return [
        call
        for call in mock_send_task.call_args_list
        if call.args and call.args[0] == _DORA_TASK
    ]


def test_dora_dispatched_for_deployments_only_no_git() -> None:
    """deployments-only (no git) => run_dora_metrics sent, org-scoped."""
    _, _, _, mock_send_task = _run_dispatch(
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
    _, _, _, mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["cicd"],
        org_id="org-456",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    assert dora_calls[0].kwargs["kwargs"] == {"org_id": "org-456"}


def test_dora_dispatched_for_incidents_only_no_git() -> None:
    """incidents-only (no git) => run_dora_metrics sent, org-scoped."""
    _, _, _, mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["incidents"],
        org_id="org-789",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    assert dora_calls[0].kwargs["kwargs"] == {"org_id": "org-789"}


def test_dora_not_dispatched_for_work_items_only() -> None:
    """work-items-only (no git, no DORA targets) => run_dora_metrics NOT sent.

    A Jira/Linear work-items sync still fires the investment chain, but DORA has
    no fresh inputs, so it must not be enqueued.
    """
    _, _, _, mock_send_task = _run_dispatch(
        provider="jira",
        sync_targets=["work-items"],
        org_id="org-123",
    )

    assert _dora_calls(mock_send_task) == []


def test_dora_dispatched_for_git_only() -> None:
    """git-only still dispatches run_dora_metrics (CHAOS-2382 unchanged)."""
    _, _, _, mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs"],
        org_id="org-123",
    )

    dora_calls = _dora_calls(mock_send_task)
    assert len(dora_calls) == 1
    assert dora_calls[0].kwargs["kwargs"] == {"org_id": "org-123"}
