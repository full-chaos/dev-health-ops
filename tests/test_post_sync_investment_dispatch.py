"""Unit tests for the post-sync investment-materialize dispatch (CHAOS-2374).

``work_unit_investments`` / ``work_unit_investment_quotes`` are written only by
``materialize_investments`` via the (chunked) ``dispatch_investment_materialize_partitioned``
/ ``run_investment_materialize_chunk`` Celery chord. That chord was never
dispatched on the live sync path, so real orgs saw an empty ``/investment``
view. (The plain, unchunked ``run_investment_materialize`` task -- only ever
called by worker_workgraph.py's POST /execute route -- was deleted under
CHAOS-3092 (leftovers); investment.materialize's River kind is entirely
native, see cmd/dev-health-worker/workgraph.go's buildNativeInvestmentExecutor.)

``_dispatch_post_sync_tasks`` now enqueues a Celery **chain**:
``dispatch_investment_materialize_partitioned`` -> ``run_membership_backfill``
(the no-LLM membership PROJECTION). The chain (not independent ``send_task``
calls)
guarantees each step only starts after its predecessor *succeeds*. CHAOS-2433
round-3 finding #2 added the projection step: the materializer writes
``work_unit_investments`` ONLY, and the full-coverage projection is the SOLE
writer of ``work_unit_membership`` + the completion marker — so a
date-windowed materialize can never publish partial coverage. The chain fires
after *either* a git or a work-item sync (org-wide persisted data accumulates
across separate configs), not only when one config carries both.

``run_work_graph_build`` (a Python Celery task) was REMOVED from this chain
under CHAOS-4924: its compute was already a 0-stats no-op (every stage
ported natively), and the Go worker already creates ``workgraph.build``
requests after a sync independent of any Celery entrypoint
(``cmd/dev-health-worker/sync_dispatch.go:273-310``'s
``workGraphPostSyncWriter.StartRequestTx``).

These tests prove the seam without a live ClickHouse: they patch the Celery
``chain`` / ``signature`` factories and assert the dispatch contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import that otherwise ERRORs isolated collection (mirrors CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.workers.post_sync_dispatch import _dispatch_post_sync_tasks

_INVESTMENT_TASK = (
    "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned"
)
_PROJECTION_TASK = "dev_health_ops.workers.tasks.run_membership_backfill"
_COMPLEXITY_TASK = "dev_health_ops.workers.tasks.run_complexity_job"


def _run_dispatch(provider: str, sync_targets: list[str], org_id: str):
    """Drive _dispatch_post_sync_tasks with chain/signature patched.

    Returns (signature_mock, chain_mock, chain_instance_mock, send_task_mock).
    """
    with (
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.signature"
        ) as mock_signature,
        patch("dev_health_ops.workers.post_sync_dispatch.chain") as mock_chain,
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.send_task"
        ) as mock_send_task,
    ):
        # Each signature() call returns a distinct marker carrying its args so
        # we can assert which task each chain position holds.
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


def test_investment_chain_dispatched_with_git_and_work_items() -> None:
    """git + work-items => materialize -> project chain, applied async."""
    mock_signature, mock_chain, chain_instance, _ = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs", "work-items"],
        org_id="org-123",
    )

    assert mock_chain.call_count == 1
    complexity_sig, materialize_sig = mock_chain.call_args.args
    assert complexity_sig.task_name == _COMPLEXITY_TASK
    assert materialize_sig.task_name == _INVESTMENT_TASK

    # All signatures are org-scoped onto the metrics queue.
    assert complexity_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    assert complexity_sig.sig_kwargs["queue"] == "metrics"
    assert complexity_sig.sig_kwargs.get("immutable") is True
    assert materialize_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    assert materialize_sig.sig_kwargs["queue"] == "default"

    # Downstream steps are linked IMMUTABLE so a parent's return dict is not
    # injected as a positional arg (which would break the next task).
    assert materialize_sig.sig_kwargs.get("immutable") is True

    # The chain is actually dispatched (not just constructed).
    chain_instance.apply_async.assert_called_once_with()


def test_investment_chain_dispatched_with_git_only() -> None:
    """git only (no work-items in this config) => chain still fires.

    Work items for the org may have been persisted by a separate sync config;
    the org-wide materialize/project must not be gated on one config
    carrying both kinds of data.
    """
    _, mock_chain, chain_instance, _ = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs"],
        org_id="org-123",
    )

    assert mock_chain.call_count == 1
    complexity_sig, materialize_sig = mock_chain.call_args.args
    assert complexity_sig.task_name == _COMPLEXITY_TASK
    assert materialize_sig.task_name == _INVESTMENT_TASK
    chain_instance.apply_async.assert_called_once_with()


def test_investment_chain_dispatched_with_work_items_only_jira() -> None:
    """Jira work-items-only sync => chain fires (the major missed live path).

    Jira/Linear configs only ever carry work-items; gating on git+work-items
    meant these orgs never enqueued the materialize at all (CHAOS-2374).
    """
    _, mock_chain, chain_instance, _ = _run_dispatch(
        provider="jira",
        sync_targets=["work-items"],
        org_id="org-123",
    )

    assert mock_chain.call_count == 1
    (materialize_sig,) = mock_chain.call_args.args
    assert materialize_sig.task_name == _INVESTMENT_TASK
    assert materialize_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    chain_instance.apply_async.assert_called_once_with()


def test_post_sync_dispatch_forwards_backfill_window() -> None:
    """The materialize window forwards from_date/to_date -- work_graph_from_date/
    work_graph_to_date no longer feed anything since CHAOS-4924 removed
    build_sig from the chain, but remain accepted params (other callers may
    still pass them) so this test still exercises them for that reason."""
    from dev_health_ops.workers.post_sync_dispatch import _dispatch_post_sync_tasks

    with (
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.signature"
        ) as window_signature,
        patch("dev_health_ops.workers.post_sync_dispatch.chain") as window_chain,
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.send_task"
        ) as window_send_task,
    ):

        def _make_sig(name, **kwargs):
            sig = MagicMock(name=f"sig:{name}")
            sig.task_name = name
            sig.sig_kwargs = kwargs
            return sig

        window_signature.side_effect = _make_sig
        _dispatch_post_sync_tasks(
            provider="linear",
            sync_targets=["work-items"],
            org_id="org-123",
            metrics_day="2026-01-14",
            metrics_backfill_days=14,
            from_date="2026-01-01",
            to_date="2026-01-14",
            work_graph_from_date="2026-01-01T00:00:00+00:00",
            work_graph_to_date="2026-01-15T00:00:00+00:00",
        )

    window_send_task.assert_not_called()
    (materialize_sig,) = window_chain.call_args.args
    assert materialize_sig.sig_kwargs["kwargs"] == {
        "org_id": "org-123",
        "from_date": "2026-01-01",
        "to_date": "2026-01-14",
    }


def test_post_sync_dispatch_does_not_serialize_llm_api_key(monkeypatch) -> None:
    monkeypatch.setenv("LLM_API_KEY", "sk-worker-secret")

    mock_signature, _, _, _ = _run_dispatch(
        provider="github",
        sync_targets=["git"],
        org_id="org-123",
    )

    materialize_calls = [
        call
        for call in mock_signature.call_args_list
        if call.args[0] == _INVESTMENT_TASK
    ]
    assert len(materialize_calls) == 1
    kwargs = materialize_calls[0].kwargs["kwargs"]
    assert "llm_api_key" not in kwargs
    assert "sk-worker-secret" not in repr(kwargs)


def test_no_investment_chain_for_feature_flags_only() -> None:
    """A sync with neither git nor work-items (e.g. feature-flags) => no chain."""
    _, mock_chain, _, mock_send_task = _run_dispatch(
        provider="launchdarkly",
        sync_targets=["feature-flags"],
        org_id="org-123",
    )

    mock_chain.assert_not_called()
    # And no investment send_task either.
    sent = [call.args[0] for call in mock_send_task.call_args_list]
    assert _INVESTMENT_TASK not in sent
