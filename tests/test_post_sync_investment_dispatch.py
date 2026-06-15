"""Unit tests for the post-sync investment-materialize dispatch (CHAOS-2374).

``work_unit_investments`` / ``work_unit_investment_quotes`` are written only by
``materialize_investments`` via the ``run_investment_materialize`` Celery task.
That task was never dispatched on the live sync path, so real orgs saw an empty
``/investment`` view.

``_dispatch_post_sync_tasks`` now enqueues a Celery **chain**:
``run_work_graph_build`` -> ``run_investment_materialize`` ->
``run_membership_backfill`` (the no-LLM membership PROJECTION). The chain (not
independent ``send_task`` calls) guarantees each step only starts after its
predecessor *succeeds*. CHAOS-2433 round-3 finding #2 added the third step: the
materializer writes ``work_unit_investments`` ONLY, and the full-coverage
projection is the SOLE writer of ``work_unit_membership`` + the completion
marker — so a date-windowed materialize can never publish partial coverage. The
chain fires after *either* a git or a work-item sync (org-wide persisted data
accumulates across separate configs), not only when one config carries both.

These tests prove the seam without a live ClickHouse: they patch the Celery
``chain`` / ``signature`` factories and assert the dispatch contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import that otherwise ERRORs isolated collection (mirrors CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.workers.sync_runtime import _dispatch_post_sync_tasks

_INVESTMENT_TASK = "dev_health_ops.workers.tasks.run_investment_materialize"
_WORK_GRAPH_TASK = "dev_health_ops.workers.tasks.run_work_graph_build"
_PROJECTION_TASK = "dev_health_ops.workers.tasks.run_membership_backfill"


def _run_dispatch(provider: str, sync_targets: list[str], org_id: str):
    """Drive _dispatch_post_sync_tasks with chain/signature patched.

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
    """git + work-items => build -> materialize -> project chain, applied async."""
    mock_signature, mock_chain, chain_instance, _ = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs", "work-items"],
        org_id="org-123",
    )

    # The chain must be built build FIRST, materialize SECOND, project THIRD.
    assert mock_chain.call_count == 1
    build_sig, materialize_sig, project_sig = mock_chain.call_args.args
    assert build_sig.task_name == _WORK_GRAPH_TASK
    assert materialize_sig.task_name == _INVESTMENT_TASK
    assert project_sig.task_name == _PROJECTION_TASK, (
        "the membership PROJECTION (no-LLM, full coverage) must run last as the "
        "sole membership writer (CHAOS-2433 round-3 #2)"
    )

    # All signatures are org-scoped onto the metrics queue.
    assert build_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    assert build_sig.sig_kwargs["queue"] == "metrics"
    assert materialize_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    assert materialize_sig.sig_kwargs["queue"] == "metrics"
    assert project_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    assert project_sig.sig_kwargs["queue"] == "metrics"

    # Downstream steps are linked IMMUTABLE so a parent's return dict is not
    # injected as a positional arg (which would break the next task).
    assert materialize_sig.sig_kwargs.get("immutable") is True
    assert project_sig.sig_kwargs.get("immutable") is True
    assert build_sig.sig_kwargs.get("immutable") is not True

    # The chain is actually dispatched (not just constructed).
    chain_instance.apply_async.assert_called_once_with()


def test_investment_chain_dispatched_with_git_only() -> None:
    """git only (no work-items in this config) => chain still fires.

    Work items for the org may have been persisted by a separate sync config;
    the org-wide build/materialize/project must not be gated on one config
    carrying both kinds of data.
    """
    _, mock_chain, chain_instance, _ = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs"],
        org_id="org-123",
    )

    assert mock_chain.call_count == 1
    build_sig, materialize_sig, project_sig = mock_chain.call_args.args
    assert build_sig.task_name == _WORK_GRAPH_TASK
    assert materialize_sig.task_name == _INVESTMENT_TASK
    assert project_sig.task_name == _PROJECTION_TASK
    chain_instance.apply_async.assert_called_once_with()


def test_investment_chain_dispatched_with_work_items_only_jira() -> None:
    """Jira work-items-only sync => chain fires (the major missed live path).

    Jira/Linear configs only ever carry work-items; gating on git+work-items
    meant these orgs never enqueued the build/materialize at all (CHAOS-2374).
    """
    _, mock_chain, chain_instance, _ = _run_dispatch(
        provider="jira",
        sync_targets=["work-items"],
        org_id="org-123",
    )

    assert mock_chain.call_count == 1
    build_sig, materialize_sig, project_sig = mock_chain.call_args.args
    assert build_sig.task_name == _WORK_GRAPH_TASK
    assert materialize_sig.task_name == _INVESTMENT_TASK
    assert project_sig.task_name == _PROJECTION_TASK
    assert materialize_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    assert project_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    chain_instance.apply_async.assert_called_once_with()


def test_no_investment_chain_for_feature_flags_only() -> None:
    """A sync with neither git nor work-items (e.g. feature-flags) => no chain."""
    _, mock_chain, _, mock_send_task = _run_dispatch(
        provider="launchdarkly",
        sync_targets=["feature-flags"],
        org_id="org-123",
    )

    mock_chain.assert_not_called()
    # And no investment/work-graph send_task either.
    sent = [call.args[0] for call in mock_send_task.call_args_list]
    assert _INVESTMENT_TASK not in sent
    assert _WORK_GRAPH_TASK not in sent


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
