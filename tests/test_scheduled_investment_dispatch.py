"""Unit tests for the daily scheduled investment-materialize dispatch (CHAOS-2439).

Investment materialization (which populates ``work_unit_membership``, read by
the work-graph theme/subcategory filter) was EVENT-DRIVEN ONLY — it ran post-sync
via the ``run_work_graph_build`` -> ``run_investment_materialize`` chain. Idle-sync
orgs and the post-deploy window therefore left membership empty, stranding theme
filters in the ``MEMBERSHIP_NOT_MATERIALIZED`` degraded state (CHAOS-2427 #925).

``dispatch_investment_materialize`` is a daily floor-cadence safety net: it fans
out the SAME immutable ``build -> materialize`` chain per active org that has
work-graph data. These tests prove the seam without a live broker/ClickHouse:
they patch the Celery ``chain`` / ``signature`` factories and the org/data
sources, and assert the chain composition (chained order, immutability,
org-scoping) and idempotent coexistence with the post-sync dispatch. They follow
``tests/test_post_sync_investment_dispatch.py`` style.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import that otherwise ERRORs isolated collection (mirrors CHAOS-2370/2374).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.workers.config import beat_schedule
from dev_health_ops.workers.work_graph_tasks import dispatch_investment_materialize

_INVESTMENT_TASK = "dev_health_ops.workers.tasks.run_investment_materialize"
_WORK_GRAPH_TASK = "dev_health_ops.workers.tasks.run_work_graph_build"
_BEAT_NAME = "run-investment-materialize-daily"
_DISPATCH_TASK = "dev_health_ops.workers.tasks.dispatch_investment_materialize"


# ---------------------------------------------------------------------------
# Beat schedule registration
# ---------------------------------------------------------------------------


def test_beat_entry_registered_with_expected_name_and_schedule() -> None:
    """The daily floor-cadence beat entry is wired with the right task + time."""
    from celery.schedules import crontab

    assert _BEAT_NAME in beat_schedule
    entry = beat_schedule[_BEAT_NAME]
    assert entry["task"] == _DISPATCH_TASK
    # Daily at 01:15 UTC — clear of run-daily-metrics (01:00) and
    # run-release-impact (01:30).
    assert entry["schedule"] == crontab(hour=1, minute=15)
    assert entry["options"]["queue"] == "default"


def test_beat_entry_does_not_collide_with_neighbours() -> None:
    """01:15 sits strictly between the 01:00 and 01:30 daily jobs."""
    from celery.schedules import crontab

    assert beat_schedule["run-daily-metrics"]["schedule"] == crontab(hour=1, minute=0)
    assert beat_schedule["run-release-impact-daily"]["schedule"] == crontab(
        hour=1, minute=30
    )
    assert beat_schedule[_BEAT_NAME]["schedule"] == crontab(hour=1, minute=15)


# ---------------------------------------------------------------------------
# Dispatch fan-out + chain composition
# ---------------------------------------------------------------------------


def _run_dispatch(
    *,
    active_org_ids: list[str],
    orgs_with_data: set[str] | None = None,
):
    """Drive dispatch_investment_materialize with chain/signature/sources patched.

    Returns (signature_mock, chain_mock, chain_instances, result).
    ``chain_instances`` is the list of per-call chain instances (one per org
    that was dispatched), so tests can assert apply_async per org.
    """
    chain_instances: list[MagicMock] = []

    def _make_chain(*sigs):
        inst = MagicMock(name=f"chain_instance[{len(chain_instances)}]")
        inst.chained_sigs = sigs
        chain_instances.append(inst)
        return inst

    def _make_sig(name, **kwargs):
        sig = MagicMock(name=f"sig:{name}")
        sig.task_name = name
        sig.sig_kwargs = kwargs
        return sig

    with (
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature"
        ) as mock_signature,
        patch("dev_health_ops.workers.work_graph_tasks.chain") as mock_chain,
        patch(
            "dev_health_ops.workers.recommendations_tasks._discover_active_org_ids",
            return_value=active_org_ids,
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks._orgs_with_work_graph_data",
            return_value=(
                orgs_with_data if orgs_with_data is not None else set(active_org_ids)
            ),
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks._get_db_url",
            return_value="clickhouse://x",
        ),
    ):
        mock_signature.side_effect = _make_sig
        mock_chain.side_effect = _make_chain
        # bind=True task: invoke .run() so `self` is supplied by Celery.
        result = dispatch_investment_materialize.run()

    return mock_signature, mock_chain, chain_instances, result


def test_dispatch_queues_build_then_materialize_chain_per_org() -> None:
    """Each org with data gets a build -> materialize chain in chained order."""
    mock_signature, mock_chain, chain_instances, result = _run_dispatch(
        active_org_ids=["org-1", "org-2"],
    )

    # One chain per org, each applied async exactly once.
    assert mock_chain.call_count == 2
    assert len(chain_instances) == 2
    for inst in chain_instances:
        inst.apply_async.assert_called_once_with()

    # Assert CHAIN composition (not parallel): build FIRST, materialize SECOND.
    for call in mock_chain.call_args_list:
        build_sig, materialize_sig = call.args
        assert build_sig.task_name == _WORK_GRAPH_TASK
        assert materialize_sig.task_name == _INVESTMENT_TASK
        # Materialize is linked IMMUTABLE so the build's return value is not
        # injected as a positional arg (the CHAOS-2374 race guard).
        assert materialize_sig.sig_kwargs.get("immutable") is True
        assert build_sig.sig_kwargs.get("immutable") is not True
        # Both org-scoped onto the metrics queue.
        assert build_sig.sig_kwargs["queue"] == "metrics"
        assert materialize_sig.sig_kwargs["queue"] == "metrics"

    # Per-org scoping: each org's chain carries that org's id on both sigs.
    dispatched_orgs = []
    for call in mock_chain.call_args_list:
        build_sig, materialize_sig = call.args
        org = build_sig.sig_kwargs["kwargs"]["org_id"]
        assert materialize_sig.sig_kwargs["kwargs"]["org_id"] == org
        dispatched_orgs.append(org)
    assert dispatched_orgs == ["org-1", "org-2"]

    assert result["dispatched"] == ["org-1", "org-2"]
    assert result["skipped"] == 0


def test_dispatch_skips_orgs_without_work_graph_data() -> None:
    """Active orgs with no work_graph_edges are skipped (not churned)."""
    _, mock_chain, chain_instances, result = _run_dispatch(
        active_org_ids=["org-has-data", "org-empty"],
        orgs_with_data={"org-has-data"},
    )

    # Only the org with data gets a chain.
    assert mock_chain.call_count == 1
    build_sig, _ = mock_chain.call_args.args
    assert build_sig.sig_kwargs["kwargs"]["org_id"] == "org-has-data"
    assert result["dispatched"] == ["org-has-data"]
    assert result["skipped"] == 1


def test_dispatch_no_orgs_with_data_dispatches_nothing() -> None:
    """When no candidate org has data, nothing is queued (clean no-op)."""
    _, mock_chain, chain_instances, result = _run_dispatch(
        active_org_ids=["org-1", "org-2"],
        orgs_with_data=set(),
    )

    mock_chain.assert_not_called()
    assert chain_instances == []
    assert result["dispatched"] == []
    assert result["skipped"] == 2


def test_dispatch_chain_shape_matches_post_sync_dispatch() -> None:
    """The scheduled chain is the SAME shape as the post-sync chain — idempotent
    and safe to coexist: same task names, same metrics queue, same immutable
    link, same org-scoped kwargs."""
    _, mock_chain, _, _ = _run_dispatch(active_org_ids=["org-1"])

    build_sig, materialize_sig = mock_chain.call_args.args
    # Identical to _dispatch_post_sync_tasks' chain contract.
    assert build_sig.task_name == _WORK_GRAPH_TASK
    assert build_sig.sig_kwargs == {"kwargs": {"org_id": "org-1"}, "queue": "metrics"}
    assert materialize_sig.task_name == _INVESTMENT_TASK
    assert materialize_sig.sig_kwargs == {
        "kwargs": {"org_id": "org-1"},
        "queue": "metrics",
        "immutable": True,
    }


def test_dispatch_retries_on_org_enumeration_failure() -> None:
    """A transient org-enumeration failure retries rather than reporting a silent
    empty success (mirrors dispatch_release_impact)."""

    class _Retry(Exception):
        pass

    with (
        patch(
            "dev_health_ops.workers.recommendations_tasks._discover_active_org_ids",
            side_effect=RuntimeError("postgres down"),
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks._get_db_url",
            return_value="clickhouse://x",
        ),
        patch.object(
            dispatch_investment_materialize,
            "retry",
            side_effect=_Retry(),
        ) as mock_retry,
    ):
        import pytest

        with pytest.raises(_Retry):
            dispatch_investment_materialize.run()
    mock_retry.assert_called_once()


# ---------------------------------------------------------------------------
# Org-with-data probe (fail-open)
# ---------------------------------------------------------------------------


def test_orgs_with_work_graph_data_fails_open_on_clickhouse_error() -> None:
    """If the ClickHouse probe errors, return all candidate orgs (fail open) so
    the safety net still runs."""
    from dev_health_ops.workers.work_graph_tasks import _orgs_with_work_graph_data

    class _BoomSink:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            self.client = self

        def query(self, *_a: Any, **_k: Any):
            raise RuntimeError("clickhouse unreachable")

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        _BoomSink,
    ):
        out = _orgs_with_work_graph_data("clickhouse://x", ["org-1", "org-2"])
    assert out == {"org-1", "org-2"}


def test_orgs_with_work_graph_data_empty_input_returns_empty() -> None:
    from dev_health_ops.workers.work_graph_tasks import _orgs_with_work_graph_data

    assert _orgs_with_work_graph_data("clickhouse://x", []) == set()
