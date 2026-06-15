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


# The default db_url _get_db_url() resolves to on the scheduled (no-override) path.
_SCHEDULED_DB_URL = "clickhouse://x"


def _run_dispatch(*, active_org_ids: list[str], db_url: str | None = None):
    """Drive dispatch_investment_materialize with chain/signature/sources patched.

    Returns (signature_mock, chain_mock, chain_instances, result).
    ``chain_instances`` is the list of per-call chain instances (one per org
    that was dispatched), so tests can assert apply_async per org.

    ``db_url`` is the explicit override passed to ``.run()`` (manual/backfill).
    When ``None`` the scheduled path resolves ``_get_db_url()`` →
    ``_SCHEDULED_DB_URL``. Either way the resolved value must be forwarded to
    BOTH child signatures.

    The dispatcher fans out to ALL active orgs (no output-table gate), so the
    discovery patch is the only org source. ``strict=True`` is exercised
    separately in the enumeration-failure test.
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
            "dev_health_ops.workers.work_graph_tasks._get_db_url",
            return_value=_SCHEDULED_DB_URL,
        ),
    ):
        mock_signature.side_effect = _make_sig
        mock_chain.side_effect = _make_chain
        # bind=True task: invoke .run() so `self` is supplied by Celery. Pass the
        # explicit override only when given so the no-arg scheduled path is
        # exercised exactly as Celery beat would call it.
        if db_url is None:
            result = dispatch_investment_materialize.run()
        else:
            result = dispatch_investment_materialize.run(db_url=db_url)

    return mock_signature, mock_chain, chain_instances, result


def test_dispatch_queues_build_then_materialize_chain_per_org() -> None:
    """Each active org gets a build -> materialize chain in chained order."""
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


def test_dispatch_does_not_gate_on_work_graph_edges_output_table() -> None:
    """CHAOS-2439 [HIGH] regression: an active org with raw synced inputs but
    ZERO work_graph_edges rows (failed prior build / truncated graph / brand-new
    synced-but-never-built tenant) MUST still be dispatched — the rebuild is
    exactly what repairs it. The dispatcher therefore must NOT gate on the
    build's own OUTPUT table.

    We prove the gate is gone two ways: (1) the module exposes no
    work_graph_edges probe helper, and (2) every active org is dispatched even
    though no ClickHouse edge data exists in this unit context.
    """
    import dev_health_ops.workers.work_graph_tasks as wgt

    # The output-table gate helper no longer exists.
    assert not hasattr(wgt, "_orgs_with_work_graph_data")

    _, mock_chain, _, result = _run_dispatch(
        active_org_ids=["org-synced-no-edges-yet"],
    )

    # Dispatched despite having no work_graph_edges rows.
    assert mock_chain.call_count == 1
    build_sig, _ = mock_chain.call_args.args
    assert build_sig.sig_kwargs["kwargs"]["org_id"] == "org-synced-no-edges-yet"
    assert result["dispatched"] == ["org-synced-no-edges-yet"]
    # No "skipped" accounting — there is no skip path anymore.
    assert "skipped" not in result


def test_dispatch_single_tenant_default_org() -> None:
    """A positively-detected single-tenant install (['default']) is dispatched
    like any other org — a cheap no-op build/materialize if it has no data."""
    _, mock_chain, _, result = _run_dispatch(active_org_ids=["default"])

    assert mock_chain.call_count == 1
    build_sig, _ = mock_chain.call_args.args
    assert build_sig.sig_kwargs["kwargs"]["org_id"] == "default"
    assert result["dispatched"] == ["default"]


def test_dispatch_chain_shape_matches_post_sync_dispatch() -> None:
    """The scheduled chain is the SAME shape as the post-sync chain — idempotent
    and safe to coexist: same task names, same metrics queue, same immutable
    link, org-scoped kwargs, and the resolved db_url forwarded to both."""
    _, mock_chain, _, _ = _run_dispatch(active_org_ids=["org-1"])

    build_sig, materialize_sig = mock_chain.call_args.args
    # The scheduled path forwards the _get_db_url()-resolved value to both children.
    assert build_sig.task_name == _WORK_GRAPH_TASK
    assert build_sig.sig_kwargs == {
        "kwargs": {"db_url": _SCHEDULED_DB_URL, "org_id": "org-1"},
        "queue": "metrics",
    }
    assert materialize_sig.task_name == _INVESTMENT_TASK
    assert materialize_sig.sig_kwargs == {
        "kwargs": {"db_url": _SCHEDULED_DB_URL, "org_id": "org-1"},
        "queue": "metrics",
        "immutable": True,
    }


def test_dispatch_forwards_explicit_db_url_override_to_both_children() -> None:
    """CHAOS-2439 [HIGH] regression: an explicit db_url override (manual/backfill)
    must reach BOTH child signatures, so build+materialize target the requested
    ClickHouse instead of the workers' ambient _get_db_url() default."""
    override = "clickhouse://override"
    _, mock_chain, _, result = _run_dispatch(
        active_org_ids=["org-1", "org-2"], db_url=override
    )

    assert mock_chain.call_count == 2
    for call in mock_chain.call_args_list:
        build_sig, materialize_sig = call.args
        org = build_sig.sig_kwargs["kwargs"]["org_id"]
        # BOTH children carry the override db_url AND the org_id.
        assert build_sig.sig_kwargs["kwargs"] == {"db_url": override, "org_id": org}
        assert materialize_sig.sig_kwargs["kwargs"] == {
            "db_url": override,
            "org_id": org,
        }
        # The override never collapses to the ambient default.
        assert build_sig.sig_kwargs["kwargs"]["db_url"] != _SCHEDULED_DB_URL
        # Immutable link preserved on materialize.
        assert materialize_sig.sig_kwargs.get("immutable") is True
    assert result["dispatched"] == ["org-1", "org-2"]


def test_dispatch_scheduled_path_forwards_resolved_default_db_url() -> None:
    """Scheduled (no-override) path: the _get_db_url()-resolved default is
    forwarded to both children — behaviour unchanged from today's default."""
    _, mock_chain, _, _ = _run_dispatch(active_org_ids=["org-1"])

    build_sig, materialize_sig = mock_chain.call_args.args
    assert build_sig.sig_kwargs["kwargs"]["db_url"] == _SCHEDULED_DB_URL
    assert materialize_sig.sig_kwargs["kwargs"]["db_url"] == _SCHEDULED_DB_URL


def test_dispatch_uses_strict_discovery() -> None:
    """The dispatcher calls discovery with strict=True so a DB outage raises
    rather than collapsing to ['default']."""
    with (
        patch(
            "dev_health_ops.workers.recommendations_tasks._discover_active_org_ids",
            return_value=["org-1"],
        ) as mock_discover,
        patch("dev_health_ops.workers.work_graph_tasks.chain"),
        patch("dev_health_ops.workers.work_graph_tasks.celery_app.signature"),
        patch(
            "dev_health_ops.workers.work_graph_tasks._get_db_url",
            return_value="clickhouse://x",
        ),
    ):
        dispatch_investment_materialize.run()

    mock_discover.assert_called_once_with(strict=True)


def test_dispatch_retries_on_org_enumeration_failure() -> None:
    """A transient org-enumeration failure (strict discovery RAISES) triggers
    retry rather than reporting a silent empty success (mirrors
    dispatch_release_impact)."""

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
# Strict org discovery (CHAOS-2439 finding 2)
# ---------------------------------------------------------------------------


def test_discover_active_org_ids_strict_raises_on_db_error() -> None:
    """strict=True: a Postgres enumeration error RAISES (so the once-daily
    dispatcher retries) instead of silently returning ['default']."""
    from dev_health_ops.workers.recommendations_tasks import _discover_active_org_ids

    class _BoomSession:
        def __enter__(self):
            raise RuntimeError("postgres down")

        def __exit__(self, *a):
            return False

    import pytest

    with patch(
        "dev_health_ops.db.get_postgres_session_sync",
        return_value=_BoomSession(),
    ):
        with pytest.raises(RuntimeError, match="postgres down"):
            _discover_active_org_ids(strict=True)


def test_discover_active_org_ids_non_strict_falls_back_on_db_error() -> None:
    """strict=False (default): a DB error still falls back to ['default'] so the
    best-effort recommendations job is unaffected."""
    from dev_health_ops.workers.recommendations_tasks import _discover_active_org_ids

    class _BoomSession:
        def __enter__(self):
            raise RuntimeError("postgres down")

        def __exit__(self, *a):
            return False

    with patch(
        "dev_health_ops.db.get_postgres_session_sync",
        return_value=_BoomSession(),
    ):
        assert _discover_active_org_ids() == ["default"]


def test_discover_active_org_ids_strict_empty_table_returns_default() -> None:
    """strict=True: a SUCCESSFUL query that finds zero active orgs still returns
    ['default'] — the positively-detected single-tenant case is NOT an error."""
    from dev_health_ops.workers.recommendations_tasks import _discover_active_org_ids

    class _Query:
        def filter(self, *_a: Any):
            return self

        def all(self):
            return []

    class _Session:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def query(self, *_a: Any):
            return _Query()

    with patch(
        "dev_health_ops.db.get_postgres_session_sync",
        return_value=_Session(),
    ):
        assert _discover_active_org_ids(strict=True) == ["default"]
