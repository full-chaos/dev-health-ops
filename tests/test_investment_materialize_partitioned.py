from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

from dev_health_ops.workers.work_graph_tasks import (
    dispatch_investment_materialize_partitioned,
    finalize_investment_materialize_partitioned,
    run_investment_materialize_chunk,
)


class _FakeSink:
    def ensure_schema(self) -> None:
        return None

    def close(self) -> None:
        return None


def _edge(component: int) -> dict[str, object]:
    return {
        "edge_id": f"edge-{component}",
        "source_type": "issue",
        "source_id": f"I-{component}",
        "target_type": "commit",
        "target_id": f"repo-{component}@abc{component}",
        "repo_id": f"repo-{component}",
    }


def _signature_factory(name: str, **kwargs: Any) -> MagicMock:
    sig = MagicMock(name=f"sig:{name}")
    sig.task_name = name
    sig.sig_kwargs = kwargs
    return sig


def test_dispatch_partitioned_materialize_chunks_components_with_shared_run() -> None:
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1), _edge(2), _edge(3)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ) as mock_signature,
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        chord_instance = MagicMock()
        mock_chord.return_value = chord_instance
        task = cast(Any, dispatch_investment_materialize_partitioned)
        result = task.run(db_url="clickhouse://x", org_id="org-1", chunk_size=2)

    assert result["status"] == "dispatched"
    assert result["components"] == 3
    assert result["chunks"] == 2
    header, callback = mock_chord.call_args.args
    assert len(header) == 2
    first_kwargs = header[0].sig_kwargs["kwargs"]
    second_kwargs = header[1].sig_kwargs["kwargs"]
    assert first_kwargs["component_indexes"] == [0, 1]
    assert second_kwargs["component_indexes"] == [2]
    assert "allow_unscoped" not in first_kwargs
    assert "allow_unscoped" not in second_kwargs
    assert first_kwargs["run_id"] == second_kwargs["run_id"] == result["run_id"]
    assert first_kwargs["computed_at"] == second_kwargs["computed_at"]
    assert callback.task_name == (
        "dev_health_ops.workers.tasks.finalize_investment_materialize_partitioned"
    )
    assert callback.sig_kwargs["kwargs"]["run_membership_backfill_after"] is True
    chord_instance.apply_async.assert_called_once_with()
    assert mock_signature.call_count == 3


def test_dispatch_partitioned_materialize_includes_allow_unscoped_when_true() -> None:
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ),
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        mock_chord.return_value = MagicMock()
        task = cast(Any, dispatch_investment_materialize_partitioned)
        task.run(
            db_url="clickhouse://x",
            org_id="",
            chunk_size=2,
            allow_unscoped=True,
        )

    header = mock_chord.call_args.args[0]
    chunk_kwargs = header[0].sig_kwargs["kwargs"]
    assert chunk_kwargs["allow_unscoped"] is True


def test_dispatch_partitioned_materialize_uses_env_batch_defaults(monkeypatch) -> None:
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_MODE", "provider_batch")
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_MIN_ITEMS", "11")
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_POLL_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS", "123")
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ),
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        mock_chord.return_value = MagicMock()
        task = cast(Any, dispatch_investment_materialize_partitioned)
        task.run(db_url="clickhouse://x", org_id="org-1", chunk_size=2)

    header = mock_chord.call_args.args[0]
    chunk_kwargs = header[0].sig_kwargs["kwargs"]
    assert chunk_kwargs["llm_batch_mode"] == "provider_batch"
    assert chunk_kwargs["llm_batch_min_items"] == 11
    assert chunk_kwargs["llm_batch_poll_interval_seconds"] == 3.5
    assert chunk_kwargs["llm_batch_timeout_seconds"] == 123.0


def test_dispatch_partitioned_materialize_runs_marker_for_windowed_org_wide_run() -> (
    None
):
    """CHAOS-2776: a WINDOWED but UNSCOPED run (org-wide, from/to only) must still
    project membership in the finalizer. The projection is full-coverage by
    construction (independent of the materialize window), so this republishes the
    org-wide marker and re-arms the read-path stale-generation guard. This is the
    post-sync path — the dispatcher always forwards the sync window — so before
    the fix the projection never ran after a post-sync materialize."""
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ),
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        mock_chord.return_value = MagicMock()
        task = cast(Any, dispatch_investment_materialize_partitioned)
        result = task.run(
            db_url="clickhouse://x",
            org_id="org-1",
            from_date="2026-01-01",
            to_date="2026-01-02",
        )

    callback = mock_chord.call_args.args[1]
    assert callback.sig_kwargs["kwargs"]["run_membership_backfill_after"] is True
    assert result["membership_in_finalizer"] is True


def test_dispatch_partitioned_materialize_skips_marker_for_repo_scoped_run() -> None:
    """A repo-SCOPED run must NOT project the org-wide marker (it would only cover
    in-scope units and blank other repos for unscoped reads)."""
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ),
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        mock_chord.return_value = MagicMock()
        task = cast(Any, dispatch_investment_materialize_partitioned)
        result = task.run(
            db_url="clickhouse://x",
            org_id="org-1",
            repo_ids=["repo-1"],
            from_date="2026-01-01",
            to_date="2026-01-02",
        )

    callback = mock_chord.call_args.args[1]
    assert callback.sig_kwargs["kwargs"]["run_membership_backfill_after"] is False
    assert result["membership_in_finalizer"] is False


def test_dispatch_partitioned_materialize_skips_marker_for_team_scoped_run() -> None:
    """A team-SCOPED run must NOT project the org-wide marker."""
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        # team_ids resolve to concrete repo_ids inside the materialize module; patch
        # there because dispatch imports _resolve_repo_ids locally from that module.
        patch(
            "dev_health_ops.work_graph.investment.materialize._resolve_repo_ids",
            return_value=["repo-1"],
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ),
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        mock_chord.return_value = MagicMock()
        task = cast(Any, dispatch_investment_materialize_partitioned)
        result = task.run(
            db_url="clickhouse://x",
            org_id="org-1",
            team_ids=["team-1"],
        )

    callback = mock_chord.call_args.args[1]
    assert callback.sig_kwargs["kwargs"]["run_membership_backfill_after"] is False
    assert result["membership_in_finalizer"] is False


def test_materialize_chunk_checkpoint_skips_completed_chunk() -> None:
    session_cm = MagicMock()
    session_cm.__enter__.return_value = MagicMock()
    session_cm.__exit__.return_value = None
    with (
        patch("dev_health_ops.db.get_postgres_session_sync", return_value=session_cm),
        patch("dev_health_ops.metrics.checkpoints.is_completed", return_value=True),
        patch("dev_health_ops.metrics.checkpoints.mark_running") as mark_running,
        patch("dev_health_ops.workers.work_graph_tasks.run_async") as run_async_mock,
    ):
        task = cast(Any, run_investment_materialize_chunk)
        result = task.run(
            db_url="clickhouse://x",
            org_id="org-1",
            run_id="run-1",
            computed_at=datetime.now(timezone.utc).isoformat(),
            component_indexes=[0],
            chunk_index=0,
            llm_provider="mock",
        )

    assert result["status"] == "skipped"
    mark_running.assert_not_called()
    run_async_mock.assert_not_called()


def test_partitioned_finalizer_aggregates_and_runs_membership_once() -> None:
    with patch(
        "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
        return_value={"memberships": 4},
    ) as backfill:
        task = cast(Any, finalize_investment_materialize_partitioned)
        result = task.run(
            [
                {
                    "stats": {
                        "records": 2,
                        "quotes": 1,
                        "skipped_existing": 3,
                        "llm_calls": 4,
                        "llm_input_tokens": 100,
                        "llm_output_tokens": 20,
                        "llm_failures": 1,
                        "llm_failure_counts": {"rate_limit": 1},
                    }
                }
            ],
            db_url="clickhouse://x",
            org_id="org-1",
            run_id="run-1",
            run_membership_backfill_after=True,
        )

    assert result["records"] == 2
    assert result["quotes"] == 1
    assert result["skipped_existing"] == 3
    assert result["llm_calls"] == 4
    assert result["llm_failure_counts"] == {"rate_limit": 1}
    assert result["membership"] == {"memberships": 4}
    backfill.assert_called_once()


def test_dispatch_freezes_max_component_nodes_into_chunks(monkeypatch) -> None:
    """CHAOS-2775 codex round 2 (HIGH): the dispatcher resolves the component
    size cap ONCE and passes it to every chunk. component_indexes are
    positional over a re-built component list, so a chunk worker resolving a
    different INVESTMENT_MAX_COMPONENT_NODES from its own env would split
    differently and index N would name a different work unit."""
    monkeypatch.setenv("INVESTMENT_MAX_COMPONENT_NODES", "7")
    with (
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=_FakeSink(),
        ),
        patch(
            "dev_health_ops.work_graph.investment.queries.fetch_work_graph_edges",
            return_value=[_edge(1), _edge(2), _edge(3)],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.celery_app.signature",
            side_effect=_signature_factory,
        ),
        patch("dev_health_ops.workers.work_graph_tasks.chord") as mock_chord,
    ):
        mock_chord.return_value = MagicMock()
        task = cast(Any, dispatch_investment_materialize_partitioned)
        task.run(db_url="clickhouse://x", org_id="org-1", chunk_size=2)

    header, _callback = mock_chord.call_args.args
    for sig in header:
        assert sig.sig_kwargs["kwargs"]["max_component_nodes"] == 7


def test_finalize_aggregates_split_stats_by_max_not_sum() -> None:
    """CHAOS-2775 codex round 2 (LOW): every chunk rebuilds the FULL component
    list, so each reports the same graph-wide split counters — the finalizer
    must aggregate them by MAX (summing would multiply by the chunk count)."""
    chunk_stats = {
        "records": 1,
        "quotes": 0,
        "skipped_existing": 0,
        "llm_calls": 0,
        "llm_input_tokens": 0,
        "llm_output_tokens": 0,
        "llm_failures": 0,
        "llm_failure_counts": {},
        "oversized_components": 2,
        "dropped_edges": 9,
        "dropped_nodes": 3,
    }
    task = cast(Any, finalize_investment_materialize_partitioned)
    result = task.run(
        [{"stats": dict(chunk_stats)}, {"stats": dict(chunk_stats)}],
        db_url="clickhouse://x",
        org_id="org-1",
        run_id="run-1",
        run_membership_backfill_after=False,
    )

    # Summable counters still sum; split counters do not.
    assert result["records"] == 2
    assert result["oversized_components"] == 2
    assert result["dropped_edges"] == 9
    assert result["dropped_nodes"] == 3


def test_partitioned_finalizer_skips_membership_when_flag_false() -> None:
    """A scoped run's finalizer (run_membership_backfill_after=False) aggregates
    stats but must NOT project membership (no org-wide marker published)."""
    with patch(
        "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
    ) as backfill:
        task = cast(Any, finalize_investment_materialize_partitioned)
        result = task.run(
            [{"stats": {"records": 2}}],
            db_url="clickhouse://x",
            org_id="org-1",
            run_id="run-1",
            run_membership_backfill_after=False,
        )

    assert result["records"] == 2
    assert "membership" not in result
    backfill.assert_not_called()
