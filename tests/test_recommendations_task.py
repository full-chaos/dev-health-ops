"""Unit tests for the live recommendations compute path (CHAOS-2373).

These prove the seam that survives CHAOS-4026's Celery-machinery cleanup:
``_compute_recommendations_for_org`` runs the ``RuleEngine`` per active
team then writes via ``sink.write_recommendations``. The Celery task
(``run_recommendations_job``) and its beat entry (``run-recommendations``)
and the multi-org fan-out helper (``_discover_active_org_ids``) were
deleted -- Go's ``recommendations_daily`` fixed schedule now owns the
periodic cadence, and Celery Beat has not scheduled this since the
2026-08-19 stop (see tests/workers/test_celery_dead_code_contract.py).
``_compute_recommendations_for_org`` itself stays live: it is invoked
synchronously by the dormant-Go operational bridge
(api/internal/worker_metrics.py::_run_recommendations).

No live ClickHouse / Postgres is touched — every collaborator (loader,
engine, sink, team discovery) is mocked.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from dev_health_ops.recommendations.snapshot import RecommendationRecord


def _fired_record(team_id: str = "team-1") -> RecommendationRecord:
    """A fired RecommendationRecord (engine.evaluate_state output for a hit)."""
    return RecommendationRecord(
        team_id=team_id,
        org_id="org-1",
        rule_id="saturation",
        rule_version="1.0.0",
        window_start=date(2025, 1, 1),
        window_end=date(2025, 1, 15),
        fired=True,
        severity="warning",
        title="WIP saturation rising",
        rationale="WIP per engineer trending up.",
        success_criterion="WIP per engineer < 3",
        evidence_json="[]",
        computed_at=datetime(2025, 1, 15, tzinfo=timezone.utc),
    )


def _tombstone_record(
    rule_id: str = "thrash", team_id: str = "team-1"
) -> RecommendationRecord:
    """A non-fired RecommendationRecord (tombstone for a recovered/quiet rule)."""
    return RecommendationRecord(
        team_id=team_id,
        org_id="org-1",
        rule_id=rule_id,
        rule_version="1.0.0",
        window_start=date(2025, 1, 1),
        window_end=date(2025, 1, 15),
        fired=False,
        severity="warning",
        title="",
        rationale="",
        success_criterion="resolve",
        evidence_json="[]",
        computed_at=datetime(2025, 1, 15, tzinfo=timezone.utc),
    )


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_compute_writes_full_state_via_sink(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_ready,
):
    """Full per-team state (fired + tombstones) must be persisted; only fired counted."""
    from dev_health_ops.workers.recommendations_tasks import (
        _compute_recommendations_for_org,
    )

    mock_ready.return_value = True
    mock_discover_teams.return_value = ["team-1", "team-2"]

    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    # Each team: one fired record + one tombstone (the full-state contract).
    mock_engine = MagicMock()
    mock_engine.evaluate_state.return_value = [
        _fired_record(),
        _tombstone_record(),
    ]
    mock_engine_cls.return_value = mock_engine

    fired = _compute_recommendations_for_org(
        org_id="org-1",
        db_url="clickhouse://fake",
        window=14,
        now=datetime(2025, 1, 16, tzinfo=timezone.utc),
        as_of_day=date(2025, 1, 15),
    )

    # Engine evaluated full state for each discovered team over the window.
    assert mock_engine.evaluate_state.call_count == 2
    assert mock_engine.evaluate_all.call_count == 0  # no fired-only path anymore
    for call in mock_engine.evaluate_state.call_args_list:
        assert call.kwargs["window"] == 14
        assert call.kwargs["org_id"] == "org-1"

    # The full state (fired AND tombstones) was written through the sink.
    mock_sink.write_recommendations.assert_called_once()
    written = mock_sink.write_recommendations.call_args.args[0]
    # 2 teams x (1 fired + 1 tombstone) = 4 rows.
    assert len(written) == 4
    assert sum(1 for r in written if r.fired) == 2
    assert sum(1 for r in written if not r.fired) == 2
    fired_titles = {r.title for r in written if r.fired}
    assert fired_titles == {"WIP saturation rising"}
    mock_sink.close.assert_called_once()

    # Return value counts FIRED only (tombstones are not "fired" recommendations).
    assert fired == 2


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
def test_compute_skips_when_daily_metrics_not_ready(
    mock_sink_cls,
    mock_discover_teams,
    mock_ready,
):
    """When daily metrics for the org/day are mid-flight, compute must not evaluate."""
    from dev_health_ops.workers.recommendations_tasks import (
        _compute_recommendations_for_org,
    )

    mock_ready.return_value = False
    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    fired = _compute_recommendations_for_org(
        org_id="org-pending",
        db_url="clickhouse://fake",
        window=14,
        now=datetime(2025, 1, 16, tzinfo=timezone.utc),
        as_of_day=date(2025, 1, 15),
    )

    # No teams discovered, no sink opened, nothing written: a clean skip.
    mock_discover_teams.assert_not_called()
    mock_sink_cls.assert_not_called()
    mock_sink.write_recommendations.assert_not_called()
    assert fired == 0


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_compute_skips_write_when_no_teams(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_ready,
):
    """When an org has no active teams, the sink write is skipped (no empty rows)."""
    from dev_health_ops.workers.recommendations_tasks import (
        _compute_recommendations_for_org,
    )

    mock_ready.return_value = True
    mock_discover_teams.return_value = []
    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    fired = _compute_recommendations_for_org(
        org_id="org-empty",
        db_url="clickhouse://fake",
        window=14,
        now=datetime(2025, 1, 16, tzinfo=timezone.utc),
        as_of_day=date(2025, 1, 15),
    )

    mock_engine_cls.assert_not_called()
    mock_sink.write_recommendations.assert_not_called()
    mock_sink.close.assert_called_once()
    assert fired == 0


# ---------------------------------------------------------------------------
# Per-team failure must surface (fail loudly) — not report success silently
# ---------------------------------------------------------------------------


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_per_team_failure_raises_so_caller_can_retry(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_ready,
):
    """A team that raises must mark the *org* compute as failed, not succeed.

    Previously the per-team exception was swallowed and the task still reported
    success — leaving the failed team's stale fired rows visible with no
    tombstone written, invisible to monitoring/retries (CHAOS-2373 round-2).
    """
    import pytest

    from dev_health_ops.workers.recommendations_tasks import (
        RecommendationsTeamFailure,
        _compute_recommendations_for_org,
    )

    mock_ready.return_value = True
    mock_discover_teams.return_value = ["team-ok", "team-bad"]
    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    mock_engine = MagicMock()
    # team-ok evaluates cleanly; team-bad raises (transient loader/rule error).
    mock_engine.evaluate_state.side_effect = [
        [_fired_record("team-ok"), _tombstone_record(team_id="team-ok")],
        RuntimeError("clickhouse timeout"),
    ]
    mock_engine_cls.return_value = mock_engine

    with pytest.raises(RecommendationsTeamFailure) as excinfo:
        _compute_recommendations_for_org(
            org_id="org-1",
            db_url="clickhouse://fake",
            window=14,
            now=datetime(2025, 1, 16, tzinfo=timezone.utc),
            as_of_day=date(2025, 1, 15),
        )

    # The failure names the bad team and the org so operators can alert on it.
    assert excinfo.value.failed_teams == ["team-bad"]
    assert excinfo.value.org_id == "org-1"
    assert excinfo.value.total_teams == 2
    # State for the team that DID evaluate is still persisted before raising.
    mock_sink.write_recommendations.assert_called_once()
    written = mock_sink.write_recommendations.call_args.args[0]
    assert {r.team_id for r in written} == {"team-ok"}
    # Sink is always closed, even on the failure path.
    mock_sink.close.assert_called_once()


# ---------------------------------------------------------------------------
# Readiness gate: _daily_metrics_ready semantics
# ---------------------------------------------------------------------------


def test_daily_metrics_ready_default_org_is_always_ready():
    """The community/single-tenant 'default' sentinel never gates."""
    from dev_health_ops.workers.recommendations_tasks import _daily_metrics_ready

    assert _daily_metrics_ready("default", date(2026, 4, 8)) is True


def test_daily_metrics_ready_proceeds_when_no_checkpoint():
    """Absent finalize checkpoint -> proceed (chord path not driving this org)."""
    from dev_health_ops.workers import recommendations_tasks

    with (
        patch("dev_health_ops.metrics.checkpoints.get_checkpoint", return_value=None),
        patch("dev_health_ops.db.get_postgres_session_sync") as mock_session,
    ):
        mock_session.return_value.__enter__.return_value = MagicMock()
        ready = recommendations_tasks._daily_metrics_ready("org-x", date(2026, 4, 8))

    assert ready is True


def test_daily_metrics_ready_blocks_when_finalize_running():
    """A RUNNING finalize checkpoint -> metrics mid-flight -> skip."""
    from dev_health_ops.metrics.checkpoints import CheckpointStatus
    from dev_health_ops.workers import recommendations_tasks

    checkpoint = MagicMock()
    checkpoint.status = CheckpointStatus.RUNNING

    with (
        patch(
            "dev_health_ops.metrics.checkpoints.get_checkpoint", return_value=checkpoint
        ),
        patch("dev_health_ops.db.get_postgres_session_sync") as mock_session,
    ):
        mock_session.return_value.__enter__.return_value = MagicMock()
        ready = recommendations_tasks._daily_metrics_ready("org-x", date(2026, 4, 8))

    assert ready is False


def test_daily_metrics_ready_proceeds_when_finalize_completed():
    """A COMPLETED finalize checkpoint -> metrics fresh -> proceed."""
    from dev_health_ops.metrics.checkpoints import CheckpointStatus
    from dev_health_ops.workers import recommendations_tasks

    checkpoint = MagicMock()
    checkpoint.status = CheckpointStatus.COMPLETED

    with (
        patch(
            "dev_health_ops.metrics.checkpoints.get_checkpoint", return_value=checkpoint
        ),
        patch("dev_health_ops.db.get_postgres_session_sync") as mock_session,
    ):
        mock_session.return_value.__enter__.return_value = MagicMock()
        ready = recommendations_tasks._daily_metrics_ready("org-x", date(2026, 4, 8))

    assert ready is True
