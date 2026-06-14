"""Unit tests for the scheduled recommendations worker task (CHAOS-2373).

These prove the *seam* that wires the live path for ``recommendations_daily``:
the task is registered + on the metrics queue + scheduled in beat, and it runs
the ``RuleEngine`` per active org/team then writes via
``sink.write_recommendations``. No live ClickHouse / Postgres is touched —
every collaborator (loader, engine, sink, org/team discovery) is mocked.
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


def test_task_is_registered_on_metrics_queue():
    """The task must be importable and registered with Celery (live path exists)."""
    from dev_health_ops.workers import tasks
    from dev_health_ops.workers.celery_app import celery_app
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    assert "run_recommendations_job" in tasks.__all__
    name = "dev_health_ops.workers.tasks.run_recommendations_job"
    assert name in celery_app.tasks
    assert run_recommendations_job.name == name
    assert run_recommendations_job.queue == "metrics"


def test_beat_schedule_entry_exists_on_metrics_queue():
    """A daily beat entry must exist so real orgs get recommendations."""
    from dev_health_ops.workers.config import beat_schedule

    entry = beat_schedule["run-recommendations"]
    assert entry["task"] == "dev_health_ops.workers.tasks.run_recommendations_job"
    assert entry["options"]["queue"] == "metrics"


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_task_writes_full_state_via_sink(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_get_db_url,
    mock_ready,
):
    """Full per-team state (fired + tombstones) must be persisted; only fired counted."""
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_ready.return_value = True
    mock_get_db_url.return_value = "clickhouse://fake"
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

    result = run_recommendations_job.run(org_id="org-1", window=14)

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

    assert result["status"] == "success"
    # Return value counts FIRED only (tombstones are not "fired" recommendations).
    assert result["fired"] == 2
    assert result["per_org"] == {"org-1": 2}


@patch("dev_health_ops.workers.recommendations_tasks._compute_recommendations_for_org")
@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
def test_as_of_anchors_window_to_include_finalized_partition(
    mock_get_db_url,
    mock_compute,
):
    """as_of must make window_end == as_of + 1 so the finalized day is *read*.

    The ClickHouse loader filters ``day < window_end`` (exclusive end) and the
    engine derives ``window_end == now.date()``. Anchoring ``now`` to the day
    AFTER the finalized partition is the only way the just-finalized ``as_of``
    partition is actually included in the evaluation window (CHAOS-2373 round-2).
    The readiness gate, however, must still key on the finalized ``as_of`` day.
    """
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_get_db_url.return_value = "clickhouse://fake"
    mock_compute.return_value = 0

    run_recommendations_job.run(org_id="org-1", as_of="2025-01-15")

    mock_compute.assert_called_once()
    passed_now = mock_compute.call_args.kwargs["now"]
    passed_as_of = mock_compute.call_args.kwargs["as_of_day"]

    # now is the day AFTER the finalized partition, so the engine's exclusive
    # window_end (= now.date()) is 2025-01-16 and the window [start, 2025-01-16)
    # INCLUDES the finalized 2025-01-15 partition.
    assert passed_now == datetime(2025, 1, 16, tzinfo=timezone.utc)
    assert passed_now.date() == date(2025, 1, 16)
    # The readiness gate still keys on the finalized partition itself.
    assert passed_as_of == date(2025, 1, 15)


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_finalized_partition_is_inside_loader_window(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_get_db_url,
    mock_ready,
):
    """End-to-end seam: the engine's window_end must exceed the finalized day.

    Drives the real ``_compute_recommendations_for_org`` (via the task) with a
    fake engine and asserts ``window_end`` (= now.date()) is strictly greater
    than the finalized ``as_of`` partition. Because the loader treats
    ``window_end`` as exclusive, ``as_of`` falls *inside* ``[start, window_end)``.
    """
    from dev_health_ops.workers import recommendations_tasks
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_ready.return_value = True
    mock_get_db_url.return_value = "clickhouse://fake"
    mock_discover_teams.return_value = ["team-1"]
    mock_sink_cls.return_value = MagicMock()

    captured: dict = {}

    def _capture_state(*, team_id, window, org_id):
        # Recreate the engine's window math to assert the contract.
        from datetime import timedelta

        now = mock_engine_cls.call_args.kwargs["now"]
        window_end = now.date()
        window_start = window_end - timedelta(days=window)
        captured["window_start"] = window_start
        captured["window_end"] = window_end
        return []

    mock_engine = MagicMock()
    mock_engine.evaluate_state.side_effect = _capture_state
    mock_engine_cls.return_value = mock_engine

    run_recommendations_job.run(org_id="org-1", window=14, as_of="2025-01-15")

    as_of = date(2025, 1, 15)
    # The finalized partition must be < the exclusive window_end (i.e. inside).
    assert captured["window_end"] > as_of
    assert captured["window_start"] <= as_of < captured["window_end"]
    # And the gate was consulted with the finalized partition, not now.date().
    assert mock_ready.call_args.args == ("org-1", as_of)
    # Silence unused-import lint for the module handle used for patching paths.
    assert recommendations_tasks.run_recommendations_job is run_recommendations_job


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
def test_task_skips_when_daily_metrics_not_ready(
    mock_sink_cls,
    mock_discover_teams,
    mock_get_db_url,
    mock_ready,
):
    """When daily metrics for the org/day are mid-flight, the job must not evaluate."""
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_ready.return_value = False
    mock_get_db_url.return_value = "clickhouse://fake"
    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    result = run_recommendations_job.run(org_id="org-pending", window=14)

    # No teams discovered, no sink opened, nothing written: a clean skip.
    mock_discover_teams.assert_not_called()
    mock_sink_cls.assert_not_called()
    mock_sink.write_recommendations.assert_not_called()
    assert result["fired"] == 0
    assert result["per_org"] == {"org-pending": 0}


@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_active_org_ids")
@patch("dev_health_ops.workers.recommendations_tasks._compute_recommendations_for_org")
def test_task_enumerates_active_orgs_when_org_not_pinned(
    mock_compute,
    mock_discover_orgs,
    mock_get_db_url,
):
    """With no org_id, the task fans out over every active org from Postgres."""
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_get_db_url.return_value = "clickhouse://fake"
    mock_discover_orgs.return_value = ["org-a", "org-b"]
    mock_compute.side_effect = [3, 5]

    result = run_recommendations_job.run()

    mock_discover_orgs.assert_called_once()
    assert mock_compute.call_count == 2
    assert result["orgs"] == 2
    assert result["fired"] == 8
    assert result["per_org"] == {"org-a": 3, "org-b": 5}


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_no_teams_skips_write(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_get_db_url,
    mock_ready,
):
    """When an org has no active teams, the sink write is skipped (no empty rows)."""
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_ready.return_value = True
    mock_get_db_url.return_value = "clickhouse://fake"
    mock_discover_teams.return_value = []
    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    result = run_recommendations_job.run(org_id="org-empty")

    mock_engine_cls.assert_not_called()
    mock_sink.write_recommendations.assert_not_called()
    mock_sink.close.assert_called_once()
    assert result["fired"] == 0


# ---------------------------------------------------------------------------
# Per-team failure must surface (fail loudly) — not report success silently
# ---------------------------------------------------------------------------


@patch("dev_health_ops.workers.recommendations_tasks._daily_metrics_ready")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_per_team_failure_raises_so_job_retries(
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


@patch("dev_health_ops.workers.recommendations_tasks._compute_recommendations_for_org")
@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_active_org_ids")
def test_task_retries_when_an_org_compute_fails(
    mock_discover_orgs,
    mock_get_db_url,
    mock_compute,
):
    """A failed org compute must trigger Celery retry, never a 'success' return."""
    import pytest
    from celery.exceptions import Retry

    from dev_health_ops.workers.recommendations_tasks import (
        RecommendationsTeamFailure,
        run_recommendations_job,
    )

    mock_get_db_url.return_value = "clickhouse://fake"
    mock_discover_orgs.return_value = ["org-a"]
    mock_compute.side_effect = RecommendationsTeamFailure(
        org_id="org-a", failed_teams=["team-bad"], total_teams=2
    )

    # eager mode: self.retry raises celery Retry rather than returning success.
    with pytest.raises(Retry):
        run_recommendations_job.apply(args=(), kwargs={}, throw=True).get()


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
