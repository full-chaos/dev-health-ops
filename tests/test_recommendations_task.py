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

from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation


def _make_recommendation(team_id: str = "team-1") -> Recommendation:
    return Recommendation(
        rule_id="saturation",
        team_id=team_id,
        org_id="org-1",
        computed_at=datetime(2025, 1, 15, tzinfo=timezone.utc),
        window_start=date(2025, 1, 1),
        window_end=date(2025, 1, 15),
        severity="warning",
        title="WIP saturation rising",
        rationale="WIP per engineer trending up.",
        success_criterion="WIP per engineer < 3",
        evidence=(
            EvidenceRef(
                team_id=team_id,
                metric_table="work_item_metrics_daily",
                window_start=date(2025, 1, 1),
                window_end=date(2025, 1, 15),
                field="wip_by_day",
                value=4.0,
            ),
        ),
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


@patch("dev_health_ops.workers.recommendations_tasks._get_db_url")
@patch("dev_health_ops.workers.recommendations_tasks._discover_team_ids")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
@patch("dev_health_ops.recommendations.loader.ClickHouseMetricsLoader")
@patch("dev_health_ops.recommendations.engine.RuleEngine")
def test_task_writes_engine_output_via_sink(
    mock_engine_cls,
    _mock_loader_cls,
    mock_sink_cls,
    mock_discover_teams,
    mock_get_db_url,
):
    """The engine output for each team must be persisted via write_recommendations."""
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_get_db_url.return_value = "clickhouse://fake"
    mock_discover_teams.return_value = ["team-1", "team-2"]

    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    rec = _make_recommendation()
    mock_engine = MagicMock()
    mock_engine.evaluate_all.return_value = [rec]
    mock_engine_cls.return_value = mock_engine

    result = run_recommendations_job.run(org_id="org-1", window=14)

    # Engine evaluated each discovered team over the requested window.
    assert mock_engine.evaluate_all.call_count == 2
    for call in mock_engine.evaluate_all.call_args_list:
        assert call.kwargs["window"] == 14
        assert call.kwargs["org_id"] == "org-1"

    # The fired recommendations were written through the sink (the wiring seam).
    mock_sink.write_recommendations.assert_called_once()
    written = mock_sink.write_recommendations.call_args.args[0]
    # One record per team (2 teams × 1 fired rec each).
    assert len(written) == 2
    # Records are RecommendationRecords carrying the engine output's title.
    assert all(r.title == "WIP saturation rising" for r in written)
    assert all(r.fired is True for r in written)
    mock_sink.close.assert_called_once()

    assert result["status"] == "success"
    assert result["fired"] == 2
    assert result["per_org"] == {"org-1": 2}


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
):
    """When an org has no active teams, the sink write is skipped (no empty rows)."""
    from dev_health_ops.workers.recommendations_tasks import run_recommendations_job

    mock_get_db_url.return_value = "clickhouse://fake"
    mock_discover_teams.return_value = []
    mock_sink = MagicMock()
    mock_sink_cls.return_value = mock_sink

    result = run_recommendations_job.run(org_id="org-empty")

    mock_engine_cls.assert_not_called()
    mock_sink.write_recommendations.assert_not_called()
    mock_sink.close.assert_called_once()
    assert result["fired"] == 0
