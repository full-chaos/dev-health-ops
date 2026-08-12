"""The sync coverage projector has one native Go owner.

The Celery-off migration cannot leave a registered Beat entry or task body
behind: either one would restore a second writer as soon as a Python worker was
started for an unrelated compatibility workload.
"""

from pathlib import Path


def test_sync_coverage_refresh_has_no_python_schedule_or_task() -> None:
    from dev_health_ops.workers import tasks
    from dev_health_ops.workers.config import beat_schedule

    assert "refresh-sync-coverage-projections" not in beat_schedule
    assert "refresh_sync_coverage_projections" not in tasks.__all__
    assert not hasattr(tasks, "refresh_sync_coverage_projections")
    assert not (
        Path(__file__).resolve().parents[2]
        / "src/dev_health_ops/workers/sync_coverage.py"
    ).exists()
