"""CHAOS-4026 contract: Go-owned cadences cannot silently resurrect.

Celery is retired (owner ratified 2026-08-21; zero Python celery services
have run in prod since the 2026-08-19 stop). This is the structural fix the
ticket asks for: an ownership-inventory entry whose Owner is a Go package
must make the same-named Python task PROVABLY unreachable, not merely
unlikely to run. Per CHAOS-4056's beat-schedule inventory sweep, the
following Python maintenance cadences have a verified Go successor and were
deleted in this cleanup:

  * dispatch-scheduled-syncs's sibling entries -- run-daily-metrics,
    run-complexity-daily, run-recommendations, run-release-impact-daily,
    run-capacity-forecast, process-ingest-streams, process-product-
    telemetry-streams, process-external-ingest-streams, external-ingest-
    stream-health, phone-home-heartbeat, dispatch-scheduled-reports,
    run-membership-backfill-daily, ask-dev-retention-sweep, and the
    never-live consume-pending-scheduled-sync-occurrences seam.

This test imports the real ``celery_app`` (autodiscovery included) and the
real ``beat_schedule`` and asserts the deleted task names are absent from
both -- so a resurrected task (a stray ``@celery_app.task`` decorator, or a
re-added beat entry) fails this test, not just a code review.

Entries this PR did NOT delete (flagged to team-lead, not this test's
concern) are asserted present in
``test_flagged_entries_were_not_silently_dropped`` below, so an accidental
drive-by deletion of a flagged-not-deleted entry also fails CI.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKERS_SRC = _REPO_ROOT / "src" / "dev_health_ops" / "workers"

# Celery task names Go now owns (dev_health_ops.workers.tasks.<name>) --
# these must be absent from the registered celery app and from tasks.__all__.
_DEAD_TASK_NAMES = (
    "dispatch_daily_metrics_for_all_orgs",
    "dispatch_daily_metrics_partitioned",
    "run_daily_metrics_batch",
    "run_daily_metrics_finalize_task",
    "dispatch_complexity_job",
    "run_recommendations_job",
    "dispatch_release_impact",
    "run_release_impact_job",
    "dispatch_capacity_forecast",
    "run_capacity_forecast_job",
    "run_ingest_consumer",
    "run_product_telemetry_consumer",
    "run_external_ingest_consumer",
    "external_ingest_stream_health",
    "dispatch_scheduled_reports",
    "dispatch_membership_backfill",
    "run_ask_dev_retention_cleanup",
    "consume_pending_scheduled_sync_occurrences",
)

# Beat schedule keys that must no longer exist.
_DEAD_BEAT_ENTRIES = (
    "run-daily-metrics",
    "run-complexity-daily",
    "run-recommendations",
    "run-release-impact-daily",
    "run-capacity-forecast",
    "process-ingest-streams",
    "process-product-telemetry-streams",
    "process-external-ingest-streams",
    "external-ingest-stream-health",
    "phone-home-heartbeat",
    "dispatch-scheduled-reports",
    "run-membership-backfill-daily",
    "ask-dev-retention-sweep",
    "consume-pending-scheduled-sync-occurrences",
)

# Whole modules deleted because every symbol they exported was dead
# machinery (fan-out dispatchers with no other caller).
_DELETED_MODULES = (
    "metrics_partitioned.py",
    "product_tasks.py",
    "report_scheduler.py",
    "ask_dev_retention.py",
    "metrics_tasks.py",
)

# Entries this PR deliberately did NOT delete -- either because they are
# still reachable by something other than the dead Celery Beat cadence
# (a dormant-Go operational HTTP bridge, the ask-dev-acceptance release
# gate's real Celery fleet), or because their removal needs its own
# reviewed pass (flagged to team-lead: reconcile-sync-dispatch,
# dispatch-scheduled-syncs). This guards the inverse mistake: a drive-by
# deletion of something this PR explicitly chose to keep.
_FLAGGED_SURVIVING_BEAT_ENTRIES = (
    "dispatch-scheduled-syncs",
    "reconcile-sync-dispatch",
    "dispatch-go-external-ingest-recompute-bridge",
    "monitor-queue-depths",
    "prune-rate-limit-observations",
    "prune-external-ingest-batches",
)
_FLAGGED_SURVIVING_TASK_NAMES = (
    "dispatch_scheduled_syncs",
    "reconcile_sync_dispatch",
    "dispatch_external_ingest_recompute_bridge",
    "monitor_queue_depths",
    "prune_rate_limit_observations",
    "prune_external_ingest_batches",
)


def _celery_app():
    from dev_health_ops.workers.celery_app import celery_app

    return celery_app


def _qualified(name: str) -> str:
    return f"dev_health_ops.workers.tasks.{name}"


def test_dead_task_names_are_absent_from_the_registered_celery_app() -> None:
    """A resurrected ``@celery_app.task`` under a dead name fails this test.

    This is the literal ask from CHAOS-4026's closing handoff: "a contract
    test that imports the celery app and asserts claimed-by-Go task names
    are absent/raise, so resurrection fails CI."
    """
    app = _celery_app()
    registered = set(app.tasks)
    for name in _DEAD_TASK_NAMES:
        qualified = _qualified(name)
        assert qualified not in registered, (
            f"{qualified!r} is registered on the celery app -- a Go-owned "
            "cadence (CHAOS-4026) resurrected. Its Python task must stay "
            "deleted; Go owns this cadence now."
        )


def test_dead_beat_entries_are_absent() -> None:
    """A re-added beat entry for a Go-owned cadence fails this test."""
    from dev_health_ops.workers.config import beat_schedule

    for entry in _DEAD_BEAT_ENTRIES:
        assert entry not in beat_schedule, (
            f"beat_schedule[{entry!r}] reappeared -- Go owns this cadence "
            "(CHAOS-4026); Celery Beat must not re-dispatch it."
        )


def test_dead_task_names_are_absent_from_tasks_module_exports() -> None:
    """A re-added import/export in workers/tasks.py fails this test."""
    from dev_health_ops.workers import tasks

    for name in _DEAD_TASK_NAMES:
        assert name not in tasks.__all__, (
            f"{name!r} reappeared in workers.tasks.__all__ -- Go owns this "
            "cadence (CHAOS-4026)."
        )
        assert not hasattr(tasks, name), (
            f"{name!r} reappeared as an attribute of workers.tasks -- Go "
            "owns this cadence (CHAOS-4026)."
        )


def test_deleted_task_modules_are_absent() -> None:
    """Deleted fan-out-dispatcher modules must not exist in the source tree."""
    for filename in _DELETED_MODULES:
        path = _WORKERS_SRC / filename
        assert not path.exists(), (
            f"{path.relative_to(_REPO_ROOT)} reappeared -- it was deleted "
            "as dead Celery machinery (CHAOS-4026); Go owns the cadences "
            "it used to dispatch."
        )


def test_runner_cli_no_longer_boots_a_real_celery_process() -> None:
    """CUT-18's "no Celery process is running" criterion must hold at the CLI.

    Scope addition ratified on CHAOS-4026 (2026-08-21 reconciliation sweep):
    runner.py's start-worker/start-scheduler subcommands booted a real
    ``celery worker``/``celery beat`` process and were the last CLI-level
    way to falsify CUT-18 (CHAOS-3931). ``inspect`` survives deliberately --
    it only reads Celery's control-plane RPC (useful for the still-live
    ask-dev-acceptance Celery fleet) and cannot itself start a process.
    """
    source = (_WORKERS_SRC / "runner.py").read_text(encoding="utf-8")
    assert 'add_parser("start-worker"' not in source
    assert "add_parser('start-worker'" not in source
    assert 'add_parser("start-scheduler"' not in source
    assert "add_parser('start-scheduler'" not in source
    assert "_cmd_start_worker" not in source
    assert "_cmd_start_scheduler" not in source

    import argparse

    from dev_health_ops.workers.runner import register_commands

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    register_commands(subparsers)
    registered = set(subparsers.choices)
    assert "start-worker" not in registered
    assert "start-scheduler" not in registered
    assert "inspect" in registered  # control-plane read-only survives


def test_flagged_entries_were_not_silently_dropped() -> None:
    """The entries this PR chose to KEEP must still be there.

    Guards the inverse mistake to the rest of this file: a drive-by
    deletion of dispatch-scheduled-syncs, reconcile-sync-dispatch, the
    CHAOS-4057 recompute bridge, or the ask-dev-acceptance-fleet-dependent
    entries (monitor-queue-depths, prune-rate-limit-observations,
    prune-external-ingest-batches) would break something this PR explicitly
    decided not to touch, without any other test catching it.
    """
    from dev_health_ops.workers import tasks
    from dev_health_ops.workers.config import beat_schedule

    app = _celery_app()
    registered = set(app.tasks)

    for entry in _FLAGGED_SURVIVING_BEAT_ENTRIES:
        assert entry in beat_schedule, (
            f"beat_schedule[{entry!r}] is missing -- this PR (CHAOS-4026) "
            "deliberately kept it (flagged to team-lead, not deleted)."
        )
    for name in _FLAGGED_SURVIVING_TASK_NAMES:
        qualified = _qualified(name)
        assert qualified in registered, (
            f"{qualified!r} is not registered on the celery app -- this PR "
            "(CHAOS-4026) deliberately kept it."
        )
    # dispatch_external_ingest_recompute_bridge is registered via a whole-module
    # import (`import ...external_ingest_recompute  # noqa: F401` in tasks.py),
    # not an individual export -- it was never in tasks.__all__, before or
    # after this PR. The rest are.
    for name in set(_FLAGGED_SURVIVING_TASK_NAMES) - {
        "dispatch_external_ingest_recompute_bridge"
    }:
        assert name in tasks.__all__


def test_orphaned_recompute_bridge_task_is_untouched() -> None:
    """CHAOS-4057: the orphaned bridge stays until port-vs-retire is decided.

    Explicit scope boundary from CHAOS-4026: this task's Celery machinery is
    NOT dead -- it is the only reader of the Go stream-external process's
    bridge_pending compatibility rows. Deleting it would be a second,
    silent data-loss incident on top of the one CHAOS-4057 already found.
    """
    from dev_health_ops.workers import external_ingest_recompute

    assert hasattr(
        external_ingest_recompute, "dispatch_external_ingest_recompute_bridge"
    )


def test_celery_transport_dispatch_plane_is_untouched() -> None:
    """CHAOS-4054 step 4 territory: not this ticket's to delete.

    provider_unit_transport.py's Celery-presence resolution and the
    worker_job_routes/sync_dispatch_transport_routes tables are explicitly
    sequenced AFTER CHAOS-4026 closes (CHAOS-4054's own decision doc).
    """
    from dev_health_ops.workers import provider_unit_transport

    assert hasattr(provider_unit_transport, "resolve_unit_transport")
    assert hasattr(provider_unit_transport, "resolve_celery_presence")
    assert hasattr(provider_unit_transport, "UnitTransport")
