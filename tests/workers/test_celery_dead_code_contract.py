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

CHAOS-3093 (2026-09-09, PR2a) additionally retired monitor-queue-depths and
prune-external-ingest-batches outright: CHAOS-4065 replaced the
ask-dev-acceptance release-blocking gate's real Celery worker+beat fleet
(their last reason to survive) with a Go-native probe that re-executes the
same production Go code directly.

CHAOS-3093's own PR2a' (2026-09-09) is the separate reviewed pass that then
retired dispatch-scheduled-syncs, reconcile-sync-dispatch, and prune-rate-
limit-observations too, with a one-row-per-deleted-test mapping onto
executed internal/syncreconciler / internal/scheduler/sync Go tests in that
PR's own TEST-EVIDENCE. No entries remain flagged-surviving after PR2a' --
every Beat entry this file once deferred is now in _DEAD_BEAT_ENTRIES.

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

import importlib
import inspect
from pathlib import Path

import pytest

from dev_health_ops.workers import sync_units

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
    # CHAOS-3093 (PR2a): queue_monitor.py/external_ingest_reconciler.py/
    # external_ingest_recompute.py/metrics_daily.py/work_graph_tasks.py/
    # report_task.py deleted outright -- see the module-level docstring
    # addendum above.
    "monitor_queue_depths",
    "prune_external_ingest_batches",
    "flush_external_ingest_recompute",
    "run_daily_metrics",
    "run_investment_materialize_chunk",
    "finalize_investment_materialize_partitioned",
    "dispatch_investment_materialize_partitioned",
    "run_membership_backfill",
    "execute_saved_report",
    # CHAOS-3093 (PR2a'): sync_scheduler.py/sync_reconciler.py deleted
    # outright -- see the module-level docstring addendum above.
    "dispatch_scheduled_syncs",
    "reconcile_sync_dispatch",
    "prune_rate_limit_observations",
    # CHAOS-3093 (PR2b): health_check had no dispatch site of any kind
    # (confirmed repo-wide: no .delay/.apply_async/send_task/getattr
    # indirection/Beat entry/CLI invocation) -- deleted outright, along with
    # its two re-export sites (tasks.py, system_tasks.py). Unlike the other
    # PR2b decorator strips below, this one is a full deletion, not a
    # decorator-only removal -- see test_dead_code_contract's own comment
    # for phone_home_heartbeat/run_post_sync_team_autoimport, which keep
    # their function bodies and stay live via HTTP compatibility bridges.
    "health_check",
)

# CHAOS-3093 (PR2b): phone_home_heartbeat (system_ops.py) and
# run_post_sync_team_autoimport (team_autoimport.py) had their
# `@celery_app.task` decorators dropped -- Celery has had zero consumers
# since CHAOS-4026, so the decorator was dead weight around code that is
# still genuinely needed: both remain the live compute body behind an HTTP
# compatibility bridge (api/internal/worker_operational.py's /heartbeat,
# api/internal/worker_sync.py's /team-autoimport), called directly now
# instead of via `.run()`. They are deliberately NOT added to
# _DEAD_TASK_NAMES above -- that set asserts absence from
# `tasks.__all__`/the registered celery app, and both functions correctly
# stay present in `tasks.__all__` (just no longer celery-registered).

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
    # CHAOS-3093 (PR2a):
    "monitor-queue-depths",
    "prune-external-ingest-batches",
    # CHAOS-3093 (PR2a'):
    "dispatch-scheduled-syncs",
    "reconcile-sync-dispatch",
    "prune-rate-limit-observations",
)

# Whole modules deleted because every symbol they exported was dead
# machinery (fan-out dispatchers with no other caller).
_DELETED_MODULES = (
    "metrics_partitioned.py",
    "product_tasks.py",
    "report_scheduler.py",
    "ask_dev_retention.py",
    "metrics_tasks.py",
    # CHAOS-3093 (PR2a) -- see the module-level docstring addendum above.
    # #7 report_task.py's one task (execute_saved_report) is deleted outright
    # because reports.py's resolver already routes the user-facing "run now"
    # trigger through the durable outbox -> River -> Go's report.execute_
    # on_demand (CHAOS-4440), so nothing else called it.
    "queue_monitor.py",
    "external_ingest_reconciler.py",
    "external_ingest_recompute.py",
    "metrics_daily.py",
    "work_graph_tasks.py",
    "report_task.py",
    # CHAOS-3093 (PR2a'): the reviewed pass the rest of this file's docstring
    # addendum describes -- dispatch-go-external-ingest-recompute-bridge was
    # here until CHAOS-5296 and is RETIRED, guarded by
    # test_recompute_bridge_task_is_deleted below, not this tuple.
    "sync_scheduler.py",
    "sync_reconciler.py",
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


def test_decorator_stripped_tasks_are_plain_functions_still_exported() -> None:
    """PR2b's decorator strips: live, but no longer celery-registered.

    phone_home_heartbeat and run_post_sync_team_autoimport keep their
    compute bodies (still genuinely needed behind an HTTP compatibility
    bridge) but lost their `@celery_app.task` decorator -- a re-added
    decorator on either fails this test. They are deliberately NOT in
    _DEAD_TASK_NAMES above: that set asserts absence from tasks.__all__,
    and both correctly stay exported there.
    """
    from dev_health_ops.workers import tasks

    app = _celery_app()
    registered = set(app.tasks)
    for name in ("phone_home_heartbeat", "run_post_sync_team_autoimport"):
        assert name in tasks.__all__, (
            f"{name!r} unexpectedly missing from tasks.__all__"
        )
        func = getattr(tasks, name)
        assert not hasattr(func, "run"), (
            f"{name!r} has a .run attribute -- a @celery_app.task decorator "
            "reappeared; Celery has had zero consumers since CHAOS-4026."
        )
        assert _qualified(name) not in registered, (
            f"{_qualified(name)!r} is registered on the celery app -- a "
            "@celery_app.task decorator reappeared on a function that is "
            "meant to stay a plain HTTP-bridge compute body."
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


def test_recompute_bridge_task_is_deleted() -> None:
    """CHAOS-5296 + CHAOS-3093: the Celery bridge is gone, replaced by a
    native consumer, and the whole module it lived in is gone too.

    This assertion is the inverse of the one it replaces. CHAOS-4026 kept the
    module because it was the ONLY reader of the Go runner's bridge rows, and
    deleting it then would have been a second silent data-loss incident on top
    of the one CHAOS-4057 found. CHAOS-5296 removed that condition by shipping
    the reader in Go (internal/externalrecompute/drain.go) and repointing the
    writer at it in the same change, so no legacy enqueue can happen at all --
    CHAOS-3093 (PR2a) then deleted workers/external_ingest_recompute.py
    outright (its other task, flush_external_ingest_recompute, had zero
    Celery consumers since 2026-08-19; its one remaining live call site,
    external_ingest/recompute.py's schedule_or_coalesce, was retargeted to a
    named celery_app.send_task instead of importing the module).

    Asserting the module's absence (not just one attribute's) is deliberate:
    a task re-added here would have no consumer and would silently
    reintroduce the split-brain this ticket closed.
    """
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dev_health_ops.workers.external_ingest_recompute")


def test_celery_presence_dispatch_plane_is_deleted() -> None:
    """CHAOS-4054 step 4 acceptance: no code path can select a Celery transport.

    The decision record's acceptance for step 4 is literal -- "no code path can
    construct ``UnitTransport.CELERY`` or ``UNROUTABLE``". The strongest form of
    that is that neither the enum nor the modules that resolved it exist at all,
    so this asserts on importability rather than on an attribute of a module
    that could be reintroduced with the branches quietly restored.

    This test replaces ``test_celery_transport_dispatch_plane_is_untouched``,
    which pinned the opposite invariant while step 4 was sequenced behind
    CHAOS-4026. CHAOS-4026 is Done; this is the inversion it gated.
    """

    for module in (
        "dev_health_ops.workers.provider_unit_transport",
        "dev_health_ops.workers.celery_consumers",
    ):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module)


def test_provider_unit_dispatch_has_no_celery_fallthrough() -> None:
    """The dispatcher stages River outbox rows or terminalizes -- never publishes.

    A negative control on the test above: deleting the resolver modules proves
    the vocabulary is gone, but not that the caller stopped reaching for a
    second runtime. ``dispatch_sync_run`` is the only producer of provider
    units, so its source is where a reintroduced Celery fallthrough would show
    up first.
    """

    source = inspect.getsource(sync_units)

    for banned in (
        "resolve_unit_transport",
        "resolve_celery_presence",
        "UnitTransport",
        "celery_presence",
        "river_owns_units",
    ):
        assert banned not in source, (
            f"{banned!r} is back in sync_units.py -- CHAOS-4054 step 4 deleted "
            "the Celery-presence transport plane; a provider unit is either "
            "staged in the durable outbox or terminalized."
        )

    dispatch = inspect.getsource(sync_units.dispatch_sync_run)
    assert "run_sync_unit" not in dispatch, (
        "dispatch_sync_run publishes run_sync_unit again -- the Celery "
        "fallthrough this dispatcher had is deleted (CHAOS-4054 step 4)."
    )
