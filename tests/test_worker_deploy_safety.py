from __future__ import annotations

# CHAOS-3093 (PR2b): workers/runner.py's `inspect` CLI subcommand (and its
# sanitization helpers `_cmd_inspect`/`_inspect_worker_tasks`/`_sanitize_*`)
# are deleted outright -- it only read Celery's control-plane RPC
# (`celery_app.control.inspect`), and CHAOS-4065 already converted the last
# claimed live consumer (the ask-dev-acceptance fleet's `worker`/`beat`
# services in tests/acceptance/compose.ask-dev.yml) to
# `entrypoint: ["sleep", "infinity"]` with Go `ask-dev-jobs-probe`
# healthchecks -- there is no Celery fleet left anywhere, live or in test
# infra, for it to inspect. The three tests that exercised it
# (test_worker_inspect_sanitizes_task_arguments,
# test_worker_inspect_quiet_json_restores_otel_on_parse_error,
# test_worker_inspect_sanitizes_nested_scheduled_request) are deleted with
# it.


def test_worker_late_ack_exclusions_are_explicit() -> None:
    from dev_health_ops.workers import config
    from dev_health_ops.workers.celery_app import celery_app

    assert config.task_acks_late is False
    assert config.task_reject_on_worker_lost is False
    assert celery_app.conf.task_acks_late is False
    assert celery_app.conf.task_reject_on_worker_lost is False
    # PR2b (CHAOS-3093) dropped phone_home_heartbeat's `@celery_app.task`
    # decorator -- the last remaining entry -- so this tuple (and the
    # annotations dict derived from it) is now empty. No per-task late-ack
    # exclusion survives; if one is ever added back, pin its exact name and
    # annotation here again rather than widening this assertion.
    assert config.late_ack_excluded_tasks == ()
    assert config.task_annotations == {}


def test_worker_late_ack_exclusions_match_registered_tasks(monkeypatch) -> None:
    monkeypatch.setenv("OTEL_ENABLED", "false")

    # Load the aggregator module so every task registers with the Celery app,
    # exactly as the worker bootstrap (autodiscover on worker init) does. Without
    # it, celery_app.tasks reflects only lazily-imported tasks and this
    # deploy-safety check becomes order-dependent under -n distribution.
    import dev_health_ops.workers.tasks  # noqa: F401
    from dev_health_ops.workers import config
    from dev_health_ops.workers.celery_app import celery_app

    missing = set(config.late_ack_excluded_tasks) - set(celery_app.tasks)
    assert missing == set()
