"""CHAOS-4020: the queue vocabulary shared by the Celery app and the Go fleet.

Every other guard of this contract moved to Go (internal/cigates/chartgates/contract_test.go,
cli*). This one test imports the Celery app's `task_queues` from Python, so it stays until that
source goes (CHAOS-3165 line).
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _celery_queue_names() -> frozenset[str]:
    """The Celery app's declared queue set, read from the producer itself.

    Generated from ``workers.config.task_queues`` rather than hand-authored, so
    a queue added on the Python side cannot drift away from this comparison
    silently.
    """
    from dev_health_ops.workers.config import task_queues

    return frozenset(task_queues)


def _go_queue_names() -> frozenset[str]:
    """The queues the Go River fleet actually serves, per the deployment manifest."""
    manifest = json.loads(
        (_REPO_ROOT / "deploy" / "go-workers" / "deployment.json").read_text(
            encoding="utf-8"
        )
    )
    return frozenset(
        queue
        for process in manifest["processes"]
        for queue in process.get("queues", [])
    )


def test_go_queue_names_that_exist_in_celery_agree_byte_for_byte() -> None:
    """Where the two runtimes name the same queue, they must spell it the same.

    The two fleets do not serve identical queue sets, so this pins the part that
    must not drift: a queue both runtimes know is spelled identically, and the
    divergence is explicit rather than accidental.
    """
    celery = _celery_queue_names()
    go = _go_queue_names()

    shared = celery & go
    assert shared == {"metrics", "reports", "sync", "webhooks"}, (
        f"the shared queue vocabulary changed: {sorted(shared)}"
    )

    # Queues the Go fleet serves that the Celery app never declared.
    assert go - celery == {
        "coverage",
        "heartbeat",
        "investment",
        "retention",
        "sync_provider",
        "workgraph",
    }, f"Go-only queues changed: {sorted(go - celery)}"

    # Celery queues no Go worker implements a handler for. A Go worker must not
    # accept these: startup validation requires the selected queue set to equal
    # the CONSTRUCTED handler set, so selecting one would fail readiness.
    assert celery - go == {
        "backfill",
        "default",
        "external-ingest",
        "ingest",
        "monitoring",
        "scheduler",
        "sync.github",
        "sync.github.heavy",
        "sync.github.light",
        "sync.github.medium",
        "sync.gitlab",
        "sync.gitlab.heavy",
        "sync.gitlab.light",
        "sync.gitlab.medium",
        "sync.jira",
        "sync.jira.medium",
        "sync.launchdarkly",
        "sync.linear",
        "sync.linear.medium",
    }, f"Celery-only queues changed: {sorted(celery - go)}"
