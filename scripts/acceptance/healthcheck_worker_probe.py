#!/usr/bin/env python3
"""Real work-receipt healthcheck for the Ask Dev acceptance `worker` service.

Codex finding (HIGH, 2026-08-05): `celery -A ... inspect ping` only proves
the worker's CONTROL-PLANE RPC is responsive -- Celery's `inspect` commands
are answered on a dedicated control channel, independent of the task pool
that actually executes queued work. A worker whose pool is wedged (deadlock,
exhausted concurrency, a task blocking forever) can keep answering `inspect
ping` while never draining a single task off `monitoring` (or any other
required queue) -- exactly the false-positive "no required job may exit
success after skipping substantive work" the plan's work-receipt principle
exists to rule out.

This dispatches a uniquely-identified real task through the SAME required
queue (`monitoring`) the worker actually consumes -- reusing
`monitor_queue_depths`, an existing, side-effect-free, already
beat-scheduled production task, rather than adding new production code --
and blocks for ITS result under a bounded timeout. Success here means: the
message reached the broker, a live worker process pulled it off the
`monitoring` queue, executed it, and the result made it back through the
result backend. A wedged pool cannot produce that chain and this times out.
"""

from __future__ import annotations

import sys
import uuid

_TIMEOUT_SECONDS = 8.0
_PROBE_TASK_NAME = "dev_health_ops.workers.tasks.monitor_queue_depths"
_PROBE_QUEUE = "monitoring"


def main() -> int:
    from dev_health_ops.workers.celery_app import celery_app

    probe_id = str(uuid.uuid4())
    try:
        result = celery_app.send_task(
            _PROBE_TASK_NAME,
            task_id=probe_id,
            queue=_PROBE_QUEUE,
        )
        payload = result.get(timeout=_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 -- healthcheck: any failure is unhealthy
        print(
            f"worker probe {probe_id} via {_PROBE_QUEUE} did not complete: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1
    if not isinstance(payload, dict) or "queues" not in payload:
        print(
            f"worker probe {probe_id} returned an unexpected payload: {payload!r}",
            file=sys.stderr,
        )
        return 1
    print(f"worker probe {probe_id} completed via {_PROBE_QUEUE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
