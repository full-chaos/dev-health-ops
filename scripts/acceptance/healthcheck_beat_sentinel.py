#!/usr/bin/env python3
"""Real work-receipt healthcheck for the Ask Dev acceptance `beat` service.

Codex finding (HIGH, 2026-08-05): checking the persistent schedule file's
mtime only proves beat's shelve store was touched recently -- ANY entry
updating (or beat's own periodic housekeeping) can refresh that mtime
without beat actually having dispatched a due task, so it does not prove
publication (beat put a message on the broker) or consumption (a worker
took it off and ran it).

`beat` cannot itself execute a probe (it only schedules), so the receipt
has to come from what beat's own dispatching already produces: this reads
the Celery Redis RESULT BACKEND -- the same one `beat`'s dispatched workers
write into -- for the most recent result shaped like
`monitor_queue_depths`'s return value (`{"queues": [...]}`), which is
ALREADY a beat-scheduled task (see workers/config.py's `monitor-queue-
depths` entry, interval 60s, queue `monitoring`) that a real worker must
have executed for a matching result to exist at all. If that result's
`date_done` is not within the bounded window below, beat has stopped
dispatching it (or every worker able to run it has stopped consuming) and
this reports unhealthy.

No task name is stored in the default Redis result payload, so this
matches by RESULT SHAPE (a dict with a "queues" key) rather than task name
-- a deliberate, narrow fingerprint chosen because `monitor_queue_depths`
is the only task in this stack's registry that returns that shape.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

# monitor-queue-depths' beat interval is 60s (workers/config.py). Bounded
# window must clear one interval plus real scheduling/consumption jitter
# without flapping; it must NOT clear silently forever if beat actually
# stopped.
_FRESHNESS_WINDOW_SECONDS = 150.0
_RESULT_KEY_PATTERN = "celery-task-meta-*"


def _parse_date_done(raw: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def main() -> int:
    import redis

    backend_url = os.environ.get("CELERY_RESULT_BACKEND")
    if not backend_url:
        print("CELERY_RESULT_BACKEND is not set", file=sys.stderr)
        return 1

    client = redis.from_url(backend_url, decode_responses=True)
    newest: datetime | None = None
    match_count = 0
    for key in client.scan_iter(match=_RESULT_KEY_PATTERN, count=200):
        raw_value = client.get(key)
        if raw_value is None:
            continue
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            continue
        if payload.get("status") != "SUCCESS":
            continue
        result = payload.get("result")
        if not isinstance(result, dict) or "queues" not in result:
            continue
        date_done = payload.get("date_done")
        if not isinstance(date_done, str):
            continue
        parsed = _parse_date_done(date_done)
        if parsed is None:
            continue
        match_count += 1
        if newest is None or parsed > newest:
            newest = parsed

    if newest is None:
        print(
            "no monitor_queue_depths-shaped result found in the result "
            "backend yet (beat has not dispatched one, or no worker has "
            "consumed one)",
            file=sys.stderr,
        )
        return 1

    age_seconds = (datetime.now(timezone.utc) - newest).total_seconds()
    if age_seconds > _FRESHNESS_WINDOW_SECONDS:
        print(
            f"newest monitor_queue_depths receipt is {age_seconds:.1f}s old "
            f"(> {_FRESHNESS_WINDOW_SECONDS}s) across {match_count} matching "
            "result(s) -- beat has stopped dispatching it, or no worker is "
            "consuming it",
            file=sys.stderr,
        )
        return 1

    print(
        f"beat sentinel fresh: newest receipt {age_seconds:.1f}s old "
        f"({match_count} matching result(s) scanned)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
