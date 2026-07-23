"""Fixed, killable process boundary for metric compatibility execution."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import traceback
from typing import Any

from dev_health_ops.api.internal.worker_metrics import (
    _canonical_json,
    _execution_from_process_payload,
    _run_execution_direct,
)

_MAX_INPUT_BYTES = 1024 * 1024


def _payload() -> object:
    encoded = sys.stdin.buffer.read(_MAX_INPUT_BYTES + 1)
    if len(encoded) > _MAX_INPUT_BYTES:
        raise ValueError("metric compatibility process input exceeds the durable bound")
    return json.loads(encoded)


def _encode_outcome(outcome: dict[str, Any]) -> str:
    return _canonical_json({"outcome": outcome})


def main() -> int:
    try:
        execution = _execution_from_process_payload(_payload())
        # Compatibility computations may write progress to stdout. Reserve it
        # for the fixed JSON protocol inherited by the parent bridge.
        with contextlib.redirect_stdout(sys.stderr):
            outcome = asyncio.run(_run_execution_direct(execution))
        sys.stdout.write(_encode_outcome(outcome))
        return 0
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
