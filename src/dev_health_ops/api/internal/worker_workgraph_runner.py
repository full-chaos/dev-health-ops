"""Fixed, killable process boundary for work-graph compatibility execution."""

from __future__ import annotations

import contextlib
import json
import sys
import traceback
from typing import Any

from dev_health_ops.api.internal.worker_workgraph import _canonical, _run_sync

_MAX_INPUT_BYTES = 1024 * 1024


def _payload() -> tuple[str, dict[str, Any]]:
    encoded = sys.stdin.buffer.read(_MAX_INPUT_BYTES + 1)
    if len(encoded) > _MAX_INPUT_BYTES:
        raise ValueError("compatibility process input exceeds the durable bound")
    decoded = json.loads(encoded)
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"kind", "arguments"}
        or not isinstance(decoded["kind"], str)
        or not isinstance(decoded["arguments"], dict)
    ):
        raise ValueError("compatibility process input is invalid")
    return decoded["kind"], decoded["arguments"]


def _encode_outcome(outcome: dict[str, Any]) -> str:
    # Match the API evidence serializer's compatibility behavior for values
    # such as datetimes and UUIDs returned by older task implementations.
    return _canonical({"outcome": outcome})


def main() -> int:
    try:
        kind, arguments = _payload()
        # Compatibility tasks occasionally use stdout for progress messages.
        # Reserve stdout for the fixed JSON protocol so the parent never has to
        # infer a result from logs.
        with contextlib.redirect_stdout(sys.stderr):
            outcome = _run_sync(kind, arguments)
        sys.stdout.write(_encode_outcome(outcome))
        return 0
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
