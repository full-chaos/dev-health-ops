"""Human vs `--json` rendering (CHAOS-2700 brief decision 8) and the
exit-code contract (brief decision 9, master-spec CC29).

`--json` is a boolean flag, not `--output=json` (decision 8: the CLI-native
convention for the target audience, and this repo already has two competing
spellings elsewhere -- pick one, don't add a third). When set, all
machine-readable output is a single JSON object per invocation on stdout
(never NDJSON -- each `push` subcommand produces one logical result); all
human/progress logging goes to stderr via the standard `logging` handler,
exactly like `recommendations compute --output-json`.

Exit codes (decision 9):
    0  success
    1  data-level failure (invalid payload / batch completed-with-rejections
       / terminal failed)
    2  usage error (bad/missing CLI args) -- matches argparse's own
       `parser.error()` -> exit 2 convention used elsewhere in `dev-hops`
    3  transport/API error after retries exhausted, or stream_unavailable
    4  poll timeout (batch still non-terminal when --poll-timeout elapses)
"""

from __future__ import annotations

import json
import sys
from typing import Any

EXIT_OK = 0
EXIT_DATA_FAILURE = 1
EXIT_USAGE_ERROR = 2
EXIT_TRANSPORT_ERROR = 3
EXIT_POLL_TIMEOUT = 4


def emit_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, default=str))


def emit_rejection_table(errors: list[dict[str, Any]]) -> None:
    """Human-readable rejected-record table (brief GOAL: "rejected-record
    table for validate/batch rejections"). Printed to stdout -- this is the
    primary result of a failed `push validate`/`push status`, not
    incidental progress logging."""
    if not errors:
        return
    print(f"{len(errors)} error(s):")
    for item in errors:
        index = item.get("index")
        loc = (
            f"records[{index}]" if isinstance(index, int) and index >= 0 else "envelope"
        )
        path = item.get("path")
        if path:
            loc = f"{loc} ({path})"
        kind = item.get("kind")
        kind_part = f" kind={kind}" if kind else ""
        print(f"  - {loc}{kind_part} [{item.get('code')}] {item.get('message')}")


def emit_api_error_human(
    status_code: int, code: str, message: str, errors: list[dict[str, Any]]
) -> None:
    print(f"error: HTTP {status_code} {code}: {message}", file=sys.stderr)
    if errors:
        emit_rejection_table(errors)


__all__ = [
    "EXIT_OK",
    "EXIT_DATA_FAILURE",
    "EXIT_USAGE_ERROR",
    "EXIT_TRANSPORT_ERROR",
    "EXIT_POLL_TIMEOUT",
    "emit_json",
    "emit_rejection_table",
    "emit_api_error_human",
]
