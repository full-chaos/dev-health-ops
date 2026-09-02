#!/usr/bin/env python3
"""Freeze what builder.py's event_ts expression does to a timestamp STRING.

Codex round 2 found this port's parser narrower than `datetime.fromisoformat`,
and the failure was silent: an unparseable value became the BUILD clock, so a
row with a wrong event_ts is indistinguishable from a row with a right one.
event_ts is already the most delicate field in this build (CHAOS-4788), which
is why a silent substitution there is worth a corpus of its own.

The axes varied here are the ones `fromisoformat` actually distinguishes,
measured against the deployed interpreter rather than recalled:

  separator          T / lowercase t / space / an arbitrary character
  format             extended (2026-09-01) vs basic (20260901)
  precision          date-only / hour / minute / second / 3, 6, 7, 9 fractions
  offset             absent / Z / +00:00 / +0000 / negative
  Z placement        trailing, and embedded (Python replaces EVERY "Z")
  calendar           ordinary dates vs ISO week dates
  boundary           24:00:00, which Python rolls into the next day

This reproduces the builder's expression, not an imitation of it: parse,
coerce a naive value to UTC, and fall back to the build clock on ValueError.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

BUILD_CLOCK = datetime(2026, 9, 2, 3, 0, 0, tzinfo=timezone.utc)

CORPUS = [
    "2026-09-01T12:34:56+00:00",
    "2026-09-01T12:34:56Z",
    "2026-09-01t12:34:56+00:00",
    "2026-09-01 12:34:56+00:00",
    "2026-09-01x12:34:56",
    "20260901T123456+0000",
    "20260901T123456",
    "20260901",
    "2026-09-01",
    "2026-09-01T12",
    "2026-09-01T12:34",
    "2026-09-01T12:34:56",
    "2026-09-01T12:34:56.123",
    "2026-09-01T12:34:56.123456",
    "2026-09-01T12:34:56.1234567",
    "2026-09-01T12:34:56.123456789",
    "2026-09-01T12:34:56+0000",
    "2026-09-01T12:34:56-05:00",
    "2026-W36-2",
    "2026-09-01T24:00:00",
    "  2026-09-01T12:34:56  ",
    "not a timestamp",
    "",
]


def observe(value: str) -> dict:
    """builder.py:886-895, verbatim, for the string arm."""
    record: dict = {"input": value}
    event_ts = value
    try:
        parsed = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
        record["outcome"] = "parsed"
    except ValueError:
        record["outcome"] = "fallback_to_build_clock"
        record["event_ts"] = BUILD_CLOCK.isoformat()
        return record
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    record["event_ts"] = parsed.astimezone(timezone.utc).isoformat()
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="tests/fixtures/event_timestamp_parity.json")
    namespace = parser.parse_args()
    document = {
        "schema": "event_timestamp_parity.v1",
        "producer": "work_graph/builder.py::_build_issue_issue_edges event_ts expression",
        "generated_by": "tests/fixtures/generate_event_timestamp_parity.py",
        "build_clock": BUILD_CLOCK.isoformat(),
        "observations": [observe(value) for value in CORPUS],
    }
    with open(namespace.out, "w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + "\n"
        )
    print(f"wrote {namespace.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
