"""Oracle for IsoformatUTC: what CPython's datetime.isoformat() actually emits.

The complexity family stores this string (``file_complexity_snapshots.ref``,
built by ``_build_ref`` in metrics/job_complexity_db.py), so the Go port has to
match CPython byte for byte rather than "be a valid RFC 3339 rendering".

Reads newline-separated "<unix_seconds> <nanoseconds>" pairs on stdin and
writes one JSON object mapping each input to CPython's rendering. Nanoseconds
beyond microsecond resolution are TRUNCATED, because datetime cannot represent
them -- the same truncation the Go side documents.

This script is the ORACLE and never the implementation: it calls isoformat()
and reports what it said.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone


def main() -> int:
    out: dict[str, str] = {}
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        secs_text, nanos_text = line.split()
        secs = int(secs_text)
        nanos = int(nanos_text)
        # Truncate to microseconds: datetime has no finer resolution, so these
        # are digits Python would never have seen.
        micros = nanos // 1000
        moment = datetime.fromtimestamp(secs, tz=timezone.utc).replace(
            microsecond=micros
        )
        out[line] = moment.isoformat()
    json.dump(out, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
