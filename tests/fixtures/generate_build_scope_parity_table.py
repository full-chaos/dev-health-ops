"""Measure the reference's verdict for every build-scope value shape.

`run_work_graph_build` gates each scope value on Python TRUTHINESS and then
hands the truthy ones to `datetime.fromisoformat`
(`workers/work_graph_tasks.py:121-135`). The Go adapter in
`cmd/dev-health-worker/workgraph_issue_pr_links.go` reproduces that gate.

That gate has been got wrong TWICE by reasoning about it -- once treating an
empty string as a parse error, once treating a whitespace-only string as
absent -- and on both occasions a hand-written Go test asserted the wrong
behaviour and passed. So the accept/reject set is MEASURED here and frozen,
and the Go test diffs against this table with an explicitly enumerated
divergence list. A shape neither matched nor enumerated is a failure.

Regenerate:
    docker cp tests/fixtures/generate_build_scope_parity_table.py dev-health-api-1:/tmp/
    docker exec -i dev-health-api-1 python /tmp/generate_build_scope_parity_table.py --stdout
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Any

# Every shape a build scope value could plausibly carry: the falsy set, the
# whitespace family (the one that caused the second defect), the ISO forms
# fromisoformat accepts including the exotic ones, offset variants, and
# non-string types (the bridge filters scope by field NAME, not type).
CASES: list[Any] = [
    None,
    "",
    " ",
    "\t",
    "\n",
    "  \t ",
    "2026-08-15",
    "20260815",
    "2026-W33-6",
    "2026-243",
    "2026-08-15T06:07",
    "2026-08-15T06:07:08",
    "2026-08-15 06:07:08",
    "2026-08-15T06:07:08.123",
    "2026-08-15T06:07:08Z",
    "2026-08-15T06:07:08+00:00",
    "2026-08-15T06:07:08+0000",
    "2026-08-15T06:07:08+05:00",
    "2026-08-15T06:07:08-08:00",
    "2026-08-15t06:07:08",
    "2026-08-15_06:07:08",
    "15/08/2026",
    "not-a-date",
    "2026-13-45",
    False,
    True,
    0,
    1,
    123,
    [],
    {},
    ["2026-08-15"],
]


def measure() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value in CASES:
        # The reference's gate, verbatim: `if to_date:` then fromisoformat.
        if not value:
            rows.append({"value": value, "verdict": "DEFAULT"})
            continue
        try:
            rows.append(
                {
                    "value": value,
                    "verdict": "PARSED",
                    "iso": datetime.fromisoformat(value).isoformat(),
                }
            )
        except Exception as error:  # noqa: BLE001 - the verdict IS the exception type
            rows.append(
                {"value": value, "verdict": "RAISES", "error": type(error).__name__}
            )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--out", default="/tmp/build_scope_parity_table.json")
    args = parser.parse_args()

    payload = (
        json.dumps(
            {"schema": "build_scope_parity_table.v1", "cases": measure()},
            indent=1,
            sort_keys=True,
        )
        + "\n"
    )
    if args.stdout:
        sys.stdout.write(payload)
    else:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(f"written {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
