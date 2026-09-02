"""Measure the BRIDGE'S OWN ADMISSION for every build-scope shape.

The Go pre-step in `cmd/dev-health-worker/workgraph_issue_pr_links.go` runs
before the Python bridge and writes to `work_graph_issue_pr`. So the property
that matters is not "does Go parse this string the same way" but:

    THE REFERENCE RAISES  =>  Go must REFUSE before writing anything.
    THE REFERENCE RUNS    =>  Go's window must equal the reference's.

Getting that backwards persists mapping rows for a build the bridge is about to
reject. Three separate defects in three review rounds were that exact shape.

So this table records the reference's admission, not a parser's opinion:

1. `worker_workgraph._scope_arguments` — the REAL function, imported and
   called. It raises on a non-object scope and on any unsupported field.
2. `run_work_graph_build`'s window derivation
   (`workers/work_graph_tasks.py:121-135`), reproduced verbatim below against a
   FROZEN `now` because the surrounding function opens a database. The six
   lines it reproduces are quoted in `_derive_window` so a reader can diff them.

The corpus is two-dimensional — the DOCUMENT's shape crossed with its VALUES —
because the value axis alone missed two real divergences (a top-level `null`
scope and the numeric-falsy family), and the shape axis is where the dangerous
direction lives.

Regenerate (prefer the container; the local interpreter is a faithful stand-in
only while it matches the shipped 3.14 line, and the header records which ran):

    docker cp tests/fixtures/generate_build_scope_parity_table.py dev-health-api-1:/tmp/
    docker exec -i dev-health-api-1 python /tmp/generate_build_scope_parity_table.py --stdout
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, "/app/src")

# A fixed instant so "now" defaults are reproducible. The Go differential uses
# the same value for its own clock.
FROZEN_NOW = datetime(2026, 9, 1, 12, 30, 45, 500000, tzinfo=timezone.utc)

# Document shapes. The scope reaches the bridge as decoded JSON, so these are
# the shapes `_scope_arguments` can actually receive.
DOCUMENTS: list[tuple[str, Any]] = [
    ("object_empty", {}),
    ("null", None),
    ("array", []),
    ("array_nonempty", ["2026-08-15"]),
    ("string", "2026-08-15"),
    ("number", 123),
    ("bool", True),
    ("object_unsupported_field", {"not_a_field": 1}),
    ("object_unsupported_plus_valid", {"to_date": "2026-08-15", "nope": 1}),
]

# Value shapes, each placed under `to_date` in an otherwise valid object.
VALUES: list[Any] = [
    None,
    "",
    " ",
    "\t",
    "\n",
    "  \t ",
    False,
    True,
    0,
    0.0,
    -0.0,
    0e100,
    1e-400,
    1,
    123,
    [],
    {},
    ["2026-08-15"],
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
    "2026-08-15T06:07:08+00",
    "2026-08-15T06:07:08+00:00:00",
    "2026-08-15T06:07:08+05:00",
    "2026-08-15T06:07:08-08:00",
    "2026-08-15t06:07:08",
    "2026-08-15_06:07:08",
    "15/08/2026",
    "not-a-date",
    "2026-13-45",
]


def _derive_window(arguments: dict[str, Any]) -> dict[str, Any]:
    """Reproduce `run_work_graph_build`'s window derivation, verbatim.

    From `src/dev_health_ops/workers/work_graph_tasks.py:121-135`:

        now = datetime.now(timezone.utc)
        if to_date:   parsed_to = datetime.fromisoformat(to_date)
        else:         parsed_to = now
        if from_date: parsed_from = datetime.fromisoformat(from_date)
        else:         parsed_from = parsed_to - timedelta(days=30)
        parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    Reproduced rather than called because the surrounding task opens a
    database. `now` is frozen so the defaults are reproducible.
    """
    import uuid

    to_date = arguments.get("to_date")
    from_date = arguments.get("from_date")
    repo_id = arguments.get("repo_id")

    parsed_to = datetime.fromisoformat(to_date) if to_date else FROZEN_NOW
    parsed_from = (
        datetime.fromisoformat(from_date)
        if from_date
        else parsed_to - timedelta(days=30)
    )
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None
    return {
        "from": parsed_from.isoformat(),
        "to": parsed_to.isoformat(),
        "repo_id": str(parsed_repo_id) if parsed_repo_id else None,
    }


def _admit(scope: Any) -> dict[str, Any]:
    """Run the reference's admission for one scope, recording RAISES or RUNS."""
    from dev_health_ops.api.internal import worker_workgraph

    row = {
        "org_id": "70d529e0-3c06-4597-8480-794fd02328b6",
        "model_ref": None,
        "llm_concurrency": 1,
    }
    try:
        arguments = worker_workgraph._scope_arguments("workgraph.build", scope, row)
    except Exception as error:  # noqa: BLE001 - the verdict IS the exception
        return {
            "verdict": "RAISES",
            "stage": "scope_arguments",
            "error": type(error).__name__,
        }
    try:
        return {"verdict": "RUNS", "window": _derive_window(arguments)}
    except Exception as error:  # noqa: BLE001
        return {"verdict": "RAISES", "stage": "window", "error": type(error).__name__}


def measure() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, document in DOCUMENTS:
        rows.append({"case": f"document:{name}", "scope": document, **_admit(document)})
    for value in VALUES:
        rows.append(
            {"case": "value", "scope": {"to_date": value}, **_admit({"to_date": value})}
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--out", default="/tmp/build_scope_parity_table.json")
    args = parser.parse_args()

    payload = (
        json.dumps(
            {
                "schema": "build_scope_parity_table.v2",
                "frozen_now": FROZEN_NOW.isoformat(),
                "measured_on": platform.python_version(),
                "cases": measure(),
            },
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
