"""Generate the CHAOS-4757 issue<->PR mapping golden from the DEPLOYED Python producer.

The golden freezes both halves of a pure-function contract:

  * the four ClickHouse reads ``_derive_issue_pr_links_from_dependencies``
    performs, captured by RECORDING them as the deployed builder issues them --
    never re-queried by this script, so the file cannot drift from what Python
    actually saw; and
  * the ``work_graph_issue_pr`` rows that producer derived from them.

The Go port (``internal/jobs/workgraph/issueprlinks``) replays the inputs
through ``Derive`` and must reproduce the rows exactly. Because the inputs are
frozen in the file, the Go test is hermetic: it never touches ClickHouse and
does not move when the live tables move (the lane-common-brief rule that
comparisons across a time gap are inadmissible).

Run it inside the api container, which owns the deployed interpreter:

    docker cp tests/fixtures/generate_issue_pr_links_python_golden.py dev-health-api-1:/tmp/
    docker exec -i dev-health-api-1 python /tmp/generate_issue_pr_links_python_golden.py --stdout

``--stdout`` is what the Go rot-guard test
(``golden_rot_guard_test.go``, gated by DEV_HEALTH_LIVE_PYTHON_ORACLES=1) uses
to re-run this generator against the live interpreter and byte-diff the result
against the checked-in file, so Python cannot silently drift out from under the
golden.

READ-ONLY. The builder is constructed WITHOUT running ``__init__`` so that
``sink.ensure_schema()`` -- the one write-capable call on the path -- never
fires, and the recording sink refuses every write except the one it captures.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, cast

sys.path.insert(0, "/app/src")

from dev_health_ops.work_graph.builder import (  # noqa: E402
    BuildConfig,
    WorkGraphBuilder,
)

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"

# Frozen so the generator is deterministic. It is only reachable as the
# fallback for a dependency row with an unusable last_synced, which the live
# table does not contain -- if it ever appears in the output, the golden makes
# it visible rather than stamping an ever-changing "now".
FROZEN_NOW = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)


class RecordingSink:
    """Wraps the real sink: records every read, captures the single write."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.reads: list[list[dict[str, Any]]] = []
        self.written: list[Any] = []

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows = self._inner.query_dicts(query, params)
        # SNAPSHOT, not a reference (CHAOS-4803). The producer is handed the
        # real rows -- handing it a copy would make the generator exercise a
        # different object graph than production does -- but what is CAPTURED
        # must be what it was handed, frozen at that moment. Recording the same
        # objects lets an in-place mutation corrupt the captured "input" and the
        # derived output CONSISTENTLY, so a regenerated golden would encode the
        # mutated state as if it had been the input all along, and the oracle
        # would accept the very regression it exists to catch.
        #
        # deepcopy, not list(rows) or dict(row): the rows are dict[str, Any]
        # whose values can themselves be mutable, so a shallow copy still shares
        # the row dicts (or their nested values) and reproduces the hole one
        # level down. Cost is irrelevant -- generation is a rare offline run.
        self.reads.append(copy.deepcopy(rows))
        return rows

    def write_work_graph_issue_pr(self, records: Any) -> None:
        # Snapshotted for the same reason. These records are minted fresh by
        # _issue_pr_to_record and not touched again today, so this is defence
        # against a future change rather than a live defect.
        self.written.extend(copy.deepcopy(list(records)))

    def __getattr__(self, name: str) -> Any:
        # Anything the derivation might reach that is NOT one of the two methods
        # above is a write path this generator must never trigger.
        #
        # AttributeError, not AssertionError: __getattr__ is part of Python's
        # attribute protocol, and `getattr(obj, name, default)` only falls back
        # to its default when AttributeError is raised. The deployed builder
        # relies on exactly that -- `getattr(getattr(self.sink, "client", None),
        # "command", None)` at builder.py:805 and :933 -- so a non-standard
        # exception here would propagate out of a probe written to tolerate a
        # missing attribute, and crash the generator with a misleading message
        # instead of returning None. Direct access (`sink.write_edges(...)`)
        # still fails loudly, which is what this guard is for.
        raise AttributeError(
            f"generator refuses sink.{name}: the golden run must stay read-only"
        )


class StringTable:
    """Interns repeated ids so the fixture stays small enough to check in.

    The dependency/work-item/link rows are dominated by long, highly repetitive
    ids (``ghpr:owner/repo#1234``, ``linear:CHAOS-1234``). Interning them is a
    lossless encoding, not a projection: every value is still present, exactly
    once, and the Go decoder reconstructs the rows verbatim.
    """

    def __init__(self) -> None:
        self._values: list[str] = []
        self._index: dict[str, int] = {}

    def intern(self, value: str) -> int:
        found = self._index.get(value)
        if found is None:
            found = len(self._values)
            self._values.append(value)
            self._index[value] = found
        return found

    def values(self) -> list[str]:
        return self._values


def _iso(value: Any) -> str:
    """Render a timestamp as a UTC RFC3339 instant.

    Timestamps are compared as INSTANTS on the Go side, never as strings
    (the house golden rule, repouser/golden_full_test.go:93-97); rendering
    them canonically here just keeps the file stable across runs.
    """
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def build_golden() -> dict[str, Any]:
    # Imported HERE, not at module scope: --replay must run with nothing but the
    # builder importable. It is the mode the CI rot guard uses, and requiring a
    # ClickHouse client for a run that never opens a connection would make the
    # guard fail for a reason unrelated to what it measures.
    from dev_health_ops.metrics.sinks.clickhouse import (  # noqa: PLC0415
        ClickHouseMetricsSink,
    )

    config = BuildConfig(dsn=os.environ["CLICKHOUSE_URI"], org_id=ORG_ID)
    inner = ClickHouseMetricsSink(config.dsn)
    sink = RecordingSink(inner)

    # Deliberately NOT WorkGraphBuilder(config): __init__ calls
    # sink.ensure_schema(), which is write-capable. Construct the object and
    # set only the three attributes the derivation reads.
    builder = object.__new__(WorkGraphBuilder)
    builder.config = config
    # cast: the recording/replay sinks deliberately implement only the two
    # methods this derivation touches, so they are not BaseMetricsSink -- that
    # narrowness IS the safety property (every other attribute raises).
    builder.sink = cast(Any, sink)
    builder._now = FROZEN_NOW

    count = builder._derive_issue_pr_links_from_dependencies()
    print(f"python derived {count} links", file=sys.stderr)

    if len(sink.reads) != 4:
        raise AssertionError(
            f"expected 4 recorded reads (dependencies, repos, prs, work_items), got {len(sink.reads)}"
        )
    dependency_rows, repo_rows, pr_rows, work_item_rows = sink.reads

    table = StringTable()

    dependencies = [
        [
            table.intern(str(row.get("org_id") or "")),
            table.intern(str(row.get("source_work_item_id") or "")),
            table.intern(str(row.get("target_work_item_id") or "")),
            table.intern(str(row.get("relationship_type_raw") or "")),
            table.intern(_iso(row.get("last_synced"))),
        ]
        for row in dependency_rows
    ]

    repos = [
        [
            table.intern(str(row.get("org_id") or "")),
            table.intern(str(row.get("id") or "")),
            table.intern(str(row.get("repo") or "")),
        ]
        for row in repo_rows
    ]

    # PROJECTION (the 4441-plan option (a), applied to the two pure lookup
    # sets). `work_items` and `git_pull_requests` are consulted only by key;
    # an entry no dependency row ever looks up cannot change any outcome, so
    # dropping it is a strict projection onto the function's real inputs, not
    # a weakening. A key that is consulted and ABSENT stays absent, so every
    # rejection the full sets would produce is still produced here.
    consulted_work_items: set[tuple[str, str]] = set()
    consulted_prs: set[tuple[str, str, int]] = set()
    repo_by_slug = {
        (str(row.get("org_id") or ORG_ID), str(row.get("repo") or "")): str(
            row.get("id") or ""
        )
        for row in repo_rows
    }
    for row in dependency_rows:
        org = str(row.get("org_id") or ORG_ID)
        target = str(row.get("target_work_item_id") or "")
        if target:
            consulted_work_items.add((org, target))
        parsed = WorkGraphBuilder._parse_pr_dependency_source(
            row.get("source_work_item_id")
        )
        if not parsed:
            continue
        repo_slug, pr_number, _provider = parsed
        repo_id = repo_by_slug.get((org, repo_slug))
        if repo_id:
            consulted_prs.add((org, repo_id, pr_number))

    work_items = [
        [
            table.intern(str(row.get("org_id") or "")),
            table.intern(str(row.get("work_item_id") or "")),
        ]
        for row in work_item_rows
        if (str(row.get("org_id") or ORG_ID), str(row.get("work_item_id") or ""))
        in consulted_work_items
    ]
    pull_requests = [
        [
            table.intern(str(row.get("org_id") or "")),
            table.intern(str(row.get("repo_id") or "")),
            int(row.get("number") or 0),
        ]
        for row in pr_rows
        if (
            str(row.get("org_id") or ORG_ID),
            str(row.get("repo_id") or ""),
            int(row.get("number") or 0),
        )
        in consulted_prs
    ]

    links = [
        [
            table.intern(str(record.repo_id)),
            table.intern(str(record.work_item_id)),
            int(record.pr_number),
            float(record.confidence),
            table.intern(str(record.provenance)),
            table.intern(str(record.evidence)),
            table.intern(_iso(record.last_synced)),
            table.intern(str(record.org_id or "")),
        ]
        for record in sink.written
    ]

    return {
        "schema": "issue_pr_links_python_golden.v1",
        "org_id": ORG_ID,
        "generated_by": "tests/fixtures/generate_issue_pr_links_python_golden.py",
        "producer": "work_graph/builder.py::_derive_issue_pr_links_from_dependencies",
        "frozen_now": _iso(FROZEN_NOW),
        "row_schemas": {
            "strings": "shared intern table; every int field below indexes it",
            "dependencies": "[org_id*, source_work_item_id*, target_work_item_id*, relationship_type_raw*, last_synced*]",
            "repos": "[org_id*, id*, repo*]",
            "pull_requests": "[org_id*, repo_id*, number]  (projected to consulted keys)",
            "work_items": "[org_id*, work_item_id*]  (projected to consulted keys)",
            "links": "[repo_id*, work_item_id*, pr_number, confidence, provenance*, evidence*, last_synced*, org_id*]",
            "note": "* = index into strings",
        },
        "counts": {
            "dependencies": len(dependencies),
            "repos": len(repos),
            "pull_requests_projected": len(pull_requests),
            "pull_requests_total": len(pr_rows),
            "work_items_projected": len(work_items),
            "work_items_total": len(work_item_rows),
            "links": len(links),
        },
        "strings": table.values(),
        "dependencies": dependencies,
        "repos": repos,
        "pull_requests": pull_requests,
        "work_items": work_items,
        "links": links,
    }


class ReplaySink:
    """Serves the FROZEN inputs to the deployed producer; records the write.

    This is what makes the rot guard hermetic. A guard that re-queried
    ClickHouse would compare two runs taken across a time gap -- the tables
    move continuously (RMT inserts, syncs), so it would fail on data drift and
    say nothing about Python drift. Replaying the frozen inputs isolates the
    only thing the guard is for: whether the deployed
    ``_derive_issue_pr_links_from_dependencies`` still turns THESE rows into
    THOSE links.
    """

    def __init__(self, reads: list[list[dict[str, Any]]]) -> None:
        self._reads = list(reads)
        self.written: list[Any] = []

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if not self._reads:
            raise AssertionError("producer issued more reads than the golden froze")
        return self._reads.pop(0)

    def write_work_graph_issue_pr(self, records: Any) -> None:
        # Snapshotted for the same reason as RecordingSink (CHAOS-4803).
        self.written.extend(copy.deepcopy(list(records)))

    def __getattr__(self, name: str) -> Any:
        # AttributeError for the same protocol reason as RecordingSink above.
        raise AttributeError(
            f"replay refuses sink.{name}: the guard must stay read-only"
        )


def replay_golden(golden: dict[str, Any]) -> list[list[Any]]:
    """Re-derive the links by feeding the golden's frozen inputs to live Python."""
    strings = golden["strings"]

    def text(index: int) -> str:
        return strings[index]

    dependency_rows = [
        {
            "org_id": text(row[0]),
            "source_work_item_id": text(row[1]),
            "target_work_item_id": text(row[2]),
            "relationship_type_raw": text(row[3]),
            "last_synced": datetime.fromisoformat(text(row[4]).replace("Z", "+00:00")),
        }
        for row in golden["dependencies"]
    ]
    repo_rows = [
        {"org_id": text(row[0]), "id": text(row[1]), "repo": text(row[2])}
        for row in golden["repos"]
    ]
    pr_rows = [
        {
            "org_id": text(row[0]),
            "repo_id": text(row[1]),
            "number": row[2],
            "created_at": None,
        }
        for row in golden["pull_requests"]
    ]
    work_item_rows = [
        {"org_id": text(row[0]), "work_item_id": text(row[1])}
        for row in golden["work_items"]
    ]

    config = BuildConfig(dsn="replay://unused", org_id=golden["org_id"])
    sink = ReplaySink([dependency_rows, repo_rows, pr_rows, work_item_rows])
    builder = object.__new__(WorkGraphBuilder)
    builder.config = config
    # cast: the recording/replay sinks deliberately implement only the two
    # methods this derivation touches, so they are not BaseMetricsSink -- that
    # narrowness IS the safety property (every other attribute raises).
    builder.sink = cast(Any, sink)
    builder._now = datetime.fromisoformat(golden["frozen_now"].replace("Z", "+00:00"))
    builder._derive_issue_pr_links_from_dependencies()

    table = StringTable()
    for value in strings:
        table.intern(value)
    return [
        [
            table.intern(str(record.repo_id)),
            table.intern(str(record.work_item_id)),
            int(record.pr_number),
            float(record.confidence),
            table.intern(str(record.provenance)),
            table.intern(str(record.evidence)),
            table.intern(_iso(record.last_synced)),
            table.intern(str(record.org_id or "")),
        ]
        for record in sink.written
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stdout", action="store_true", help="write the golden to stdout"
    )
    parser.add_argument("--out", default="/tmp/issue_pr_links_python_golden.json")
    parser.add_argument(
        "--replay",
        metavar="GOLDEN_JSON",
        help="re-derive links from the golden's frozen inputs and print them as JSON "
        "(the rot guard's mode -- no ClickHouse, no data drift)",
    )
    args = parser.parse_args()

    if args.replay:
        with open(args.replay, encoding="utf-8") as handle:
            frozen = json.load(handle)
        sys.stdout.write(
            json.dumps(replay_golden(frozen), separators=(",", ":"), sort_keys=True)
            + "\n"
        )
        return 0

    golden = build_golden()
    # Compact separators: the row arrays are machine-read (row_schemas documents
    # the positions), and an indented encoding costs ~2x for no reader benefit.
    # Keeping the fixture small is what lets it be checked in at all.
    payload = json.dumps(golden, separators=(",", ":"), sort_keys=True) + "\n"
    if args.stdout:
        sys.stdout.write(payload)
    else:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(f"written {args.out} ({len(payload)} bytes)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
