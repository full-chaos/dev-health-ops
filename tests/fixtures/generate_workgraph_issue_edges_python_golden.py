"""Generate the CHAOS-4766 issue<->issue edge golden from the DEPLOYED Python producer.

The golden freezes both halves of ``work_graph/builder.py::_build_issue_issue_edges``:

  * every ClickHouse read it performs, captured by RECORDING them as the deployed
    builder issues them -- never re-queried by this script, so the file cannot
    drift from what Python actually saw; and
  * everything it produces: the ``work_graph_edges`` rows, the
    ``work_graph_projection_runs`` watermark row, and the two ``ALTER TABLE ...
    DELETE`` mutations its cleanup step issues.

The mutations are part of the contract, not an implementation detail: a port that
rewrites edges without deleting the historical orientations leaves both the old
and the new row alive under different edge_ids. So they are recorded (SQL +
parameters) and asserted, never executed -- see ``RecordingClient``.

The Go port (``internal/jobs/workgraph/edges``) replays the frozen inputs and must
reproduce all four outputs, with ONE enumerated exception: the CHAOS-4752/4758
variant-C confidence policy ranks the associative dependency family strictly below
the delivery tier, which Python does not do (chris withdrew that Python exception
on 2026-09-01). The Go test names that delta explicitly and fails on any other
divergence -- see ``edges/golden_full_test.go``.

Run it inside the api container, which owns the deployed interpreter:

    docker cp tests/fixtures/generate_workgraph_issue_edges_python_golden.py dev-health-api-1:/tmp/
    docker exec -i dev-health-api-1 python /tmp/generate_workgraph_issue_edges_python_golden.py --stdout

``--replay <golden>`` is what the Go rot-guard test
(``edges/golden_rot_guard_test.go``, gated by DEV_HEALTH_LIVE_PYTHON_ORACLES=1)
uses: it feeds the golden's OWN frozen reads back through the deployed producer
and re-emits the outputs for a byte diff. Replaying rather than re-querying is
what makes the guard hermetic -- a guard that re-queried ClickHouse would compare
two runs across a time gap, and these tables move continuously (RMT inserts,
syncs), so it would fail on data drift while saying nothing about Python drift.
``--stdout`` re-queries and is for REGENERATING the file, not for guarding it.

REGENERATION IS NOT BYTE-STABLE, AND THAT IS NOT DRIFT (CHAOS-4788). The producer
reads ``work_item_dependencies`` with no ``FINAL``, no ``argMax`` and no ``ORDER
BY``, while the table is a ReplacingMergeTree carrying unmerged duplicate
versions (6,531 raw rows for 6,365 distinct keys today). Its dict dedup is
last-write-wins, so which version's ``last_synced`` becomes an edge's
``event_ts`` is decided by ClickHouse merge state. Two captures 40 minutes apart
produced an IDENTICAL edge_id set but differed in ``event_ts`` on 155 edges and
``day`` on 27. The structure is stable; the timestamps are not. This is why the
rot guard REPLAYS frozen rows instead of regenerating -- and why a regeneration
that differs only in ``event_ts``/``day``/``input_watermark`` is expected.

READ-ONLY, enforced by the SERVER. The connection is opened with ``readonly=1``,
so ClickHouse refuses any mutation with ``Code: 164 (READONLY)`` regardless of
which object issued it. That is the control; the rest is defence in depth: the
builder is constructed WITHOUT running ``__init__`` so ``sink.ensure_schema()``
never fires, ``_refuse_non_read`` rejects a non-read before it is sent, the
recording sink refuses every method except the reads and writes it captures, and
the recording client records ``command()`` without executing it.

Note what is NOT claimed: the object graph is not sealed. A bound method exposes
its receiver via ``__self__``, a closure via ``__closure__``; hiding the sink in
Python is a promise rather than a control, which is why the enforcement lives at
the connection instead.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, cast

sys.path.insert(0, "/app/src")

import clickhouse_connect  # noqa: E402

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink  # noqa: E402
from dev_health_ops.metrics.sinks.clickhouse.connection import (  # noqa: E402
    clickhouse_client_kwargs,
)
from dev_health_ops.work_graph.builder import (  # noqa: E402
    BuildConfig,
    WorkGraphBuilder,
    _format_datetime_for_clickhouse,
)

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"

# Frozen so the generator is deterministic. `_build_issue_issue_edges` stamps
# `discovered_at`/`last_synced` from the builder's construction clock (NOT from
# the row), and falls back to it for a dependency row whose `last_synced` will
# not parse. Freezing it makes all three visible in the golden instead of
# stamping an ever-changing "now".
FROZEN_NOW = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)


class RecordingClient:
    """Records ``command()`` mutations WITHOUT executing them.

    `_delete_dependency_edge_candidates` issues two `ALTER TABLE ... DELETE`
    statements. They are part of the behaviour the Go port must reproduce, so
    they belong in the golden -- but this generator runs against the SHARED
    stack, and a golden generator that mutates the data it is capturing would
    both corrupt the stack and make itself non-idempotent. Recording is
    strictly more useful than executing here: the assertion the port needs is
    "did it issue these statements", not "did the rows disappear".
    """

    def __init__(self) -> None:
        self.commands: list[dict[str, Any]] = []

    def command(self, statement: str, parameters: Any = None) -> None:
        self.commands.append(
            {"statement": " ".join(statement.split()), "parameters": parameters or {}}
        )


class RecordingSink:
    """Wraps the real sink: records every read, captures every write."""

    def __init__(self, read: Any) -> None:
        # A bound `query_dicts`, NOT the sink object.
        #
        # `__getattr__` only fires for attributes that do not exist, so holding
        # the real sink as `self._inner` left it reachable: a producer path
        # `self.sink._inner.ensure_schema()` would have reached the real
        # ClickHouseMetricsSink, which runs migrations and `client.command`
        # writes. Keeping only the one bound method means there is no writable
        # object on this class to reach -- the barrier is structural rather than
        # a set of names we remembered to refuse.
        self._read = read
        self.client = RecordingClient()
        self.reads: list[list[dict[str, Any]]] = []
        # The QUERY TEXT is recorded, not just the rows. Rows alone cannot show
        # which input dimensions a producer actually consults, and a dimension
        # nobody froze is a dimension the oracle silently ignores -- exactly the
        # blind spot lane-pathb-go's review found (its generator passed only DSN
        # and org, so the build WINDOW sat outside an oracle that looked
        # exhaustive). Freezing the text makes the absence of a date bound an
        # asserted structural fact rather than a reading of the source, and a
        # future Python change that adds one turns the rot guard red.
        self.queries: list[dict[str, Any]] = []
        self.edges: list[Any] = []
        self.projection_runs: list[Any] = []

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        # The read barrier is HERE, not only in __getattr__. `query_dicts` is an
        # allowed method that forwards arbitrary SQL to the real ClickHouse
        # client, so refusing every OTHER attribute does not make this generator
        # read-only -- a producer change that routed a mutation through this
        # method would execute it against the shared stack. Gate the statement.
        _refuse_non_read(query)
        rows = self._read(query, params)
        # SNAPSHOT at capture, then hand the ORIGINALS to the producer.
        #
        # Recording the same objects the producer holds does not freeze anything:
        # the golden is serialised after the producer returns, so a producer that
        # mutates a row in place -- `row["relationship_type_raw"] = ...` -- rewrites
        # the captured "input" too. Both halves then agree, and an oracle that
        # derives its expectation from that input derives it FROM the corruption
        # and accepts it. Adversarial review round 5 constructed exactly that and
        # got `accepted=3548/3548`.
        #
        # The copy is the recording; the producer keeps the originals so its
        # behaviour is unchanged and anything it does to them is visible as a
        # divergence rather than absorbed as the new reference answer.
        self.reads.append(copy.deepcopy(rows))
        self.queries.append(_normalize_query(query, params))
        return rows

    def write_work_graph_edges(self, records: Any) -> None:
        self.edges.extend(records)

    def write_work_graph_projection_runs(self, records: Any) -> None:
        self.projection_runs.extend(records)

    def __getattr__(self, name: str) -> Any:
        # Anything the producer might reach that is NOT one of the methods above
        # is a path this generator must never trigger.
        raise AssertionError(
            f"generator refuses sink.{name}: the golden run must stay read-only"
        )


class StringTable:
    """Interns repeated ids so the fixture stays small enough to check in.

    Lossless, not a projection: every value is still present, exactly once, and
    the Go decoder reconstructs the rows verbatim.
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


_READ_PREFIXES = ("select", "with")


def _refuse_non_read(statement: str) -> None:
    """Refuse anything that is not a read before it reaches ClickHouse."""
    head = statement.strip().lstrip("(").lstrip().lower()
    if not head.startswith(_READ_PREFIXES):
        raise AssertionError(
            "generator refuses a non-read statement through query_dicts; the "
            f"golden run must stay read-only: {' '.join(statement.split())[:200]}"
        )


def _normalize_query(statement: str, params: Any) -> dict[str, Any]:
    """Canonical form of one read, for freezing and for replay comparison."""
    return {"statement": " ".join(statement.split()), "parameters": params or {}}


def _iso(value: Any) -> str:
    """Render a timestamp as a UTC RFC3339 instant.

    Timestamps are compared as INSTANTS on the Go side, never as strings; this
    just keeps the file stable across runs. Note `work_item_dependencies.last_synced`
    is `DateTime64(3)` with NO timezone while `work_graph_edges.last_synced` is
    `DateTime64(3, 'UTC')` -- the naive->UTC coercion below is the same one the
    producer performs, and the Go decoder must perform it too.
    """
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _finite(value: Any) -> float:
    """Confidence, rejected loudly if it is not a finite float.

    `work_graph_edges.confidence` is a plain Float32 with no validating writer,
    so NaN/Inf are storable -- and lane-4441 (CHAOS-4441, commit ca0b40349)
    proved a NaN changes which edges survive the component split. The golden
    must not silently carry one into the Go fixture.
    """
    number = float(value)
    if not math.isfinite(number):
        raise AssertionError(f"non-finite confidence in producer output: {value!r}")
    return number


# A window with SUB-SECOND components, deliberately.
#
# `_format_datetime_for_clickhouse` (builder.py:57-60) renders bounds with
# strftime("%Y-%m-%d %H:%M:%S"), so milliseconds are DROPPED while the columns
# they are compared against are DateTime64(3). A Go writer that bound the raw
# instant would move every window boundary by up to a second in both directions.
# Freezing a window whose sub-second part is non-zero makes that truncation
# visible in the fixture instead of invisible.
FROM_TS = datetime(2026, 8, 18, 12, 34, 56, 789000, tzinfo=timezone.utc)
TO_TS = datetime(2026, 9, 1, 1, 2, 3, 456000, tzinfo=timezone.utc)


def _read_only_sink(dsn: str) -> Any:
    """A sink whose SERVER refuses to write, whatever the object graph allows.

    Three rounds of adversarial review kept finding the same class here, and the
    third one is the reason this exists: hiding the real sink is not achievable in
    Python. Removing ``_inner`` still left ``self._read.__self__`` pointing at the
    live ``ClickHouseMetricsSink``, and closing that would leave ``__closure__``,
    then ``gc.get_referrers``. An object-graph barrier in this language is a
    promise, not a control.

    ``readonly=1`` moves the control to the server, where it is enforceable and
    cannot be walked around: ClickHouse answers a mutation with
    ``Code: 164 ... Cannot execute query in readonly mode`` no matter which object
    issued it, while ordinary reads are unaffected (both verified against the
    running instance). The SQL gate in ``_refuse_non_read`` stays as the near-side
    check that fails fast with a legible message; this is the far-side one that
    actually holds.
    """
    client = clickhouse_connect.get_client(
        **clickhouse_client_kwargs(
            dsn, settings={"max_query_size": 1 * 1024 * 1024, "readonly": 1}
        )
    )
    return ClickHouseMetricsSink(dsn, client=client)


def build_golden() -> dict[str, Any]:
    config = BuildConfig(
        dsn=os.environ["CLICKHOUSE_URI"],
        org_id=ORG_ID,
        from_date=FROM_TS,
        to_date=TO_TS,
    )
    sink = RecordingSink(_read_only_sink(config.dsn).query_dicts)

    # Deliberately NOT WorkGraphBuilder(config): __init__ calls
    # sink.ensure_schema(), which is write-capable. Construct the object and set
    # only the three attributes this producer reads.
    builder = object.__new__(WorkGraphBuilder)
    builder.config = config
    # cast: the recording/replay sinks deliberately implement only the methods
    # this producer touches, so they are not BaseMetricsSink -- that narrowness
    # IS the safety property (every other attribute raises).
    builder.sink = cast(Any, sink)
    builder._now = FROZEN_NOW

    count = builder._build_issue_issue_edges()
    print(f"python created {count} issue<->issue edges", file=sys.stderr)

    if not sink.reads:
        raise AssertionError("producer issued no reads")
    dependency_rows = sink.reads[0]
    existing_edge_pages = sink.reads[1:]

    table = StringTable()

    dependencies = [
        [
            table.intern(str(row.get("source_work_item_id") or "")),
            table.intern(str(row.get("target_work_item_id") or "")),
            table.intern(str(row.get("relationship_type") or "")),
            table.intern(str(row.get("relationship_type_raw") or "")),
            table.intern(str(row.get("relationship_semantics_version") or "")),
            table.intern(_iso(row.get("last_synced"))),
        ]
        for row in dependency_rows
    ]

    # The cleanup step pages `work_graph_edges FINAL` on an `edge_id > cursor`
    # cursor. Page BOUNDARIES are part of the contract -- an unpaged port works
    # on this org and silently truncates on a large one -- so they are frozen as
    # a list of pages, not flattened.
    existing_edge_ids = [
        [table.intern(str(row.get("edge_id") or "")) for row in page]
        for page in existing_edge_pages
    ]

    edges = [
        [
            table.intern(str(record.edge_id)),
            table.intern(str(record.source_type)),
            table.intern(str(record.source_id)),
            table.intern(str(record.target_type)),
            table.intern(str(record.target_id)),
            table.intern(str(record.edge_type)),
            table.intern(str(record.provenance)),
            _finite(record.confidence),
            table.intern(str(record.evidence)),
            table.intern(_iso(record.discovered_at)),
            table.intern(_iso(record.last_synced)),
            table.intern(_iso(record.event_ts)),
            table.intern(str(record.day)),
            table.intern(str(record.org_id or "")),
        ]
        for record in sink.edges
    ]

    projection_runs = [
        [
            table.intern(str(record.org_id or "")),
            table.intern(str(record.projection_name)),
            table.intern(str(record.scope_repo_id or "")),
            table.intern(str(record.rule_version)),
            table.intern(_iso(record.input_watermark)),
            int(record.row_count),
            table.intern(_iso(record.completed_at)),
        ]
        for record in sink.projection_runs
    ]

    return {
        "schema": "workgraph_issue_edges_python_golden.v1",
        "org_id": ORG_ID,
        "generated_by": "tests/fixtures/generate_workgraph_issue_edges_python_golden.py",
        "producer": "work_graph/builder.py::_build_issue_issue_edges",
        "frozen_now": _iso(FROZEN_NOW),
        # The full producer input, not just the rows. `clickhouse_bounds` is what
        # _format_datetime_for_clickhouse renders from from_ts/to_ts -- both are
        # frozen so the second-truncation contract is testable directly from the
        # fixture rather than restated in a Go constant.
        "config": {
            "org_id": ORG_ID,
            "from_ts": _iso(FROM_TS),
            "to_ts": _iso(TO_TS),
            "repo_id": None,
            "heuristic_days_window": config.heuristic_days_window,
            "heuristic_confidence": config.heuristic_confidence,
            "clickhouse_bounds": {
                "from": _format_datetime_for_clickhouse(FROM_TS),
                "to": _format_datetime_for_clickhouse(TO_TS),
            },
        },
        "queries": sink.queries,
        "row_schemas": {
            "strings": "shared intern table; every int field below indexes it",
            "dependencies": (
                "[source_work_item_id*, target_work_item_id*, relationship_type*, "
                "relationship_type_raw*, relationship_semantics_version*, last_synced*]"
            ),
            "existing_edge_ids": "one list per cursor page, in page order; [edge_id*]",
            "edges": (
                "[edge_id*, source_type*, source_id*, target_type*, target_id*, "
                "edge_type*, provenance*, confidence, evidence*, discovered_at*, "
                "last_synced*, event_ts*, day*, org_id*]"
            ),
            "projection_runs": (
                "[org_id*, projection_name*, scope_repo_id*, rule_version*, "
                "input_watermark*, row_count, completed_at*]"
            ),
            "mutations": "recorded, never executed; {statement, parameters} in issue order",
            "queries": "every read the producer issued, {statement, parameters}, in issue order",
            "config": "the FULL producer input; clickhouse_bounds is the second-truncated rendering",
            "note": "* = index into strings",
        },
        "counts": {
            "dependencies": len(dependencies),
            "existing_edge_pages": len(existing_edge_ids),
            "existing_edge_ids": sum(len(page) for page in existing_edge_ids),
            "edges": len(edges),
            "projection_runs": len(projection_runs),
            "mutations": len(sink.client.commands),
            "queries": len(sink.queries),
        },
        "strings": table.values(),
        "dependencies": dependencies,
        "existing_edge_ids": existing_edge_ids,
        "edges": edges,
        "projection_runs": projection_runs,
        "mutations": sink.client.commands,
    }


class ReplaySink:
    """Serves the FROZEN inputs to the deployed producer; captures the outputs.

    This is what makes the rot guard hermetic. See the module docstring: the
    guard's only question is whether the deployed ``_build_issue_issue_edges``
    still turns THESE rows into THOSE edges, and re-querying ClickHouse would
    answer a different, unanswerable question.
    """

    def __init__(
        self,
        reads: list[list[dict[str, Any]]],
        queries: list[dict[str, Any]],
    ) -> None:
        self._reads = list(reads)
        self._queries = list(queries)
        self.client = RecordingClient()
        self.edges: list[Any] = []
        self.projection_runs: list[Any] = []

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if not self._reads:
            raise AssertionError("producer issued more reads than the golden froze")
        # Serving rows by ordinal while IGNORING the statement would make the rot
        # guard blind to the thing it exists to catch: if Python changed its query
        # -- dropped the org predicate, added a time bound, reordered the
        # projection -- the guard would hand the NEW query the OLD rows and go
        # green. The frozen query contract is checked here, so a producer whose
        # reads have changed shape fails loudly instead of being fed answers to a
        # question it no longer asks.
        expected = self._queries.pop(0)
        actual = _normalize_query(query, params)
        if actual != expected:
            raise AssertionError(
                "the producer's read contract changed; the golden froze\n"
                f"  statement: {expected['statement']}\n"
                f"  parameters: {expected['parameters']}\n"
                "but the deployed producer now issues\n"
                f"  statement: {actual['statement']}\n"
                f"  parameters: {actual['parameters']}\n"
                "Replaying frozen rows against a changed query would prove nothing. "
                "If the change is intended, regenerate the golden."
            )
        return self._reads.pop(0)

    def write_work_graph_edges(self, records: Any) -> None:
        self.edges.extend(records)

    def write_work_graph_projection_runs(self, records: Any) -> None:
        self.projection_runs.extend(records)

    def assert_fully_consumed(self) -> None:
        if self._reads or self._queries:
            remaining = [entry["statement"][:160] for entry in self._queries]
            raise AssertionError(
                f"the producer issued {len(self._reads)} fewer read(s) than the golden "
                "froze; it has stopped performing a read it used to perform, and "
                "replaying the rest proves nothing about the reads it dropped. "
                f"Unconsumed: {remaining}"
            )

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            f"replay refuses sink.{name}: the guard must stay read-only"
        )


def _parse_iso(value: str) -> datetime:
    """Rebuild an aware UTC datetime from the golden's rendered instant.

    The reads are replayed as ``datetime`` objects, not strings, because that is
    what ClickHouse handed the producer originally -- and the producer takes a
    DIFFERENT branch for each (``isinstance(event_ts, str)`` vs the tz-coercion
    below it). Replaying the wrong type would exercise the wrong branch and the
    guard would stop guarding the path production uses.
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def replay_golden(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        golden = json.load(handle)
    strings: list[str] = golden["strings"]

    dependency_rows = [
        {
            "source_work_item_id": strings[row[0]],
            "target_work_item_id": strings[row[1]],
            "relationship_type": strings[row[2]],
            "relationship_type_raw": strings[row[3]],
            "relationship_semantics_version": strings[row[4]],
            "last_synced": _parse_iso(strings[row[5]]),
        }
        for row in golden["dependencies"]
    ]
    existing_pages = [
        [{"edge_id": strings[index]} for index in page]
        for page in golden["existing_edge_ids"]
    ]

    frozen_config = golden["config"]
    config = BuildConfig(
        dsn="clickhouse://replay/none",
        org_id=golden["org_id"],
        from_date=_parse_iso(frozen_config["from_ts"]),
        to_date=_parse_iso(frozen_config["to_ts"]),
    )
    sink = ReplaySink([dependency_rows, *existing_pages], golden["queries"])
    builder = object.__new__(WorkGraphBuilder)
    builder.config = config
    builder.sink = cast(Any, sink)
    builder._now = _parse_iso(golden["frozen_now"])
    builder._build_issue_issue_edges()
    # Validating only the reads that HAPPEN leaves a producer free to drop a
    # trailing one: if its observable rows are unchanged the guard would go
    # green on a read it no longer performs. The frozen sequence is a contract in
    # both directions.
    sink.assert_fully_consumed()

    # Seed the intern table with the golden's own strings so an unchanged replay
    # reproduces byte-identical indices. A genuinely new value appends at the
    # end -- which is exactly what drift should look like.
    table = StringTable()
    for value in strings:
        table.intern(value)

    return {
        "edges": [
            [
                table.intern(str(record.edge_id)),
                table.intern(str(record.source_type)),
                table.intern(str(record.source_id)),
                table.intern(str(record.target_type)),
                table.intern(str(record.target_id)),
                table.intern(str(record.edge_type)),
                table.intern(str(record.provenance)),
                _finite(record.confidence),
                table.intern(str(record.evidence)),
                table.intern(_iso(record.discovered_at)),
                table.intern(_iso(record.last_synced)),
                table.intern(_iso(record.event_ts)),
                table.intern(str(record.day)),
                table.intern(str(record.org_id or "")),
            ]
            for record in sink.edges
        ],
        "projection_runs": [
            [
                table.intern(str(record.org_id or "")),
                table.intern(str(record.projection_name)),
                table.intern(str(record.scope_repo_id or "")),
                table.intern(str(record.rule_version)),
                table.intern(_iso(record.input_watermark)),
                int(record.row_count),
                table.intern(_iso(record.completed_at)),
            ]
            for record in sink.projection_runs
        ],
        "mutations": sink.client.commands,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="re-query ClickHouse and write the golden to stdout; for REGENERATING the file",
    )
    parser.add_argument(
        "--replay",
        metavar="GOLDEN",
        help=(
            "replay the golden's own frozen reads through the deployed producer and "
            "emit {edges, projection_runs, mutations} to stdout; used by the Go rot guard"
        ),
    )
    parser.add_argument(
        "--out",
        default="/tmp/workgraph_issue_edges_python_golden.json",
        help="path to write when neither --stdout nor --replay is given",
    )
    namespace = parser.parse_args()

    if namespace.replay:
        payload = replay_golden(namespace.replay)
        sys.stdout.write(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        )
        return 0

    golden = build_golden()
    rendered = json.dumps(golden, sort_keys=True, separators=(",", ":")) + "\n"
    if namespace.stdout:
        sys.stdout.write(rendered)
    else:
        with open(namespace.out, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        print(f"wrote {namespace.out} ({len(rendered)} bytes)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
