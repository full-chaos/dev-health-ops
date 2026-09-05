"""Generate the CHAOS-4924 operational-edges golden from the DEPLOYED Python producer.

Freezes both slices this port's PR A covers:

  * ``operational_edges.py::build_operational_incident_edges`` -- a pure
    function, called directly (no write path exists to intercept).
  * ``work_graph/builder.py::WorkGraphBuilder._build_flag_guards_edges`` -- an
    instance method with two writes of its own (work_graph_edges via
    ``self._write_edges``, and feature_flag_link via
    ``self.sink.write_feature_flag_links``). Called for REAL (not
    reimplemented) via a capturing sink so the golden reflects the actual
    deployed code path, not a hand-transcribed copy of it.

REGENERATION IS NOT BYTE-STABLE ON discovered_at/last_synced, AND THAT IS NOT
DRIFT. operational_edges.py's `_edge()` helper never passes discovered_at or
last_synced to WorkGraphEdge -- both fall back to the dataclass's
`field(default_factory=lambda: datetime.now(timezone.utc))`, evaluated FRESH
PER INSTANCE at construction, not from this generator's FROZEN_NOW. So every
edge gets its own true wall-clock microsecond, different on every run and
even between edges within one run -- unlike `_build_issue_issue_edges`
(edges/edges.go's own doc comment), which explicitly stamps both from the
builder's construction clock for the whole batch. This is a genuine port
divergence CHAOS-4924's Go code must decide on explicitly (batch-clock like
the sibling producer, since true per-edge wall-clock cannot be golden-tested
byte-exact and is not a meaningful distinction Python intended -- it reads as
an oversight-shaped artifact of the dataclass default rather than a designed
per-edge property) -- NOT something to silently reproduce or silently ignore.
event_ts/day, by contrast, ARE explicitly passed and ARE stable modulo the
underlying data.

READ-ONLY, same two-layer control as
generate_workgraph_issue_edges_python_golden.py: the underlying ClickHouse
connection is opened with ``readonly=1`` (server-enforced, Code 164 on any
mutation regardless of which object issues it), and the capturing sink below
intercepts the two write methods and records their arguments instead of
calling through. The builder is constructed via ``object.__new__`` rather than
``WorkGraphBuilder(config)`` so ``__init__``'s ``sink.ensure_schema()`` (a
CREATE-TABLE-IF-NOT-EXISTS DDL call) never fires either.

KNOWN COVERAGE GAP on org 70d529e0 as of 2026-09-05 (recorded here rather than
left implicit): operational_service_repository_mappings, operational_alerts,
operational_incident_responders and operational_incident_notes are all EMPTY
for this org, and none of its 7 registered feature-flag keys appear verbatim
in any of its 4928 work items' title/description. So this golden exercises
has_incident/escalates_with/has_timeline_event with provider=pagerduty,
repo_id=null -- and does NOT exercise maps_to_repository/repo_id-non-null,
has_alert/has_responder, the deployment-window linked_incident heuristic, the
jira-key/github-PR-URL text-evidence paths, or ANY flag_guards_edges row. A
green Go test against this golden alone is not evidence for those paths (same
principle as CHAOS-5138's "a green run over data that can't exercise the
failure class is not evidence") -- CHAOS-4924's PR A needs a second,
synthetic/hand-authored fixture for them, the same way CHAOS-4771 kept its
frozen pre-fix fixture AND added synthetic cases rather than relying on one
org's live shape.

Run inside the api container, which owns the deployed interpreter:

    docker compose --env-file env -f compose.yml -f compose/compose.go.workers.yml \
      -f compose/compose.metrics-api.local.yml exec -T api \
      python tests/fixtures/generate_workgraph_operational_edges_python_golden.py --stdout
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, cast

sys.path.insert(0, "/app/src")

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink  # noqa: E402
from dev_health_ops.metrics.sinks.clickhouse.connection import (  # noqa: E402
    clickhouse_client_kwargs,
)
from dev_health_ops.work_graph.builder import (  # noqa: E402
    BuildConfig,
    WorkGraphBuilder,
)
from dev_health_ops.work_graph.models import WorkGraphEdge  # noqa: E402
from dev_health_ops.work_graph.operational_edges import (  # noqa: E402
    build_operational_incident_edges,
)

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"

# Frozen so the generator is deterministic -- both producers stamp
# discovered_at/last_synced/event_ts from the caller's clock for several edge
# classes (e.g. has_incident falls back to `now` when started_at is absent).
FROZEN_NOW = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)

HEURISTIC_DAYS_WINDOW = 7
HEURISTIC_CONFIDENCE = 0.3


def _read_only_client(dsn: str) -> Any:
    """Same control as generate_workgraph_issue_edges_python_golden.py's
    _read_only_sink: readonly=1 is enforced by the SERVER, not by which
    methods this script happens to call."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        **clickhouse_client_kwargs(
            dsn, settings={"max_query_size": 1 * 1024 * 1024, "readonly": 1}
        )
    )


class CapturingSink:
    """Wraps a read-only-enforced ClickHouseMetricsSink: query_dicts passes
    through (the server already refuses a mutation issued through it), while
    the two writes _build_flag_guards_edges performs are intercepted and
    recorded instead of called through -- belt-and-braces alongside the
    server-side readonly control, matching this repo's own stated reasoning
    that hiding a sink is a promise, not a control, in Python."""

    def __init__(self, inner: ClickHouseMetricsSink) -> None:
        self._inner = inner
        self.edge_records: list[Any] = []
        self.flag_link_records: list[Any] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return self._inner.query_dicts(query, parameters)

    def write_work_graph_edges(self, rows: Any) -> None:
        self.edge_records.extend(rows)

    def write_feature_flag_links(self, rows: Any) -> None:
        self.flag_link_records.extend(rows)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _edge_to_dict(edge: WorkGraphEdge) -> dict[str, Any]:
    d = asdict(edge)
    d["source_type"] = edge.source_type.value
    d["target_type"] = edge.target_type.value
    d["edge_type"] = edge.edge_type.value
    d["provenance"] = edge.provenance.value
    d["repo_id"] = str(edge.repo_id) if edge.repo_id else None
    d["discovered_at"] = _iso(edge.discovered_at)
    d["last_synced"] = _iso(edge.last_synced)
    d["event_ts"] = _iso(edge.event_ts)
    d["day"] = edge.day.isoformat() if edge.day else None
    return d


def _flag_link_record_to_dict(record: Any) -> dict[str, Any]:
    d = asdict(record)
    d["valid_from"] = _iso(record.valid_from)
    d["valid_to"] = _iso(record.valid_to)
    d["last_synced"] = _iso(record.last_synced)
    return d


def build_golden() -> dict[str, Any]:
    dsn = os.environ["CLICKHOUSE_URI"]
    client = _read_only_client(dsn)
    read_only_sink = ClickHouseMetricsSink(dsn, client=client)
    capturing = CapturingSink(read_only_sink)

    # --- slice 1: build_operational_incident_edges -- pure function, no write
    # path to intercept, called directly against the read-only sink.
    incident_edges = build_operational_incident_edges(
        read_only_sink,
        ORG_ID,
        FROZEN_NOW,
        HEURISTIC_DAYS_WINDOW,
        HEURISTIC_CONFIDENCE,
        None,
        None,
        None,
    )

    # --- slice 2: WorkGraphBuilder._build_flag_guards_edges -- the REAL
    # deployed method, not a reimplementation. Deliberately NOT
    # WorkGraphBuilder(config): __init__ calls sink.ensure_schema(), which is
    # write-capable. Construct the object and set only the attributes this
    # producer reads.
    config = BuildConfig(dsn=dsn, org_id=ORG_ID)
    builder = object.__new__(WorkGraphBuilder)
    builder.config = config
    builder.sink = cast(Any, capturing)
    builder._now = FROZEN_NOW

    flag_edge_count = builder._build_flag_guards_edges()
    print(
        f"python created {len(incident_edges)} incident edges, "
        f"{flag_edge_count} flag-guards edges "
        f"({len(capturing.flag_link_records)} feature_flag_link rows)",
        file=sys.stderr,
    )

    return {
        "generated_by": "tests/fixtures/generate_workgraph_operational_edges_python_golden.py",
        "producer": [
            "operational_edges.py::build_operational_incident_edges",
            "work_graph/builder.py::WorkGraphBuilder._build_flag_guards_edges",
        ],
        "org_id": ORG_ID,
        "frozen_now": FROZEN_NOW.isoformat(),
        "config": {
            "org_id": ORG_ID,
            "heuristic_days_window": HEURISTIC_DAYS_WINDOW,
            "heuristic_confidence": HEURISTIC_CONFIDENCE,
            "repo_id": None,
            "from_date": None,
            "to_date": None,
        },
        "counts": {
            "operational_incident_edges": len(incident_edges),
            "flag_guards_edges": flag_edge_count,
            "flag_guards_edges_captured": len(capturing.edge_records),
            "feature_flag_link_rows": len(capturing.flag_link_records),
        },
        "known_nondeterminism": [
            "operational_incident_edges[*].discovered_at",
            "operational_incident_edges[*].last_synced",
        ],
        "known_coverage_gap": (
            "org 70d529e0 has zero operational_service_repository_mappings, "
            "operational_alerts, operational_incident_responders and "
            "operational_incident_notes rows, and none of its 7 flag keys "
            "appear in any work_item text -- maps_to_repository/repo_id-"
            "non-null, has_alert, has_responder, linked_incident and every "
            "flag_guards_edges/feature_flag_link row are UNEXERCISED by this "
            "fixture. A synthetic fixture is needed for those paths."
        ),
        "operational_incident_edges": [_edge_to_dict(e) for e in incident_edges],
        "flag_guards_edges": [_edge_to_dict(e) for e in capturing.edge_records],
        "feature_flag_link_rows": [
            _flag_link_record_to_dict(r) for r in capturing.flag_link_records
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stdout", action="store_true", help="print the golden JSON to stdout"
    )
    args = parser.parse_args()
    golden = build_golden()
    if args.stdout:
        print(json.dumps(golden, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
