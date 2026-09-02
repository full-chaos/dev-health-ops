#!/usr/bin/env python3
"""Regenerate the work-graph component/work_unit_id golden (CHAOS-4441).

The Go port of ``work_graph/investment/components.py`` + ``work_unit_id``
(``internal/jobs/workgraph/units``) must group nodes and hash them
BIT-IDENTICALLY to Python.  ``work_unit_id`` is a SHA256 over a component's
node set, and two separate jobs address rows by that hash -- the LLM
materializer writes ``work_unit_investments`` and the no-LLM projection writes
``work_unit_membership`` (``work_graph/investment/backfill.py:113-127``: sharing
the grouping implementation is "REQUIRED for correctness ... if this path
grouped nodes even slightly differently ... the projected membership would
target non-existent unit ids").  A grouping divergence between the planes is
SILENT data corruption -- no crash, no error row -- so it needs a golden, not a
code review.

This generator is deliberately HERMETIC: it reads a frozen edge set from
``workgraph_components_split_edges.json`` rather than querying ClickHouse, so
the rot guard (``internal/jobs/workgraph/units/golden_rot_guard_test.go``) can
re-run it against the live interpreter on any machine and byte-compare the
result.  The frozen input was captured from the DEPLOYED Python builder over
real synced data in org 70d529e0-3c06-4597-8480-794fd02328b6 on 2026-09-01 (the
full untrimmed capture, including the fields ``build_components`` never reads,
is kept out-of-tree at
``.remember/lanes/lane-4752-go/component_oracle_70d529e0_2026-09-01.json``).

WHY THIS PARTICULAR EDGE SET, AND WHY IT IS FROZEN FOREVER
    It is the only input we have that reaches phase (b) of the oversized-
    component split (``components.py::_remove_hubs``): 1 oversized component,
    4 dropped edges, 242 dropped nodes.  ``_remove_hubs`` is the most dangerous
    function in the port -- it DELETES nodes outright and tie-breaks on
    ``min(node_id)`` among the max-degree nodes.  A later capture taken after
    the CHAOS-4758 edge-confidence policy change will NOT exercise it at all
    (that policy exists precisely to stop hub removal firing), so this file is
    permanent split-path coverage and must not be replaced by a fresher one.

Usage:
    python tests/fixtures/generate_workgraph_components_python_golden.py            # rewrite in place
    python tests/fixtures/generate_workgraph_components_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

from dev_health_ops.utils.normalization import work_unit_id
from dev_health_ops.work_graph.investment.components import (
    ComponentBuildStats,
    build_components,
)

FIXTURES = pathlib.Path(__file__).resolve().parent
EDGES_PATH = FIXTURES / "workgraph_components_split_edges.json"
GOLDEN_PATH = FIXTURES / "workgraph_components_split_python_golden.json"


def _load_edges(document: dict) -> list[dict]:
    """Decode the columnar frozen input into the dict rows build_components reads.

    The input is stored one array per edge with the field order declared in
    ``columns`` -- a lossless encoding chosen only to keep the fixture small
    (it saves ~450 KB of repeated JSON key names over 6,040 rows).  Row order
    is preserved exactly and must not be re-sorted here: ``_discover_components``
    walks edges in input order, so component order -- which partitioned
    materialization addresses by numeric index -- depends on it.
    """
    columns = document["columns"]
    return [dict(zip(columns, row, strict=True)) for row in document["edges"]]


def render() -> str:
    document = json.loads(EDGES_PATH.read_text())
    edges = _load_edges(document)
    max_component_nodes = document["max_component_nodes"]

    stats = ComponentBuildStats()
    components = build_components(
        edges, max_component_nodes=max_component_nodes, stats=stats
    )

    rendered = {
        # Component ORDER is part of the contract, not an artifact: partitioned
        # materialization dispatches numeric component_indexes and each chunk
        # worker re-derives the list, so index N must name the same component on
        # both planes.  The list below is therefore emitted in build_components'
        # own order and MUST NOT be sorted.
        "components": [
            {
                "work_unit_id": work_unit_id(list(dict.fromkeys(nodes))),
                "nodes": sorted([list(node) for node in dict.fromkeys(nodes)]),
                "edge_ids": sorted(
                    {str(edge.get("edge_id") or "") for edge in component_edges} - {""}
                ),
            }
            for nodes, component_edges in components
        ],
        "max_component_nodes": max_component_nodes,
        "source_edges": EDGES_PATH.name,
        "stats": stats.as_dict(),
    }
    return json.dumps(rendered, sort_keys=True, separators=(",", ":")) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="print the rendered golden instead of rewriting the frozen file",
    )
    arguments = parser.parse_args()

    rendered = render()
    if arguments.stdout:
        sys.stdout.write(rendered)
        return 0
    GOLDEN_PATH.write_text(rendered)
    sys.stderr.write(f"wrote {GOLDEN_PATH}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
