#!/usr/bin/env python3
"""Generate the loader post-processing parity corpus from the LIVE reference.

Drives the real ``ClickHouseMetricsLoader`` with an injected fake ClickHouse client, so
the snapshot recorded here is produced by the shipped Python code rather than by
a restatement of it. No database is involved: the fake returns canned rows, which
is exactly the seam ``MetricsLoader._qd`` reads through.

What this pins is the POST-PROCESSING -- null handling, the complexity
denominator floor, the absent-versus-zero churn decision, gini and the list
builds. It does NOT pin the SQL text; that needs a real ClickHouse and is
covered separately by the Testcontainers test.

Usage:
    python generate_recommendations_loader_golden.py            # write beside this file
    python generate_recommendations_loader_golden.py --stdout
"""

from __future__ import annotations

import argparse
import json
import platform
import struct
import sys
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT / "src"))

from dev_health_ops.recommendations.loader import (  # noqa: E402
    ClickHouseMetricsLoader,
)

OUTPUT_PATH = Path(__file__).parent / "recommendations_loader_golden.json"

TEAM_ID = "team-70d529e0"
ORG_ID = "org-70d529e0"
WINDOW_START = date(2026, 8, 1)
WINDOW_END = date(2026, 9, 1)

NAN = float("nan")
INF = float("inf")


class FakeResult:
    """Mimics the clickhouse-connect result object MetricsLoader._qd reads."""

    def __init__(self, column_names, result_rows):
        self.column_names = column_names
        self.result_rows = result_rows


class FakeClient:
    """Dispatches on the table the query selects FROM.

    Deliberately keyed on the table name rather than on call order: the loader
    is free to reorder its calls, and a positional fake would silently feed the
    wrong rows to the wrong query if it ever did.
    """

    def __init__(self, tables: dict):
        self._tables = tables
        self.seen: list[str] = []

    def query(self, query: str, parameters=None):  # noqa: ARG002
        table = None
        for candidate in (
            "work_item_metrics_daily",
            "repo_metrics_daily",
            "user_metrics_daily",
            "team_metrics_daily",
            "repo_complexity_daily",
            "file_hotspot_daily",
            "compounding_risk_daily",
        ):
            if candidate in query:
                table = candidate
                break
        if table is None:
            raise AssertionError(f"fake client saw an unrecognised query:\n{query}")

        # work_item_metrics_daily is read TWICE with different shapes -- the
        # wip/throughput pair and the per-day cycle time. Disambiguate on a
        # column that appears in only one of them.
        if table == "work_item_metrics_daily":
            table = (
                "cycle_time" if "cycle_time_p50_hours" in query else "wip_throughput"
            )
        # repo_metrics_daily likewise serves latency and rework.
        elif table == "repo_metrics_daily":
            table = "rework" if "pr_rework_ratio" in query else "latency"

        self.seen.append(table)
        columns, rows = self._tables.get(table, ([], []))
        return FakeResult(list(columns), [list(row) for row in rows])


def bits(value):
    return None if value is None else struct.pack(">d", float(value)).hex()


def encode_optional(value):
    return None if value is None else bits(value)


def encode_list(values):
    return [bits(v) for v in values]


# The fake client's table map: column names plus rows of arbitrary cell types.
# Annotated because mypy cannot infer a useful type for a heterogeneous literal,
# and an un-annotated dict[str, object] is not unpackable into typed callables.
TableMap = dict[str, tuple[list[str], list[list[Any]]]]


def scenario(
    name: str, **tables: tuple[list[str], list[list[Any]]]
) -> tuple[str, TableMap]:
    base: TableMap = {
        "wip_throughput": (["day", "wip_total", "tp_total"], []),
        "latency": (["avg_p75"], []),
        "user_metrics_daily": (["author_email", "total_reviews"], []),
        "rework": (["avg_rework"], []),
        "team_metrics_daily": (["avg_ratio"], []),
        "cycle_time": (["day", "avg_ct"], []),
        "repo_complexity_daily": (["first_half", "second_half"], []),
        "file_hotspot_daily": (["total"], []),
        "compounding_risk_daily": (["score", "severity"], []),
    }
    base.update(tables)
    return name, base


def build_scenarios():
    D = date(2026, 8, 5)
    scenarios = []

    scenarios.append(scenario("everything-empty"))

    # --- wip / throughput: NULLs are KEPT as 0.0, so length is preserved ---
    scenarios.append(
        scenario(
            "wip-with-nulls",
            wip_throughput=(
                ["day", "wip_total", "tp_total"],
                [[D, 1.0, 5.0], [D, None, None], [D, 3.0, 1.0], [D, 0.0, -0.0]],
            ),
        )
    )
    scenarios.append(
        scenario(
            "wip-nonfinite",
            wip_throughput=(
                ["day", "wip_total", "tp_total"],
                [[D, NAN, INF], [D, -INF, NAN], [D, 1.0, 2.0]],
            ),
        )
    )

    # --- cycle time: NULLs are DROPPED, so length CHANGES ---
    scenarios.append(
        scenario(
            "cycle-time-with-nulls",
            cycle_time=(["day", "avg_ct"], [[D, 1.0], [D, None], [D, 3.0], [D, None]]),
        )
    )
    scenarios.append(
        scenario(
            "cycle-time-all-null",
            cycle_time=(["day", "avg_ct"], [[D, None], [D, None]]),
        ),
    )
    scenarios.append(
        scenario(
            "cycle-time-nonfinite",
            cycle_time=(["day", "avg_ct"], [[D, INF], [D, NAN], [D, 2.0]]),
        )
    )

    # --- scalars through _safe_float: None and NaN absent, infinities pass ---
    for label, value in [
        ("none", None),
        ("zero", 0.0),
        ("nan", NAN),
        ("posinf", INF),
        ("neginf", -INF),
        ("value", 30.5),
    ]:
        scenarios.append(scenario(f"latency-{label}", latency=(["avg_p75"], [[value]])))
        scenarios.append(
            scenario(f"rework-{label}", rework=(["avg_rework"], [[value]]))
        )
        scenarios.append(
            scenario(
                f"afterhours-{label}", team_metrics_daily=(["avg_ratio"], [[value]])
            )
        )

    # --- gini over the review loads ---
    # Annotated because the literal below pairs a str label with a nested list;
    # mypy otherwise widens the tuple's element type to object, which it will
    # not iterate. Each group is a list of (author_email, total_reviews) pairs.
    gini_loads: list[tuple[str, list[list[tuple[str, float | None]]]]] = [
        ("empty", []),
        ("single", [[("a@x", 5.0)]]),
        ("two-equal", [[("a@x", 5.0)], [("b@x", 5.0)]]),
        ("skewed", [[("a@x", 90.0)], [("b@x", 5.0)], [("c@x", 5.0)]]),
        ("with-null", [[("a@x", None)], [("b@x", 4.0)], [("c@x", 6.0)]]),
        ("with-zeros", [[("a@x", 0.0)], [("b@x", 0.0)], [("c@x", 7.0)]]),
        ("all-zero", [[("a@x", 0.0)], [("b@x", 0.0)]]),
        ("nonfinite", [[("a@x", INF)], [("b@x", 1.0)], [("c@x", 2.0)]]),
    ]
    for label, loads in gini_loads:
        rows: list[list[Any]] = [
            [pair[0], pair[1]] for group in loads for pair in group
        ]
        scenarios.append(
            scenario(
                f"gini-{label}",
                user_metrics_daily=(["author_email", "total_reviews"], rows),
            )
        )

    # --- complexity halves: the denominator floor and its edges ---
    for label, first, second in [
        ("both-none", None, None),
        ("first-none", None, 5.0),
        ("second-none", 5.0, None),
        ("floor-applies", 0.25, 0.5),
        ("floor-exact-one", 1.0, 2.0),
        ("above-floor", 4.0, 6.0),
        ("negative-first", -3.0, 1.0),
        ("zero-first", 0.0, 2.0),
        ("falling", 8.0, 2.0),
        ("flat", 3.0, 3.0),
        ("nan-first", NAN, 2.0),
        ("inf-first", INF, 2.0),
        ("inf-second", 2.0, INF),
        ("huge", 1e308, -1e308),
    ]:
        for hotspots in (0, 7):
            scenarios.append(
                scenario(
                    f"complexity-{label}-hotspots{hotspots}",
                    repo_complexity_daily=(
                        ["first_half", "second_half"],
                        [[first, second]],
                    ),
                    file_hotspot_daily=(["total"], [[hotspots]]),
                )
            )

    # --- persisted composite, including the empty-severity collapse ---
    for label, score, severity in [
        ("no-rows", "SKIP", "SKIP"),
        ("none-none", None, None),
        ("none-empty", None, ""),
        ("score-empty-severity", 0.9, ""),
        ("elevated", 0.5, "elevated"),
        ("high", 0.95, "high"),
        ("low", 0.1, "low"),
        ("nan-score", NAN, "high"),
        ("inf-score", INF, "elevated"),
        ("negzero-score", -0.0, "elevated"),
    ]:
        rows = [] if score == "SKIP" else [[score, severity]]
        scenarios.append(
            scenario(
                f"composite-{label}",
                compounding_risk_daily=(["score", "severity"], rows),
            )
        )

    return scenarios


def encode_tables(tables):
    encoded = {}
    for key, (columns, rows) in tables.items():
        encoded[key] = {
            "columns": list(columns),
            "rows": [
                [
                    cell.isoformat()
                    if isinstance(cell, date)
                    else cell
                    if isinstance(cell, (str, int)) and not isinstance(cell, bool)
                    else encode_optional(cell)
                    for cell in row
                ]
                for row in rows
            ],
        }
    return encoded


def encode_snapshot(snapshot):
    return {
        "wip_by_day": encode_list(snapshot.wip_by_day),
        "throughput_by_cycle": encode_list(snapshot.throughput_by_cycle),
        "review_latency_p75_hours": encode_optional(snapshot.review_latency_p75_hours),
        "reviewer_gini": encode_optional(snapshot.reviewer_gini),
        "rework_churn_ratio": encode_optional(snapshot.rework_churn_ratio),
        "after_hours_ratio": encode_optional(snapshot.after_hours_ratio),
        "cycle_time_by_day": encode_list(snapshot.cycle_time_by_day),
        "hotspot_complexity_delta": encode_optional(snapshot.hotspot_complexity_delta),
        "hotspot_churn_overlap": encode_optional(snapshot.hotspot_churn_overlap),
        "compounding_risk_score": encode_optional(snapshot.compounding_risk_score),
        "compounding_risk_severity": snapshot.compounding_risk_severity,
    }


def build_document():
    cases = []
    for name, tables in build_scenarios():
        client = FakeClient(tables)
        loader = ClickHouseMetricsLoader(client, org_id=ORG_ID)
        snapshot = loader.load_team_metrics_window(
            TEAM_ID, ORG_ID, WINDOW_START, WINDOW_END
        )
        cases.append(
            {
                "name": name,
                "tables": encode_tables(tables),
                "snapshot": encode_snapshot(snapshot),
                "queries_seen": client.seen,
            }
        )
    return {
        "purpose": (
            "Loader post-processing parity: the real MetricsLoader driven by an "
            "injected fake ClickHouse client. Pins null handling, the complexity "
            "denominator floor, the absent-vs-zero churn decision, gini and the "
            "list builds. Does NOT pin SQL text."
        ),
        # Portable fields only -- see the rule corpus generator for why
        # sys.version cannot be frozen.
        "environment": {
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "float_repr_style": sys.float_repr_style,
            "machine_not_compared": platform.machine(),
        },
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()

    document = build_document()
    text = json.dumps(document, indent=2, sort_keys=True) + "\n"
    if args.stdout:
        sys.stdout.write(text)
        return 0
    args.out.write_text(text, encoding="utf-8")
    print(f"wrote {args.out}: {len(document['cases'])} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
