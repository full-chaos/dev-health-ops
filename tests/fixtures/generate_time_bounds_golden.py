"""Generate the compute_time_bounds golden for CHAOS-4441.

Drives `evidence.compute_time_bounds` itself -- imported, never imitated.

THE AXIS THAT MATTERS MOST HERE
-------------------------------
compute_time_bounds is min() over starts and max() over ends. Python compares
aware datetimes by INSTANT, not by wall clock, so two values reading 10:30 in
different zones are different points and order accordingly.

That is precisely what plan section 5f was about. PR2's normalizeTimestamp
rebuilt the wall clock instead of converting, which for a `DateTime64(3)` column
on a non-UTC server shifted every value by the server offset -- and a uniform
shift is invisible to a min/max unless the corpus MIXES zones. So the corpus
mixes them: a value that is earliest by instant but latest by wall clock, and
vice versa. A port that compares wall clocks passes an all-UTC corpus and fails
these.

THE OTHER AXES
--------------
  node type      issue / pr / commit / unknown
  presence       node in the component but MISSING from its map
  fallback       completed_at vs updated_at; merged_at vs closed_at vs neither
  cardinality    zero nodes, one node, many; starts-without-ends
  ordering       the extreme value first, last, and in the middle

Usage:
    uv run python tests/fixtures/generate_time_bounds_golden.py [--stdout]
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.evidence import compute_time_bounds

OUTPUT_PATH = Path(__file__).parent / "time_bounds_python_golden.json"

UTC = timezone.utc
KOLKATA = timezone(timedelta(hours=5, minutes=30))  # +05:30, non-integral
CHATHAM = timezone(timedelta(hours=12, minutes=45))  # +12:45, the awkward one
LA = timezone(timedelta(hours=-7))


def _dt(
    year: int, month: int, day: int, hour: int, minute: int, tz: timezone
) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=tz)


def _encode(value: datetime | None) -> str | None:
    """ISO 8601 with offset, so Go reads back the same INSTANT and zone."""
    return None if value is None else value.isoformat()


def _scenarios() -> list[dict[str, Any]]:
    # A wall clock of 10:30 in +12:45 is EARLIER as an instant than 10:30 in
    # -07:00 by nearly twenty hours. A port comparing wall clocks sees them as
    # equal; one comparing instants orders them.
    early_by_instant_late_by_clock = _dt(2026, 9, 2, 23, 0, CHATHAM)  # 10:15Z
    late_by_instant_early_by_clock = _dt(2026, 9, 2, 6, 0, LA)  # 13:00Z

    return [
        {
            "label": "empty_node_list",
            "nodes": [],
            "work_items": {},
            "prs": {},
            "commits": {},
        },
        {
            "label": "single_issue_completed",
            "nodes": [["issue", "i1"]],
            "work_items": {
                "i1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "completed_at": _dt(2026, 9, 3, 17, 0, UTC),
                    "updated_at": _dt(2026, 9, 2, 12, 0, UTC),
                }
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "issue_falls_back_to_updated_at",
            "nodes": [["issue", "i1"]],
            "work_items": {
                "i1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "completed_at": None,
                    "updated_at": _dt(2026, 9, 2, 12, 0, UTC),
                }
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "pr_merged_wins_over_closed",
            "nodes": [["pr", "p1"]],
            "work_items": {},
            "prs": {
                "p1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "merged_at": _dt(2026, 9, 4, 10, 0, UTC),
                    "closed_at": _dt(2026, 9, 9, 10, 0, UTC),
                }
            },
            "commits": {},
        },
        {
            "label": "pr_falls_back_to_closed",
            "nodes": [["pr", "p1"]],
            "work_items": {},
            "prs": {
                "p1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "merged_at": None,
                    "closed_at": _dt(2026, 9, 4, 10, 0, UTC),
                }
            },
            "commits": {},
        },
        {
            "label": "pr_with_neither_end_uses_start",
            "nodes": [["pr", "p1"]],
            "work_items": {},
            "prs": {
                "p1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "merged_at": None,
                    "closed_at": None,
                }
            },
            "commits": {},
        },
        {
            "label": "commit_uses_author_when",
            "nodes": [["commit", "c1"]],
            "work_items": {},
            "prs": {},
            "commits": {
                "c1": {
                    "author_when": _dt(2026, 9, 2, 8, 0, UTC),
                    "committer_when": _dt(2026, 9, 5, 8, 0, UTC),
                }
            },
        },
        {
            "label": "node_missing_from_its_map_contributes_nothing",
            "nodes": [["issue", "present"], ["issue", "dangling"]],
            "work_items": {
                "present": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "completed_at": _dt(2026, 9, 3, 17, 0, UTC),
                    "updated_at": None,
                }
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "only_dangling_nodes_yields_none",
            "nodes": [["issue", "a"], ["pr", "b"], ["commit", "c"]],
            "work_items": {},
            "prs": {},
            "commits": {},
        },
        {
            "label": "unknown_node_type_contributes_nothing",
            "nodes": [["release", "r1"], ["issue", "i1"]],
            "work_items": {
                "i1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "completed_at": _dt(2026, 9, 3, 17, 0, UTC),
                    "updated_at": None,
                }
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "only_unknown_types_yields_none",
            "nodes": [["release", "r1"], ["deployment", "d1"]],
            "work_items": {},
            "prs": {},
            "commits": {},
        },
        # --- the timezone axis ---
        {
            "label": "mixed_zones_min_is_by_instant_not_wall_clock",
            "nodes": [["issue", "chatham"], ["issue", "la"]],
            "work_items": {
                "chatham": {
                    "created_at": early_by_instant_late_by_clock,
                    "completed_at": _dt(2026, 9, 10, 12, 0, UTC),
                    "updated_at": None,
                },
                "la": {
                    "created_at": late_by_instant_early_by_clock,
                    "completed_at": _dt(2026, 9, 10, 12, 0, UTC),
                    "updated_at": None,
                },
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "mixed_zones_max_is_by_instant_not_wall_clock",
            "nodes": [["issue", "chatham"], ["issue", "la"]],
            "work_items": {
                "chatham": {
                    "created_at": _dt(2026, 9, 1, 0, 0, UTC),
                    "completed_at": early_by_instant_late_by_clock,
                    "updated_at": None,
                },
                "la": {
                    "created_at": _dt(2026, 9, 1, 0, 0, UTC),
                    "completed_at": late_by_instant_early_by_clock,
                    "updated_at": None,
                },
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "same_wall_clock_different_zones_are_different_instants",
            "nodes": [["issue", "utc"], ["issue", "kolkata"]],
            "work_items": {
                "utc": {
                    "created_at": _dt(2026, 9, 2, 10, 30, UTC),
                    "completed_at": _dt(2026, 9, 2, 10, 30, UTC),
                    "updated_at": None,
                },
                "kolkata": {
                    "created_at": _dt(2026, 9, 2, 10, 30, KOLKATA),
                    "completed_at": _dt(2026, 9, 2, 10, 30, KOLKATA),
                    "updated_at": None,
                },
            },
            "prs": {},
            "commits": {},
        },
        # --- ordering: the extreme value first, last, middle ---
        {
            "label": "extremes_at_both_ends_of_the_node_list",
            "nodes": [
                ["issue", "earliest"],
                ["pr", "middle"],
                ["commit", "latest"],
            ],
            "work_items": {
                "earliest": {
                    "created_at": _dt(2026, 8, 1, 0, 0, UTC),
                    "completed_at": _dt(2026, 8, 2, 0, 0, UTC),
                    "updated_at": None,
                }
            },
            "prs": {
                "middle": {
                    "created_at": _dt(2026, 9, 1, 0, 0, UTC),
                    "merged_at": _dt(2026, 9, 2, 0, 0, UTC),
                    "closed_at": None,
                }
            },
            "commits": {
                "latest": {
                    "author_when": _dt(2026, 10, 1, 0, 0, UTC),
                    "committer_when": None,
                }
            },
        },
        {
            "label": "duplicate_nodes_do_not_change_the_bounds",
            "nodes": [["issue", "i1"], ["issue", "i1"], ["issue", "i1"]],
            "work_items": {
                "i1": {
                    "created_at": _dt(2026, 9, 1, 9, 0, UTC),
                    "completed_at": _dt(2026, 9, 3, 17, 0, UTC),
                    "updated_at": None,
                }
            },
            "prs": {},
            "commits": {},
        },
        {
            "label": "start_after_end_is_reproduced_not_corrected",
            # An issue completed BEFORE it was created. Nothing in the source
            # orders or validates the pair, so the inversion is carried through.
            "nodes": [["issue", "i1"]],
            "work_items": {
                "i1": {
                    "created_at": _dt(2026, 9, 10, 0, 0, UTC),
                    "completed_at": _dt(2026, 9, 1, 0, 0, UTC),
                    "updated_at": None,
                }
            },
            "prs": {},
            "commits": {},
        },
    ]


def main() -> None:
    cases: list[dict[str, Any]] = []
    for scenario in _scenarios():
        bounds = compute_time_bounds(
            [(node_type, node_id) for node_type, node_id in scenario["nodes"]],
            scenario["work_items"],
            scenario["prs"],
            scenario["commits"],
        )
        cases.append(
            {
                "label": scenario["label"],
                "nodes": scenario["nodes"],
                "work_items": {
                    key: {field: _encode(value) for field, value in fields.items()}
                    for key, fields in scenario["work_items"].items()
                },
                "prs": {
                    key: {field: _encode(value) for field, value in fields.items()}
                    for key, fields in scenario["prs"].items()
                },
                "commits": {
                    key: {field: _encode(value) for field, value in fields.items()}
                    for key, fields in scenario["commits"].items()
                },
                "start": _encode(bounds.start) if bounds else None,
                "end": _encode(bounds.end) if bounds else None,
                "is_none": bounds is None,
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_time_bounds_golden.py. "
            "Do not hand-edit."
        ),
        "_note": (
            "min/max compare aware datetimes by INSTANT, not wall clock. The "
            "mixed-zone cases fail any port that compares wall clocks, which an "
            "all-UTC corpus cannot detect -- see plan section 5f."
        ),
        "cases": cases,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return
    OUTPUT_PATH.write_text(rendered)
    none_count = sum(1 for case in cases if case["is_none"])
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(cases)}  ({none_count} yield None)")


if __name__ == "__main__":
    main()
