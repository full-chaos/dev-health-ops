"""Golden generator for CHAOS-4824 (Go naive summation vs CPython's
Neumaier-compensated float `sum()`).

CPython applies Neumaier compensated summation to `sum()` over floats since
3.12 (gh-100425); a naive Go `total += x` loop is NOT equivalent. This
generator IMPORTS and calls the real production Python function
(`compute_code_ownership_gini`), so `expected_bits` is CPython's own IEEE-754
bit pattern, not a hand-derived guess.

Per pythonparity.Sum's own doc comment (lane-4441, #2103): compensation is
always zero below three summands, so every corpus entry here uses >= 3
authors/churn values -- a corpus that stayed at 1-2 elements would prove
nothing about this defect class, which is exactly how it first shipped
undetected in a sibling function (MeanEdgeConfidence).
"""

from __future__ import annotations

import argparse
import json
import struct
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.knowledge import compute_code_ownership_gini
from dev_health_ops.metrics.schemas import CommitStatRow

OUTPUT = Path(__file__).with_name("pysum_golden.json")
REPO_ID = "00000000-0000-4000-8000-00000000000a"
REPO_UUID = uuid.UUID(REPO_ID)


def bits_hex(value: float) -> str:
    return "0x" + format(struct.unpack(">Q", struct.pack(">d", value))[0], "016x")


def _window_stats(churns: list[float]) -> list[CommitStatRow]:
    """One synthetic commit per author, additions carrying the exact churn
    value (as an int -- CommitStatRow.additions/deletions are ints in
    production; using only integral churns here keeps the corpus faithful
    to what the Go port actually receives from ClickHouse)."""
    rows: list[CommitStatRow] = []
    for index, churn in enumerate(churns):
        rows.append(
            {
                "repo_id": REPO_UUID,
                "commit_hash": f"c{index}",
                "author_email": f"a{index}@example.com",
                "author_name": None,
                "committer_when": datetime(2026, 9, 1, tzinfo=timezone.utc),
                "file_path": "src/f.py",
                "additions": int(churn),
                "deletions": 0,
            }
        )
    return rows


def _gini_corpus() -> list[list[float]]:
    return [
        [1, 5, 20],  # 3 authors: the minimum where compensation is nonzero
        [1, 1, 1, 1, 1],  # perfect equality
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [1000, 1, 1, 1, 1, 1, 1, 1],  # one dominant author
        [100_000, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        [7, 7, 7, 13, 13, 13, 13, 91, 91, 5000],
        list(range(1, 41)),  # 40 authors, a realistic-sized team churn spread
    ]


def _gini_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for churns in _gini_corpus():
        expected = compute_code_ownership_gini(REPO_ID, _window_stats(churns))
        rows.append({"churns": churns, "expected_bits": bits_hex(expected)})
    return rows


def render() -> str:
    value = {"schema_version": 1, "gini": _gini_cases()}
    return (
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.stdout:
        print(rendered, end="")
        return 0
    if args.check:
        return 0 if OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
