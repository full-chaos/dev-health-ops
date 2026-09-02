"""Golden generator for CHAOS-4824 (Go naive summation vs CPython's
Neumaier-compensated float `sum()`).

CPython applies Neumaier compensated summation to `sum()` over floats since
3.12 (gh-100425); a naive Go `total += x` loop is NOT equivalent. Two
families here:

  - gini: IMPORTS and calls the real production
    dev_health_ops.metrics.knowledge.compute_code_ownership_gini.
  - pipeline_stability: computes `weighted_success_rate_7d` /
    `success_rate_trend` using the EXACT expressions from
    compute_testops_risk.py:200-216 (compute_pipeline_stability), copied
    verbatim rather than reimplemented, because that function only returns
    values already rounded to 4 decimals for storage -- and 4-decimal
    rounding is far coarser than the few-ULP difference naive vs
    compensated summation produces (measured: 0 divergent ROUNDED values in
    200,000 random 3-8 element trials), so a golden against the rounded,
    stored value would almost never go red on the naive baseline. The
    UNROUNDED value, which internal/jobs/metrics/daily's
    weightedSuccessRate7d/successRateTrendFromRates now expose for exactly
    this reason, is where the defect is actually visible.

Per pythonparity.Sum's own doc comment (lane-4441, #2103): compensation is
always zero below three summands, and a corpus that varies only VALUES
while holding the summand count low proves nothing -- every case here uses
>= 3 elements, and the corpus varies count (3-8) and magnitude (uniform
[0,1), near-1 clustered, wide-magnitude) deliberately.
"""

from __future__ import annotations

import argparse
import json
import random
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


def _weighted_success_rate_7d(success_rates: list[float]) -> float:
    """compute_testops_risk.py:200-204, copied verbatim."""
    n = len(success_rates)
    weights = [1.0 + i * 0.5 for i in range(n)]
    total_weight = sum(weights)
    return sum(rate * w for rate, w in zip(success_rates, weights)) / total_weight


def _success_rate_trend(success_rates: list[float]) -> float:
    """compute_testops_risk.py:207-215, copied verbatim, for n >= 2."""
    n = len(success_rates)
    x_mean = (n - 1) / 2.0
    y_mean = sum(success_rates) / n
    num = sum((i - x_mean) * (rate - y_mean) for i, rate in enumerate(success_rates))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else 0.0


def _pipeline_stability_corpus() -> list[list[float]]:
    corpus: list[list[float]] = [
        [0.83, 0.91, 0.76],
        [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
        [1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0],
        [0.999999, 0.000001, 0.5],
        [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
    ]
    # Deterministic pseudo-random trials across the summand-count axis (3-8)
    # and value magnitude (uniform [0,1)) -- per pythonparity.Sum's own
    # lesson, the corpus must vary COUNT, not just values, and a large
    # enough batch is what actually finds naive-vs-compensated divergences
    # (measured this session: ~26% of random 3-8 element [0,1) trials
    # diverge at the unrounded level).
    rng = random.Random(20260902)
    for _ in range(60):
        n = rng.randint(3, 8)
        corpus.append([rng.uniform(0.0, 1.0) for _ in range(n)])
    return corpus


def _pipeline_stability_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for success_rates in _pipeline_stability_corpus():
        row: dict[str, Any] = {
            "success_rates": success_rates,
            "weighted_success_rate_7d_bits": bits_hex(
                _weighted_success_rate_7d(success_rates)
            ),
        }
        if len(success_rates) >= 2:
            row["success_rate_trend_bits"] = bits_hex(
                _success_rate_trend(success_rates)
            )
        rows.append(row)
    return rows


def render() -> str:
    value = {
        "schema_version": 1,
        "gini": _gini_cases(),
        "pipeline_stability": _pipeline_stability_cases(),
    }
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
