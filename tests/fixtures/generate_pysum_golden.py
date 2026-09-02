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


def _gini_multi_row_window_stats(author_rows: list[list[int]]) -> list[CommitStatRow]:
    """Like _window_stats, but each author can contribute MULTIPLE rows whose
    additions SUM to that author's total churn -- needed to reproduce codex
    round 4's construction (2.1 BILLION int32-max rows overflow a native
    int64 ACCUMULATOR even though each individual row and the final total
    are each representable; _window_stats's one-row-per-author shape cannot
    exercise the accumulation step at all, only the post-aggregation value)."""
    rows: list[CommitStatRow] = []
    for author_index, row_values in enumerate(author_rows):
        for row_index, value in enumerate(row_values):
            rows.append(
                {
                    "repo_id": REPO_UUID,
                    "commit_hash": f"a{author_index}c{row_index}",
                    "author_email": f"a{author_index}@example.com",
                    "author_name": None,
                    "committer_when": datetime(2026, 9, 1, tzinfo=timezone.utc),
                    "file_path": "src/f.py",
                    "additions": value,
                    "deletions": 0,
                }
            )
    return rows


def _gini_multi_row_cases() -> list[dict[str, Any]]:
    """codex round 4 (P2, EXECUTED): a per-author accumulator that is a
    native int64 overflows given enough int32-max rows for one author (the
    30-day loader has no row cap) -- a DIFFERENT mechanism from rounds 1-2
    (which were about the value AFTER aggregation exceeding a float64's
    exact-integer range, not the accumulation itself overflowing a
    fixed-width integer type). Codex's own construction needed 2.1 billion
    rows to overflow int64 through int32-max values; reproduced here with 3
    rows of a much larger per-row value (still representable in a single Go
    int, i.e. < int64 max) so the corpus stays fast -- what matters is that
    SOME combination of rows sums past int64 max during accumulation, not
    that the specific row count/magnitude split matches production exactly.
    """
    int64_max = 2**63 - 1
    cases: list[dict[str, Any]] = [
        {
            # 3 rows of 4e18 each = 1.2e19, well past int64_max (~9.223e18).
            "author_rows": [[4_000_000_000_000_000_000] * 3, [1]],
        },
        {
            # Exactly astride the int64 boundary: 2 rows summing to
            # int64_max + 1 for one author.
            "author_rows": [
                [(int64_max + 1) // 2, (int64_max + 1) - (int64_max + 1) // 2],
                [7],
            ],
        },
    ]
    rows: list[dict[str, Any]] = []
    for case in cases:
        author_rows = case["author_rows"]
        stats = _gini_multi_row_window_stats(author_rows)
        expected = compute_code_ownership_gini(REPO_ID, stats)
        rows.append({"author_rows": author_rows, "expected_bits": bits_hex(expected)})
    return rows


def _gini_corpus() -> list[list[float]]:
    corpus: list[list[float]] = [
        [1, 5, 20],  # 3 authors: the minimum where compensation is nonzero
        [1, 1, 1, 1, 1],  # perfect equality
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [1000, 1, 1, 1, 1, 1, 1, 1],  # one dominant author
        [100_000, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        [7, 7, 7, 13, 13, 13, 13, 91, 91, 5000],
        list(range(1, 41)),  # 40 authors, a realistic-sized team churn spread
    ]
    # codex round 1 on #2107 (P2, EXECUTED): every value above is small enough
    # that BOTH every individual term AND the running numerator sum stay well
    # under 2**53 -- which is the actual condition for naive and compensated
    # summation to agree, not "every input is an integer" as an earlier
    # RISK-NOTES draft claimed. `numerator = sum((i+1)*val for i, val in
    # enumerate(churns))` grows roughly as len(churns)**2/2 * max(churns), so
    # a large CONTRIBUTOR COUNT is enough to cross 2**53 even with every
    # individual churn value comfortably inside int32. 3000 authors at
    # int32-max churn each (a schema-valid, if extreme, input -- additions is
    # int32 in ClickHouse and the 30-day loader has no row cap) reproduces
    # codex's divergence with a much smaller, faster corpus entry than their
    # 100,000-author repro:
    #   naive numerator   bits 0x43412bfeffdda81c
    #   sum() numerator    bits 0x43412bfeffdda802   (measured, this session)
    corpus.append([2_147_483_647] * 3000)
    # codex round 2 on #2107 (P2, EXECUTED): a DIFFERENT mechanism -- a
    # single author's own aggregated total exceeding 2**53 loses precision
    # at the int-to-float64 CONVERSION, before any summation runs at all.
    # Their repro (reduced here to 3 authors, matching their construction):
    corpus.append(
        [12_489_292_407_867_864, 12_713_596_315_088_591, 12_834_794_751_636_030]
    )
    # team-lead ruling (CHAOS-4824, round 2): the proper fix mirrors
    # Python's conversion POINT (exact int arithmetic until one final
    # division) rather than patching each construction, which is why every
    # case above is expected to pass now regardless of author count or
    # magnitude. Corpus below varies BOTH axes explicitly, up to and past
    # 2**53 (9,007,199,254,740,992) and 2**63 (9,223,372,036,854,775,808,
    # one past int64 max -- exercises the big.Int-based numerator/
    # denominator arithmetic at the edge of what a single int64 churn field
    # can hold; reaching this magnitude through real per-row accumulation
    # would need far more rows than is realistic, a disclosed, separate
    # bound from the numerator/denominator arithmetic this corpus targets).
    magnitudes = [
        1,
        2**31 - 1,  # int32 max: the largest a single row's additions/deletions can be
        2**53 - 1,  # largest exactly-representable float64 integer
        2**53,  # first integer float64 cannot represent exactly
        2**53 + 1,
        2**62,
        2**63 - 1,  # int64 max: the largest a single author's accumulated total can be
    ]
    author_counts = [3, 5, 10, 50, 500]
    for magnitude in magnitudes:
        for count in author_counts:
            # Vary per-author values around `magnitude` so churns are not
            # all identical (an all-equal corpus is Gini=0 by construction
            # in exact arithmetic and can mask a divergence that only shows
            # up when authors differ -- see the round-1 "expected value
            # equals the fallback" lesson).
            corpus.append([max(1, magnitude - i) for i in range(count)])
    return corpus


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
        "gini_multi_row": _gini_multi_row_cases(),
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
