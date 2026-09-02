"""Golden generator for CHAOS-4818 (Go FMA fusion on arm64 vs CPython double rounding).

Every case here targets exactly one code shape: an unguarded `x*y + z` (or a
weighted sum of several such products) that Go's spec permits fusing into a
single fused-multiply-add on arm64 -- one rounding -- where CPython always
rounds the multiply and the add as two separate operations.

This generator IMPORTS and calls the real production Python functions (never
reimplements their formulas), so the frozen `expected_bits` values are
CPython's own IEEE-754 bit pattern for each input, not a hand-derived guess.
`expected_bits` is the hex of `struct.unpack('>Q', struct.pack('>d', value))[0]`
-- the same 64-bit pattern `math.Float64bits` returns in Go -- so the Go test
asserts bit-for-bit equality, never "fused != unfused" (which is a no-op
assertion on amd64, where the fusion this ticket is about does not happen).

Four families, one per fixed Go site (CHAOS-4818 RISK-NOTES table):
  - release_confidence: dev_health_ops.metrics.release_impact._compute_confidence
    vs internal/jobs/metrics/numerical.ReleaseImpactConfidence. Grid matches
    the ticket's own 28,987-input sweep shape (coverage x sessions x
    concurrent_deploys), so it necessarily contains representatives of the
    12.5% CHAOS-4818 measured as fused != unfused.
  - percentile_float: dev_health_ops.metrics.compute._percentile (byte-identical
    across compute.py / compute_cicd.py / compute_deployments.py /
    compute_incidents.py / compute_work_items.py) vs the five duplicated Go
    percentile functions (deployPercentile, cicd.percentile,
    incidentPercentile, repouser.percentile, testops.percentile).
  - percentile_int: dev_health_ops.metrics.compute_capacity._percentile vs
    internal/jobs/metrics/numerical.IntegerPercentiles. Int-truncating, so the
    assertion is exact int equality (a truncation boundary can still flip on
    one ULP), not a bit pattern.
  - hotspot_score: dev_health_ops.metrics.hotspots.compute_file_hotspots vs
    internal/jobs/metrics/daily/filehotspots.ComputeFileHotspots's inline
    weighted sum.
"""

from __future__ import annotations

import argparse
import json
import struct
import uuid
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.compute import _percentile as _percentile_float
from dev_health_ops.metrics.compute_capacity import _percentile as _percentile_int
from dev_health_ops.metrics.hotspots import compute_file_hotspots
from dev_health_ops.metrics.release_impact import _compute_confidence
from dev_health_ops.metrics.schemas import CommitStatRow

OUTPUT = Path(__file__).with_name("fma_golden.json")
REPO_ID = uuid.UUID("00000000-0000-4000-8000-00000000000a")


def bits_hex(value: float) -> str:
    """The IEEE-754 bit pattern of `value`, as `math.Float64bits` would print it."""
    return "0x" + format(struct.unpack(">Q", struct.pack(">d", value))[0], "016x")


def _release_confidence() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sessions_grid = [0, 1, 2, 4, 8, 20, 40]
    concurrent_grid = list(range(7))
    coverage_grid = [i / 50.0 for i in range(51)]
    for coverage in coverage_grid:
        for sessions in sessions_grid:
            for concurrent in concurrent_grid:
                expected = _compute_confidence(coverage, sessions, concurrent, 30)
                rows.append(
                    {
                        "coverage": coverage,
                        "total_sessions": sessions,
                        "concurrent_deploys": concurrent,
                        "minimum_sessions": 30,
                        "expected_bits": bits_hex(expected),
                    }
                )
    # Branch coverage for `minimum_sessions <= 0` (sampleScore forced to 1.0)
    # and a much larger minimum_sessions, at a few representative coverages.
    for minimum in (0, -5, 1, 300, 1000):
        for coverage in (0.0, 0.35, 0.5, 0.72, 1.0):
            for sessions, concurrent in ((0, 0), (1, 2), (7, 3), (40, 6)):
                expected = _compute_confidence(coverage, sessions, concurrent, minimum)
                rows.append(
                    {
                        "coverage": coverage,
                        "total_sessions": sessions,
                        "concurrent_deploys": concurrent,
                        "minimum_sessions": minimum,
                        "expected_bits": bits_hex(expected),
                    }
                )
    return rows


def _value_sets() -> list[list[float]]:
    return [
        [1.0, 2.0],
        [0.1, 0.2, 0.3],
        [-5.5, 3.3, 0.0, 12.75, 9.9],
        [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8],
        [0.01 * n for n in range(1, 14)],
        [float(n * n) - 0.37 for n in range(1, 21)],
        [1e6, 2e6, 3e6, 1e-3, 5.5],
    ]


def _percentiles_swept() -> list[float]:
    return [float(n) for n in range(1, 100)]  # 1 .. 99 step 1


def _percentile_float_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for values in _value_sets():
        for pct in _percentiles_swept():
            expected = _percentile_float(values, pct)
            rows.append(
                {
                    "values": values,
                    "percentile": pct,
                    "expected_bits": bits_hex(expected),
                }
            )
    return rows


def _percentile_int_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    int_value_sets = [
        [1, 2],
        [0, 5, 15, 50, 85, 95, 100],
        [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5],
        list(range(0, 200, 7)),
        [1_000_003, 7, 999_999, 42, 8_675_309, 0, 13],
    ]
    for values in int_value_sets:
        sorted_values = sorted(values)
        for pct in _percentiles_swept():
            expected = _percentile_int(sorted_values, pct)
            rows.append(
                {
                    "sorted_values": sorted_values,
                    "percentile": pct,
                    "expected": expected,
                }
            )
    return rows


def _hotspot_score_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    # Deliberately excludes churn values where Go's math.Log1p disagrees with
    # CPython's math.log1p by one ULP on its OWN (e.g. log1p(2): Go bits
    # ...030a vs CPython ...030b) -- measured over 0..1999, 35/2000 (1.75%)
    # diverge, sparse and unrelated to CHAOS-4818's FMA fusion (confirmed via
    # `go tool objdump`: ComputeFileHotspots emits no FMA instruction at all,
    # so every failure this corpus could produce would be this OTHER,
    # library-level divergence). Filed separately (CHAOS-4829) rather than
    # conflated here. Every value below was checked against a live Go/Python
    # cross-check and agrees exactly.
    churns = [0, 1, 3, 5, 7, 10, 23, 99, 250, 1000, 1999]
    contributor_counts = [1, 2, 3, 5, 8, 13, 21]
    commit_counts = [1, 2, 3, 5, 8, 13]
    for churn in churns:
        for contributors in contributor_counts:
            for commits_count in commit_counts:
                window_stats = _synthetic_window_stats(
                    churn, contributors, commits_count
                )
                records = compute_file_hotspots(
                    repo_id=REPO_ID,
                    day=__import__("datetime").date(2026, 9, 2),
                    window_stats=window_stats,
                    computed_at=__import__("datetime").datetime(
                        2026, 9, 2, tzinfo=__import__("datetime").timezone.utc
                    ),
                )
                assert len(records) == 1, (churn, contributors, commits_count, records)
                record = records[0]
                assert record.churn == churn
                assert record.contributors == contributors
                assert record.commits_count == commits_count
                rows.append(
                    {
                        "churn": churn,
                        "contributors": contributors,
                        "commits_count": commits_count,
                        "expected_bits": bits_hex(record.hotspot_score),
                    }
                )
    return rows


def _synthetic_window_stats(
    churn: int, contributors: int, commits_count: int
) -> list[CommitStatRow]:
    """One file, exactly `contributors` distinct authors and `commits_count`
    distinct commits, total additions+deletions == churn (all on row 0)."""
    n = max(contributors, commits_count, 1)
    rows: list[CommitStatRow] = []
    for i in range(n):
        rows.append(
            {
                "repo_id": REPO_ID,
                "commit_hash": f"c{i % commits_count}",
                "author_email": f"a{i % contributors}@example.com",
                "author_name": None,
                "committer_when": __import__("datetime").datetime(
                    2026, 9, 1, tzinfo=__import__("datetime").timezone.utc
                ),
                "file_path": "src/hot.py",
                "additions": churn if i == 0 else 0,
                "deletions": 0,
            }
        )
    return rows


def render() -> str:
    value = {
        "schema_version": 1,
        "release_confidence": _release_confidence(),
        "percentile_float": _percentile_float_cases(),
        "percentile_int": _percentile_int_cases(),
        "hotspot_score": _hotspot_score_cases(),
    }
    return (
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Render to stdout instead of writing the checked-in file. The live "
            "rot guard (internal/jobs/metrics/numerical) uses this to compare "
            "what TODAY's production Python produces against the frozen file."
        ),
    )
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
