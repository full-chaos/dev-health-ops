"""Golden generator for the CHAOS-4818 AST-lint follow-up's live finding.

Rebuilding the lint (internal/jobs/fma_lint_test.go) to cover compound
assignment (`total += a*b`, previously invisible to a checker that only
inspected explicit `+`/`-` `*ast.BinaryExpr` nodes -- codex round 3 on
#2106) found two unguarded sites STILL LIVE on main at the time, in
functions #2107 (then closed, not yet merged) would also have touched:

  - hotspot_risk_score: internal/jobs/metrics/daily/filehotspots.sampleZScores
    (`sumSquares += diff * diff`) vs dev_health_ops.metrics.hotspots.
    compute_file_risk_hotspots's nested `get_z_scores` helper. Isolated by
    holding complexity constant across every file in a case (population
    variance 0 -> z_comp is exactly [0.0]*n on both sides via the shared
    "stdev == 0" early return), so `risk_score = z_churn[i] + z_comp[i]`
    reduces to `z_churn[i]` alone -- the site under test, and nothing else.
    STILL a real float64 site after #2123 merged (the compound assignment
    was extracted away, but the underlying multiply-and-store is still
    float64 arithmetic) -- kept here.
  - ownership_gini: internal/jobs/metrics/daily/repouser.CodeOwnershipGini
    (`numerator += float64(index+1) * value` -- only the int operand was
    converted, not the product). REMOVED from this golden once this branch
    rebased onto PR #2123 (CHAOS-4824, merged): that PR rewrote
    CodeOwnershipGini to use math/big.Int exclusively until the one final
    division, eliminating float64 arithmetic from this loop entirely --
    the FMA-fusion class this golden existed to catch is now structurally
    impossible at that site, not merely guarded, and #2123's own
    pysum_golden.json (`gini`/`gini_multi_row` families) already covers
    CodeOwnershipGini's correctness at a scale (magnitudes past 2**53 and
    2**63) this golden never attempted. Keeping a redundant golden for a
    site with no remaining float64 arithmetic would test nothing new.

Same discipline as generate_fma_golden.py: IMPORTS and calls the real
production functions, never reimplements their formulas; `expected_bits`
is the live CPython IEEE-754 bit pattern, so the Go test asserts bit-for-bit
equality, never "fused != unfused" (a no-op assertion on amd64).
"""

from __future__ import annotations

import argparse
import json
import struct
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.hotspots import compute_file_risk_hotspots
from dev_health_ops.metrics.schemas import CommitStatRow, FileComplexitySnapshot

OUTPUT = Path(__file__).with_name("fma_followup_golden.json")
REPO_ID = uuid.UUID("00000000-0000-4000-8000-00000000000b")
DAY = date(2026, 8, 1)
COMPUTED_AT = datetime(2026, 8, 1, tzinfo=timezone.utc)


def bits_hex(value: float) -> str:
    """The IEEE-754 bit pattern of `value`, as `math.Float64bits` would print it."""
    return "0x" + format(struct.unpack(">Q", struct.pack(">d", value))[0], "016x")


def _commit_row(path: str, additions: int, deletions: int, index: int) -> CommitStatRow:
    return CommitStatRow(
        repo_id=REPO_ID,
        commit_hash=f"c{path}{index}",
        author_email=f"a{index}@example.com",
        author_name=None,
        committer_when=COMPUTED_AT,
        file_path=path,
        additions=additions,
        deletions=deletions,
    )


def _complexity(path: str, total: int, avg: float) -> FileComplexitySnapshot:
    return FileComplexitySnapshot(
        repo_id=REPO_ID,
        as_of_day=DAY,
        ref="main",
        file_path=path,
        language="go",
        loc=100,
        functions_count=5,
        cyclomatic_total=total,
        cyclomatic_avg=avg,
        high_complexity_functions=0,
        very_high_complexity_functions=0,
        computed_at=COMPUTED_AT,
    )


def _hotspot_risk_score_cases() -> list[dict[str, Any]]:
    # Every case holds complexity CONSTANT across all files in that case
    # (see module docstring): isolates z_churn as the only nonzero term in
    # risk_score, which is exactly what sampleZScores/get_z_scores compute.
    churn_sets = [
        [5, 13, 41],
        [
            1,
            1,
            1,
            1,
        ],  # zero variance in churn too -- both terms 0.0, still exercises the n>=2/stdev==0 branch pair
        [100, 250, 999, 17, 3],
        [7, 7, 8],  # near-zero-variance, stresses the subtraction in (x - mean)
        [2, 4, 8, 16, 32, 64, 128],
        [999983, 17, 5, 999979],  # large-vs-small magnitude spread
        [123, 456, 789, 1011, 1213, 1415, 1617],
        [0, 0, 5],
        [10**6, 3, 3, 3, 3],
    ]
    cases: list[dict[str, Any]] = []
    for set_index, churns in enumerate(churn_sets):
        window_stats = []
        for file_index, churn in enumerate(churns):
            path = f"src/f{file_index}.go"
            # additions/deletions split so max(0, additions)+max(0, deletions) == churn.
            window_stats.append(_commit_row(path, churn, 0, file_index))
        complexity_map = {
            f"src/f{file_index}.go": _complexity(
                f"src/f{file_index}.go", total=5, avg=1.0
            )
            for file_index in range(len(churns))
        }
        records = compute_file_risk_hotspots(
            repo_id=REPO_ID,
            day=DAY,
            window_stats=window_stats,
            complexity_map=complexity_map,
            blame_map=None,
            computed_at=COMPUTED_AT,
        )
        by_path = {r.file_path: r for r in records}
        for file_index, churn in enumerate(churns):
            path = f"src/f{file_index}.go"
            cases.append(
                {
                    "case": f"set{set_index}_file{file_index}",
                    "churns": churns,
                    "file_index": file_index,
                    "expected_bits": bits_hex(by_path[path].risk_score),
                }
            )
    return cases


def render() -> str:
    value = {
        "schema_version": 1,
        "hotspot_risk_score": _hotspot_risk_score_cases(),
    }
    return (
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"
    )


def main() -> int:
    # Same --check/--stdout convention as generate_fma_golden.py: --stdout
    # renders to stdout without writing OUTPUT (the live rot guard,
    # fma_followup_golden_rot_guard_test.go, uses this to compare TODAY's
    # production Python against the frozen file byte-for-byte); --check
    # exits nonzero if OUTPUT would change; the default writes OUTPUT.
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.stdout:
        print(rendered, end="")
        return 0
    if args.check:
        return 0 if OUTPUT.exists() and OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    print(f"wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
