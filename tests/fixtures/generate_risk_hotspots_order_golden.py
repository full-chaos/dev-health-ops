"""Golden generator for CHAOS-4863: ComputeFileRiskHotspots' risk_score must
not depend on iteration order.

Background (recorded on the ticket): compute_file_risk_hotspots
(hotspots.py:151) builds its file list from `set(churn_map.keys()) |
set(complexity_map.keys())` -- a Python SET, not a dict. CPython's set
iteration order for string keys depends on hash randomization
(PYTHONHASHSEED unset by default) and genuinely differs across separate
process invocations -- there is no fixed "Python's insertion order" here to
replicate. What IS true, verified empirically: CPython's Neumaier-
compensated sum() (CHAOS-4824) produces bit-identical risk_score values
regardless of that varying set order, for every case measured so far.

This generator does not just trust that finding once -- it RE-VERIFIES it
for every case in this corpus, live, at generation time: each case is run
through several SEPARATE `python3` subprocess invocations (fresh hash seed
each time, confirmed by construction -- os.fork() would share the parent's
seed, subprocess.run() does not), each with the SAME churn/complexity
values fed through a DIFFERENT window_stats/complexity_map row order (a
permutation of the same content, not just a different process). If any two
of those invocations disagree, this generator RAISES rather than silently
picking one -- an order-dependence in Python's own output would be exactly
the finding CHAOS-4863's ticket ruling anticipated ("one divergent case is
a finding to report, not a flake to retry"), and freezing a golden built on
top of it would hide that finding instead of surfacing it.

The Go side (ComputeFileRiskHotspots) is fixed to sort the union of file
paths lexicographically before summing -- a canonical order chosen for
being independent of any source ordering, not for matching Python's (which
has none to match). TestComputeFileRiskHotspotsOrderInvariantMatchesLivePythonBitExact
feeds the SAME churn/complexity content through several DIFFERENT
windowStats/complexityMap construction orders on the Go side too, proving
Go's own result is invariant under both Go's map randomization AND input
permutation, and matches the (also order-invariant, per the above)
Python value.
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
from pathlib import Path
from typing import Any

OUTPUT = Path(__file__).with_name("risk_hotspots_order_golden.json")
REPO_ID = "00000000-0000-4000-8000-00000000000d"
DAY = "2026-08-01"

# How many separate `python3` subprocess invocations verify each case
# (fresh hash seed each time) before its expected_bits is trusted.
CROSS_PROCESS_CONFIRMATIONS = 6

_WORKER = r"""
import json, struct, sys, uuid
from datetime import date, datetime, timezone
from dev_health_ops.metrics.hotspots import compute_file_risk_hotspots
from dev_health_ops.metrics.schemas import CommitStatRow, FileComplexitySnapshot

def bits_hex(value):
    return "0x" + format(struct.unpack(">Q", struct.pack(">d", value))[0], "016x")

payload = json.load(sys.stdin)
repo_id = uuid.UUID(payload["repo_id"])
day = date.fromisoformat(payload["day"])
computed_at = datetime.fromisoformat(payload["computed_at"])

window_stats = []
for entry in payload["window_stats"]:
    window_stats.append(CommitStatRow(
        repo_id=repo_id, commit_hash=entry["commit_hash"], author_email=entry["author_email"],
        author_name=None, committer_when=computed_at, file_path=entry["file_path"],
        additions=entry["additions"], deletions=0,
    ))

complexity_map = {}
for path, spec in payload["complexity_map"].items():
    complexity_map[path] = FileComplexitySnapshot(
        repo_id=repo_id, as_of_day=day, ref="main", file_path=path, language="go",
        loc=100, functions_count=5, cyclomatic_total=spec["total"], cyclomatic_avg=spec["avg"],
        high_complexity_functions=0, very_high_complexity_functions=0, computed_at=computed_at,
    )

records = compute_file_risk_hotspots(
    repo_id=repo_id, day=day, window_stats=window_stats, complexity_map=complexity_map,
    blame_map=None, computed_at=computed_at,
)
by_path = {r.file_path: r for r in records}
out = {path: bits_hex(r.risk_score) for path, r in by_path.items()}
json.dump(out, sys.stdout)
"""


def _run_once(payload: dict[str, Any]) -> dict[str, str]:
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parents[2],
    )
    return json.loads(proc.stdout)


def _case_payload(case_id: str, churns: dict[str, int], row_order: list[str]) -> dict[str, Any]:
    window_stats = [
        {"commit_hash": f"c{i}", "author_email": f"a{i}@example.com", "file_path": path, "additions": churns[path]}
        for i, path in enumerate(row_order)
    ]
    complexity_map = {path: {"total": 5, "avg": 1.0} for path in churns}
    return {
        "repo_id": REPO_ID,
        "day": DAY,
        "computed_at": f"{DAY}T00:00:00+00:00",
        "window_stats": window_stats,
        "complexity_map": complexity_map,
    }


def _verified_case(case_id: str, churns: dict[str, int], rng: random.Random) -> dict[str, Any]:
    """Runs CROSS_PROCESS_CONFIRMATIONS separate python3 invocations, each
    with a DIFFERENT permutation of window_stats row order, and asserts
    every one agrees bit-exactly before returning a golden entry. Raises
    (does not silently pick a value) on any disagreement -- see module
    docstring."""
    paths = list(churns.keys())
    observed: dict[str, set[str]] = {path: set() for path in paths}
    row_orders_used = []
    for _ in range(CROSS_PROCESS_CONFIRMATIONS):
        row_order = paths[:]
        rng.shuffle(row_order)
        row_orders_used.append(row_order)
        payload = _case_payload(case_id, churns, row_order)
        result = _run_once(payload)
        for path in paths:
            observed[path].add(result[path])

    divergent = {path: bits for path, bits in observed.items() if len(bits) > 1}
    if divergent:
        raise SystemExit(
            f"case {case_id}: Python's own risk_score is ORDER-DEPENDENT for "
            f"{len(divergent)} file(s) across {CROSS_PROCESS_CONFIRMATIONS} "
            f"permutations -- this contradicts the empirical finding CHAOS-4863's "
            f"fix relies on. Report this, do not silently freeze a golden over it. "
            f"Divergent: {divergent}"
        )

    return {
        "case": case_id,
        "churns": churns,
        "expected_bits": {path: next(iter(bits)) for path, bits in observed.items()},
    }


def _cases() -> list[dict[str, Any]]:
    rng = random.Random(20260902)
    specs: list[tuple[str, dict[str, int]]] = []

    # Cardinality sweep, magnitudes small and uniform.
    for n in (2, 3, 5, 10, 25, 60):
        churns = {f"src/f{i}.go": rng.randint(1, 100) for i in range(n)}
        specs.append((f"cardinality{n}", churns))

    # Magnitude spread: tiny alongside huge, the shape most likely to stress
    # compensated-sum order sensitivity (values crossing magnitude
    # "shells" depending on accumulation order).
    for n in (8, 20, 45):
        churns = {
            f"src/g{i}.go": rng.choice(
                [rng.randint(1, 9), rng.randint(1, 10**9), rng.randint(1, 50), rng.randint(10**6, 10**7)]
            )
            for i in range(n)
        }
        specs.append((f"magnitude_spread{n}", churns))

    # Near-duplicate paths (byte-order sort must still separate them
    # correctly) and a case with a path prefix collision.
    specs.append(("path_prefixes", {
        "src/a.go": 5, "src/aa.go": 13, "src/ab.go": 41, "src/b.go": 7,
        "src/z.go": 999983, "src/Z.go": 3,  # uppercase sorts before lowercase in byte order
    }))

    return [_verified_case(case_id, churns, rng) for case_id, churns in specs]


def render() -> str:
    value = {"schema_version": 1, "cases": _cases()}
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"


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
        return 0 if OUTPUT.exists() and OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    print(f"wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
