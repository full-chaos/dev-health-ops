#!/usr/bin/env python3
"""Regenerate ci/venue_oracle_weights.tsv from saved venue-oracles run logs (CHAOS-6891).

    gh run view <RUN_ID> --repo <owner>/<repo> --log > run.log
    python3 ci/venue_oracle_weights.py run.log [more.log ...] > /tmp/weights.tsv
    mv /tmp/weights.tsv ci/venue_oracle_weights.tsv

ci/venue_oracle_shard.awk balances the venue-oracles legs by the seconds this
file records for each registry `run` row. Input: the `go test -v` output of the
legs (a hosted run's `gh run view --log`, whose lines carry a
`job<TAB>step<TAB>timestamp ` prefix, or plain output). Each `--- PASS|FAIL:
TestX (12.3s)` line is one test's time, credited to the package whose
`ok  <module>/<package>  N.NNNs` line follows it. A test seen in several logs
keeps its LARGEST time (a slow runner is the case to plan for); a registry row
no log shows keeps the weight already in the file, else the default (60s) and
is listed on stderr as unmeasured. Rows whose test left the registry are
dropped. A wrong weight costs balance, never coverage.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEIGHTS = ROOT / "ci" / "venue_oracle_weights.tsv"
REGISTRY_DIR = ROOT / "ci" / "venue_oracle_registry.d"
DEFAULT_WEIGHT = 60

HEADER = """\
# Measured seconds of one venue-oracles registry `run` row (CHAOS-6891): the weight
# ci/venue_oracle_shard.awk balances the venue legs by (longest-processing-time
# assignment). Columns: package dir, test, seconds. Regenerate from a saved run log:
#   gh run view <RUN_ID> --log > run.log && python3 ci/venue_oracle_weights.py run.log
# A stale weight costs balance, never coverage (the legs always partition the run
# rows); tests/tooling/test_venue_oracle_shards.py fails a PR whose registry has a run
# row with no weight here, a weight for a test that left the registry, or a heaviest
# leg over the leg's test-time budget. A row no log has measured carries the default
# (60). Sorted (LC_ALL=C) by package, then test.
"""

_PREFIX = re.compile(r"^([^\t]*)\t[^\t]*\t\S+Z (.*)$")
_TEST = re.compile(r"^\s*--- (?:PASS|FAIL): (Test\w+) \(([\d.]+)s\)$")
_PACKAGE = re.compile(r"^(?:ok|FAIL)\s+\S+?/dev-health-ops/(\S+)\s+[\d.]+s")


def parse_log(lines: list[str], seen: dict[tuple[str, str], float]) -> None:
    """Add each test's seconds (the largest across calls) to `seen`."""
    pending: dict[str, list[tuple[str, float]]] = {}
    for raw in lines:
        line = raw.rstrip("\n")
        job = ""
        match = _PREFIX.match(line)
        if match:
            job, line = match.group(1), match.group(2)
        test = _TEST.match(line)
        if test:
            pending.setdefault(job, []).append((test.group(1), float(test.group(2))))
            continue
        package = _PACKAGE.match(line)
        if package:
            for name, seconds in pending.pop(job, []):
                key = (package.group(1), name)
                seen[key] = max(seen.get(key, 0.0), seconds)


def registry_run_rows() -> list[tuple[str, str]]:
    rows = []
    for path in sorted(REGISTRY_DIR.glob("*.tsv")):
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = line.split("\t")
            if len(parts) == 3 and parts[2] == "run":
                rows.append((parts[0], parts[1]))
    return sorted(rows)


def read_weights(path: Path) -> dict[tuple[str, str], int]:
    weights: dict[tuple[str, str], int] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            package, test, seconds = line.split("\t")
            weights[(package, test)] = int(seconds)
    return weights


def build(
    rows: list[tuple[str, str]],
    seen: dict[tuple[str, str], float],
    existing: dict[tuple[str, str], int],
) -> tuple[str, list[tuple[str, str]]]:
    """The file text for `rows`, and the rows nothing measured."""
    out = [HEADER]
    unmeasured = []
    for row in sorted(rows):
        if row in seen:
            seconds = max(1, math.ceil(seen[row]))
        elif row in existing:
            seconds = existing[row]
        else:
            seconds = DEFAULT_WEIGHT
            unmeasured.append(row)
        out.append(f"{row[0]}\t{row[1]}\t{seconds}\n")
    return "".join(out), unmeasured


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("logs", nargs="+", type=Path, help="saved `go test -v` / `gh run view --log` files")
    parser.add_argument("--weights", type=Path, default=WEIGHTS, help="the existing file (kept for unmeasured rows)")
    args = parser.parse_args(argv)
    seen: dict[tuple[str, str], float] = {}
    for log in args.logs:
        parse_log(log.read_text(encoding="utf-8", errors="replace").splitlines(), seen)
    rows = registry_run_rows()
    text, unmeasured = build(rows, seen, read_weights(args.weights))
    sys.stdout.write(text)
    measured = sum(1 for row in rows if row in seen)
    print(f"venue_oracle_weights: {measured} of {len(rows)} run rows measured from {len(args.logs)} log(s); "
          f"{len(unmeasured)} unmeasured", file=sys.stderr)
    for package, test in unmeasured:
        print(f"  unmeasured (default {DEFAULT_WEIGHT}s): {package} {test}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
