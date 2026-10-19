#!/usr/bin/env python3
"""Regenerate ci/venue_oracle_weights.d/ from saved venue-oracles run logs (CHAOS-6891).

    gh run view <RUN_ID> --repo <owner>/<repo> --log > run.log
    python3 ci/venue_oracle_weights.py run.log [more.log ...]

It rewrites the per-package files of ci/venue_oracle_weights.d/ in place (CHAOS-6926:
one file per package, named like the registry's, so PRs that add venue rows in
different packages never conflict) and drops the file of a package that has no
registry run row left.

ci/venue_oracle_shard.awk balances the venue-oracles legs by the seconds this
file records for each registry `run` row. Input: the `go test -v` output of the
legs (a hosted run's `gh run view --log`, whose lines carry a
`job<TAB>step<TAB>timestamp ` prefix, or plain output). Each `--- PASS|FAIL:
TestX (12.3s)` line is one test's time, credited to the package whose
`ok  <module>/<package>  N.NNNs` line follows it. A test seen in several logs
keeps its LARGEST time (a slow runner is the case to plan for); a registry row
no log shows keeps the weight already in the file, else the provisional 600s marked
`unmeasured` (listed on stderr). With no log at all it only syncs the file with the
registry: it adds a new run row as unmeasured and drops a row that left. Rows whose test left the registry are
dropped. A wrong weight costs balance, never coverage.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEIGHTS = ROOT / "ci" / "venue_oracle_weights.d"
REGISTRY_DIR = ROOT / "ci" / "venue_oracle_registry.d"
# A registry row nobody has measured yet (a new test: venue oracles only run on
# main, so its author cannot measure it) is planned PESSIMISTICALLY and flagged
# `unmeasured` in the file. 600 s is above the slowest row ever measured (560 s,
# run 36230983526): the first plan of CHAOS-6891 gave two unmeasured rows the
# default 60 s and one of them took 560 s, which made its leg 587 s slower than
# planned (2109 s of tests against a 2400 s cap). The next run's log replaces it.
PROVISIONAL_WEIGHT = 600
UNMEASURED = "unmeasured"

HEADER = """\
# Measured seconds of one venue-oracles registry `run` row (CHAOS-6891): the weight
# ci/venue_oracle_shard.awk balances the venue legs by (longest-processing-time
# assignment). Columns: package dir, test, seconds. One file per package in
# ci/venue_oracle_weights.d/ (CHAOS-6926). Regenerate from a saved run log:
#   gh run view <RUN_ID> --log > run.log && python3 ci/venue_oracle_weights.py run.log
# A stale weight costs balance, never coverage (the legs always partition the run
# rows); tests/tooling/test_venue_oracle_shards.py fails a PR whose registry has a run
# row with no weight here, a weight for a test that left the registry, or a heaviest
# leg over the leg's test-time budget. A row no log has measured carries the
# provisional weight (600, above the slowest row ever measured) and the marker
# `unmeasured` in a fourth column, until a run log measures it. Sorted (LC_ALL=C)
# by package, then test.
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


def weight_files(path: Path) -> list[Path]:
    """The *.tsv row files of a weights directory (C-locale order), or the one file."""
    if path.is_dir():
        return sorted(path.glob("*.tsv"), key=lambda p: p.name.encode())
    return [path] if path.exists() else []


def read_weights(path: Path) -> dict[tuple[str, str], tuple[int, bool]]:
    """{(package, test): (seconds, marked unmeasured)} from a directory or one file."""
    weights: dict[tuple[str, str], tuple[int, bool]] = {}
    for file in weight_files(path):
        for line in file.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split("\t")
            weights[(fields[0], fields[1])] = (
                int(fields[2]),
                len(fields) > 3 and fields[3] == UNMEASURED,
            )
    return weights


def file_name(package: str) -> str:
    """The row file of a package: the registry's spelling, "/" written "__"."""
    return package.replace("/", "__") + ".tsv"


def split_by_package(text: str) -> dict[str, str]:
    """{file name: rows} of a build() text: the header stays in README.md."""
    files: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        files.setdefault(file_name(line.split("\t")[0]), []).append(line + "\n")
    return {name: "".join(rows) for name, rows in files.items()}


def write_weights(directory: Path, text: str) -> None:
    """Replace the directory's row files with the rows of `text`, one file per package."""
    directory.mkdir(parents=True, exist_ok=True)
    files = split_by_package(text)
    for old in directory.glob("*.tsv"):
        if old.name not in files:
            old.unlink()
    for name, rows in files.items():
        (directory / name).write_text(rows, encoding="utf-8")


def build(
    rows: list[tuple[str, str]],
    seen: dict[tuple[str, str], float],
    existing: dict[tuple[str, str], tuple[int, bool]],
) -> tuple[str, list[tuple[str, str]]]:
    """The file text for `rows`, and the rows nothing measured."""
    out = [HEADER]
    unmeasured = []
    for row in sorted(rows):
        marker = ""
        if row in seen:
            seconds = max(1, math.ceil(seen[row]))
        elif row in existing and not existing[row][1]:
            seconds = existing[row][0]
        else:
            seconds = existing[row][0] if row in existing else PROVISIONAL_WEIGHT
            marker = f"\t{UNMEASURED}"
            unmeasured.append(row)
        out.append(f"{row[0]}\t{row[1]}\t{seconds}{marker}\n")
    return "".join(out), unmeasured


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "logs",
        nargs="*",
        type=Path,
        help="saved `go test -v` / `gh run view --log` files",
    )
    parser.add_argument(
        "--weights",
        type=Path,
        default=WEIGHTS,
        help="the weights directory (or one file): read for the unmeasured rows, rewritten in place",
    )
    args = parser.parse_args(argv)
    seen: dict[tuple[str, str], float] = {}
    for log in args.logs:
        parse_log(log.read_text(encoding="utf-8", errors="replace").splitlines(), seen)
    rows = registry_run_rows()
    text, unmeasured = build(rows, seen, read_weights(args.weights))
    write_weights(args.weights, text)
    measured = sum(1 for row in rows if row in seen)
    print(
        f"venue_oracle_weights: {measured} of {len(rows)} run rows measured from {len(args.logs)} log(s); "
        f"{len(unmeasured)} unmeasured",
        file=sys.stderr,
    )
    for package, test in unmeasured:
        print(
            f"  unmeasured (provisional {PROVISIONAL_WEIGHT}s): {package} {test}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
