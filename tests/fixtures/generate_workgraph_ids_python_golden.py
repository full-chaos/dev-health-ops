#!/usr/bin/env python3
"""Regenerate the parse_pr_from_id/parse_commit_from_id golden (CHAOS-4441).

``work_graph/ids.py:189-227`` splits a canonical PR/commit id on a fixed
separator and parses each half with ``uuid.UUID()``/``int()``. This corpus
IMPORTS both functions rather than reimplementing their logic -- the exact
audit question CHAOS-4803 asks of every golden generator in this port (see
plan.md section 5b: a generator that imitates instead of importing cannot
detect a change to the real function).

Axes varied: separator count (0, 1, 2 occurrences -- Python's str.split
splits on EVERY occurrence, so 2 occurrences produces 3 parts and fails the
same way 0 occurrences does), the repo-id half's UUID acceptability, and the
number/hash half's shape -- including forms int() accepts that a naive Go
port would not (a leading sign, leading zeros, PEP 515 digit-group
underscores).

Usage:
    python tests/fixtures/generate_workgraph_ids_python_golden.py            # rewrite
    python tests/fixtures/generate_workgraph_ids_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dev_health_ops.work_graph.ids import (  # noqa: E402
    parse_commit_from_id,
    parse_pr_from_id,
)

# Discovered by internal/jobs/workgraph/units/live_python_corpus_guard_test.go's
# TestEveryDiscoverableCorpusStillMatchesLivePython via its OUTPUT_PATH
# declaration pattern below -- do not rename the variable or switch back to
# a fully-qualified pathlib.Path(...) spelling; either would drop this
# generator out of discovery and into the maxUnguardableGenerators ratchet,
# which is already at its ceiling. Do NOT quote the assignment line itself
# in a comment anywhere in this file -- the discovery regex has no comment
# awareness and matches the first quoted occurrence it finds byte-for-byte,
# comment or not.
OUTPUT_PATH = Path(__file__).parent / "workgraph_ids_python_golden.json"

REPO = "7b9583ee-1234-4a12-8abc-34f815bebdd7"
REPO_UPPER = "7B9583EE-1234-4A12-8ABC-34F815BEBDD7"
REPO_BRACED = "{7b9583ee-1234-4a12-8abc-34f815bebdd7}"

PR_CASES = [
    ("basic", f"{REPO}#pr42"),
    ("uppercase_repo", f"{REPO_UPPER}#pr42"),
    ("braced_repo", f"{REPO_BRACED}#pr42"),
    ("no_separator", f"{REPO}issue42"),
    ("separator_twice", f"{REPO}#pr4#pr2"),
    ("empty_string", ""),
    ("invalid_repo", "not-a-uuid#pr42"),
    ("leading_zeros_number", f"{REPO}#pr0042"),
    ("underscored_number", f"{REPO}#pr1_000"),
    ("negative_number", f"{REPO}#pr-5"),
    ("plus_signed_number", f"{REPO}#pr+5"),
    ("non_numeric_number", f"{REPO}#prabc"),
    ("empty_number", f"{REPO}#pr"),
    ("whitespace_number", f"{REPO}#pr 42 "),
]

COMMIT_CASES = [
    ("basic", f"{REPO}@abc123def456"),
    ("uppercase_repo", f"{REPO_UPPER}@abc123def456"),
    ("braced_repo", f"{REPO_BRACED}@abc123def456"),
    ("no_separator", f"{REPO}abc123def456"),
    ("separator_twice", f"{REPO}@abc@123"),
    ("empty_string", ""),
    ("invalid_repo", "not-a-uuid@abc123def456"),
    ("empty_hash", f"{REPO}@"),
    ("short_hash", f"{REPO}@a"),
]


def _pr_result(pr_id: str) -> dict[str, object]:
    repo_id, number = parse_pr_from_id(pr_id)
    return {
        "repo_id": str(repo_id) if repo_id is not None else None,
        "number": number,
    }


def _commit_result(commit_id: str) -> dict[str, object]:
    repo_id, commit_hash = parse_commit_from_id(commit_id)
    return {
        "repo_id": str(repo_id) if repo_id is not None else None,
        "hash": commit_hash,
    }


def build_golden() -> dict[str, object]:
    return {
        "pr_cases": [
            {"label": label, "input": value, "expected": _pr_result(value)}
            for label, value in PR_CASES
        ],
        "commit_cases": [
            {"label": label, "input": value, "expected": _commit_result(value)}
            for label, value in COMMIT_CASES
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()

    golden = build_golden()
    text = json.dumps(golden, indent=2, sort_keys=True) + "\n"

    if args.stdout:
        sys.stdout.write(text)
        return 0

    OUTPUT_PATH.write_text(text)
    print(f"wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
