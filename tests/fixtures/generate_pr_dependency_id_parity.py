"""Freeze what the DEPLOYED `_parse_pr_dependency_source` actually does.

This corpus exists because the Go port of that function decides WHICH PIPELINE
OWNS A ROW: a PR-shaped id is skipped by the issue<->issue build on the
assumption the mapping writer claims it. A divergence therefore does not produce
a wrong edge, it produces a row one pipeline skips and the other never sees,
while each pipeline's own read == written + rejected still balances.

# WHY THIS IS GENERATED RATHER THAN WRITTEN

The Go test previously carried Python's behaviour as hand-transcribed strings,
measured once and typed in. That asserts the author's transcription, not the
reference: a mistyped expectation is indistinguishable from a correct one, and it
never goes stale loudly. Here the reference is IMPORTED and CALLED, so the
expectations are its output.

The function is a pure staticmethod over a string, so this needs no database and
runs anywhere the package imports.

    python tests/fixtures/generate_pr_dependency_id_parity.py --stdout
"""

from __future__ import annotations

import argparse
import json
import sys

sys.path.insert(0, "/app/src")

from dev_health_ops.work_graph.builder import WorkGraphBuilder  # noqa: E402

# The corpus varies the SHAPE of the id, not only its values: prefix present or
# absent, separator present/absent/wrong-for-the-provider, slug empty or
# containing the separator, and a number that is ASCII / non-ASCII decimal /
# Numeric_Type=Digit-but-not-decimal / signed / zero / empty / whitespace.
#
# The last group is the one that matters: Python's `isdigit()` guard and its
# `int()` conversion do not accept the same characters, and Go's strconv.Atoi
# accepts a third set. A corpus that varied only "valid vs invalid" would miss
# every one of those.
CORPUS = [
    "ghpr:owner/repo#5",
    "gitlab:group/proj!42",
    "ghpr:o/r#-5",
    "ghpr:o/r#+5",
    "ghpr:o/r#٥",  # Arabic-Indic five: isdigit yes, int yes
    "ghpr:o/r#５",  # fullwidth five: isdigit yes, int yes
    "ghpr:o/r#²",  # superscript two: isdigit yes, int RAISES
    "ghpr:o/r#5²",  # mixed decimal + superscript
    "linear:CHAOS-4766",
    "",
    "ghpr:",
    "ghpr:owner/repo",
    "ghpr:#5",
    "ghpr:o/r#",
    "ghpr:o/r#0",
    "ghpr:o/r# 5",
    "ghpr:o/r#x#7",  # slug contains the separator; rsplit takes the LAST
    "ghpr:o/r!5",  # gitlab separator inside a github id
    "gitlab:g/p#5",  # github separator inside a gitlab id
    "GHPR:o/r#5",  # prefix case: startswith is case-SENSITIVE
]


def observe(value: str) -> dict:
    """Call the reference and record what it did, including a raise."""
    try:
        parsed = WorkGraphBuilder._parse_pr_dependency_source(value)
    except Exception as exc:  # noqa: BLE001 - recording the raise IS the point
        return {"input": value, "outcome": "raises", "exception": type(exc).__name__}
    if parsed is None:
        return {"input": value, "outcome": "none"}
    repo_slug, pr_number, provider = parsed
    return {
        "input": value,
        "outcome": "parsed",
        "repo_slug": repo_slug,
        "pr_number": pr_number,
        "provider": provider,
    }


# MAGNITUDE. Added after codex round 1 found a P2 this corpus could not see:
# every positive id here was small (the largest was 42), so a Go-side bound at
# 1<<31 rejected `ghpr:o/r#3000000000` as malformed while the reference parsed
# it happily. Python's ints are arbitrary-precision, so magnitude is NEVER a
# rejection reason -- there is no large value `int()` refuses.
#
# The all-zero entries matter as much as the huge ones: `int(...) > 0` is FALSE
# for them, so the reference returns None (a silent skip), which no bounded
# conversion distinguishes from "too large to represent".
CORPUS += [
    "ghpr:o/r#2147483647",  # int32 max
    "ghpr:o/r#2147483648",  # int32 max + 1
    "ghpr:o/r#3000000000",  # the round-1 failing input
    "ghpr:o/r#9223372036854775807",  # int64 max
    "ghpr:o/r#9223372036854775808",  # int64 max + 1: real to Python, not representable in Go
    "ghpr:o/r#" + "9" * 40,  # far beyond any fixed-width integer
    "ghpr:o/r#" + "0" * 40,  # not positive -> None, not an error
    "ghpr:o/r#0",  # the same rule at length 1
    "ghpr:o/r#" + "9" * 40 + "\u00b2",  # huge AND non-decimal: int() still raises
    # CPython's own bound, found by codex round 2 AFTER round 1 removed a bound
    # this port had invented. `sys.get_int_max_str_digits()` defaults to 4300;
    # above it `int()` raises, so these two entries sit either side of a real
    # cliff in the reference. Removing my bound without checking whether the
    # reference had one of its own was an over-correction, and the corpus is
    # where that gets caught rather than argued.
    "ghpr:o/r#" + "9" * 4300,  # exactly at the limit: converts
    "ghpr:o/r#" + "9" * 4301,  # one past: RAISES
    "ghpr:o/r#" + "0" * 4301,  # leading zeros still count toward it
]

# ENDPOINT_CORPUS varies the third axis: WHICH FIELD carries the value.
#
# The single-value corpus above cannot see this axis at all. builder.py:871-874
# is one expression over TWO fields:
#
#     if self._parse_pr_dependency_source(source_id) or self._parse_pr_dependency_source(target_id):
#         continue
#
# `or` SHORT-CIRCUITS, so the two endpoints are not interchangeable. A valid PR
# in the source means the target is never parsed -- and therefore a malformed
# target that would otherwise raise is masked entirely. Which field carries the
# value decides whether the build survives.
#
# The frozen golden cannot exercise this: it holds 2828 PR-shaped sources and
# ZERO PR-shaped targets, so every source-only implementation reproduces it
# perfectly. Measured, not assumed.
#
# What is frozen here is the builder's OWN expression, evaluated over pairs --
# the combined admission, not the single-field parse.
ISSUE = "gh:acme/app#41"
PR = "ghpr:acme/app#7"
BAD = "ghpr:acme/app#5\u00b2"  # isdigit() accepts, int() rejects

ENDPOINT_CORPUS = [
    (ISSUE, ISSUE),
    (PR, ISSUE),
    (ISSUE, PR),
    (PR, PR),
    # The masking case: source parses truthy, so the malformed target is never
    # reached. Python SKIPS this row rather than raising.
    (PR, BAD),
    # No short circuit: the source is falsy, so the malformed target is reached.
    (ISSUE, BAD),
    # The source raises before the target is looked at, whatever it holds.
    (BAD, ISSUE),
    (BAD, PR),
    (BAD, BAD),
    ("", BAD),
    (BAD, ""),
]


def observe_pair(source: str, target: str) -> dict:
    """Evaluate builder.py:871-874's expression verbatim over one row."""
    record = {"source": source, "target": target}
    try:
        skipped = bool(
            WorkGraphBuilder._parse_pr_dependency_source(source)
            or WorkGraphBuilder._parse_pr_dependency_source(target)
        )
    except ValueError as error:
        record["outcome"] = "raises"
        record["exception"] = f"{type(error).__name__}: {error}"
        return record
    record["outcome"] = "skipped" if skipped else "kept"
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--out", default="tests/fixtures/pr_dependency_id_parity.json")
    namespace = parser.parse_args()

    document = {
        "schema": "pr_dependency_id_parity.v2",
        "producer": "work_graph/builder.py::_parse_pr_dependency_source",
        "generated_by": "tests/fixtures/generate_pr_dependency_id_parity.py",
        "note": (
            "Expectations are the reference's OWN output, not transcribed. "
            "`raises` is a real recorded outcome: Python's isdigit() accepts "
            "characters int() rejects, and the conversion is unguarded "
            "(CHAOS-4811)."
        ),
        "observations": [observe(value) for value in CORPUS],
        "endpoint_observations": [
            observe_pair(source, target) for source, target in ENDPOINT_CORPUS
        ],
    }
    rendered = json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    if namespace.stdout:
        sys.stdout.write(rendered)
    else:
        with open(namespace.out, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        print(f"wrote {namespace.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
