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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--out", default="tests/fixtures/pr_dependency_id_parity.json")
    namespace = parser.parse_args()

    document = {
        "schema": "pr_dependency_id_parity.v1",
        "producer": "work_graph/builder.py::_parse_pr_dependency_source",
        "generated_by": "tests/fixtures/generate_pr_dependency_id_parity.py",
        "note": (
            "Expectations are the reference's OWN output, not transcribed. "
            "`raises` is a real recorded outcome: Python's isdigit() accepts "
            "characters int() rejects, and the conversion is unguarded "
            "(CHAOS-4811)."
        ),
        "observations": [observe(value) for value in CORPUS],
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
