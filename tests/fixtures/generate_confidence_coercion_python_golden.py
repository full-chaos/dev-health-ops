#!/usr/bin/env python3
"""Regenerate the edge-confidence string-coercion golden (CHAOS-4441).

``components.py:72-83 _edge_confidence`` coerces whatever the ClickHouse driver
hands it into a float, and its string branch is a bare ``float(value)`` with
``ValueError -> 0.0``.  The Go port must agree on EVERY form, because the
coerced value decides which edges the oversized-component split protects, which
decides component membership, which decides ``work_unit_id`` -- and that id
addresses rows in two tables written by two different jobs.

WHY A GENERATED CORPUS RATHER THAN A HAND-WRITTEN TABLE
    Two hand-written matrices already missed real divergences on this exact
    function.  ``strconv.ParseFloat`` is not a drop-in for ``float()`` in either
    direction: it is BROADER (it accepts C99 hex literals -- ``float("0x1p2")``
    raises in Python, ParseFloat returns 4) and NARROWER (it reports ErrRange
    for magnitudes Python saturates -- ``float("1e309")`` is ``inf``).  Guessing
    which forms matter is what let both through.  This corpus enumerates the
    forms instead, and its expectations come from the interpreter rather than
    from anyone's reading of the docs.

Results are rendered as STRINGS, not JSON numbers: ``inf`` and ``nan`` have no
JSON representation, and they are exactly the values that matter here.

Usage:
    python tests/fixtures/generate_confidence_coercion_python_golden.py            # rewrite
    python tests/fixtures/generate_confidence_coercion_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys

GOLDEN_PATH = (
    pathlib.Path(__file__).resolve().parent / "confidence_coercion_python_golden.json"
)

# Ordinary values, then every form known to separate the two parsers, then the
# junk a mis-typed or mis-migrated column could realistically hold.
CORPUS: list[str] = [
    # plain numbers
    "0",
    "1",
    "2",
    "0.5",
    "1.0",
    "0.9",
    "12.5",
    "+3",
    "-1",
    ".5",
    "5.",
    "+.5",
    "-.5",
    "1.",
    "017",
    # exponent forms
    "1e3",
    "1E3",
    "1e-3",
    # range boundaries: Python saturates, Go reports ErrRange
    "1e309",
    "-1e309",
    "1e-400",
    # non-finite words
    "inf",
    "Inf",
    "INF",
    "infinity",
    "Infinity",
    "-inf",
    "+inf",
    "nan",
    "NaN",
    "NAN",
    "-nan",
    # hexadecimal and other bases: Go accepts hex floats, Python accepts none
    "0x1p2",
    "0X1P2",
    "0x1.8p1",
    "-0x1p2",
    "0b101",
    "0o17",
    # PEP 515 digit separators
    "1_0",
    "1_000",
    "1__0",
    "_1",
    "1_",
    # whitespace handling
    "",
    " ",
    "  ",
    "\t",
    "\n",
    " 1 ",
    "\t2.5\n",
    "1 ",
    " 1",
    # malformed
    "1e",
    "e3",
    "--1",
    "++1",
    "1 2",
    "1,5",
    "1%",
    "%1",
    "1.0.0",
    "1e1e1",
    # non-numeric
    "abc",
    "true",
    "True",
    "None",
    "null",
    # non-ASCII decimal digits: Python accepts these, Go does not
    "١٢",
]


def coerce(value: str) -> str:
    """Exactly components.py's string branch, rendered for JSON transport."""
    try:
        result = float(value)
    except ValueError:
        return "0.0"
    if math.isnan(result):
        return "nan"
    if math.isinf(result):
        return "inf" if result > 0 else "-inf"
    return repr(result)


def render() -> str:
    document = {
        "note": (
            "Expectations are the return value of components.py's _edge_confidence "
            "string branch (float(value), ValueError -> 0.0), produced by the "
            "interpreter itself. Rendered as strings because inf and nan have no "
            "JSON representation and are precisely the values under test."
        ),
        "cases": [{"input": value, "expected": coerce(value)} for value in CORPUS],
    }
    return json.dumps(document, indent=1, sort_keys=True, ensure_ascii=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="print the rendered golden instead of rewriting the frozen file",
    )
    arguments = parser.parse_args()

    rendered = render()
    if arguments.stdout:
        sys.stdout.write(rendered)
        return 0
    GOLDEN_PATH.write_text(rendered)
    sys.stderr.write(f"wrote {GOLDEN_PATH}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
