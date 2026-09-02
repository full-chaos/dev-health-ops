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

from dev_health_ops.work_graph.investment.components import _edge_confidence

GOLDEN_PATH = (
    pathlib.Path(__file__).resolve().parent / "confidence_coercion_python_golden.json"
)

# Ordinary values, then every form known to separate the two parsers, then the
# junk a mis-typed or mis-migrated column could realistically hold.
STRING_CORPUS: list[str] = [
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


# The TYPE axis. The string corpus above varies VALUE only; every entry reaches
# the same `isinstance(value, str)` branch. These reach the others, and the delta
# between them is where a Go port written from the type signature goes wrong.
#
# `True` and `False` are the point of this list. Python's `bool` SUBCLASSES
# `int`, so `_edge_confidence` checks `isinstance(value, bool)` BEFORE the
# numeric branch specifically to stop `True` becoming 1.0 -- it coerces to 0.0.
# A port that checks `int` first, or that relies on Go's type switch where
# `bool` and numeric types are unrelated, inverts the meaning of every boolean
# confidence. Until this list existed that case had no generated evidence at
# all: it was asserted only by a hand-written Go table, which is the instrument
# the corpus was introduced to replace.
TYPE_CORPUS: list[object] = [
    True,
    False,
    0,
    1,
    -3,
    0.0,
    0.5,
    1.0,
    float("inf"),
    float("-inf"),
    float("nan"),
    None,
    [],
    [1.0],
    {},
    {"confidence": 1.0},
    (),
    "0.5",
]


def coerce(value: object) -> str:
    """Call the REFERENCE function, do not reimplement it.

    An earlier revision of this generator reproduced ``_edge_confidence``'s
    ``try: float(value) / except ValueError: return 0.0`` inline.  That made the
    rot guard useless for its actual purpose: it byte-compares this generator's
    output against the frozen file, so an imitation stays green while the real
    function changes underneath it, and the frozen file then documents a rule
    nothing enforces.  Importing it means the corpus is frozen against the
    producer rather than against someone's reading of it (CHAOS-4803).

    ``_edge_confidence`` takes the EDGE, not the value, so the value is wrapped
    the way ``build_components`` would supply it.
    """
    result = _edge_confidence({"confidence": value})
    if math.isnan(result):
        return "nan"
    if math.isinf(result):
        return "inf" if result > 0 else "-inf"
    return repr(result)


def render() -> str:
    document = {
        "note": (
            "Expectations are the return value of components.py's _edge_confidence, "
            "CALLED DIRECTLY rather than reimplemented -- an imitation would leave "
            "the rot guard green while the real function changed underneath it. "
            "Two axes: string_cases varies the VALUE inside the str branch; "
            "type_cases varies the TYPE across branches, and carries repr() rather "
            "than the value because JSON would erase the very thing under test. "
            "The type axis exists for True/False: bool subclasses int in Python, so "
            "the isinstance(value, bool) check runs BEFORE the numeric branch and "
            "True coerces to 0.0, not 1.0."
        ),
        "string_cases": [
            {"input": value, "expected": coerce(value)} for value in STRING_CORPUS
        ],
        # repr() rather than the value itself: the type axis carries values with
        # no JSON representation (inf, nan, tuples), and the POINT of these rows
        # is which Python TYPE was passed, which JSON would erase by coercing
        # True to true and () to [].
        "type_cases": [
            {"input_repr": repr(value), "expected": coerce(value)}
            for value in TYPE_CORPUS
        ],
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
