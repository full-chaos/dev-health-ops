"""Generate the golden corpus for CPython float rounding and text rendering.

Run from the ops worktree with the project interpreter:

    .venv/bin/python internal/pythonparity/testdata/generate_float_text_golden.py

The Go mirrors (`pythonparity.Round`, `.Repr`, `.FormatFixed`) are asserted against
this file. It is generated from the SHIPPED interpreter, never hand-written: the
three behaviours it pins are all CPython implementation details.

* ``round(x, n)`` is banker's rounding on the EXACT BINARY value of ``x`` --
  ``round(2.675, 2) == 2.67`` because 2.675 is really 2.67499999999999982...,
  and ``round(0.125, 2) == 0.12`` because that IS a tie and CPython breaks ties
  toward even. Go's ``math.Round`` disagrees on both.
* ``str(x)`` / ``repr(x)`` always keeps a fractional part for an integral float
  (``str(0.0) == '0.0'``), where Go's ``fmt.Sprint``/``FormatFloat('g',-1)``
  render ``'0'``. These strings are interpolated into ``recommendations_daily``
  rationale text, so they are byte contracts.
* ``format(x, '.Nf')`` is the ``:.3f`` / ``:.1f`` / ``:.2f`` family used in the
  same rationale strings.

Axes deliberately varied (a corpus is blind to any axis it holds constant):
magnitude, number of decimals, sign (including -0.0), ties vs non-ties,
subnormals, values at and beyond float64 integral precision, and the
non-finite specials.
"""

from __future__ import annotations

import json
import math
import platform
import sys
import unicodedata

# --- inputs -----------------------------------------------------------------

_SPECIALS = [
    0.0,
    -0.0,
    1.0,
    -1.0,
    0.5,
    -0.5,
    2.5,
    -2.5,
    0.1,
    0.125,
    -0.125,
    0.375,
    2.675,
    -2.675,
    1.005,
    2.665,
    1e-323,  # subnormal
    5e-324,  # smallest positive subnormal
    2.2250738585072014e-308,  # smallest normal
    1.7976931348623157e308,  # max float
    9007199254740992.0,  # 2**53, integral precision boundary
    9007199254740993.0,  # not representable; rounds to 2**53
    1e16,
    1e17,
    1e22,
    1e23,
    123456789.123456789,
    float("inf"),
    float("-inf"),
    float("nan"),
]

_MAGNITUDES = [1e-8, 1e-4, 1e-2, 1e0, 1e2, 1e5, 1e10, 1e15]
# ndigits deliberately reaches the band where round() RAISES. CPython's
# OverflowError for the largest float is non-contiguous -- measured at
# -308, -307, -306, -305, -304, -299, -298, -294, -293 and nowhere else in
# -320..-291 -- so the corpus sweeps the whole neighbourhood rather than a
# hand-picked value. A corpus that stopped at -2 (as the first version of this
# file did) could not represent the raising case at all, and the Go mirror
# silently returned +Inf there.
# The POSITIVE side needs the same treatment as the negative one. Jumping from
# 10 straight to 400 left the whole 11..399 range unrepresented, so a mutant
# that moved the short-circuit down to `ndigits > 300` passed the corpus AND
# the live rot guard while returning the input for Round(0x1p-1073, 301) --
# bits 0x2 where CPython gives +0.0. The interesting structure is a TRANSITION,
# not a single edge: for the smallest subnormal, round(x, n) is +0.0 up to
# about n=320 and the value itself from about n=350, because that is where the
# requested decimal place starts to reach the value's own digits. Sweep across
# it rather than picking one point on either side.
_NDIGITS = (
    [0, 1, 2, 3, 4, 6, 10, -1, -2, -3, -5]
    + list(range(-320, -290))
    + [20, 50, 100, 200, 300, 301]
    + list(range(310, 361, 5))
    + [370, 380, 390, 399]
    # The boundary that actually bites, in both directions. 323 is the largest
    # ndigits at which round(x, n) != x for ANY float64 (witnessed by the
    # smallest subnormal); at 324 and beyond every value is returned unchanged.
    # Covering 321..325 is what makes lowering the short-circuit observable --
    # a threshold anywhere above 324 is a genuine equivalent and no test can
    # distinguish it.
    + [321, 322, 323, 324, 325, -321, -322, -323, -324, -325]
    + [-400, -401, 400, 401, 1000]
)

# Precision is SWEPT, not hand-listed. Three review rounds each found a
# different unenumerated value on this corpus's axes -- the negative-precision
# refusal, the positive-ndigits band, and then a mutant clamping precision > 6
# that passed everything because the axis jumped 6 -> nothing. Hand-listing
# reproduces the author's model of which values matter, so each round finds the
# next value the author did not think of. Enumerate the small band exhaustively
# and pin the boundaries instead.
#
# The accepted range is bounded by a C `int`, NOT by sys.maxsize: format()
# takes a precision up to 2147483647 and raises "ValueError: precision too big"
# at 2147483648 (measured).
#
# The distinction matters and was worth measuring rather than assuming, because
# the fixture records sys.maxsize and it is tempting to read these boundary
# cases as derived from it. They are not: on this build sys.maxsize is
# 2**63-1 = 9223372036854775807 while the precision cap is 2**31-1. The cap
# comes from the format-spec parser's C int, so a 32-bit build would move
# sys.maxsize without moving this boundary. That is why the rot guard does not
# compare sys.maxsize: it is recorded as environment provenance, and nothing
# in this corpus depends on it (CHAOS-4870). The huge accepted values are covered for NON-FINITE values only,
# where the output is 3 characters -- format(1.0, ".2147483647f") would build a
# two-gigabyte string, so a corpus cannot hold it and this file must not try.
_PRECISIONS = (
    list(range(0, 21))
    + [21, 25, 30, 40, 50, 75, 100, 101, 120]
    + [
        -1,
        -2,
        -3,
        -10,
    ]
)

#: The FINITE tail, bounded by measurement rather than by taste.
#:
#: Round 4 found a mutant clamping finite precision > 100 that passed, because
#: the axis above was exhaustive to 20 and then hand-picked (30, 50, 100) --
#: half a sweep. The tail was still a list of the values I thought mattered,
#: which is the same defect the previous three rounds found in three other
#: places.
#:
#: There is a real bound: the smallest subnormal is 2**-1074, whose exact
#: decimal expansion ends at fractional place 1074, so for EVERY float64
#: format(x, ".Nf") with N > 1074 only appends zeros (measured). 1074 is to
#: format() what 323 is to round(): the last place where the answer can still
#: carry information.
#:
#: These are applied to a value subset rather than the whole corpus purely for
#: size -- a 1100-place string is ~1.1 kB per case. That costs nothing in
#: coverage for this defect class: a clamp changes the STRING LENGTH, so any
#: value detects it; the subset is about robustness against a clamp that is
#: conditional on the value, not about whether a pure clamp is caught at all.
_TAIL_PRECISIONS = [150, 250, 500, 1000, 1073, 1074, 1075, 1100]

#: Precisions covered only for non-finite values (cheap output) plus the
#: refusal boundary, which raises for every value including the specials --
#: CPython validates the spec before it looks at the value.
_LARGE_PRECISIONS_SPECIALS_ONLY = [1000, 100000, 2147483646, 2147483647]
_REFUSED_PRECISIONS = [2147483648, 4294967296]


def _values() -> list[float]:
    values: list[float] = list(_SPECIALS)
    # Deterministic sweep: a few ticks around each magnitude, both signs, plus
    # values engineered to sit exactly on a rounding tie for each ndigits.
    for mag in _MAGNITUDES:
        for step in (1, 3, 7):
            for sign in (1.0, -1.0):
                values.append(sign * mag * step)
                values.append(sign * math.nextafter(mag * step, math.inf))
                values.append(sign * math.nextafter(mag * step, -math.inf))
    for nd in (0, 1, 2, 3, 4):
        half = 0.5 * (10.0**-nd)
        for base in (0.0, 1.0, 2.0, 3.0, 12.0):
            values.append(base + half)
            values.append(-(base + half))
    return values


def _round_entry(value: float, ndigits: int) -> dict[str, object] | None:
    try:
        result = round(value, ndigits)
    except (OverflowError, ValueError) as exc:
        return {
            "value_hex": float.hex(value) if not math.isnan(value) else "nan",
            "ndigits": ndigits,
            "raises": type(exc).__name__,
        }
    return {
        "value_hex": float.hex(value),
        "ndigits": ndigits,
        "result_hex": float.hex(result) if not math.isnan(result) else "nan",
        "result_is_nan": math.isnan(result),
    }


def main() -> None:
    values = _values()
    rounds = []
    for value in values:
        for ndigits in _NDIGITS:
            entry = _round_entry(value, ndigits)
            if entry is not None:
                rounds.append(entry)

    reprs = [
        {
            "value_hex": float.hex(v) if not math.isnan(v) else "nan",
            "is_nan": math.isnan(v),
            "repr": repr(v),
            "str": str(v),
        }
        for v in values
    ]

    formats = []
    non_finite = [float("nan"), float("inf"), float("-inf")]
    # Spans magnitude, sign, both subnormal extremes and the integral boundary,
    # so a value-conditional clamp in the tail has somewhere to show up.
    tail_values = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.1,
        2.675,
        5e-324,
        1e-323,
        2.2250738585072014e-308,
        1.7976931348623157e308,
        9007199254740992.0,
        123456789.123456789,
    ]
    # The tail_values list is a CLAIM about coverage, and until this assertion
    # existed it was not an effective one. Membership here does not create a
    # corpus entry -- the loop below only ADDS tail precisions to values that
    # are already in `values`. 0.1 sat in this list while being absent from
    # _SPECIALS and from the magnitude sweep, so it received no formats entry at
    # any precision, and the Go law test's baseline for it was never pinned to
    # CPython. 11 of 12 by luck (CHAOS-4870).
    #
    # Assert rather than document: a declared value that cannot be covered is a
    # generator bug, and it must fail here rather than produce a corpus that
    # quietly omits it.
    missing_from_values = [
        candidate
        for candidate in tail_values
        if not any(
            math.isnan(candidate) if math.isnan(existing) else candidate == existing
            for existing in values
        )
    ]
    if missing_from_values:
        raise SystemExit(
            "tail_values entries are absent from the generated value set, so they "
            f"would receive no corpus entry: {[float.hex(v) for v in missing_from_values]}. "
            "Add them to _SPECIALS."
        )

    for value in values:
        precisions = list(_PRECISIONS) + _REFUSED_PRECISIONS
        if any(
            math.isnan(value) if math.isnan(candidate) else value == candidate
            for candidate in tail_values
        ):
            precisions += _TAIL_PRECISIONS
        # The huge ACCEPTED precisions are only tractable for the non-finite
        # values, whose output ignores the precision entirely.
        if any(
            math.isnan(value) if math.isnan(candidate) else value == candidate
            for candidate in non_finite
        ):
            precisions += _LARGE_PRECISIONS_SPECIALS_ONLY
        for precision in precisions:
            # Named distinctly from the rounds loop's `entry`: that one is
            # typed `dict[str, object] | None` (a skipped case is None), and
            # reusing the name here rebinds it to that union, so the indexed
            # assignments below stop type-checking.
            format_entry: dict[str, object] = {
                "value_hex": float.hex(value) if not math.isnan(value) else "nan",
                "is_nan": math.isnan(value),
                "precision": precision,
            }
            # A negative precision is not "shortest representation" to CPython,
            # it is a malformed spec. Recording the raise is the only way the
            # corpus can hold the Go mirror to refusing it too.
            try:
                format_entry["text"] = format(value, f".{precision}f")
            except (ValueError, OverflowError) as exc:
                format_entry["raises"] = type(exc).__name__
            formats.append(format_entry)

    document = {
        "_marker": "recommendations-float-text-golden",
        "_generator": "internal/pythonparity/testdata/generate_float_text_golden.py",
        "generating_interpreter": {
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "unicode_version": unicodedata.unidata_version,
            "machine": platform.machine(),
            "float_repr_style": sys.float_repr_style,
            "maxsize": sys.maxsize,
        },
        "rounds": rounds,
        "reprs": reprs,
        "formats": formats,
    }
    text = json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n"
    sys.stdout.write(text)


if __name__ == "__main__":
    main()
