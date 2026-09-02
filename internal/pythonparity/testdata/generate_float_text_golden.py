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

# Precision includes 0 and NEGATIVE values. Go reads a negative precision as
# "shortest representation" and would happily return "1"; CPython's format spec
# raises ValueError. Holding this axis to [1, 2, 3] is what let a mutant that
# mishandled precision 0 pass both the frozen corpus and the live rot guard.
_PRECISIONS = [0, 1, 2, 3, 6, -1, -2]


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
    for value in values:
        for precision in _PRECISIONS:
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
