"""Emit CPython's math.isclose behaviour (CHAOS-4288).

WHY THIS EXISTS
---------------
`math.isclose(a, b, *, rel_tol=1e-09, abs_tol=0.0)` tests

    |a-b| <= max(rel_tol * max(|a|, |b|), abs_tol)

so **rel_tol is 1e-09 by DEFAULT and stays active when a caller passes only
abs_tol**. The obvious Go transliteration of `math.isclose(x, y, abs_tol=1e-9)`
-- `math.Abs(x-y) <= 1e-9` -- is therefore wrong whenever both operands are
large:

    isclose(1000.0, 1000.0000005, abs_tol=1e-9)  -> True   (rel term 1.0e-06)
    abs(1000.0 - 1000.0000005) <= 1e-9           -> False

THE AXIS THAT MAKES IT INVISIBLE
--------------------------------
Most benchmarking call sites compare against ZERO (`isclose(x, 0.0,
abs_tol=t)`), where the relative term is `rel_tol * max(|x|, 0)` and collapses
to `|x| <= t` for any x small enough to matter. Those sites agree with the
naive form, so a corpus built only from them proves nothing. The axis is the
MAGNITUDE OF THE OPERANDS, so this corpus pairs values spanning 1e-12 to 1e9,
both signs, and both infinities -- and deliberately includes pairs that differ
by more than abs_tol but less than `rel_tol * max(|a|, |b|)`, which is exactly
the region where the two forms disagree.

WHERE IT BITES
--------------
`anomalies.py:69`, `isclose(current_point.value, baseline_value, abs_tol=1e-9)`
-- both operands are metric values and routinely far greater than 1. It decides
whether a zero-variance history yields z_score 0.0 or 3.0, which selects the
anomaly's severity ("info" vs "critical") and whether the row is emitted at
all. A disagreement there is an alerting-level output change.

EDGE CASES
----------
Identical values are close even at infinity (CPython short-circuits on `a == b`
BEFORE subtracting, so `isclose(inf, inf)` is True where `inf - inf` would be
NaN); mixed-sign infinities are never close; NaN is never close to anything,
including itself.

Regenerate with `PYTHONPATH=src python tests/fixtures/generate_python_isclose_golden.py`.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

OUTPUT = Path(__file__).with_name("python_isclose_python_golden.json")

# Spans the magnitudes where rel_tol and abs_tol trade dominance, both signs,
# plus the exact-equality and infinity short-circuits.
VALUES: list[float] = [
    0.0,
    -0.0,
    1e-12,
    1e-9,
    1e-8,
    0.5,
    1.0,
    1.000000001,
    3.0,
    3.0000000001,
    1000.0,
    1000.0000005,
    1e9,
    1e9 + 1.0,
    -1000.0,
    -1000.0000005,
    float("inf"),
    float("-inf"),
    float("nan"),
]

# (rel_tol, abs_tol). The second entry is the shape every benchmarking call
# site uses -- abs_tol supplied, rel_tol left at its default.
TOLERANCES: list[tuple[float, float]] = [
    (1e-09, 0.0),
    (1e-09, 1e-9),
    (1e-09, 1e-12),
    (0.0, 1e-9),
    (0.5, 0.0),
]


def _encode(value: float) -> str:
    """Floats travel as their repr so no JSON round-trip can perturb them."""
    return repr(value)


def main() -> str:
    cases = []
    for a in VALUES:
        for b in VALUES:
            for rel_tol, abs_tol in TOLERANCES:
                cases.append(
                    {
                        "a": _encode(a),
                        "b": _encode(b),
                        "rel_tol": _encode(rel_tol),
                        "abs_tol": _encode(abs_tol),
                        "close": math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol),
                        # Recorded so the fixture itself shows how many cases
                        # actually discriminate between the two forms; a
                        # regression that drops them all is then visible.
                        "naive_abs_only": bool(abs(a - b) <= abs_tol)
                        if not (
                            math.isnan(a)
                            or math.isnan(b)
                            or math.isinf(a)
                            or math.isinf(b)
                        )
                        else None,
                    }
                )
    document = {"cases": cases}
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    import sys

    rendered = main()
    if "--stdout" in sys.argv:
        sys.stdout.write(rendered)
    else:
        OUTPUT.write_text(rendered)
        print(f"wrote {OUTPUT}")
