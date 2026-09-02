"""Emit what CPython's `json.dumps(value)` writes, with every default in force.

WHY THIS EXISTS
---------------
`pythonparity.MarshalPythonJSONInsertionOrder` reproduces `json.dumps(value)`.
The other encoder in that package reproduces `json.dumps(value, sort_keys=True)`.
They differ on real data:

    json.dumps([ev])                  [{"team_id": ..., "metric_table": ...}]
    json.dumps([ev], sort_keys=True)  [{"field": ..., "metric_table": ...}]

Different bytes, same data, and both spellings look right in review. Anything
hashing or byte-comparing the output gets a different answer.

`recommendations/loader.py:448` writes the `evidence_json` column with the first
form, so a Go port of that writer must produce those bytes.

THE allow_nan EDGE, WHICH IS WHY THE NON-FINITE CASES ARE HERE
--------------------------------------------------------------
`json.dumps` defaults to `allow_nan=True`, so CPython emits the BARE tokens
`Infinity`, `-Infinity` and `NaN`. They are not valid JSON. The corpus pins them
because the Go side must emit them too -- it reproduces the call, not an
improvement on it.

On the evidence path the reachability is asymmetric: `_safe_float` returns None
for NaN but passes +/-Inf through, so the infinities are LIVE and NaN is not.
NaN is pinned anyway; the encoder equals the Python call rather than the caller
that feeds it today.

FLOAT RENDERING is `float.__repr__`, not `str(float)` and not `%g`: `24.0` stays
`24.0`, and `1e10` is `10000000000.0` rather than `1e+10`. That is the same rule
`pythonparity.Repr` implements, and the float cases here are chosen to cross the
notation window where a `%g`-shaped implementation would diverge.

Usage:
    uv run python tests/fixtures/generate_python_json_insertion_order_golden.py [--stdout]
"""

from __future__ import annotations

import json
import platform
import sys
from pathlib import Path
from typing import Any

OUTPUT_PATH = Path(__file__).parent / "python_json_insertion_order_python_golden.json"


def _evidence(value: Any) -> list[dict[str, Any]]:
    """The real evidence row from recommendations/loader.py:425-434.

    Key order is the literal's, and it is NOT alphabetical -- which is the whole
    point of the fixture. Sorted, `field` would come first; here `team_id` does.
    """
    return [
        {
            "team_id": "70d529e0-3c06-4597-8480-794fd02328b6",
            "metric_table": "dora_deployments",
            "window_start": "2026-08-01T00:00:00+00:00",
            "window_end": "2026-08-31T00:00:00+00:00",
            "field": "deployment_frequency",
            "value": value,
        }
    ]


def cases() -> list[dict[str, Any]]:
    return [
        # --- the shape this exists for, including the live non-finite values ---
        {"name": "evidence: ordinary float", "value": _evidence(1.5)},
        {"name": "evidence: integral float keeps .0", "value": _evidence(24.0)},
        {
            "name": "evidence: +Infinity (reachable: _safe_float passes it)",
            "value": _evidence(float("inf")),
        },
        {"name": "evidence: -Infinity (reachable)", "value": _evidence(float("-inf"))},
        {
            "name": "evidence: NaN (unreachable via _safe_float; pinned anyway)",
            "value": _evidence(float("nan")),
        },
        {"name": "evidence: null value", "value": _evidence(None)},
        # --- key order, stated as its own case so a sorted implementation fails
        #     on something whose name says what broke ---
        {
            "name": "key order is insertion, not sorted",
            "value": {"zebra": 1, "alpha": 2, "mango": 3},
        },
        # --- floats: chosen to cross the notation window ---
        {"name": "float: zero", "value": 0.0},
        {"name": "float: negative zero", "value": -0.0},
        {"name": "float: 0.1 repr", "value": 0.1},
        {"name": "float: 1e10 stays positional", "value": 1e10},
        {"name": "float: 1e16 switches to exponent", "value": 1e16},
        {"name": "float: 1e-5 positional", "value": 1e-05},
        {"name": "float: 1e-6 switches", "value": 1e-06},
        {"name": "float: smallest subnormal", "value": 5e-324},
        {"name": "float: max", "value": 1.7976931348623157e308},
        {"name": "float: 2**53", "value": 9007199254740992.0},
        # --- scalars and containers ---
        {"name": "int", "value": 42},
        {"name": "int: negative", "value": -7},
        {"name": "bool true", "value": True},
        {"name": "bool false", "value": False},
        {"name": "null", "value": None},
        {"name": "empty object", "value": {}},
        {"name": "empty list", "value": []},
        {
            "name": "nested list of objects",
            "value": [{"b": 1, "a": [2, 3]}, {"c": None}],
        },
        # --- strings: ensure_ascii=True is a default, so non-ASCII escapes ---
        {"name": "string: ascii", "value": {"k": "plain"}},
        {"name": "string: accented escapes under ensure_ascii", "value": {"k": "café"}},
        {
            "name": "string: astral becomes a surrogate pair",
            "value": {"k": "\U0001f600"},
        },
        {"name": "string: control characters", "value": {"k": "a\tb\nc\x00d"}},
        {"name": "string: DEL is escaped", "value": {"k": "a\x7fb"}},
        {"name": "string: quote and backslash", "value": {"k": 'a"b\\c'}},
        {"name": "string: solidus stays unescaped", "value": {"k": "a/b"}},
        # --- separators are ", " and ": " by default, which a Go encoder omits ---
        {"name": "separators: multi-key object", "value": {"a": 1, "b": 2}},
        {"name": "separators: multi-element list", "value": [1, 2, 3]},
    ]


def main() -> None:
    rendered_cases = []
    for case in cases():
        rendered_cases.append(
            {
                "name": case["name"],
                # repr() so the fixture records the INPUT unambiguously: json
                # cannot round-trip NaN/Infinity through a reader that rejects
                # them, and -0.0 is invisible in a JSON number.
                "input_repr": repr(case["value"]),
                "expected": json.dumps(case["value"]),
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_python_json_insertion_order_golden.py. "
            "Do not hand-edit."
        ),
        "_policy": (
            "Every `expected` is json.dumps(value) with ALL defaults: no sort_keys "
            "(so key order is insertion order), ensure_ascii=True, separators "
            "', ' and ': ', allow_nan=True (so bare Infinity/-Infinity/NaN tokens). "
            "Floats render via float.__repr__."
        ),
        "generating_interpreter": {
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "float_repr_style": sys.float_repr_style,
        },
        "cases": rendered_cases,
    }

    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(text)
        return
    OUTPUT_PATH.write_text(text, encoding="utf-8")
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(rendered_cases)}")
    print(f"  interpreter: {platform.python_version()} ({sys.float_repr_style} repr)")


if __name__ == "__main__":
    main()
