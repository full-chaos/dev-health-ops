"""Golden generator for MarshalPythonJSONIndentSorted
(jsonindentsorted.go) -- reproduces
    json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)

JSON itself can't carry the int-vs-float distinction that this encoder
exists to get right (a bare `1` round-trips ambiguously), so each case's
INPUT is written as a small tagged-value tree the Go test decodes back into
the exact Go type (int, int64-shaped still just int, float64, string, bool,
nil, list, map) rather than letting json.Unmarshal guess. The case's OUTPUT
is the real `json.dumps(...)` bytes -- never hand-imitated.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python internal/pythonparity/testdata/generate_json_indent_sorted_golden.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT_DIR = Path(__file__).parent


def tag(value: Any) -> Any:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": value}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, list):
        return {"type": "list", "value": [tag(v) for v in value]}
    if isinstance(value, dict):
        return {"type": "map", "value": {k: tag(v) for k, v in value.items()}}
    raise TypeError(f"unsupported case value type: {type(value)!r}")


CASES: dict[str, Any] = {
    "empty_map": {},
    "empty_list": [],
    "whole_number_float": {"share": 1.0},
    "fractional_float": {"share": 0.333333},
    "negative_zero_float": {"share": -0.0},
    "int_stays_bare": {"count": 5},
    "mixed_scalars": {"a": 1, "b": 1.0, "c": "x", "d": True, "e": False, "f": None},
    "nested_structures": {
        "outer": {"inner": {"deep": [1, 2.0, "three"]}},
        "list_of_maps": [{"x": 1}, {"y": 2}],
    },
    "unicode_and_control": {
        "text": 'café \U0001f600 \x7f\x1f   " \\ \n \t',
    },
    "unsorted_input_keys": {"zeta": 1, "alpha": 2, "Mu": 3, "beta": 4},
    "big_and_small_floats": {"big": 1e10, "small": 1e-10, "neg": -42.5},
    "empty_string_key_and_value": {"": ""},
}


def main() -> None:
    for case_name, value in CASES.items():
        golden = {
            "case": case_name,
            "input": tag(value),
            "output": json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        }
        out_path = OUT_DIR / f"json_indent_sorted__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
