"""Golden generator for MarshalPythonJSONSorted (jsonsorted.go) --
reproduces json.dumps(value, sort_keys=True) (ensure_ascii=True default,
no indent). CHAOS-4977's cache-key hash needs exactly this contract.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python internal/pythonparity/testdata/generate_json_sorted_golden.py
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
    "cache_key_shaped": {
        "filters": {
            "scope": {"level": "repo", "ids": ["r1", "r2"]},
            "why": {"work_category": ["velocity", "quality.bugfix"]},
            "date_range": {"start": "2026-01-01", "end": "2026-02-01"},
            "limit": 200,
            "include_text": True,
        },
        "theme": "velocity",
        "subcategory": None,
        "org_id": "org-golden-4977",
    },
    "empty_map": {},
    "empty_list_field": {"ids": []},
    "unicode_value": {"note": "café 😀"},
    "whole_number_float": {"share": 1.0},
    "negative_and_zero_ints": {"a": -5, "b": 0},
    "nested_list_of_maps": {"items": [{"x": 1}, {"y": 2}]},
    "bool_values": {"flag_true": True, "flag_false": False},
    "unsorted_keys": {"zeta": 1, "alpha": 2, "Mu": 3},
}


def main() -> None:
    for case_name, value in CASES.items():
        golden = {
            "case": case_name,
            "input": tag(value),
            "output": json.dumps(value, sort_keys=True),
        }
        out_path = OUT_DIR / f"json_sorted__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
