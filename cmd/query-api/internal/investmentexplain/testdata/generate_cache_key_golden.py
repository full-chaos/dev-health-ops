"""Golden generator for ComputeCacheKey (cache.go) -- CHAOS-4977 step 4.

Calls the REAL _compute_cache_key directly (module-private, imported via
attribute access).

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_cache_key_golden.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dev_health_ops.api.services import investment_mix_explain

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


CASES: dict[str, dict[str, Any]] = {
    "typical_filters": {
        "filters": {
            "scope": {"level": "repo", "ids": ["r1", "r2"]},
            "why": {"work_category": ["velocity", "quality.bugfix"]},
            "limit": 200,
        },
        "theme": "velocity",
        "subcategory": None,
        "org_id": "org-golden-4977",
    },
    "no_theme_no_subcategory": {
        "filters": {"scope": {"level": "org", "ids": []}},
        "theme": None,
        "subcategory": None,
        "org_id": "org-golden-4977",
    },
    "with_subcategory": {
        "filters": {"scope": {"level": "org", "ids": []}},
        "theme": "velocity",
        "subcategory": "velocity.feature",
        "org_id": "org-golden-4977",
    },
    "different_org_same_filters": {
        "filters": {"scope": {"level": "org", "ids": []}},
        "theme": None,
        "subcategory": None,
        "org_id": "org-other-4977",
    },
    "empty_org_id": {
        "filters": {"scope": {"level": "org", "ids": []}},
        "theme": None,
        "subcategory": None,
        "org_id": "",
    },
}


def main() -> None:
    for case_name, case in CASES.items():
        # _compute_cache_key takes `filters: Any` and calls
        # filters.model_dump(mode="json") when available, else .dict(),
        # else str(filters) -- since this port has no MetricFilter type,
        # the case's filters dict IS the already-dumped filter_data a real
        # MetricFilter.model_dump(mode="json") would have produced, passed
        # straight through the `else: filter_data = str(filters)` branch's
        # sibling would be wrong here, so call the function with a plain
        # dict that has neither model_dump nor dict -- str(dict) would
        # corrupt it. Instead, patch a minimal stand-in with model_dump.
        class _Filters:
            def __init__(self, data):
                self._data = data

            def model_dump(self, mode=None):
                return self._data

        cache_key = investment_mix_explain._compute_cache_key(
            _Filters(case["filters"]),
            case["theme"],
            case["subcategory"],
            case["org_id"],
        )
        golden = {
            "case": case_name,
            "input": {
                "filters": tag(case["filters"]),
                "theme": tag(case["theme"]),
                "subcategory": tag(case["subcategory"]),
                "org_id": case["org_id"],
            },
            "cache_key": cache_key,
        }
        out_path = OUT_DIR / f"cache_key__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}  cache_key={cache_key}")


if __name__ == "__main__":
    main()
