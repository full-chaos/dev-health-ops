"""Golden generator for BuildPrompt (prompt.go) -- CHAOS-4977 step 2.

Calls the REAL build_prompt(base_prompt=load_prompt(), payload=...) and
freezes its output. The payload's own JSON encoding is tagged the same way
generate_json_indent_sorted_golden.py tags pythonparity cases (JSON can't
carry the int-vs-float distinction Python's json.dumps renders
differently), so the Go test can reconstruct the exact same Go value.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_build_prompt_golden.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dev_health_ops.llm.explainers.investment_mix_explainer import (
    build_prompt,
    load_prompt,
)

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
    "empty_payload": {},
    "simple_shares": {
        "theme_distribution_pct": {"velocity": 62.5, "quality": 37.5},
        "org_id": "org-golden-4977",
    },
    "nested_with_whole_number_floats_and_unicode": {
        "theme_distribution_pct": {"velocity": 100.0, "quality": 0.0},
        "top_findings": [
            {"finding": "café growth 😀", "share_pct": 50.0},
            {"finding": "second", "share_pct": 50.0},
        ],
        "confidence": {
            "quality_mean": 0.42,
            "quality_stddev": None,
            "band_mix": {
                "high": 3,
                "moderate": 1,
                "low": 0,
                "very_low": 0,
                "unknown": 0,
            },
        },
    },
}


def main() -> None:
    base_prompt = load_prompt()
    if not base_prompt:
        raise RuntimeError("load_prompt() returned empty -- prompt file missing?")

    for case_name, payload in CASES.items():
        rendered = build_prompt(base_prompt=base_prompt, payload=payload)
        golden = {
            "case": case_name,
            "base_prompt": base_prompt,
            "payload": tag(payload),
            "rendered": rendered,
        }
        out_path = OUT_DIR / f"build_prompt__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
