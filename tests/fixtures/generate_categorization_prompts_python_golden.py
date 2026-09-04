#!/usr/bin/env python3
"""Regenerate the categorization-prompt golden (CHAOS-4441).

Drives `categorization_prompts.build_prompt`/`build_categorization_prompt`/
`build_repair_prompt` themselves -- imported, never imitated (plan.md
section 5b's audit question).

AXES VARIED
-----------
  * source block: empty (the "(EMPTY)" substitution), plain text, text
    containing characters that would matter to a template engine (braces,
    a literal "{subcategories}" substring) to prove the substitution is a
    single fixed-placeholder swap, not a general template scan.
  * repair error lists: one case per `_repair_guidance` branch (each error
    code shape, in isolation and combined), plus the no-match fallback --
    this is the function's entire branching surface.
  * previous_response text containing quotes, backslashes, newlines, and
    -- the sharpest case -- the LITERAL SUBSTRING "{errors}"/"{guidance}"
    inside the previous response text itself. A port using sequential
    string-replace calls instead of a single substitution pass would
    incorrectly re-substitute inside the already-embedded previous
    response; Python's str.format() does a single pass and never does
    this, so this case pins that property directly rather than by
    reasoning about it.

previous_response is kept ASCII-only throughout this corpus: Python's call
uses `ensure_ascii=False` (literal UTF-8 in the embedded JSON string), and
the Go port deliberately uses the ensure_ascii=TRUE (\\uXXXX-escaped) shape
instead, documented in prompts.go as a safe divergence (this text is never
a hash input and the two planes are never compared side by side on the
same unit). A non-ASCII case would therefore legitimately produce
different bytes on the two sides and does not belong in a byte-exact
golden; it is covered instead by a Go-only test asserting JSON-decoded
equality despite the differing encoding.

Usage:
    python tests/fixtures/generate_categorization_prompts_python_golden.py            # rewrite
    python tests/fixtures/generate_categorization_prompts_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dev_health_ops.work_graph.investment.categorization_prompts import (  # noqa: E402
    build_categorization_prompt,
    build_prompt,
    build_repair_prompt,
)
from dev_health_ops.work_graph.investment.types import TextBundle  # noqa: E402

OUTPUT_PATH = Path(__file__).parent / "categorization_prompts_python_golden.json"


def _prompt_cases() -> list[dict[str, Any]]:
    return [
        {"label": "empty_source_block", "source_block": ""},
        {
            "label": "plain_text_source_block",
            "source_block": "[issue] E1\nFix the login bug\n",
        },
        {
            "label": "source_block_with_braces",
            "source_block": "config: {retries: 3, timeout: {ms: 500}}",
        },
        {
            "label": "source_block_with_literal_placeholder_text",
            "source_block": "the docs literally say {subcategories} here as an example",
        },
    ]


def _repair_cases() -> list[dict[str, Any]]:
    return [
        {
            "label": "evidence_quote_too_long_alone",
            "errors": ["evidence_quote_too_long: quote is 300 chars"],
        },
        {"label": "all_weights_zero_alone", "errors": ["all_weights_zero"]},
        {
            "label": "invalid_weight_alone",
            "errors": ["invalid_weight: quality.testing"],
        },
        {
            "label": "non_finite_weight_alone",
            "errors": ["non_finite_weight: risk.security"],
        },
        {
            "label": "negative_weight_alone",
            "errors": ["negative_weight: maintenance.debt"],
        },
        {"label": "weight_sum_not_finite_alone", "errors": ["weight_sum_not_finite"]},
        {
            "label": "no_recognised_shape_falls_back",
            "errors": ["some_other_error_code"],
        },
        {
            "label": "multiple_shapes_combined",
            "errors": [
                "evidence_quote_too_long: too long",
                "all_weights_zero",
                "invalid_weight: risk.compliance",
                "weight_sum_not_finite",
            ],
        },
        {
            "label": "previous_response_needs_json_escaping",
            "errors": ["all_weights_zero"],
            "previous_response": 'a "quoted" value\nwith a backslash \\ and a\ttab',
        },
        {
            "label": "previous_response_contains_literal_placeholder_text",
            "errors": ["all_weights_zero"],
            "previous_response": "the model echoed back {errors} and {guidance} as literal text",
        },
    ]


def build_golden() -> dict[str, object]:
    prompt_cases = []
    for case in _prompt_cases():
        source_block = case["source_block"]
        prompt_cases.append(
            {
                "label": case["label"],
                "source_block": source_block,
                "build_prompt": build_prompt(source_block),
                "build_categorization_prompt": build_categorization_prompt(
                    TextBundle(
                        source_block=source_block,
                        source_texts={"issue": {}, "pr": {}, "commit": {}},
                        handle_map={},
                        input_hash="unused",
                        text_source_count=0,
                        text_char_count=0,
                    )
                ),
            }
        )

    repair_cases = []
    for case in _repair_cases():
        previous_response = case.get("previous_response", "ok")
        source_block = "[issue] E1\nSome evidence text\n"
        repair_cases.append(
            {
                "label": case["label"],
                "errors": case["errors"],
                "previous_response": previous_response,
                "build_repair_prompt": build_repair_prompt(
                    case["errors"], source_block, previous_response
                ),
            }
        )

    return {"prompt_cases": prompt_cases, "repair_cases": repair_cases}


def main() -> int:
    golden = build_golden()
    text = json.dumps(golden, indent=2, sort_keys=True) + "\n"

    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(text)
        return 0

    OUTPUT_PATH.write_text(text)
    print(f"wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
