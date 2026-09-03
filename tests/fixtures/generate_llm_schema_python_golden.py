#!/usr/bin/env python3
"""Regenerate the LLM-response-schema golden (CHAOS-4441).

Drives `llm_schema.parse_llm_json`/`validate_llm_payload` themselves --
imported, never imitated (plan.md section 5b's audit question). Same
httpx2-free import path as the prompt-construction golden's own module, so
this generator IS runnable in the CI live-oracle closure (llm_schema.py
only imports taxonomy.py and utils.py, neither of which touches
llm.providers).

AXES VARIED -- this function's entire branch surface
------------------------------------------------------
  * top-level key set: missing, extra, both, exact.
  * subcategories: not-an-object, unknown key, bool weight (Python's
    isinstance(x, bool) trap -- true/false must NOT be accepted as 0/1),
    non-finite weight (via a value that overflows on summation, not a
    literal NaN/Infinity token -- see the ERRORS_ONLY note below),
    negative weight, all-zero, a sum far from 1.0 (the warning path, not
    an error).
  * evidence_quotes: not-a-list, empty list and an 11-item list (both
    outside [1, 10]), a non-object entry, missing/extra keys on an entry,
    non-string fields, an empty-after-strip quote, an over-280-char quote,
    an invalid source value, a missing id, an id absent from handle_map,
    a resolved handle whose source text is empty, a quote that is not a
    substring of its source (even after whitespace normalization), and a
    quote whose INTERNAL whitespace differs from the source (proving the
    \\s+-joined regex recovery, not a literal substring check).
  * uncertainty: wrong type, missing (empty after strip), over-280-chars.
  * a fully valid payload -> the normalized 15-key subcategory vector.

ERRORS_ONLY NOTE (not varied here, documented as a known divergence in
schema.go instead): Python's json.loads accepts bare NaN/Infinity/-Infinity
tokens as a non-standard extension; Go's encoding/json does not and fails
the whole parse instead. Both reject the payload (non-empty errors either
way), just via a different specific error code -- an adversarial-input
case the prompt already forbids the LLM from producing, not a normal
operation path, so it is documented rather than added to the byte-exact
corpus.

ERROR ORDER NOTE: this corpus's subcategories payloads intentionally carry
AT MOST ONE erroring key each, so the two planes' differing iteration
order (Python: JSON-source order; Go: sorted order, see schema.go's own
documented divergence) never becomes visible in a single-error case. The
Go test compares errors as a SORTED set, not a raw list, for exactly this
reason -- documented in both places rather than only one.

Usage:
    python tests/fixtures/generate_llm_schema_python_golden.py            # rewrite
    python tests/fixtures/generate_llm_schema_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dev_health_ops.work_graph.investment.llm_schema import (  # noqa: E402
    parse_llm_json,
    validate_llm_payload,
)

OUTPUT_PATH = Path(__file__).parent / "llm_schema_python_golden.json"

VALID_SUBCATEGORIES = {
    "feature_delivery.customer": 0.0,
    "feature_delivery.roadmap": 0.0,
    "feature_delivery.enablement": 0.0,
    "operational.incident_response": 0.0,
    "operational.on_call": 0.0,
    "operational.support": 0.0,
    "maintenance.refactor": 0.0,
    "maintenance.upgrade": 0.0,
    "maintenance.debt": 1.0,
    "quality.testing": 0.0,
    "quality.bugfix": 0.0,
    "quality.reliability": 0.0,
    "risk.security": 0.0,
    "risk.compliance": 0.0,
    "risk.vulnerability": 0.0,
}

VALID_QUOTE = {"quote": "Fix the login bug", "source": "issue", "id": "E1"}

SOURCE_TEXTS = {
    "issue": {"i1": "Please Fix the login bug   before release. Thanks."},
    "pr": {},
    "commit": {},
}
HANDLE_MAP = {"E1": ("issue", "i1")}


def _payload_cases() -> list[dict[str, Any]]:
    valid = {
        "subcategories": VALID_SUBCATEGORIES,
        "evidence_quotes": [VALID_QUOTE],
        "uncertainty": "moderate confidence",
    }

    def with_subcats(**overrides: Any) -> dict[str, Any]:
        subcats = dict(VALID_SUBCATEGORIES)
        subcats.update(overrides)
        return {**valid, "subcategories": subcats}

    def with_quotes(quotes: list[Any]) -> dict[str, Any]:
        return {**valid, "evidence_quotes": quotes}

    return [
        {"label": "fully_valid_payload", "payload": valid},
        {
            "label": "missing_top_level_key",
            "payload": {"subcategories": VALID_SUBCATEGORIES, "uncertainty": "x"},
        },
        {
            "label": "extra_top_level_key",
            "payload": {**valid, "extra_field": True},
        },
        {
            "label": "subcategories_not_object",
            "payload": {**valid, "subcategories": "not an object"},
        },
        {
            "label": "unknown_subcategory_key",
            "payload": {
                **valid,
                "subcategories": {**VALID_SUBCATEGORIES, "made.up": 1.0},
            },
        },
        {
            "label": "bool_weight_rejected_not_treated_as_zero_or_one",
            "payload": with_subcats(**{"quality.testing": True}),
        },
        {
            "label": "string_weight_invalid",
            "payload": with_subcats(**{"quality.testing": "high"}),
        },
        {
            "label": "negative_weight",
            "payload": with_subcats(**{"quality.testing": -1.0}),
        },
        {
            "label": "weight_sum_overflows_to_non_finite",
            "payload": with_subcats(
                **{"maintenance.debt": 1.0e308, "quality.testing": 1.0e308}
            ),
        },
        {
            "label": "all_weights_zero",
            "payload": {
                **valid,
                "subcategories": {k: 0.0 for k in VALID_SUBCATEGORIES},
            },
        },
        {
            "label": "weights_normalized_warning_not_error",
            "payload": with_subcats(
                **{"maintenance.debt": 1.0, "quality.testing": 1.0}
            ),
        },
        {
            "label": "evidence_quotes_not_list",
            "payload": {**valid, "evidence_quotes": "not a list"},
        },
        {"label": "evidence_quotes_empty", "payload": with_quotes([])},
        {
            "label": "evidence_quotes_too_many",
            "payload": with_quotes([VALID_QUOTE] * 11),
        },
        {
            "label": "evidence_quote_not_object",
            "payload": with_quotes(["not an object"]),
        },
        {
            "label": "evidence_quote_missing_key",
            "payload": with_quotes([{"quote": "x", "source": "issue"}]),
        },
        {
            "label": "evidence_quote_extra_key",
            "payload": with_quotes([{**VALID_QUOTE, "extra": 1}]),
        },
        {
            "label": "evidence_quote_non_string_field",
            "payload": with_quotes([{**VALID_QUOTE, "quote": 123}]),
        },
        {
            "label": "evidence_quote_empty_after_strip",
            "payload": with_quotes([{**VALID_QUOTE, "quote": "   "}]),
        },
        {
            "label": "evidence_quote_too_long",
            "payload": with_quotes([{**VALID_QUOTE, "quote": "x" * 281}]),
        },
        {
            "label": "evidence_quote_invalid_source",
            "payload": with_quotes([{**VALID_QUOTE, "source": "wiki"}]),
        },
        {
            "label": "evidence_quote_missing_id",
            "payload": with_quotes([{**VALID_QUOTE, "id": "  "}]),
        },
        {
            "label": "evidence_quote_unknown_handle",
            "payload": with_quotes([{**VALID_QUOTE, "id": "E99"}]),
        },
        {
            "label": "evidence_quote_not_a_substring",
            "payload": with_quotes(
                [{**VALID_QUOTE, "quote": "totally unrelated text"}]
            ),
        },
        {
            "label": "evidence_quote_whitespace_normalized_recovery",
            # Source has "Fix the login bug" with a single space; the quote
            # here uses a different internal whitespace run -- proves the
            # \s+-joined regex recovers the ORIGINAL source span rather
            # than requiring a literal substring match.
            "payload": with_quotes([{**VALID_QUOTE, "quote": "Fix the   login bug"}]),
        },
        {
            "label": "uncertainty_wrong_type",
            "payload": {**valid, "uncertainty": 42},
        },
        {
            "label": "uncertainty_missing_after_strip",
            "payload": {**valid, "uncertainty": "   "},
        },
        {
            "label": "uncertainty_too_long",
            "payload": {**valid, "uncertainty": "x" * 281},
        },
    ]


def _quote_json(quote: Any) -> Any:
    return quote


def build_golden() -> dict[str, object]:
    parse_cases: list[dict[str, Any]] = [
        {
            "label": "valid_json_object",
            "raw_text": json.dumps({"a": 1}),
        },
        {"label": "invalid_json_syntax", "raw_text": "{not valid json"},
        {"label": "valid_json_but_not_object", "raw_text": json.dumps([1, 2, 3])},
    ]
    for parse_case in parse_cases:
        parsed_payload, parse_errors = parse_llm_json(parse_case["raw_text"])
        parse_case["payload_is_object"] = isinstance(parsed_payload, dict)
        parse_case["errors"] = parse_errors

    validate_cases = []
    for case in _payload_cases():
        case_payload = case["payload"]
        result = validate_llm_payload(case_payload, SOURCE_TEXTS, HANDLE_MAP)
        validate_cases.append(
            {
                "label": case["label"],
                "payload": case_payload,
                "ok": result.ok,
                "errors": sorted(result.errors),
                "subcategories": result.subcategories,
                "evidence_quotes": [
                    {
                        "quote": q.quote,
                        "source_type": q.source_type,
                        "source_id": q.source_id,
                    }
                    for q in result.evidence_quotes
                ],
                "uncertainty": result.uncertainty,
                "warnings": sorted(result.warnings),
            }
        )

    return {"parse_cases": parse_cases, "validate_cases": validate_cases}


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
