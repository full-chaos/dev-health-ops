"""Golden generator for ParseInvestmentMixResponse (parser.go, validation.go)
-- CHAOS-4977 step 3.

Calls the REAL parse_investment_mix_response for every case and freezes
its (status, output) result. text is built as real JSON via json.dumps so
malformed-JSON cases are genuine parse failures, not hand-typed strings
that only look malformed.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_parser_golden.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dev_health_ops.llm.explainers.investment_mix_parser import (
    parse_investment_mix_response,
)

OUT_DIR = Path(__file__).parent

THEME_SHARES = {"velocity": 62.5, "quality": 37.5}
SUBCATEGORY_SHARES = {"velocity.feature": 40.0, "quality.bugfix": 20.0}

VALID_FINDING = {
    "finding": "Velocity work leans toward feature delivery",
    "evidence": {
        "theme": "velocity",
        "subcategory": "velocity.feature",
        "share_pct": 40.0,
        "delta_pct_points": None,
        "evidence_quality_mean": None,
        "evidence_quality_band": None,
    },
}

VALID_ACTION = {
    "action": "Review feature-delivery evidence quotes",
    "why": "Confirms the dominant velocity subcategory",
    "where": "Work unit evidence panel",
}

VALID_CONFIDENCE = {
    "level": "moderate",
    "quality_mean": 0.6,
    "quality_stddev": 0.1,
    "band_mix": {"high": 2, "moderate": 3, "low": 1, "very_low": 0, "unknown": 0},
    "drivers": ["thin_component"],
}


def body(**overrides: Any) -> dict[str, Any]:
    base = {
        "summary": "Effort leans toward velocity work with a smaller quality share.",
        "top_findings": [VALID_FINDING],
        "confidence": VALID_CONFIDENCE,
        "what_to_check_next": [VALID_ACTION],
        "anti_claims": ["This does not indicate declining quality investment."],
    }
    base.update(overrides)
    return base


def default_kwargs() -> dict[str, Any]:
    return {
        "theme_shares_pct": THEME_SHARES,
        "subcategory_shares_pct": SUBCATEGORY_SHARES,
        "fallback_level": "moderate",
        "fallback_quality_band": "high",
        "fallback_band_mix": {
            "high": 2,
            "moderate": 3,
            "low": 1,
            "very_low": 0,
            "unknown": 0,
        },
        "fallback_drivers": ["thin_component"],
        "fallback_mean": 0.6,
        "fallback_stddev": 0.1,
    }


def case_text(payload: dict[str, Any]) -> str:
    return json.dumps(payload)


CASES: dict[str, dict[str, Any]] = {}


def add(name: str, *, text: str, kwargs: dict[str, Any] | None = None) -> None:
    CASES[name] = {"text": text, "kwargs": kwargs or default_kwargs()}


add("valid_full", text=case_text(body()))

add(
    "valid_minimal_empty_lists",
    text=case_text(
        body(
            top_findings=[],
            what_to_check_next=[],
            anti_claims=[],
            confidence={
                "level": "unknown",
                "quality_mean": None,
                "quality_stddev": None,
                "band_mix": {
                    "high": 0,
                    "moderate": 0,
                    "low": 0,
                    "very_low": 0,
                    "unknown": 0,
                },
                "drivers": [],
            },
        )
    ),
)

add(
    "valid_with_optional_nulls",
    text=case_text(
        body(
            top_findings=[
                {
                    "finding": "Quality work has no linked subcategory signal",
                    "evidence": {
                        "theme": "quality",
                        "subcategory": None,
                        "share_pct": 37.5,
                        "delta_pct_points": None,
                        "evidence_quality_mean": None,
                        "evidence_quality_band": None,
                    },
                }
            ]
        )
    ),
)

add("empty_text", text="")
add("whitespace_only_text", text="   \n\t  ")
add("no_braces_at_all", text="just some prose with no JSON at all")
add("malformed_json_inside_braces", text="{not: valid, json,}")

add(
    "extra_top_level_key",
    text=case_text({**body(), "unexpected_extra": "value"}),
)
missing = body()
del missing["anti_claims"]
add("missing_top_level_key", text=case_text(missing))

add(
    "summary_with_ascii_digit",
    text=case_text(body(summary="Velocity work grew by 5 percent this period.")),
)
add(
    "summary_with_unicode_digit",
    text=case_text(
        body(summary="Velocity work grew by १ (Devanagari one) this period.")
    ),
)
add("summary_too_long", text=case_text(body(summary="x" * 1001)))
add("summary_blank_after_strip", text=case_text(body(summary="    ")))

add(
    "confidence_invalid_level",
    text=case_text(body(confidence={**VALID_CONFIDENCE, "level": "very_low"})),
)
add(
    "confidence_band_mix_float_shaped_int",
    text='{"summary": "Effort leans toward velocity work overall.", '
    '"top_findings": [], "what_to_check_next": [], "anti_claims": [], '
    '"confidence": {"level": "moderate", "quality_mean": 0.6, '
    '"quality_stddev": 0.1, "band_mix": {"high": 3.0, "moderate": 0, '
    '"low": 0, "very_low": 0, "unknown": 0}, "drivers": []}}',
)
add(
    "confidence_band_mix_negative",
    text=case_text(
        body(
            confidence={
                **VALID_CONFIDENCE,
                "band_mix": {
                    "high": -1,
                    "moderate": 3,
                    "low": 1,
                    "very_low": 0,
                    "unknown": 0,
                },
            }
        )
    ),
)
add(
    "confidence_band_mix_missing_key",
    text=case_text(
        body(
            confidence={
                **VALID_CONFIDENCE,
                "band_mix": {"high": 2, "moderate": 3, "low": 1, "very_low": 0},
            }
        )
    ),
)

add(
    "finding_theme_not_in_shares",
    text=case_text(
        body(
            top_findings=[
                {
                    "finding": "Unknown theme work observed",
                    "evidence": {
                        "theme": "unknown_theme",
                        "subcategory": None,
                        "share_pct": 10.0,
                        "delta_pct_points": None,
                        "evidence_quality_mean": None,
                        "evidence_quality_band": None,
                    },
                }
            ]
        )
    ),
)
add(
    "finding_subcategory_theme_mismatch",
    text=case_text(
        body(
            top_findings=[
                {
                    "finding": "Mismatched subcategory theme prefix",
                    "evidence": {
                        "theme": "velocity",
                        "subcategory": "quality.bugfix",
                        "share_pct": 40.0,
                        "delta_pct_points": None,
                        "evidence_quality_mean": None,
                        "evidence_quality_band": None,
                    },
                }
            ]
        )
    ),
)
add(
    "finding_share_pct_out_of_range",
    text=case_text(
        body(
            top_findings=[
                {
                    "finding": "Out of range share percentage",
                    "evidence": {
                        "theme": "velocity",
                        "subcategory": None,
                        "share_pct": 150.0,
                        "delta_pct_points": None,
                        "evidence_quality_mean": None,
                        "evidence_quality_band": None,
                    },
                }
            ]
        )
    ),
)
add(
    "finding_extra_evidence_key",
    text=case_text(
        body(
            top_findings=[
                {
                    "finding": "Extra key in evidence block",
                    "evidence": {
                        "theme": "velocity",
                        "subcategory": None,
                        "share_pct": 40.0,
                        "delta_pct_points": None,
                        "evidence_quality_mean": None,
                        "evidence_quality_band": None,
                        "unexpected": "value",
                    },
                }
            ]
        )
    ),
)

add(
    "action_with_digit",
    text=case_text(
        body(
            what_to_check_next=[{**VALID_ACTION, "why": "Confirms share of 40 percent"}]
        )
    ),
)

add(
    "anti_claim_too_long",
    text=case_text(body(anti_claims=["x" * 301])),
)
add(
    "anti_claim_blank_dropped_causes_length_mismatch",
    text=case_text(body(anti_claims=["Valid claim text here.", "   "])),
)

add(
    "forbidden_language_standalone_is",
    text=case_text(
        body(summary="This pattern is the dominant one this period overall.")
    ),
)
add(
    "forbidden_language_without_question",
    text=case_text(
        body(summary="The data shows this pattern without    question at all.")
    ),
)
add(
    "forbidden_language_word_boundary_not_triggered",
    text=case_text(
        body(
            summary="This isolated visitor pattern leans toward velocity work overall."
        )
    ),
)

too_many_findings = [
    {
        "finding": f"Finding number {n} placeholder text without digits spelled out",
        "evidence": {
            "theme": "velocity",
            "subcategory": None,
            "share_pct": 40.0,
            "delta_pct_points": None,
            "evidence_quality_mean": None,
            "evidence_quality_band": None,
        },
    }
    for n in range(11)
]
# Strip digits from the placeholder finding text itself (the golden must not
# accidentally trip the SAME numeric-content rule it isn't testing).
for item in too_many_findings:
    item["finding"] = "Finding placeholder text without any digits present at all here"
add("too_many_findings", text=case_text(body(top_findings=too_many_findings)))


def main() -> None:
    for case_name, case in CASES.items():
        result = parse_investment_mix_response(case["text"], **case["kwargs"])
        golden = {
            "case": case_name,
            "text": case["text"],
            "kwargs": case["kwargs"],
            "status": result.status,
            "output": result.output,
        }
        out_path = OUT_DIR / f"parser__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}  status={result.status}")


if __name__ == "__main__":
    main()
