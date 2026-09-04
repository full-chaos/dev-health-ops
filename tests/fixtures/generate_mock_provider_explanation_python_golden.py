#!/usr/bin/env python3
"""Regenerate the mock-provider investment-mix-explanation golden (CHAOS-4977).

Drives `MockProvider().complete()` itself -- imported, never imitated,
same discipline as generate_mock_provider_python_golden.py -- but this
script exercises mock.py's OTHER branch: the one
generate_mock_provider_python_golden.py's own docstring flags as
out-of-scope for investment.categorize. That branch answers the
investment-mix-explanation use case CHAOS-4977's Go port needs a golden
for.

Python's MockProvider picks this branch by NOT matching the categorization
markers ("Output schema"+'"subcategories"'+'"evidence_quotes"', or
"matching the schema"); the Go port instead branches on an explicit
CompletionRequest.ResponseFormatName field (see provider.go). Both must
produce the SAME response for the SAME prompt text, which is what this
golden proves -- the prompt shapes here are constructed to match what the
line-parsing logic looks for ("Evidence Quality: (band)" markers,
"  - category: NN.NN%" lines), not to look like a real
investment_mix_explain_prompt.txt rendering (no Go caller builds that
prompt yet).

AXES VARIED
-----------
  * evidence quality band: high/moderate/low/very_low, and none present
    (falls back to the "moderate" default).
  * category percentage lines: single line, multiple lines (highest wins),
    a line below the 0.25 default threshold (ignored), and no percentage
    lines at all (falls back to feature_delivery.customer/0.25).
  * malformed percentage line (non-numeric): must be silently skipped,
    not raise.

This is NOT a byte-exact golden (same reasoning as the categorization
one): MockProvider is dev/test-only. The comparison is STRUCTURAL --
same dominant_themes[0], same confidence_note's embedded band/category.

Usage:
    python tests/fixtures/generate_mock_provider_explanation_python_golden.py            # rewrite
    python tests/fixtures/generate_mock_provider_explanation_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dev_health_ops.llm.providers.mock import MockProvider  # noqa: E402

OUTPUT_PATH = Path(__file__).parent / "mock_provider_explanation_python_golden.json"


def _cases() -> list[dict[str, Any]]:
    return [
        {
            "label": "high_band_single_category",
            "prompt": "Evidence Quality: strong (high)\n  - risk.security: 62.00%\n",
        },
        {
            "label": "low_band_multiple_categories_highest_wins",
            "prompt": (
                "Evidence Quality: weak (low)\n"
                "  - maintenance.refactor: 30.00%\n"
                "  - quality.bugfix: 55.00%\n"
                "  - risk.security: 15.00%\n"
            ),
        },
        {
            "label": "very_low_band_below_default_threshold_ignored",
            "prompt": "Evidence Quality: minimal (very_low)\n  - feature_delivery.roadmap: 10.00%\n",
        },
        {
            "label": "no_band_no_percentage_lines_defaults",
            "prompt": "No structured evidence markers in this prompt at all.\n",
        },
        {
            "label": "moderate_band_explicit",
            "prompt": "Evidence Quality: fair (moderate)\n  - operational.incident_response: 40.00%\n",
        },
        {
            "label": "malformed_percentage_line_skipped",
            "prompt": "Evidence Quality: fair (moderate)\n  - risk.compliance: not-a-number%\n",
        },
    ]


async def _run_case(prompt: str) -> str:
    provider = MockProvider()
    result = await provider.complete(prompt)
    await provider.aclose()
    return result.text


def build_golden() -> dict[str, object]:
    cases = []
    for case in _cases():
        raw_text = asyncio.run(_run_case(case["prompt"]))
        payload = json.loads(raw_text)
        cases.append(
            {
                "label": case["label"],
                "prompt": case["prompt"],
                "summary": payload["summary"],
                "dominant_themes": payload["dominant_themes"],
                "key_drivers": payload["key_drivers"],
                "operational_signals": payload["operational_signals"],
                "confidence_note": payload["confidence_note"],
            }
        )

    return {"cases": cases}


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
