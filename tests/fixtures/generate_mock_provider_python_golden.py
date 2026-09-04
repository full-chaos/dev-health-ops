#!/usr/bin/env python3
"""Regenerate the mock-provider golden (CHAOS-4441).

Drives `MockProvider().complete()` itself -- imported, never imitated
(plan.md section 5b's audit question). Runnable in the CI live-oracle
closure: llm.providers.mock imports llm.providers.base only, and the
llm.providers PACKAGE's own __init__.py (run on import) lazy-imports every
concrete provider client (openai/anthropic/gemini/...) INSIDE get_provider's
branches rather than at module scope, so importing the package itself never
pulls in the httpx2-carrying modules -- verified directly (`python3 -c
"from dev_health_ops.llm.providers import mock"` succeeds standalone), not
assumed by analogy with the categorization-prompts golden's exclusion.

SCOPE: only the CATEGORIZATION branch of MockProvider.complete is a
target here -- the canonical prompt always contains "Output schema",
'"subcategories"' and '"evidence_quotes"', which routes to
_mock_categorization. mock.py's OTHER branch (investment-view
explanation) answers a different LLM use case entirely and is out of
scope for investment.categorize.

AXES VARIED
-----------
  * keyword-priority chain: one case per category tier (incident/outage,
    refactor/cleanup, bug/fix, security/vulnerability), and one with no
    keyword match at all (the feature_delivery.customer default).
  * source-block discovery: [issue]/[pr]/[commit] headers, MULTIPLE
    source blocks in one prompt (only the first is used), and NO source
    block at all (falls back to "incremental improvement").
  * quote truncation: source text longer than 80 characters.

This is NOT a byte-exact golden the way the prompt/schema ports are --
MockProvider is a dev/test-only utility, never a real provider, and its
JSON float rendering (0.5/14.0's exact digit string) is not something
either plane's actual behavior depends on matching. The comparison is
STRUCTURAL: does the same category win, does the same quote/source get
picked, does the response pass validate_llm_payload.

Usage:
    python tests/fixtures/generate_mock_provider_python_golden.py            # rewrite
    python tests/fixtures/generate_mock_provider_python_golden.py --stdout   # print (rot guard)
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dev_health_ops.llm.providers.mock import MockProvider  # noqa: E402

OUTPUT_PATH = Path(__file__).parent / "mock_provider_python_golden.json"

CATEGORIZATION_MARKER = (
    'Output schema:\n{\n  "subcategories": {...}, "evidence_quotes": [...]}\n'
)


def _prompt(source_block: str) -> str:
    return f"{CATEGORIZATION_MARKER}\nSource text (quotes must be exact substrings):\n{source_block}"


def _cases() -> list[dict[str, Any]]:
    return [
        {
            "label": "incident_keyword",
            "source_block": "[issue] E1\nProduction incident caused an outage overnight\n",
        },
        {
            "label": "refactor_keyword",
            "source_block": "[pr] E1\nRefactor the legacy auth module, cleanup dead code\n",
        },
        {
            "label": "bugfix_keyword",
            "source_block": "[commit] E1\nFix the flaky test and improve reliability\n",
        },
        {
            "label": "security_keyword",
            "source_block": "[issue] E1\nAddress a security vulnerability in the login flow\n",
        },
        {
            "label": "no_keyword_match_defaults_feature_delivery",
            "source_block": "[issue] E1\nAdd a new export button to the dashboard\n",
        },
        {
            "label": "multiple_source_blocks_uses_first_only",
            "source_block": (
                "[issue] E1\nFirst block, mentions a security vulnerability\n\n"
                "[pr] E2\nSecond block, mentions a refactor\n"
            ),
        },
        {"label": "no_source_block_at_all", "source_block": "(EMPTY)"},
        {
            "label": "quote_truncated_to_80_chars",
            "source_block": "[issue] E1\n" + ("word " * 30) + "\n",
        },
    ]


async def _run_case(source_block: str) -> str:
    provider = MockProvider()
    result = await provider.complete(_prompt(source_block))
    await provider.aclose()
    return result.text


def build_golden() -> dict[str, object]:
    cases = []
    for case in _cases():
        raw_text = asyncio.run(_run_case(case["source_block"]))
        payload = json.loads(raw_text)
        subcategories = payload["subcategories"]
        top_category = max(subcategories, key=lambda k: subcategories[k])
        cases.append(
            {
                "label": case["label"],
                "source_block": case["source_block"],
                "top_category": top_category,
                "top_weight": subcategories[top_category],
                "evidence_quotes": payload["evidence_quotes"],
                "uncertainty": payload["uncertainty"],
                "subcategory_count": len(subcategories),
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
