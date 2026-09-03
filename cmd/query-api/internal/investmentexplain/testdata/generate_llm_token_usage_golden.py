"""Golden generator for BuildLLMTokenUsageRecord (cachewrite.go) --
CHAOS-4977 step 4 write side. Calls the REAL write_llm_token_usage with a
capturing stub sink so the exact LLMTokenUsageRecord it would have
written is observed directly, not re-derived from reading the source.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_llm_token_usage_golden.py
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.llm_token_usage import write_llm_token_usage

OUT_DIR = Path(__file__).parent
FIXED_COMPUTED_AT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class _CapturingSink:
    def __init__(self):
        self.captured: list[Any] = None

    def write_llm_token_usage(self, rows):
        self.captured = list(rows)


CASES: dict[str, dict[str, Any]] = {
    "typical": {
        "org_id": "org-golden-4977",
        "provider": "openai",
        "model": "gpt-5",
        "input_tokens": 120,
        "output_tokens": 45,
    },
    "model_none_falls_back_unknown": {
        "org_id": "org-golden-4977",
        "provider": "openai",
        "model": None,
        "input_tokens": 10,
        "output_tokens": 5,
    },
    "provider_empty_falls_back_unknown": {
        "org_id": "org-golden-4977",
        "provider": "",
        "model": "gpt-5",
        "input_tokens": 10,
        "output_tokens": 5,
    },
    "both_token_counts_none": {
        "org_id": "org-golden-4977",
        "provider": "openai",
        "model": "gpt-5",
        "input_tokens": None,
        "output_tokens": None,
    },
    "one_token_count_zero_one_present": {
        "org_id": "org-golden-4977",
        "provider": "openai",
        "model": "gpt-5",
        "input_tokens": 0,
        "output_tokens": 7,
    },
}


def main() -> None:
    for case_name, case in CASES.items():
        sink = _CapturingSink()
        persisted = write_llm_token_usage(
            sink,
            org_id=case["org_id"],
            provider=case["provider"],
            model=case["model"],
            source="investment_mix_explain",
            input_tokens=case["input_tokens"],
            output_tokens=case["output_tokens"],
            computed_at=FIXED_COMPUTED_AT,
        )
        record = None
        if sink.captured:
            record = dataclasses.asdict(sink.captured[0])
            record["computed_at"] = record["computed_at"].isoformat()

        golden = {
            "case": case_name,
            "input": case,
            "persisted": persisted,
            "wrote_a_row": sink.captured is not None,
            "record": record,
        }
        out_path = OUT_DIR / f"llm_token_usage__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
