"""CHAOS-4977 golden generator for the Go port of fetch_investment_breakdown
and fetch_mock_fixture_investment_row_count (api/queries/investment.py).

Never hand-imitate the Python query-building logic: this script calls the
REAL functions with a stubbed `_query_investment_dicts` that captures the
(query, params) pair each call would have executed, instead of hitting a
database, then writes each case as its own JSON golden. The Go test suite
(reader_golden_test.go) reads these back and asserts the Go query builder
produces the byte-identical SQL text and an equivalent binding set for the
same inputs -- it does not need a live ClickHouse for this, only for the
full differential run CHAOS-4977's step 7 does on bigboy.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_breakdown_query_goldens.py
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from dev_health_ops.api.queries import investment

OUT_DIR = Path(__file__).parent

START_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)
END_TS = datetime(2026, 2, 1, tzinfo=timezone.utc)
ORG_ID = "org-golden-4977"

CASES: dict[str, dict[str, Any]] = {
    "no_filters": {},
    "themes_only": {"themes": ["velocity", "quality"]},
    "subcategories_only": {"subcategories": ["velocity.feature", "quality.bugfix"]},
    "themes_and_subcategories": {
        "themes": ["velocity"],
        "subcategories": ["quality.bugfix"],
    },
    "repo_scope": {
        "scope_filter": " AND repo_id IN %(scope_ids)s",
        "scope_params": {"scope_ids": ["repo-1", "repo-2"]},
    },
    "repo_scope_and_themes": {
        "scope_filter": " AND repo_id IN %(scope_ids)s",
        "scope_params": {"scope_ids": ["repo-1"]},
        "themes": ["velocity"],
    },
}


class _CaptureSentinel(Exception):
    """Raised by the stub to abort before any real query executes."""


def _make_capturing_stub(captured: dict[str, Any]):
    async def _stub(sink, query, params):
        captured["query"] = query
        captured["params"] = params
        raise _CaptureSentinel

    return _stub


async def _capture(case_name: str, fn, kwargs: dict[str, Any]) -> dict[str, Any]:
    captured: dict[str, Any] = {}
    with mock.patch.object(
        investment, "_query_investment_dicts", _make_capturing_stub(captured)
    ):
        try:
            await fn(
                sink=None,
                start_ts=START_TS,
                end_ts=END_TS,
                scope_filter=kwargs.get("scope_filter", ""),
                scope_params=kwargs.get("scope_params", {}),
                org_id=ORG_ID,
                themes=kwargs.get("themes"),
                subcategories=kwargs.get("subcategories"),
            )
        except _CaptureSentinel:
            # Expected control flow, not a swallowed error: the stub raises
            # this deliberately (see _make_capturing_stub) right after
            # capturing query/params, so the real DB call is never reached.
            pass
    if "query" not in captured:
        raise RuntimeError(f"{case_name}: stub was never reached")
    return captured


async def main() -> None:
    for fn_name, fn in (
        ("fetch_investment_breakdown", investment.fetch_investment_breakdown),
        (
            "fetch_mock_fixture_investment_row_count",
            investment.fetch_mock_fixture_investment_row_count,
        ),
    ):
        for case_name, kwargs in CASES.items():
            captured = await _capture(case_name, fn, kwargs)
            params = dict(captured["params"])
            # datetimes aren't JSON-serializable; every case uses the same
            # fixed START_TS/END_TS so the Go test supplies them directly
            # rather than round-tripping through the golden file.
            params.pop("start_ts", None)
            params.pop("end_ts", None)
            golden = {
                "function": fn_name,
                "case": case_name,
                "org_id": ORG_ID,
                "query": captured["query"],
                "params": params,
            }
            out_path = OUT_DIR / f"{fn_name}__{case_name}.json"
            out_path.write_text(json.dumps(golden, indent=2, sort_keys=True) + "\n")
            print(f"wrote {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
