"""Golden generator for ExplainInvestmentMix (explain.go) -- CHAOS-4977
step 6b. Calls the REAL explain_investment_mix end to end, with every
ClickHouse reader function monkeypatched to return FIXED fixture rows
(no live database -- consistent with the bigboy testcontainer pause) and
either the real "mock" provider (exercises the invalid_llm_output path,
since MockProvider's investment-mix-explanation response never satisfies
investment_mix_parser.py's TOP_LEVEL_KEYS) or a small fake provider
returning a fixed, schema-valid completion (exercises the valid path).

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_explain_investment_mix_golden.py
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from dev_health_ops.api.models.filters import MetricFilter
from dev_health_ops.api.services import investment as investment_service
from dev_health_ops.api.services import investment_mix_explain
from dev_health_ops.api.services import work_units as work_units_service
from dev_health_ops.llm.providers.base import CompletionResult

OUT_DIR = Path(__file__).parent
ORG_ID = "org-golden-4977"
NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

BREAKDOWN_ROWS = [
    {"subcategory": "velocity.feature", "theme": "velocity", "value": 40.0},
    {"subcategory": "quality.bugfix", "theme": "quality", "value": 10.0},
]

WORK_UNIT_ROWS = [
    {
        "work_unit_id": "unit-1",
        "work_unit_type": "issue",
        "work_unit_name": "Ship the new thing",
        "from_ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "to_ts": datetime(2026, 1, 3, tzinfo=timezone.utc),
        "repo_id": "repo-1",
        "provider": "github",
        "effort_metric": "churn_loc",
        "effort_value": 40.0,
        "theme_distribution_json": json.dumps({"velocity": 40.0}),
        "subcategory_distribution_json": json.dumps({"velocity.feature": 40.0}),
        "structural_evidence_json": json.dumps({"issues": ["issue-a"], "prs": []}),
        "evidence_quality": 0.8,
        "evidence_quality_band": "high",
        "categorization_status": "complete",
        "categorization_model_version": "v1",
        "categorization_run_id": "run-1",
        "computed_at": NOW,
    },
    {
        "work_unit_id": "unit-2",
        "work_unit_type": "issue",
        "work_unit_name": "Fix the bug",
        "from_ts": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "to_ts": datetime(2026, 1, 4, tzinfo=timezone.utc),
        "repo_id": "repo-1",
        "provider": "github",
        "effort_metric": "churn_loc",
        "effort_value": 10.0,
        "theme_distribution_json": json.dumps({"quality": 10.0}),
        "subcategory_distribution_json": json.dumps({"quality.bugfix": 10.0}),
        "structural_evidence_json": json.dumps({"issues": ["issue-b"], "prs": []}),
        "evidence_quality": 0.3,
        "evidence_quality_band": "low",
        "categorization_status": "complete",
        "categorization_model_version": "v1",
        "categorization_run_id": "run-1",
        "computed_at": NOW,
    },
]

QUOTE_ROWS = [
    {
        "work_unit_id": "unit-1",
        "quote": "Ship the new thing quote",
        "source_type": "issue",
        "source_id": "issue-a",
        "categorization_run_id": "run-1",
    },
]

REPO_SCOPES = {"repo-1": "myorg/myrepo"}
TEAM_ASSIGNMENTS = {
    "issue-a": {"team_id": "team-1", "team_name": "Platform"},
    "issue-b": {"team_id": "team-1", "team_name": "Platform"},
}

VALID_COMPLETION_TEXT = json.dumps(
    {
        "summary": "Effort appears to lean toward velocity work this period.",
        "top_findings": [
            {
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
        ],
        "confidence": {
            "level": "moderate",
            "quality_mean": 0.55,
            "quality_stddev": 0.25,
            "band_mix": {
                "high": 1,
                "moderate": 0,
                "low": 1,
                "very_low": 0,
                "unknown": 0,
            },
            "drivers": [],
        },
        "what_to_check_next": [
            {
                "action": "Review feature-delivery evidence quotes",
                "why": "Confirms the dominant velocity subcategory",
                "where": "Work unit evidence panel",
            }
        ],
        "anti_claims": ["This does not indicate declining quality investment."],
    }
)


class _FakeSinkContext:
    """Stands in for `async with clickhouse_client(db_url) as sink:` --
    the sink object itself is never touched (every fetch_* call is
    monkeypatched to ignore it), only the context-manager protocol and
    require_clickhouse_backend's pass-through matter."""

    async def __aenter__(self):
        return mock.MagicMock()

    async def __aexit__(self, *exc_info):
        return False


class _FakeProvider:
    def __init__(self, text: str):
        self._text = text

    async def complete(self, prompt: str) -> CompletionResult:
        return CompletionResult(
            text=self._text, input_tokens=11, output_tokens=22, model="fake-recorded"
        )

    async def aclose(self) -> None:
        pass


def _patches(llm_provider: str):
    async def fake_fetch_investment_breakdown(sink, **kwargs):
        return BREAKDOWN_ROWS

    async def fake_fetch_mock_fixture_investment_row_count(sink, **kwargs):
        return 0

    async def fake_fetch_investment_quality_stats(sink, **kwargs):
        return {}

    async def fake_fetch_work_unit_investments(sink, **kwargs):
        return WORK_UNIT_ROWS

    async def fake_fetch_repo_scopes(sink, **kwargs):
        return REPO_SCOPES

    async def fake_fetch_repo_identities(sink, **kwargs):
        return {}

    async def fake_fetch_work_item_team_assignments(sink, **kwargs):
        return TEAM_ASSIGNMENTS

    async def fake_fetch_work_unit_investment_quotes(sink, **kwargs):
        return QUOTE_ROWS

    patches = [
        mock.patch.object(
            investment_service, "clickhouse_client", lambda db_url: _FakeSinkContext()
        ),
        mock.patch.object(
            investment_service, "require_clickhouse_backend", lambda sink: None
        ),
        # _tables_present/_columns_present issue a real presence-check
        # query against `sink` before ever reaching fetch_investment_
        # breakdown -- against the MagicMock sink from _FakeSinkContext
        # that query can't behave like a real one, so short-circuit the
        # guard itself rather than trying to make the mock answer it.
        mock.patch.object(
            investment_service, "_tables_present", mock.AsyncMock(return_value=True)
        ),
        mock.patch.object(
            investment_service, "_columns_present", mock.AsyncMock(return_value=True)
        ),
        mock.patch.object(
            investment_service,
            "fetch_investment_breakdown",
            fake_fetch_investment_breakdown,
        ),
        mock.patch.object(
            investment_service,
            "fetch_mock_fixture_investment_row_count",
            fake_fetch_mock_fixture_investment_row_count,
        ),
        mock.patch.object(
            investment_service,
            "fetch_investment_quality_stats",
            fake_fetch_investment_quality_stats,
        ),
        mock.patch.object(
            investment_service,
            "resolve_repo_filter_ids",
            mock.AsyncMock(return_value=[]),
        ),
        mock.patch.object(
            work_units_service, "clickhouse_client", lambda db_url: _FakeSinkContext()
        ),
        mock.patch.object(
            work_units_service, "require_clickhouse_backend", lambda sink: None
        ),
        mock.patch.object(
            work_units_service,
            "fetch_work_unit_investments",
            fake_fetch_work_unit_investments,
        ),
        mock.patch.object(
            work_units_service, "fetch_repo_scopes", fake_fetch_repo_scopes
        ),
        mock.patch.object(
            work_units_service, "fetch_repo_identities", fake_fetch_repo_identities
        ),
        mock.patch.object(
            work_units_service,
            "fetch_work_item_team_assignments",
            fake_fetch_work_item_team_assignments,
        ),
        mock.patch.object(
            work_units_service,
            "fetch_work_unit_investment_quotes",
            fake_fetch_work_unit_investment_quotes,
        ),
        mock.patch.object(
            work_units_service,
            "resolve_repo_filter_ids",
            mock.AsyncMock(return_value=[]),
        ),
        # ClickHouseMetricsSink itself is patched separately in run_case,
        # via a capturing fake rather than a bare MagicMock, so the
        # write_investment_explanation call can be observed.
    ]
    if llm_provider != "mock":
        patches.append(
            mock.patch.object(
                investment_mix_explain,
                "get_provider",
                lambda *a, **k: _FakeProvider(VALID_COMPLETION_TEXT),
            )
        )
    return patches


async def run_case(case_name: str, llm_provider: str) -> dict[str, Any]:
    filters = MetricFilter()
    patches = _patches(llm_provider)
    env_patch = None
    if llm_provider != "mock":
        # is_llm_available/resolve_provider_name run FOR REAL (not
        # patched) and need a recognized, "configured" provider name --
        # get_provider itself is the only thing faked (_FakeProvider),
        # so the credential presence check still has to pass honestly.
        env_patch = mock.patch.dict(
            "os.environ", {"OPENAI_API_KEY": "sk-fixture-not-real"}
        )
        env_patch.start()

    # Capture the EXACT record write_investment_explanation would have
    # received -- explanation_json is json.dumps(explanation_data) as
    # Python actually writes it (field order, separators, no sort_keys),
    # which the OUTER json.dumps(golden, indent=2, sort_keys=True) call
    # this script uses for the golden FILE ITSELF would otherwise erase.
    # Only a non-mock call ever reaches write_investment_explanation
    # (investment_mix_explain.py:532), so this stays None for the mock
    # case, matching that real gate.
    captured_record: dict[str, Any] | None = None

    class _CapturingSink:
        def write_investment_explanation(self, record):
            nonlocal captured_record
            captured_record = {
                "cache_key": record.cache_key,
                "explanation_json": record.explanation_json,
                "llm_provider": record.llm_provider,
                "llm_model": record.llm_model,
            }

        def write_llm_token_usage(self, rows):
            # _persist_investment_mix_token_usage also opens a sink;
            # not under test here (llm_token_usage_golden_test.go already
            # covers BuildLLMTokenUsageRecord's own defaulting logic), so
            # this only needs to not raise.
            pass

        def close(self):
            pass

    for p in patches:
        p.start()
    sink_patch = mock.patch.object(
        investment_mix_explain, "ClickHouseMetricsSink", lambda db_url: _CapturingSink()
    )
    sink_patch.start()
    try:
        result = await investment_mix_explain.explain_investment_mix(
            db_url="unused://fixture",
            filters=filters,
            theme=None,
            subcategory=None,
            org_id=ORG_ID,
            llm_provider=llm_provider,
            llm_model=None,
            force_refresh=True,
        )
    finally:
        sink_patch.stop()
        for p in patches:
            p.stop()
        if env_patch is not None:
            env_patch.stop()

    dump = result.model_dump() if hasattr(result, "model_dump") else result.dict()
    return {
        "case": case_name,
        "llm_provider": llm_provider,
        "result": dump,
        "written_cache_record": captured_record,
    }


async def main() -> None:
    cases = [
        ("mock_provider_invalid_llm_output", "mock"),
        ("recorded_fixture_provider_valid", "openai"),
    ]
    for case_name, llm_provider in cases:
        golden = await run_case(case_name, llm_provider)
        out_path = OUT_DIR / f"explain_investment_mix__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}  status={golden['result'].get('status')}")


if __name__ == "__main__":
    asyncio.run(main())
