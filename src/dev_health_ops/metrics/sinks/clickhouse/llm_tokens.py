from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import (
    LLMTokenSpendLegacyRecord,
    LLMTokenSpendRunRecord,
    LLMTokenSpendSummaryRecord,
    LLMTokenUsageRecord,
)

DEFAULT_LLM_SPEND_LIMIT = 20
MAX_LLM_SPEND_LIMIT = 100
DEFAULT_LLM_SPEND_DAYS = 30

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


class LLMTokenUsageMixin(_ClickHouseSinkBase):
    def write_llm_token_usage(self, rows: Sequence[LLMTokenUsageRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "llm_token_usage",
            [
                "org_id",
                "run_id",
                "provider",
                "model",
                "source",
                "input_tokens",
                "output_tokens",
                "calls",
                "computed_at",
            ],
            rows,
        )

    def read_llm_token_spend(
        self,
        *,
        org_id: str,
        limit: int = DEFAULT_LLM_SPEND_LIMIT,
        since: datetime | None = None,
    ) -> LLMTokenSpendSummaryRecord:
        bounded_limit = min(
            max(1, int(limit or DEFAULT_LLM_SPEND_LIMIT)), MAX_LLM_SPEND_LIMIT
        )
        window_start = since or datetime.now(timezone.utc) - timedelta(
            days=DEFAULT_LLM_SPEND_DAYS
        )
        latest_rows = (
            self.client.query(
                """
            SELECT run_id, max(computed_at) AS last_at
            FROM llm_token_usage
            WHERE org_id = {org_id:String}
              AND run_id != ''
              AND computed_at >= {since:DateTime}
            GROUP BY run_id
            ORDER BY last_at DESC, run_id DESC
            LIMIT {limit:UInt32}
            """,
                parameters={
                    "org_id": org_id,
                    "since": window_start,
                    "limit": bounded_limit,
                },
            ).result_rows
            or []
        )
        run_ids = [str(row[0]) for row in latest_rows if row and str(row[0])]
        runs = self._read_llm_spend_runs(
            org_id=org_id,
            since=window_start,
            run_ids=run_ids,
        )
        legacy = self._read_llm_spend_legacy(org_id=org_id, since=window_start)
        return LLMTokenSpendSummaryRecord(
            since=window_start,
            limit=bounded_limit,
            runs=runs,
            legacy=legacy,
        )

    def _read_llm_spend_runs(
        self,
        *,
        org_id: str,
        since: datetime,
        run_ids: Sequence[str],
    ) -> list[LLMTokenSpendRunRecord]:
        if not run_ids:
            return []
        rows = (
            self.client.query(
                """
            SELECT
                run_id,
                provider,
                model,
                sum(calls) AS calls,
                sum(input_tokens) AS input_tokens,
                sum(output_tokens) AS output_tokens,
                max(computed_at) AS computed_at
            FROM llm_token_usage
            WHERE org_id = {org_id:String}
              AND run_id IN {run_ids:Array(String)}
              AND computed_at >= {since:DateTime}
            GROUP BY run_id, provider, model
            ORDER BY computed_at DESC, run_id DESC, model ASC
            """,
                parameters={"org_id": org_id, "since": since, "run_ids": list(run_ids)},
            ).result_rows
            or []
        )
        failures = self._read_llm_spend_failures(
            org_id=org_id,
            since=since,
            run_ids=run_ids,
        )
        return [
            LLMTokenSpendRunRecord(
                run_id=str(row[0]),
                provider=str(row[1]),
                model=str(row[2]),
                calls=int(row[3] or 0),
                input_tokens=int(row[4] or 0),
                output_tokens=int(row[5] or 0),
                computed_at=row[6],
                failures_by_class=dict(failures.get(str(row[0]), Counter())),
            )
            for row in rows
        ]

    def _read_llm_spend_legacy(
        self, *, org_id: str, since: datetime
    ) -> list[LLMTokenSpendLegacyRecord]:
        rows = (
            self.client.query(
                """
            SELECT
                provider,
                model,
                sum(calls) AS calls,
                sum(input_tokens) AS input_tokens,
                sum(output_tokens) AS output_tokens,
                max(computed_at) AS computed_at
            FROM llm_token_usage
            WHERE org_id = {org_id:String}
              AND run_id = ''
              AND computed_at >= {since:DateTime}
            GROUP BY provider, model
            ORDER BY computed_at DESC, model ASC
            """,
                parameters={"org_id": org_id, "since": since},
            ).result_rows
            or []
        )
        return [
            LLMTokenSpendLegacyRecord(
                provider=str(row[0]),
                model=str(row[1]),
                calls=int(row[2] or 0),
                input_tokens=int(row[3] or 0),
                output_tokens=int(row[4] or 0),
                computed_at=row[5],
            )
            for row in rows
        ]

    def _read_llm_spend_failures(
        self,
        *,
        org_id: str,
        since: datetime,
        run_ids: Sequence[str],
    ) -> dict[str, Counter[str]]:
        rows = (
            self.client.query(
                """
            SELECT
                categorization_run_id,
                categorization_status,
                categorization_errors_json
            FROM work_unit_investments
            WHERE org_id = {org_id:String}
              AND categorization_run_id IN {run_ids:Array(String)}
              AND computed_at >= {since:DateTime}
            """,
                parameters={"org_id": org_id, "since": since, "run_ids": list(run_ids)},
            ).result_rows
            or []
        )
        failures: defaultdict[str, Counter[str]] = defaultdict(Counter)
        for run_id, status, errors_json in rows:
            outcome_class = _categorization_outcome_class(
                status=str(status or ""),
                errors_json=str(errors_json or ""),
            )
            if outcome_class:
                failures[str(run_id)][outcome_class] += 1
        return dict(failures)


def _categorization_outcome_class(*, status: str, errors_json: str) -> str:
    if status == "ok":
        return ""
    if status:
        return status
    try:
        parsed = json.loads(errors_json or "[]")
    except json.JSONDecodeError:
        return "categorization_error"
    if isinstance(parsed, list) and parsed:
        return "categorization_error"
    return "unknown_outcome"
