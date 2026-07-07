"""Unit tests for the standalone compounding-risk job (CHAOS-2888).

Covers the Workstream B contract: full-input recompute produces a non-null
score, missing complexity history is surfaced via the shared
``missing_complexity_delta`` reason, and repeated recompute with unchanged
persisted inputs is deterministic (append-only writes, equivalent
score/severity across recomputes).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import date, timedelta
from typing import Any

from dev_health_ops.metrics import job_compounding_risk
from dev_health_ops.metrics.compounding_risk import (
    MISSING_INPUT_REASONS,
    REASON_MISSING_COMPLEXITY_DELTA,
)
from dev_health_ops.metrics.job_compounding_risk import run_compounding_risk_job

DAY = date(2026, 5, 20)


class _FakeSink:
    """Duck-typed stand-in for ``ClickHouseMetricsSink`` used by the job.

    Only implements the surface ``run_compounding_risk_job`` actually calls:
    ``ensure_tables``, ``get_all_teams`` (async), ``query_dicts``, and
    ``write_compounding_risk_daily``.
    """

    def __init__(
        self,
        *,
        repo_metrics_by_day: dict[date, list[dict[str, Any]]],
        complexity_by_repo: dict[str, dict[str, float | None] | None] | None = None,
    ) -> None:
        self._repo_metrics_by_day = repo_metrics_by_day
        self._complexity_by_repo = complexity_by_repo or {}
        self.written: list[Any] = []

    def ensure_tables(self) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM repos" in query:
            return []
        if "FROM repo_metrics_daily" in query:
            return list(self._repo_metrics_by_day.get(parameters["day"], []))
        if "FROM repo_complexity_daily" in query:
            result = self._complexity_by_repo.get(parameters["repo_id"])
            return [result] if result is not None else []
        raise AssertionError(f"unexpected query: {query}")

    def write_compounding_risk_daily(self, rows: list[Any]) -> None:
        self.written.extend(rows)


def _run(*, day: date = DAY, backfill_days: int = 1) -> int:
    return asyncio.run(
        run_compounding_risk_job(
            db_url="clickhouse://localhost:8123/default",
            day=day,
            backfill_days=backfill_days,
            org_id="acme",
        )
    )


def test_full_input_recompute_writes_non_null_non_unknown_row(
    monkeypatch: Any,
) -> None:
    repo_id = uuid.uuid4()
    sink = _FakeSink(
        repo_metrics_by_day={
            DAY: [
                {
                    "repo_id": str(repo_id),
                    "rework_churn_ratio_30d": 0.12,
                    "single_owner_file_ratio_30d": 0.5,
                    "code_ownership_gini": 0.4,
                    "bus_factor": 2,
                    "pr_first_review_p90_hours": 18.0,
                }
            ]
        },
        complexity_by_repo={str(repo_id): {"first_half": 100.0, "second_half": 118.0}},
    )
    monkeypatch.setattr(
        job_compounding_risk, "ClickHouseMetricsSink", lambda db_url: sink
    )

    exit_code = _run()

    assert exit_code == 0
    assert len(sink.written) == 1
    row = sink.written[0]
    assert row.compounding_risk is not None
    assert row.severity != "unknown"


def test_missing_complexity_history_reports_missing_complexity_delta_reason(
    monkeypatch: Any, caplog: Any
) -> None:
    repo_id = uuid.uuid4()
    sink = _FakeSink(
        repo_metrics_by_day={
            DAY: [
                {
                    "repo_id": str(repo_id),
                    "rework_churn_ratio_30d": 0.10,
                    "single_owner_file_ratio_30d": 0.4,
                    "code_ownership_gini": 0.3,
                    "bus_factor": 3,
                    "pr_first_review_p90_hours": 20.0,
                }
            ]
        },
        complexity_by_repo={},  # no persisted repo_complexity_daily history
    )
    monkeypatch.setattr(
        job_compounding_risk, "ClickHouseMetricsSink", lambda db_url: sink
    )

    with caplog.at_level("INFO", logger=job_compounding_risk.logger.name):
        exit_code = _run()

    assert exit_code == 0
    row = sink.written[0]
    assert row.compounding_risk is None
    assert row.severity == "unknown"
    assert any(
        REASON_MISSING_COMPLEXITY_DELTA in message for message in caplog.messages
    )


def test_repeated_recompute_with_same_inputs_is_idempotent(monkeypatch: Any) -> None:
    repo_id = uuid.uuid4()
    sink = _FakeSink(
        repo_metrics_by_day={
            DAY: [
                {
                    "repo_id": str(repo_id),
                    "rework_churn_ratio_30d": 0.15,
                    "single_owner_file_ratio_30d": 0.6,
                    "code_ownership_gini": 0.5,
                    "bus_factor": 2,
                    "pr_first_review_p90_hours": 30.0,
                }
            ]
        },
        complexity_by_repo={str(repo_id): {"first_half": 100.0, "second_half": 125.0}},
    )
    monkeypatch.setattr(
        job_compounding_risk, "ClickHouseMetricsSink", lambda db_url: sink
    )

    _run()
    _run()  # recompute: persisted inputs unchanged, second write is appended

    # Append-only: both rows are present (no overwrite/dedup of the first
    # write), and the second row's computed_at is never earlier than the
    # first's -- each recompute stamps its own compute moment rather than
    # mutating the prior row in place.
    assert len(sink.written) == 2
    first, second = sink.written
    assert first.scope_id == second.scope_id == str(repo_id)
    assert second.computed_at >= first.computed_at
    # Recompute with unchanged inputs must not drift the score/severity
    # between the two writes (a reader resolves the latest row via
    # argMax(computed_at)).
    assert first.compounding_risk == second.compounding_risk
    assert first.severity == second.severity


def test_backfill_day_with_no_repo_metrics_rows_is_named_in_final_summary(
    monkeypatch: Any, caplog: Any
) -> None:
    repo_id = uuid.uuid4()
    sink = _FakeSink(
        repo_metrics_by_day={
            DAY: [
                {
                    "repo_id": str(repo_id),
                    "rework_churn_ratio_30d": 0.12,
                    "single_owner_file_ratio_30d": 0.5,
                    "code_ownership_gini": 0.4,
                    "bus_factor": 2,
                    "pr_first_review_p90_hours": 18.0,
                }
            ]
            # DAY - 1 intentionally has no repo_metrics_daily rows.
        },
        complexity_by_repo={str(repo_id): {"first_half": 100.0, "second_half": 118.0}},
    )
    monkeypatch.setattr(
        job_compounding_risk, "ClickHouseMetricsSink", lambda db_url: sink
    )

    empty_day = DAY - timedelta(days=1)
    with caplog.at_level("INFO", logger=job_compounding_risk.logger.name):
        exit_code = _run(backfill_days=2)

    assert exit_code == 0
    assert len(sink.written) == 1  # only the day with rows produced output

    done_messages = [
        m for m in caplog.messages if m.startswith("compounding-risk: done")
    ]
    assert len(done_messages) == 1
    done_message = done_messages[0]
    # The zero-row day is named in the final summary, so operators can
    # distinguish "no repos synced this day" from "repos synced but all
    # required inputs were null".
    assert empty_day.isoformat() in done_message
    # Fixed reason keys are always present, in stable order, even though
    # only one of the two backfilled days actually produced diagnostics.
    for reason in MISSING_INPUT_REASONS:
        assert f"'{reason}': 0" in done_message
