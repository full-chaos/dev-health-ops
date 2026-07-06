from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, cast

import pytest

from dev_health_ops.metrics.llm_token_usage import write_llm_token_usage
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def insert(
        self, table: str, matrix: list[list[object]], column_names: list[str]
    ) -> None:
        self.inserts.append((table, matrix, column_names))


def test_llm_token_sink_accrues_counts_per_org():
    client = FakeClickHouseClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    write_llm_token_usage(
        sink,
        org_id="org-a",
        provider="openai",
        model="gpt-5-mini",
        source="investment_materialize",
        input_tokens=10,
        output_tokens=5,
        calls=1,
        computed_at=now,
    )
    write_llm_token_usage(
        sink,
        org_id="org-a",
        provider="openai",
        model="gpt-5-mini",
        source="investment_materialize",
        input_tokens=7,
        output_tokens=3,
        calls=2,
        computed_at=now,
    )
    write_llm_token_usage(
        sink,
        org_id="org-b",
        provider="openai",
        model="gpt-5-mini",
        source="investment_materialize",
        input_tokens=100,
        output_tokens=50,
        calls=1,
        computed_at=now,
    )

    def default_totals() -> dict[str, int]:
        return {"input": 0, "output": 0, "calls": 0}

    totals: defaultdict[object, dict[str, int]] = defaultdict(default_totals)
    for table, matrix, columns in client.inserts:
        assert table == "llm_token_usage"
        for row in matrix:
            values = dict(zip(columns, row))
            org_totals = totals[values["org_id"]]
            org_totals["input"] += int(cast(Any, values["input_tokens"]))
            org_totals["output"] += int(cast(Any, values["output_tokens"]))
            org_totals["calls"] += int(cast(Any, values["calls"]))

    assert totals["org-a"] == {"input": 17, "output": 8, "calls": 3}
    assert totals["org-b"] == {"input": 100, "output": 50, "calls": 1}


def test_llm_token_sink_writes_run_id_and_defaults_legacy_empty():
    client = FakeClickHouseClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    write_llm_token_usage(
        sink,
        org_id="org-a",
        run_id="run-1",
        provider="openai",
        model="gpt-5-mini",
        source="investment_materialize",
        input_tokens=10,
        output_tokens=5,
        calls=1,
        computed_at=now,
    )
    write_llm_token_usage(
        sink,
        org_id="org-a",
        provider="openai",
        model="gpt-5-mini",
        source="investment_materialize",
        input_tokens=1,
        output_tokens=1,
        calls=1,
        computed_at=now,
    )

    assert len(client.inserts) == 2
    first_columns = client.inserts[0][2]
    assert "run_id" in first_columns

    first_values = dict(zip(first_columns, client.inserts[0][1][0]))
    second_values = dict(zip(client.inserts[1][2], client.inserts[1][1][0]))
    assert first_values["run_id"] == "run-1"
    assert second_values["run_id"] == ""


class FakeQueryResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.result_rows = rows


class FakeSpendClient:
    def __init__(
        self,
        *,
        latest_runs: list[tuple[object, ...]] | None = None,
        run_rows: list[tuple[object, ...]] | None = None,
        legacy_rows: list[tuple[object, ...]] | None = None,
        failure_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.latest_runs = latest_runs or []
        self.run_rows = run_rows or []
        self.legacy_rows = legacy_rows or []
        self.failure_rows = failure_rows or []
        self.calls: list[tuple[str, dict[str, object]]] = []

    def query(self, query: str, parameters: dict[str, object]) -> FakeQueryResult:
        self.calls.append((query, parameters))
        assert parameters["org_id"] == "org-a"
        if "FROM llm_token_usage" in query and "run_id IN" in query:
            return FakeQueryResult(self.run_rows)
        if "FROM llm_token_usage" in query and "run_id = ''" in query:
            return FakeQueryResult(self.legacy_rows)
        if "GROUP BY run_id" in query:
            return FakeQueryResult(self.latest_runs)
        if "FROM work_unit_investments" in query:
            return FakeQueryResult(self.failure_rows)
        pytest.fail(f"unexpected query: {query}")


def test_llm_spend_reader_aggregates_runs_failures_and_caps_limit():
    computed_at = datetime(2026, 1, 2, 3, tzinfo=timezone.utc)
    client = FakeSpendClient(
        latest_runs=[("run-new", computed_at), ("run-old", computed_at)],
        run_rows=[
            ("run-new", "openai", "gpt-5-mini", 3, 30, 15, computed_at),
            ("run-old", "anthropic", "claude-test", 1, 7, 4, computed_at),
        ],
        failure_rows=[
            ("run-new", "llm_task_failed", '["llm_task_failed"]'),
            ("run-new", "ok", "[]"),
            ("run-old", "invalid_llm_output", '["schema_mismatch"]'),
        ],
    )
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)

    summary = sink.read_llm_token_spend(org_id="org-a", limit=999)

    assert client.calls[0][1]["limit"] == 100
    assert [run.run_id for run in summary.runs] == ["run-new", "run-old"]
    assert summary.runs[0].calls == 3
    assert summary.runs[0].input_tokens == 30
    assert summary.runs[0].output_tokens == 15
    assert summary.runs[0].failures_by_class == {"llm_task_failed": 1}
    assert summary.runs[1].failures_by_class == {"invalid_llm_output": 1}
    assert summary.legacy == []


def test_llm_spend_reader_returns_empty_summary_without_rows():
    client = FakeSpendClient()
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)

    summary = sink.read_llm_token_spend(org_id="org-a")

    assert summary.runs == []
    assert summary.legacy == []


def test_llm_spend_reader_marks_legacy_empty_run_id_rows_explicitly():
    computed_at = datetime(2026, 1, 2, 3, tzinfo=timezone.utc)
    client = FakeSpendClient(
        legacy_rows=[("openai", "gpt-legacy", 2, 20, 8, computed_at)],
    )
    sink = ClickHouseMetricsSink("clickhouse://localhost:9000/default", client=client)

    summary = sink.read_llm_token_spend(org_id="org-a")

    assert summary.runs == []
    assert len(summary.legacy) == 1
    assert summary.legacy[0].marker == "legacy_empty_run_id"
    assert summary.legacy[0].run_id == ""
    assert summary.legacy[0].calls == 2
