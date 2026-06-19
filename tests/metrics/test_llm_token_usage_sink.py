from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, cast

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
