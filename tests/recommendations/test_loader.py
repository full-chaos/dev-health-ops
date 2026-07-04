from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import MagicMock

from dev_health_ops.recommendations.loader import ClickHouseMetricsLoader


@dataclass(frozen=True, slots=True)
class _QueryResult:
    column_names: list[str]
    result_rows: list[tuple[float | None, str]]


def test_compounding_risk_loader_preserves_latest_null_score_same_row_severity() -> (
    None
):
    client = MagicMock()
    client.query.return_value = _QueryResult(
        column_names=["score", "severity"],
        result_rows=[(None, "high")],
    )
    loader = ClickHouseMetricsLoader(client=client, org_id="org-1")

    score, severity = loader._load_compounding_risk_persisted(
        "team-1",
        date(2026, 5, 1),
        date(2026, 5, 31),
    )

    assert score is None
    assert severity == "high"
    query: str = client.query.call_args.args[0]
    parameters: dict[str, Any] = client.query.call_args.kwargs["parameters"]
    assert (
        "argMax(tuple(compounding_risk, severity), computed_at) AS latest_row" in query
    )
    assert "tupleElement(latest_row, 1) AS score" in query
    assert "tupleElement(latest_row, 2) AS severity" in query
    assert "argMax(compounding_risk, computed_at)" not in query
    assert "argMax(severity" not in query
    assert parameters == {
        "team_id": "team-1",
        "start": date(2026, 5, 1),
        "end": date(2026, 5, 31),
        "org_id": "org-1",
    }
