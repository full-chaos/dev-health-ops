"""Durability dimension scorer.

Signals: coverage_line_pct, test_pass_rate, test_flake_rate_inverse, coverage_branch_pct.
Sources: testops_test_metrics_daily, testops_coverage_metrics_daily.
"""

from __future__ import annotations

from datetime import date

from dev_health_ops.metrics.scoring.dimensions import (
    ClickHouseClient,
    DimensionScorer,
    _clamp,
)

_TEST_TABLE = "testops_test_metrics_daily"
_COVERAGE_TABLE = "testops_coverage_metrics_daily"


class DurabilityScorer(DimensionScorer):
    @property
    def dimension_name(self) -> str:
        return "durability"

    @property
    def signal_definitions(self) -> list[tuple[str, float, str]]:
        return [
            ("coverage_line_pct", 0.30, _COVERAGE_TABLE),
            ("test_pass_rate", 0.30, _TEST_TABLE),
            ("test_flake_rate_inverse", 0.25, _TEST_TABLE),
            ("coverage_branch_pct", 0.15, _COVERAGE_TABLE),
        ]

    def _fetch_signals(
        self,
        client: ClickHouseClient,
        org_id: str,
        day: date,
        team_id: str | None,
    ) -> dict[str, float | None]:
        signals: dict[str, float | None] = {}

        team_clause = "AND team_id = {team_id:String}" if team_id else ""
        test_query = f"""
            SELECT
                avg(pass_rate)  AS avg_pass_rate,
                avg(flake_rate) AS avg_flake_rate
            FROM {_TEST_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_clause}
        """
        params: dict[str, object] = {"org_id": org_id, "day": str(day)}
        if team_id:
            params["team_id"] = team_id

        result = client.query(test_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            pass_rate = row[col_map["avg_pass_rate"]]
            if pass_rate is not None:
                signals["test_pass_rate"] = _clamp(float(pass_rate))

            flake_rate = row[col_map["avg_flake_rate"]]
            if flake_rate is not None:
                signals["test_flake_rate_inverse"] = _clamp(1.0 - float(flake_rate))

        cov_query = f"""
            SELECT
                avg(line_coverage_pct)   AS avg_line_cov,
                avg(branch_coverage_pct) AS avg_branch_cov
            FROM {_COVERAGE_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_clause}
        """
        result = client.query(cov_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            line_cov = row[col_map["avg_line_cov"]]
            if line_cov is not None:
                signals["coverage_line_pct"] = _clamp(float(line_cov) / 100.0)

            branch_cov = row[col_map["avg_branch_cov"]]
            if branch_cov is not None:
                signals["coverage_branch_pct"] = _clamp(float(branch_cov) / 100.0)

        return signals
