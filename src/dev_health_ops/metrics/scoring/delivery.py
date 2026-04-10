"""Delivery dimension scorer.

Signals: pipeline_success_rate, pipeline_duration_p95, pr_cycle_time, throughput.
Sources: testops_pipeline_metrics_daily, repo_metrics_daily.
"""

from __future__ import annotations

from datetime import date

from dev_health_ops.metrics.scoring.dimensions import (
    ClickHouseClient,
    DimensionScorer,
    _clamp,
)

_PIPELINE_TABLE = "testops_pipeline_metrics_daily"
_REPO_TABLE = "repo_metrics_daily"

_DURATION_P95_CEILING_SEC = 3600.0  # 1h
_PR_CYCLE_CEILING_HOURS = 168.0  # 1w
_THROUGHPUT_CEILING = 50.0  # PRs/day


class DeliveryScorer(DimensionScorer):
    @property
    def dimension_name(self) -> str:
        return "delivery"

    @property
    def signal_definitions(self) -> list[tuple[str, float, str]]:
        return [
            ("pipeline_success_rate", 0.35, _PIPELINE_TABLE),
            ("pipeline_duration_p95", 0.25, _PIPELINE_TABLE),
            ("pr_cycle_time", 0.25, _REPO_TABLE),
            ("throughput", 0.15, _REPO_TABLE),
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
        pipeline_query = f"""
            SELECT
                avgMerge(success_rate) AS avg_success_rate,
                maxMerge(p95_duration_seconds) AS max_p95_duration
            FROM (
                SELECT
                    avgState(success_rate) AS success_rate,
                    maxState(p95_duration_seconds) AS p95_duration_seconds
                FROM {_PIPELINE_TABLE}
                WHERE org_id = {{org_id:String}}
                  AND day = {{day:Date}}
                  {team_clause}
                GROUP BY repo_id
            )
        """
        params: dict[str, object] = {"org_id": org_id, "day": str(day)}
        if team_id:
            params["team_id"] = team_id

        result = client.query(pipeline_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            success_rate = row[col_map["avg_success_rate"]]
            if success_rate is not None:
                signals["pipeline_success_rate"] = float(success_rate)

            p95_dur = row[col_map["max_p95_duration"]]
            if p95_dur is not None:
                signals["pipeline_duration_p95"] = _clamp(
                    1.0 - float(p95_dur) / _DURATION_P95_CEILING_SEC
                )

        repo_query = f"""
            SELECT
                avg(median_pr_cycle_hours) AS avg_pr_cycle,
                sum(prs_merged)            AS total_prs_merged
            FROM {_REPO_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
        """
        result = client.query(
            repo_query, parameters={"org_id": org_id, "day": str(day)}
        )
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            pr_cycle = row[col_map["avg_pr_cycle"]]
            if pr_cycle is not None:
                signals["pr_cycle_time"] = _clamp(
                    1.0 - float(pr_cycle) / _PR_CYCLE_CEILING_HOURS
                )

            prs_merged = row[col_map["total_prs_merged"]]
            if prs_merged is not None and prs_merged > 0:
                signals["throughput"] = _clamp(float(prs_merged) / _THROUGHPUT_CEILING)

        return signals
