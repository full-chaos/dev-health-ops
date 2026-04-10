"""Well-being dimension scorer.

Signals: pipeline_queue_time_inverse, rerun_rate_inverse,
         after_hours_ratio_inverse, weekend_ratio_inverse.
Sources: testops_pipeline_metrics_daily, team_metrics_daily.
"""

from __future__ import annotations

from datetime import date

from dev_health_ops.metrics.scoring.dimensions import (
    ClickHouseClient,
    DimensionScorer,
    _clamp,
)

_PIPELINE_TABLE = "testops_pipeline_metrics_daily"
_TEAM_TABLE = "team_metrics_daily"

_QUEUE_CEILING_SEC = 600.0  # 10 min


class WellbeingScorer(DimensionScorer):
    @property
    def dimension_name(self) -> str:
        return "wellbeing"

    @property
    def signal_definitions(self) -> list[tuple[str, float, str]]:
        return [
            ("pipeline_queue_time_inverse", 0.30, _PIPELINE_TABLE),
            ("rerun_rate_inverse", 0.25, _PIPELINE_TABLE),
            ("after_hours_ratio_inverse", 0.25, _TEAM_TABLE),
            ("weekend_ratio_inverse", 0.20, _TEAM_TABLE),
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
                avg(avg_queue_seconds) AS avg_queue,
                avg(rerun_rate)        AS avg_rerun
            FROM {_PIPELINE_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_clause}
        """
        params: dict[str, object] = {"org_id": org_id, "day": str(day)}
        if team_id:
            params["team_id"] = team_id

        result = client.query(pipeline_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            avg_queue = row[col_map["avg_queue"]]
            if avg_queue is not None:
                signals["pipeline_queue_time_inverse"] = _clamp(
                    1.0 - float(avg_queue) / _QUEUE_CEILING_SEC
                )

            rerun_rate = row[col_map["avg_rerun"]]
            if rerun_rate is not None:
                signals["rerun_rate_inverse"] = _clamp(1.0 - float(rerun_rate))

        team_filter = "AND team_id = {team_id:String}" if team_id else ""
        team_query = f"""
            SELECT
                avg(after_hours_commit_ratio) AS avg_after_hours,
                avg(weekend_commit_ratio)     AS avg_weekend
            FROM {_TEAM_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_filter}
        """
        team_params: dict[str, object] = {"org_id": org_id, "day": str(day)}
        if team_id:
            team_params["team_id"] = team_id

        result = client.query(team_query, parameters=team_params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            after_hours = row[col_map["avg_after_hours"]]
            if after_hours is not None:
                signals["after_hours_ratio_inverse"] = _clamp(1.0 - float(after_hours))

            weekend = row[col_map["avg_weekend"]]
            if weekend is not None:
                signals["weekend_ratio_inverse"] = _clamp(1.0 - float(weekend))

        return signals
