"""Dynamics dimension scorer.

Signals: quality_drag_inverse, failure_ownership, wip_congestion_inverse,
         pipeline_failure_rate_inverse.
Sources: testops_quality_drag, testops_pipeline_metrics_daily, work_item_metrics_daily.
"""

from __future__ import annotations

from datetime import date

from dev_health_ops.metrics.scoring.dimensions import (
    ClickHouseClient,
    DimensionScorer,
    _clamp,
)

_QUALITY_DRAG_TABLE = "testops_quality_drag"
_PIPELINE_TABLE = "testops_pipeline_metrics_daily"
_WORK_ITEM_TABLE = "work_item_metrics_daily"

_DRAG_CEILING_HOURS = 8.0  # hours/day ceiling


class DynamicsScorer(DimensionScorer):
    @property
    def dimension_name(self) -> str:
        return "dynamics"

    @property
    def signal_definitions(self) -> list[tuple[str, float, str]]:
        return [
            ("quality_drag_inverse", 0.35, _QUALITY_DRAG_TABLE),
            ("failure_ownership", 0.25, _PIPELINE_TABLE),
            ("wip_congestion_inverse", 0.20, _WORK_ITEM_TABLE),
            ("pipeline_failure_rate_inverse", 0.20, _PIPELINE_TABLE),
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
        params: dict[str, object] = {"org_id": org_id, "day": str(day)}
        if team_id:
            params["team_id"] = team_id

        drag_query = f"""
            SELECT avg(drag_hours) AS avg_drag
            FROM {_QUALITY_DRAG_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_clause}
        """
        result = client.query(drag_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}
            drag = row[col_map["avg_drag"]]
            if drag is not None:
                signals["quality_drag_inverse"] = _clamp(
                    1.0 - float(drag) / _DRAG_CEILING_HOURS
                )

        pipeline_query = f"""
            SELECT
                avg(failure_rate)                       AS avg_failure_rate,
                avgIf(rerun_rate, failure_count > 0)    AS avg_rerun_on_failure
            FROM {_PIPELINE_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_clause}
        """
        result = client.query(pipeline_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}

            failure_rate = row[col_map["avg_failure_rate"]]
            if failure_rate is not None:
                signals["pipeline_failure_rate_inverse"] = _clamp(
                    1.0 - float(failure_rate)
                )

            rerun_on_failure = row[col_map["avg_rerun_on_failure"]]
            if rerun_on_failure is not None:
                signals["failure_ownership"] = _clamp(float(rerun_on_failure))

        wip_query = f"""
            SELECT avg(wip_congestion_ratio) AS avg_congestion
            FROM {_WORK_ITEM_TABLE}
            WHERE org_id = {{org_id:String}}
              AND day = {{day:Date}}
              {team_clause}
        """
        result = client.query(wip_query, parameters=params)
        if result.result_rows:
            row = result.result_rows[0]
            col_map = {n: i for i, n in enumerate(result.column_names)}
            congestion = row[col_map["avg_congestion"]]
            if congestion is not None:
                signals["wip_congestion_inverse"] = _clamp(1.0 - float(congestion))

        return signals
