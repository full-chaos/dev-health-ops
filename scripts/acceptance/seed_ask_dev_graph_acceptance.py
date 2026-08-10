#!/usr/bin/env python3
"""Seed the W3 graph trial from restored-world canonical team metrics."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import clickhouse_connect

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm.projection import build_projection
from dev_health_ops.context_fabric.graph_arm.records import (
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
)
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    GraphEntityKind,
    GraphObservationKind,
)
from dev_health_ops.fixtures.world import derive_id

_OUTLIER_RATIO = 1.20
_METRICS = ("work_in_progress", "cycle_time_p90_days")

_TEAM_METRICS_SQL = """
WITH metric_rows AS (
    SELECT
        lowerUTF8(team_name) AS team_key,
        sum(wip_count_end_of_day) AS work_in_progress,
        avgOrNull(cycle_time_p90_hours) / 24 AS cycle_time_p90_days
    FROM work_item_metrics_daily FINAL
    WHERE org_id = {org_id:String}
      AND day = {measurement_day:Date}
      AND lowerUTF8(team_name) IN ('core', 'growth', 'platform')
    GROUP BY team_key
)
SELECT
    toString(id) AS canonical_id,
    name AS label,
    metric_rows.work_in_progress,
    metric_rows.cycle_time_p90_days
FROM teams FINAL
INNER JOIN metric_rows ON lowerUTF8(teams.name) = metric_rows.team_key
WHERE org_id = {org_id:String} AND is_active = 1
ORDER BY canonical_id
"""


def _primary_org_id(manifest: dict[str, Any]) -> str:
    primary = next(org for org in manifest["orgs"] if org["alias"] == "primary")
    return str(derive_id(int(manifest["master_seed"]), str(primary["id_seed"])))


def _measurement_day(manifest: dict[str, Any]) -> date:
    pinned_now = datetime.fromisoformat(str(manifest["pinned_now"]))
    return (pinned_now - timedelta(days=9)).date()


def build_batch(
    *, org_id: str, rows: list[dict[str, Any]], observed_at: datetime
) -> IngestionBatch:
    if len(rows) < 3:
        raise RuntimeError("graph acceptance requires at least three canonical teams")
    medians = {
        metric: float(statistics.median(float(row[metric]) for row in rows))
        for metric in _METRICS
    }
    corroborated = [
        row
        for row in rows
        if all(
            float(row[metric]) > medians[metric] * _OUTLIER_RATIO for metric in _METRICS
        )
    ]
    if not corroborated:
        raise RuntimeError(
            "restored world has no team with two canonical pressure signals "
            "more than 20% above the cohort median"
        )

    entities: list[EntityRecord] = []
    observations: list[ObservationRecord] = []
    for row in rows:
        canonical_id = str(row["canonical_id"])
        subject = CanonicalRef(kind=GraphEntityKind.TEAM, canonical_id=canonical_id)
        entities.append(
            EntityRecord(
                org_id=org_id,
                kind=GraphEntityKind.TEAM,
                canonical_id=canonical_id,
                display_label=str(row["label"]),
                source_class=SourceClass.WORK_ITEM,
                observed_at=observed_at,
            )
        )
        for metric in _METRICS:
            observations.append(
                ObservationRecord(
                    org_id=org_id,
                    kind=GraphObservationKind.MEASUREMENT,
                    canonical_id=f"w3_{canonical_id}_{metric}",
                    title=f"{metric} measurement",
                    source_class=SourceClass.WORK_ITEM,
                    observed_at=observed_at,
                    subjects=(subject,),
                    attributes={
                        "measurement_metric": metric,
                        "measurement_value": float(row[metric]),
                        "measurement_unit": (
                            "items" if metric == "work_in_progress" else "days"
                        ),
                        "measurement_basis": "canonical_service",
                        "measurement_cohort_median": medians[metric],
                    },
                )
            )
    return IngestionBatch(
        org_id=org_id,
        entities=tuple(entities),
        observations=tuple(observations),
    )


def _query_rows(
    *, dsn: str, org_id: str, measurement_day: date
) -> list[dict[str, Any]]:
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        result = client.query(
            _TEAM_METRICS_SQL,
            parameters={"org_id": org_id, "measurement_day": measurement_day},
        )
        return [
            dict(zip(result.column_names, row, strict=True))
            for row in result.result_rows
        ]
    finally:
        client.close()


async def _write(*, manifest_path: Path, dsn: str) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    org_id = _primary_org_id(manifest)
    measurement_day = _measurement_day(manifest)
    rows = await asyncio.to_thread(
        _query_rows, dsn=dsn, org_id=org_id, measurement_day=measurement_day
    )
    batch = build_batch(org_id=org_id, rows=rows, observed_at=datetime.now(UTC))
    projection = build_projection(batch)
    store = GraphArmStore.for_org(org_id)
    try:
        await store.build_indices()
        result = await store.write_projection(projection)
    finally:
        await store.close()
    if result.nodes_written <= len(batch.entities):
        raise RuntimeError("graph seed wrote no canonical measurement nodes")
    print(
        "W3_GRAPH_SEED=PASSED "
        f"org_id={org_id} measurement_day={measurement_day.isoformat()} "
        f"teams={len(batch.entities)} observations={len(batch.observations)} "
        f"nodes={result.nodes_written}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    dsn = os.getenv("CLICKHOUSE_URI", "").strip()
    if not dsn:
        raise SystemExit("CLICKHOUSE_URI is required")
    asyncio.run(_write(manifest_path=args.manifest, dsn=dsn))


if __name__ == "__main__":
    main()
