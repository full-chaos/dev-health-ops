"""Golden generator for CHAOS-4818's tenth site (codex round 3, P1 EXECUTED):
computePipelineStability's successRate7d/num/den compound-assignment
accumulators (testops_risk_native_clickhouse.go), found unguarded by
`go tool objdump` (FMADDD/FMSUBD present) even though the ordinary
`x*y + z` shape was already covered elsewhere in this PR.

IMPORTS and calls the real production compute_pipeline_stability
(compute_testops_risk.py), never reimplemented. Records are constructed
with a fixed failure/cancel shape (so failure_clustering_score is constant
across cases and does not confound the success_rate_7d/success_rate_trend
comparison this golden targets) and varying success_rate values.

This tests the STORED (4-decimal-rounded) values, matching what
computePipelineStability itself returns -- coarser than the unrounded
bit-pattern goldens elsewhere in this repo, but sufficient here: the
corpus was chosen from values already known (from the sibling CHAOS-4824
PR's unrounded corpus) to produce a fused-vs-separated divergence before
rounding, and this generator's own the live-Python comparison is the
authority on whether that divergence survives rounding for this specific
site.
"""

from __future__ import annotations

import argparse
import json
import struct
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.compute_testops_risk import compute_pipeline_stability
from dev_health_ops.metrics.testops_schemas import PipelineMetricsDailyRecord

OUTPUT = Path(__file__).with_name("pipeline_stability_fma_golden.json")
REPO_ID = uuid.UUID("00000000-0000-4000-8000-00000000000a")
DAY = date(2026, 9, 2)
COMPUTED_AT = datetime(2026, 9, 2, tzinfo=timezone.utc)


def bits_hex(value: float) -> str:
    return "0x" + format(struct.unpack(">Q", struct.pack(">d", value))[0], "016x")


def _corpus() -> list[list[float]]:
    return [
        [0.83, 0.91, 0.76],
        [0.999999, 0.000001, 0.5],
        [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        [
            0.6096000518410029,
            0.7468696412598451,
            0.8209786640137855,
            0.971871000250248,
            0.08854444946100504,
        ],
        [
            0.9998759444562416,
            0.19747191272446663,
            0.6319593847790337,
            0.7055509208213548,
            0.17178150658707536,
        ],
    ]


def _cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for success_rates in _corpus():
        records = [
            PipelineMetricsDailyRecord(
                repo_id=REPO_ID,
                day=DAY,
                pipelines_count=10,
                success_count=8,
                failure_count=1,
                cancelled_count=1,
                success_rate=rate,
                failure_rate=0.1,
                cancel_rate=0.1,
                rerun_rate=0.0,
                median_duration_seconds=None,
                p95_duration_seconds=None,
                avg_queue_seconds=None,
                p95_queue_seconds=None,
                computed_at=COMPUTED_AT,
            )
            for rate in success_rates
        ]
        result = compute_pipeline_stability(
            day=DAY, pipeline_metrics_7d=records, computed_at=COMPUTED_AT
        )[0]
        rows.append(
            {
                "success_rates": success_rates,
                "success_rate_7d_bits": bits_hex(result.success_rate_7d),
                "success_rate_trend_bits": bits_hex(result.success_rate_trend),
            }
        )
    return rows


def render() -> str:
    value = {"schema_version": 1, "pipeline_stability": _cases()}
    return (
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.stdout:
        print(rendered, end="")
        return 0
    if args.check:
        return 0 if OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
