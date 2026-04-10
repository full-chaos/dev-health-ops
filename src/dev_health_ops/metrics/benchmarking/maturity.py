from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from dev_health_ops.metrics.testops_schemas import (
    BenchmarkBaselineRecord,
    MaturityBandRecord,
)


def _band_for_percentile(percentile_rank: float) -> str:
    if percentile_rank < 25.0:
        return "emerging"
    if percentile_rank < 50.0:
        return "developing"
    if percentile_rank < 75.0:
        return "established"
    return "leading"


def _confidence_for_percentile(percentile_rank: float) -> float:
    boundaries = (25.0, 50.0, 75.0)
    distance = min(abs(percentile_rank - boundary) for boundary in boundaries)
    scaled = min(distance, 25.0) / 25.0
    return round(0.5 + (scaled * 0.5), 4)


def classify_maturity_bands(
    baselines: Sequence[BenchmarkBaselineRecord], *, computed_at: datetime | None = None
) -> list[MaturityBandRecord]:
    del computed_at
    records: list[MaturityBandRecord] = []
    for baseline in baselines:
        records.append(
            MaturityBandRecord(
                metric_name=baseline.metric_name,
                scope_type=baseline.scope_type,
                scope_key=baseline.scope_key,
                period_start=baseline.period_start,
                period_end=baseline.period_end,
                value=baseline.current_value,
                percentile_rank=baseline.percentile_rank,
                maturity_band=_band_for_percentile(baseline.percentile_rank),
                confidence=_confidence_for_percentile(baseline.percentile_rank),
                computed_at=baseline.computed_at,
                org_id=baseline.org_id,
            )
        )
    return records
