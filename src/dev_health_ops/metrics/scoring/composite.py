"""Composite scorer blending all four health dimensions."""

from __future__ import annotations

from datetime import date, datetime

from dev_health_ops.metrics.scoring.delivery import DeliveryScorer
from dev_health_ops.metrics.scoring.dimensions import ClickHouseClient
from dev_health_ops.metrics.scoring.durability import DurabilityScorer
from dev_health_ops.metrics.scoring.dynamics import DynamicsScorer
from dev_health_ops.metrics.scoring.schemas import CompositeScore, DimensionScore
from dev_health_ops.metrics.scoring.wellbeing import WellbeingScorer

_DIMENSION_WEIGHTS: dict[str, float] = {
    "delivery": 0.30,
    "durability": 0.25,
    "wellbeing": 0.25,
    "dynamics": 0.20,
}


class CompositeScorer:
    def __init__(self) -> None:
        self._scorers = [
            DeliveryScorer(),
            DurabilityScorer(),
            WellbeingScorer(),
            DynamicsScorer(),
        ]

    def compute(
        self,
        client: ClickHouseClient,
        org_id: str,
        day: date,
        team_id: str | None = None,
        computed_at: datetime | None = None,
    ) -> CompositeScore:
        now = computed_at or datetime.utcnow()
        dimensions: list[DimensionScore] = []
        weighted_sum = 0.0
        weight_sum = 0.0

        for scorer in self._scorers:
            dim_score = scorer.compute(
                client, org_id, day, team_id=team_id, computed_at=now
            )
            dimensions.append(dim_score)

            if dim_score.score is not None:
                w = _DIMENSION_WEIGHTS[dim_score.dimension]
                weighted_sum += dim_score.score * w
                weight_sum += w

        composite = round(weighted_sum / weight_sum, 4) if weight_sum > 0 else None

        return CompositeScore(
            score=composite,
            dimensions=dimensions,
            day=day,
            org_id=org_id,
            team_id=team_id,
            computed_at=now,
        )
