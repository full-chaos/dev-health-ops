from __future__ import annotations

from typing import List

from ..models.filters import MetricFilter
from ..models.schemas import OpportunitiesResponse, OpportunityCard
from .home import build_home_response
from .cache import TTLCache


async def build_opportunities_response(
    *,
    db_url: str,
    filters: MetricFilter,
    cache: TTLCache,
) -> OpportunitiesResponse:
    home = await build_home_response(
        db_url=db_url,
        filters=filters,
        cache=cache,
    )

    negative = [d for d in home.deltas if d.delta_pct > 0]
    ranked = sorted(negative, key=lambda d: d.delta_pct, reverse=True)
    cards: List[OpportunityCard] = []

    for idx, delta in enumerate(ranked[:4], start=1):
        cards.append(
            OpportunityCard(
                id=f"opp-{idx}",
                title=f"Reduce {delta.label}",
                rationale=(
                    f"{delta.label} climbed {delta.delta_pct:.0f}% in the last "
                    f"{filters.time.range_days} days."
                ),
                evidence_links=[
                    f"/api/v1/explain?metric={delta.metric}"
                    f"&scope_type={filters.scope.level}"
                    f"&scope_id={_primary_scope_id(filters)}"
                    f"&range_days={filters.time.range_days}"
                    f"&compare_days={filters.time.compare_days}"
                ],
                suggested_experiments=[
                    "Triage the top 10 longest-running work items.",
                    "Introduce a rotating on-call reviewer for stalled PRs.",
                ],
            )
        )

    if not cards:
        cards.append(
            OpportunityCard(
                id="opp-0",
                title="Maintain steady flow",
                rationale="Key metrics are stable. Focus on sustaining current practices.",
                evidence_links=[
                    f"/api/v1/home?scope_type={filters.scope.level}"
                    f"&scope_id={_primary_scope_id(filters)}"
                ],
                suggested_experiments=["Share the current playbook with new teams."],
            )
        )

    return OpportunitiesResponse(items=cards)


def _primary_scope_id(filters: MetricFilter) -> str:
    if filters.scope.ids:
        return filters.scope.ids[0]
    return ""
