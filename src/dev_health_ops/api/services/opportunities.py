from __future__ import annotations

from ..models.filters import MetricFilter
from ..models.schemas import OpportunitiesResponse, OpportunityCard
from .cache import TTLCache
from .home import build_home_response

_DEFAULT_SUGGESTED_EXPERIMENTS = [
    "Triage the top 10 longest-running work items.",
    "Introduce a rotating on-call reviewer for stalled PRs.",
]

_METRIC_SUGGESTED_EXPERIMENTS = {
    "cycle_time": [
        "Trace the oldest active items to their current waiting state.",
        "Split one long-running item into the next reviewable slice.",
    ],
    "review_latency": [
        "Reserve a daily review block for PRs waiting longest for first response.",
        "Pair authors with likely reviewers before opening complex PRs.",
    ],
    "throughput": [
        "Audit recently completed items for the smallest repeatable delivery pattern.",
        "Pause new starts until the team finishes the highest-value active work.",
    ],
    "deploy_freq": [
        "Identify the smallest safe release candidate and ship it behind existing controls.",
        "Review deploy blockers from the last cycle and remove one manual handoff.",
    ],
    "churn": [
        "Review the files with the largest churn increase for unclear ownership or scope.",
        "Timebox a design checkpoint before the next high-churn change expands.",
    ],
    "wip_saturation": [
        "Set a short-term WIP limit and finish active items before starting more.",
        "Reassign one blocked item owner to unblock or explicitly park it today.",
    ],
    "blocked_work": [
        "Escalate the top blocked item with the dependency owner and a target unblock date.",
        "Convert recurring blocked states into explicit dependency tickets.",
    ],
    "change_failure_rate": [
        "Review recent failed changes for the earliest detectable signal before release.",
        "Add one pre-release check to the riskiest deployment path.",
    ],
    "rework_ratio": [
        "Compare reopened or rewritten work against its original acceptance criteria.",
        "Add a short pre-implementation alignment review for similar upcoming work.",
    ],
    "ci_success": [
        "Classify the latest CI failures by flaky test, environment, or product defect.",
        "Fix or quarantine the highest-frequency flaky check before adding new coverage.",
    ],
}


async def build_opportunities_response(
    *,
    db_url: str,
    filters: MetricFilter,
    cache: TTLCache,
    org_id: str = "",
) -> OpportunitiesResponse:
    home = await build_home_response(
        db_url=db_url,
        filters=filters,
        cache=cache,
        org_id=org_id,
    )

    negative = [d for d in home.deltas if d.delta_pct > 0]
    ranked = sorted(negative, key=lambda d: d.delta_pct, reverse=True)
    cards: list[OpportunityCard] = []

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
                suggested_experiments=_suggested_experiments_for(delta.metric),
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


def _suggested_experiments_for(metric: str) -> list[str]:
    return _METRIC_SUGGESTED_EXPERIMENTS.get(metric, _DEFAULT_SUGGESTED_EXPERIMENTS)
