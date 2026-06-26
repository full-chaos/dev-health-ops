from __future__ import annotations

from typing import TYPE_CHECKING

from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
    BudgetEstimator,
)

if TYPE_CHECKING:
    from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

__all__ = [
    "BudgetBucketKey",
    "BudgetDimension",
    "BudgetEstimate",
    "BudgetEstimator",
    "estimate_provider_budget",
]


def estimate_provider_budget(context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
    if context.provider.lower() == "github":
        from dev_health_ops.providers.github.budget import GitHubBudgetEstimator

        return GitHubBudgetEstimator().estimate(context)
    if context.provider.lower() == "launchdarkly":
        from dev_health_ops.providers.launchdarkly.budget import (
            LaunchDarklyBudgetEstimator,
        )

        return LaunchDarklyBudgetEstimator().estimate(context)
    return ()
