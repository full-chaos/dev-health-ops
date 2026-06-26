from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.sync.budget_types import BudgetDimension


@dataclass(frozen=True)
class LaunchDarklyBudgetRouteFamily:
    route_family: str
    dimension: BudgetDimension
    endpoint_patterns: tuple[str, ...]
    cost_drivers: tuple[str, ...]
    confidence: str


LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES: tuple[LaunchDarklyBudgetRouteFamily, ...] = (
    LaunchDarklyBudgetRouteFamily(
        route_family="projects",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=(
            "GET /api/v2/projects",
            "GET /api/v2/projects/{projectKey}/environments",
            "GET /api/v2/projects/{projectKey}/environments/{environmentKey}",
        ),
        cost_drivers=(
            "project count",
            "environments per project",
            "expand=environments nested pagination",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="flags",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/flags/{projectKey}",),
        cost_drivers=(
            "project count",
            "flag count",
            "environment-scoped filtering",
            "summary and expand options",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="segments",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/segments/{projectKey}/{environmentKey}",),
        cost_drivers=(
            "project count",
            "environment count",
            "segment count",
            "big and synced segment expansion",
        ),
        confidence="low",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="audit_log",
        dimension=BudgetDimension.SECONDARY_ABUSE_RISK,
        endpoint_patterns=("GET /api/v2/auditlog", "POST /api/v2/auditlog"),
        cost_drivers=(
            "incremental window size",
            "backfill span",
            "resource-scoped searches",
            "20-entry page cap",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="members",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/members",),
        cost_drivers=(
            "member count",
            "custom role expansion",
            "role attribute expansion",
        ),
        confidence="low",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="code_refs",
        dimension=BudgetDimension.SECONDARY_ABUSE_RISK,
        endpoint_patterns=("GET /api/v2/code-refs/repositories",),
        cost_drivers=(
            "repository count",
            "branch count",
            "references per flag",
            "default branch reference expansion",
        ),
        confidence="low",
    ),
)


LAUNCHDARKLY_BUDGET_ROUTE_FAMILY_KEYS = frozenset(
    family.route_family for family in LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES
)
