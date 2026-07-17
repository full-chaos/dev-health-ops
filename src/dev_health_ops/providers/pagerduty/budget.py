"""PagerDuty route families and conservative read budget estimation."""

import hashlib
from collections.abc import Mapping

from dev_health_ops.providers.usage import OperationResolver, UsageRouteFamily
from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
)

PAGERDUTY_ROUTE_FAMILIES = tuple(
    UsageRouteFamily(
        name, BudgetDimension.REST_CORE, "rest", (name.replace("pagerduty_", ""),)
    )
    for name in (
        "pagerduty_incidents",
        "pagerduty_services",
        "pagerduty_business_services",
        "pagerduty_escalation_policies",
        "pagerduty_schedules",
        "pagerduty_oncalls",
        "pagerduty_users",
        "pagerduty_teams",
    )
)
PAGERDUTY_OPERATION_RESOLVER = OperationResolver(
    families=PAGERDUTY_ROUTE_FAMILIES,
    defaults=(("rest", "pagerduty_read", BudgetDimension.REST_CORE),),
)


class PagerDutyBudgetEstimator:
    """Estimate one paginated read budget for a PagerDuty sync dataset."""

    def estimate(self, context: object) -> tuple[BudgetEstimate, ...]:
        if getattr(context, "provider", "").lower() != "pagerduty":
            return ()
        credentials = getattr(context, "decrypted_credentials", {})
        mapping = credentials if isinstance(credentials, Mapping) else {}
        region = str(mapping.get("region", "us"))
        host = "api.eu.pagerduty.com" if region == "eu" else "api.pagerduty.com"
        fingerprint = hashlib.sha256(
            str(mapping.get("subdomain", "env")).encode()
        ).hexdigest()
        bucket = BudgetBucketKey(
            "pagerduty",
            str(getattr(context, "org_id")),
            host,
            fingerprint,
            BudgetDimension.REST_CORE,
        )
        dataset = str(getattr(context, "dataset_key", "read"))
        return (
            BudgetEstimate(
                bucket,
                2,
                "medium",
                f"pagerduty_{dataset}",
                ("PagerDuty offset pagination",),
            ),
        )
