"""PagerDuty route families and conservative read budget estimation."""

import hashlib
from collections.abc import Mapping

from dev_health_ops.providers.usage import OperationResolver, UsageRouteFamily
from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
)

_DEFAULT_ENRICHMENT_CAP = 100
_ESTIMATED_INCIDENT_PAGES = 2
_PAGERDUTY_PAGE_SIZE = 100
_DATASET_ROUTE_FAMILIES = {
    "incidents": "pagerduty_incidents",
    "services": "pagerduty_services",
    "business-services": "pagerduty_business_services",
    "escalation-policies": "pagerduty_escalation_policies",
    "schedules": "pagerduty_schedules",
    "on-calls": "pagerduty_oncalls",
    "users": "pagerduty_users",
    "teams": "pagerduty_teams",
}
_ENRICHMENT_ROUTE_FAMILIES = {
    "incident-alerts": "pagerduty_alerts",
    "incident-log-entries": "pagerduty_log_entries",
    "incident-notes": "pagerduty_notes",
}

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
        "pagerduty_alerts",
        "pagerduty_log_entries",
        "pagerduty_notes",
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
        enrichment_route_family = _ENRICHMENT_ROUTE_FAMILIES.get(dataset)
        if enrichment_route_family is not None:
            options = getattr(context, "dataset_options", {})
            enrichment_cap = (
                options.get("enrichment_cap", _DEFAULT_ENRICHMENT_CAP)
                if isinstance(options, Mapping)
                else _DEFAULT_ENRICHMENT_CAP
            )
            if not isinstance(enrichment_cap, int) or isinstance(enrichment_cap, bool):
                enrichment_cap = _DEFAULT_ENRICHMENT_CAP
            if enrichment_cap < 0:
                enrichment_cap = _DEFAULT_ENRICHMENT_CAP
            estimates = [
                BudgetEstimate(
                    bucket,
                    _ESTIMATED_INCIDENT_PAGES,
                    "medium",
                    _DATASET_ROUTE_FAMILIES["incidents"],
                    ("PagerDuty incident pagination",),
                )
            ]
            if enrichment_cap > 0:
                child_pages_per_incident = -(-enrichment_cap // _PAGERDUTY_PAGE_SIZE)
                estimates.append(
                    BudgetEstimate(
                        bucket,
                        (
                            _ESTIMATED_INCIDENT_PAGES
                            * _PAGERDUTY_PAGE_SIZE
                            * child_pages_per_incident
                        ),
                        "low",
                        enrichment_route_family,
                        ("PagerDuty enrichment fan-out capped per sync unit",),
                    )
                )
            return tuple(estimates)

        return (
            BudgetEstimate(
                bucket,
                _ESTIMATED_INCIDENT_PAGES,
                "medium",
                _DATASET_ROUTE_FAMILIES.get(dataset, f"pagerduty_{dataset}"),
                ("PagerDuty offset pagination",),
            ),
        )
