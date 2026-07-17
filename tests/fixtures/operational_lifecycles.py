from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from dev_health_ops.models.operational import (
    OperationalAlert,
    OperationalIncident,
    OperationalService,
)

_OBSERVED_AT = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class OperationalLifecycleFixture:
    provider: str
    service: OperationalService
    incident: OperationalIncident
    alert: OperationalAlert


def equivalent_operational_lifecycles() -> tuple[OperationalLifecycleFixture, ...]:
    """Return sanitized equivalent operational lifecycles from four source shapes."""
    lifecycles: list[OperationalLifecycleFixture] = []
    for provider, instance in (
        ("atlassian_jsm", "jsm-cloud-example"),
        ("github", "github-org-example"),
        ("gitlab", "gitlab-group-example"),
        ("pagerduty", "pagerduty-account-example"),
    ):
        service = OperationalService(
            org_id="org-example",
            provider=provider,
            provider_instance_id=instance,
            source_entity_type="service",
            external_id="payments-api",
            source_url="https://example.invalid/services/payments-api",
            source_event_at=_OBSERVED_AT,
            observed_at=_OBSERVED_AT,
            last_synced=_OBSERVED_AT,
            raw_status="active",
            raw_severity="critical",
            raw_priority="P1",
            normalized_status="active",
            normalized_severity="critical",
            normalized_priority="high",
            name="Payments API",
        )
        incident = OperationalIncident(
            org_id="org-example",
            provider=provider,
            provider_instance_id=instance,
            source_entity_type="incident",
            external_id="inc-2026-07-17-001",
            source_url="https://example.invalid/incidents/001",
            source_event_at=_OBSERVED_AT,
            observed_at=_OBSERVED_AT,
            last_synced=_OBSERVED_AT,
            raw_status="resolved",
            raw_severity="SEV-1",
            raw_priority="P1",
            normalized_status="resolved",
            normalized_severity="critical",
            normalized_priority="high",
            service_id=service.id,
            title="Payments API availability incident",
            started_at=_OBSERVED_AT,
            resolved_at=_OBSERVED_AT,
        )
        alert = OperationalAlert(
            org_id="org-example",
            provider=provider,
            provider_instance_id=instance,
            source_entity_type="alert",
            external_id="alert-001",
            source_url="https://example.invalid/alerts/001",
            source_event_at=_OBSERVED_AT,
            observed_at=_OBSERVED_AT,
            last_synced=_OBSERVED_AT,
            raw_status="triggered",
            raw_severity="critical",
            raw_priority="P1",
            normalized_status="open",
            normalized_severity="critical",
            normalized_priority="high",
            service_id=service.id,
            incident_id=incident.id,
            title="Payments API error rate alert",
            triggered_at=_OBSERVED_AT,
        )
        lifecycles.append(
            OperationalLifecycleFixture(
                provider=provider,
                service=service,
                incident=incident,
                alert=alert,
            )
        )
    return tuple(lifecycles)
