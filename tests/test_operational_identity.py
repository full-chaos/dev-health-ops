from __future__ import annotations

from dataclasses import fields
from datetime import datetime, timezone
from uuid import UUID

import pytest

from dev_health_ops.models.operational import (
    OperationalBatch,
    OperationalIncident,
    OperationalService,
    ServiceRepositoryMapping,
    canonical_operational_id,
)
from tests.fixtures.operational_entities import all_operational_entities

_SOURCE_VERSION = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _service(
    *,
    provider: str = "pagerduty",
    source_entity_type: str = "native.service",
    source_id: UUID | None = None,
) -> OperationalService:
    return OperationalService(
        org_id="org-example",
        provider=provider,
        provider_instance_id="shared-instance-name",
        source_entity_type=source_entity_type,
        external_id="payments-api",
        source_version_at=_SOURCE_VERSION,
        source_id=source_id,
        name="Payments API",
    )


def test_identity_includes_provider_and_uses_the_table_derived_family() -> None:
    # Given: two systems sharing an integration instance and two ingest paths for one service.
    pagerduty = _service(provider="pagerduty")
    atlassian = _service(provider="atlassian_jsm")
    pushed = _service(
        source_entity_type="external_push.service",
        source_id=UUID("00000000-0000-0000-0000-000000000001"),
    )

    # When: their internal ids are derived.
    ids = {pagerduty.id, atlassian.id, pushed.id}

    # Then: systems do not collide while source descriptors never fork one canonical row.
    assert pagerduty.id != atlassian.id
    assert pushed.id == pagerduty.id
    assert len(ids) == 2


@pytest.mark.parametrize(
    ("org_id", "provider", "instance", "family", "external_id"),
    [
        ("", "pagerduty", "instance", "operational_service", "service"),
        ("org", "", "instance", "operational_service", "service"),
        ("org", "pagerduty", "", "operational_service", "service"),
        ("org", "pagerduty", "instance", "", "service"),
        ("org", "pagerduty", "instance", "operational_service", ""),
    ],
)
def test_identity_rejects_empty_seed_components(
    org_id: str,
    provider: str,
    instance: str,
    family: str,
    external_id: str,
) -> None:
    # Given: one missing immutable identity component.

    # When: a caller requests the deterministic id.
    with pytest.raises(ValueError):
        canonical_operational_id(org_id, provider, instance, family, external_id)

    # Then: no ambiguous id is emitted.


def test_mapping_and_pagerduty_relationships_are_typed_and_present() -> None:
    # Given: canonical service, incident, and repository mapping records.
    service = _service()
    incident = OperationalIncident(
        org_id=service.org_id,
        provider=service.provider,
        provider_instance_id=service.provider_instance_id,
        source_entity_type="incident",
        external_id="incident-1",
        source_version_at=_SOURCE_VERSION,
        service_id=service.id,
        escalation_policy_id="policy-1",
        started_at=_SOURCE_VERSION,
    )
    mapping = ServiceRepositoryMapping(
        org_id=service.org_id,
        provider=service.provider,
        provider_instance_id=service.provider_instance_id,
        source_entity_type="service_repository_mapping",
        external_id="payments-api:repo",
        source_version_at=_SOURCE_VERSION,
        service_id=service.id,
        repo_id=UUID("00000000-0000-0000-0000-000000000002"),
        rule_id="service-catalog-repository-link",
    )

    # When: their fields are inspected.
    service_fields = {item.name for item in fields(service)}
    incident_fields = {item.name for item in fields(incident)}

    # Then: PagerDuty and UUID repository relationships are expressible.
    assert (
        service.escalation_policy_id == "policy-1"
        or "escalation_policy_id" in service_fields
    )
    assert incident.escalation_policy_id == "policy-1"
    assert mapping.repo_id == UUID("00000000-0000-0000-0000-000000000002")
    assert mapping.rule_id == "service-catalog-repository-link"
    assert "source_version_at" in incident_fields
    assert "source_event_id" in incident_fields


def test_batch_rejects_rows_from_another_source_context() -> None:
    # Given: a batch context and a row from a different provider instance.
    service = _service()
    foreign = OperationalService(
        org_id=service.org_id,
        provider=service.provider,
        provider_instance_id="other-instance",
        source_entity_type="service",
        external_id="other-service",
        source_version_at=_SOURCE_VERSION,
        name="Other Service",
    )

    # When: the envelope is constructed.
    with pytest.raises(ValueError):
        OperationalBatch(
            org_id=service.org_id,
            provider=service.provider,
            provider_instance_id=service.provider_instance_id,
            services=(service, foreign),
        )

    # Then: mismatched source context cannot cross the ingestion boundary.


def test_fixture_covers_every_canonical_entity_and_external_push_parity() -> None:
    # Given: the sanitized all-entity operational fixture.
    entities = all_operational_entities()

    # When: canonical families and native/external-push services are inspected.
    families = {entity.entity_family for entity in entities}
    services = [entity for entity in entities if isinstance(entity, OperationalService)]

    # Then: all twelve families exist and the two ingestion paths share an id.
    assert len(families) == 12
    assert len(services) == 2
    assert services[0].id == services[1].id
