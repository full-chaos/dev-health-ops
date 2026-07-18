from datetime import datetime, timezone

from dev_health_ops.models.operational import OperationalService
from dev_health_ops.providers.pagerduty.service_repository_mapping import (
    PagerDutyServiceRepositoryMappingSource,
    mappings_from_service_metadata,
)


def test_service_metadata_maps_exact_repository_urls_with_provenance() -> None:
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    service = OperationalService(
        org_id="org-a",
        provider="pagerduty",
        provider_instance_id="pd-a",
        source_entity_type="service",
        external_id="svc-1",
        source_version_at=observed_at,
        name="checkout",
    )

    mappings = mappings_from_service_metadata(
        service,
        {
            "integrations": [
                {"repository": "https://github.com/full-chaos/checkout"},
                {"repo": "full-chaos/checkout"},
            ]
        },
        observed_at,
    )

    assert len(mappings) == 1
    mapping = mappings[0]
    assert mapping.service_id == service.id
    assert mapping.repo_provider == "github"
    assert mapping.repo_full_name == "full-chaos/checkout"
    assert mapping.mapping_kind == "pagerduty_service_metadata_exact"
    assert mapping.relationship_provenance == "pagerduty_service_metadata"
    assert mapping.relationship_confidence == 0.95
    assert mapping.rule_id == PagerDutyServiceRepositoryMappingSource.METADATA.rule_id
    assert mapping.valid_from == observed_at
    assert mapping.valid_to is None
    assert mapping.is_active is True


def test_service_metadata_ignores_unlabeled_slug_heuristics() -> None:
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    service = OperationalService(
        org_id="org-a",
        provider="pagerduty",
        provider_instance_id="pd-a",
        source_entity_type="service",
        external_id="svc-1",
        source_version_at=observed_at,
        name="checkout",
    )

    mappings = mappings_from_service_metadata(
        service,
        {"description": "Coordinate with full-chaos/checkout before release."},
        observed_at,
    )

    assert mappings == ()
