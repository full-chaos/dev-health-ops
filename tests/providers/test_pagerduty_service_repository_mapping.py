from datetime import datetime, timezone
from uuid import uuid4

from dev_health_ops.models.operational import OperationalService
from dev_health_ops.providers.pagerduty.service_repository_mapping import (
    PagerDutyServiceRepositoryMappingInputs,
    PagerDutyServiceRepositoryMappingSource,
    RepositoryReference,
    mapping_from_repository_reference,
    mappings_from_service_metadata,
    resolve_repository_mappings,
    select_preferred_mappings,
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


def test_repository_catalog_resolution_keeps_unmatched_metadata_as_evidence() -> None:
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
    matched_repo_id = uuid4()
    mappings = (
        mapping_from_repository_reference(
            service,
            RepositoryReference("github", "full-chaos/checkout"),
            PagerDutyServiceRepositoryMappingSource.METADATA,
            observed_at,
        ),
        mapping_from_repository_reference(
            service,
            RepositoryReference("github", "full-chaos/missing"),
            PagerDutyServiceRepositoryMappingSource.METADATA,
            observed_at,
        ),
    )

    resolved = resolve_repository_mappings(
        mappings,
        ((matched_repo_id, "github", "full-chaos/checkout"),),
    )

    assert resolved[0].repo_id == matched_repo_id
    assert resolved[1].repo_id is None
    assert resolved[1].repo_full_name == "full-chaos/missing"


def test_mapping_precedence_selects_admin_over_exact_metadata() -> None:
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
    reference = RepositoryReference("github", "full-chaos/checkout")
    metadata = mapping_from_repository_reference(
        service,
        reference,
        PagerDutyServiceRepositoryMappingSource.METADATA,
        observed_at,
    )
    admin = mapping_from_repository_reference(
        service,
        reference,
        PagerDutyServiceRepositoryMappingSource.ADMIN_CONFIGURATION,
        observed_at,
    )

    preferred = select_preferred_mappings((metadata, admin))

    assert preferred == (admin,)


def test_mapping_inputs_from_dataset_options_parses_admin_and_compass() -> None:
    inputs = PagerDutyServiceRepositoryMappingInputs.from_dataset_options(
        {
            "service_repository_mappings": {
                "admin": {
                    "svc-1": [{"provider": "github", "full_name": "full-chaos/api"}],
                },
                "compass": {
                    "svc-2": [
                        {"provider": "gitlab", "full_name": "full-chaos/worker"},
                    ],
                },
            }
        }
    )

    assert inputs.admin == {"svc-1": (RepositoryReference("github", "full-chaos/api"),)}
    assert inputs.compass == {
        "svc-2": (RepositoryReference("gitlab", "full-chaos/worker"),)
    }
    assert inputs.heuristic == {}


def test_mapping_inputs_from_dataset_options_defaults_to_empty() -> None:
    empty = PagerDutyServiceRepositoryMappingInputs.empty()
    assert PagerDutyServiceRepositoryMappingInputs.from_dataset_options({}) == empty
    assert (
        PagerDutyServiceRepositoryMappingInputs.from_dataset_options(
            {"service_repository_mappings": "not-a-mapping"}
        )
        == empty
    )


def test_mapping_inputs_from_dataset_options_skips_malformed_entries() -> None:
    inputs = PagerDutyServiceRepositoryMappingInputs.from_dataset_options(
        {
            "service_repository_mappings": {
                "admin": {
                    "svc-1": [
                        {"provider": "github", "full_name": "full-chaos/api"},
                        {"provider": "github"},
                        {"full_name": "full-chaos/no-provider"},
                        {"provider": "", "full_name": "full-chaos/blank"},
                        "not-a-dict",
                    ],
                    "svc-empty": [{"provider": 1, "full_name": 2}],
                },
            }
        }
    )

    assert inputs.admin == {"svc-1": (RepositoryReference("github", "full-chaos/api"),)}
    assert inputs.compass == {}
