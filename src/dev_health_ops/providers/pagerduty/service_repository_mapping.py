"""Evidence-preserving PagerDuty service-to-repository mapping population."""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import JsonValue

from dev_health_ops.models.operational import (
    OperationalService,
    ServiceRepositoryMapping,
)

_GITHUB_URL = re.compile(r"https?://(?:www\.)?github\.com/([^/\s]+)/([^/#?\s]+)")
_GITLAB_URL = re.compile(r"https?://(?:www\.)?gitlab\.com/([^\s]+)/?$")
_REPOSITORY_KEYS = frozenset({"repo", "repository", "repository_slug", "repo_slug"})


class PagerDutyServiceRepositoryMappingSource(str, Enum):
    """Ordered mapping sources, from explicit configuration to bounded inference."""

    ADMIN_CONFIGURATION = "admin_configuration"
    METADATA = "pagerduty_service_metadata"
    COMPASS = "compass_service_catalog"
    HEURISTIC = "bounded_service_repository_heuristic"

    @property
    def confidence(self) -> float:
        match self:
            case PagerDutyServiceRepositoryMappingSource.ADMIN_CONFIGURATION:
                return 1.0
            case PagerDutyServiceRepositoryMappingSource.METADATA:
                return 0.95
            case PagerDutyServiceRepositoryMappingSource.COMPASS:
                return 0.9
            case PagerDutyServiceRepositoryMappingSource.HEURISTIC:
                return 0.4

    @property
    def mapping_kind(self) -> str:
        return f"{self.value}_exact" if self is not self.HEURISTIC else self.value

    @property
    def rule_id(self) -> str:
        match self:
            case PagerDutyServiceRepositoryMappingSource.ADMIN_CONFIGURATION:
                return "service_repository_mapping.admin.v1"
            case PagerDutyServiceRepositoryMappingSource.METADATA:
                return "pagerduty.service_metadata.repository_url_or_key.v1"
            case PagerDutyServiceRepositoryMappingSource.COMPASS:
                return "compass.service_repository_relationship.v1"
            case PagerDutyServiceRepositoryMappingSource.HEURISTIC:
                return "pagerduty.service_repository.bounded_name_match.v1"


@dataclass(frozen=True, slots=True)
class RepositoryReference:
    """A repository reference supplied by a mapping source."""

    provider: str
    full_name: str


def mapping_from_repository_reference(
    service: OperationalService,
    reference: RepositoryReference,
    source: PagerDutyServiceRepositoryMappingSource,
    observed_at: datetime,
    *,
    rule_id: str | None = None,
) -> ServiceRepositoryMapping:
    """Create one auditable mapping without inventing a repository identity."""
    resolved_rule_id = rule_id or source.rule_id
    if source is PagerDutyServiceRepositoryMappingSource.HEURISTIC and not rule_id:
        raise ValueError("bounded heuristic mappings require a stable rule_id")
    return ServiceRepositoryMapping(
        org_id=service.org_id,
        provider=service.provider,
        provider_instance_id=service.provider_instance_id,
        source_entity_type=source.value,
        external_id=f"{service.external_id}:{reference.provider}:{reference.full_name}:{resolved_rule_id}",
        source_version_at=observed_at,
        source_url=service.source_url,
        source_event_id="pagerduty_sync",
        service_id=service.id,
        repo_full_name=reference.full_name,
        repo_provider=reference.provider,
        mapping_kind=source.mapping_kind,
        rule_id=resolved_rule_id,
        valid_from=observed_at,
        is_active=True,
        relationship_provenance=source.value,
        relationship_confidence=source.confidence,
    )


def mappings_from_service_metadata(
    service: OperationalService,
    metadata: dict[str, JsonValue],
    observed_at: datetime,
) -> tuple[ServiceRepositoryMapping, ...]:
    """Extract only exact repository URLs or explicitly labeled repository slugs."""
    references = {
        reference
        for key, value in _walk_metadata(metadata)
        for reference in _repository_references(key, value)
    }
    return tuple(
        mapping_from_repository_reference(
            service,
            reference,
            PagerDutyServiceRepositoryMappingSource.METADATA,
            observed_at,
        )
        for reference in sorted(
            references, key=lambda item: (item.provider, item.full_name)
        )
    )


def _walk_metadata(
    value: JsonValue, key: str | None = None
) -> Iterator[tuple[str | None, str]]:
    match value:
        case str() as text:
            yield key, text
        case dict() as values:
            for nested_key, nested_value in values.items():
                yield from _walk_metadata(nested_value, nested_key.casefold())
        case list() as values:
            for nested_value in values:
                yield from _walk_metadata(nested_value, key)
        case _:
            return


def _repository_references(
    key: str | None, value: str
) -> tuple[RepositoryReference, ...]:
    references: list[RepositoryReference] = []
    for pattern, provider in ((_GITHUB_URL, "github"), (_GITLAB_URL, "gitlab")):
        match = pattern.search(value)
        if match is not None:
            full_name = "/".join(
                segment.removesuffix(".git") for segment in match.groups()
            )
            references.append(RepositoryReference(provider, full_name))
    if key in _REPOSITORY_KEYS and value.count("/") == 1:
        references.append(RepositoryReference("github", value.removesuffix(".git")))
    return tuple(references)
