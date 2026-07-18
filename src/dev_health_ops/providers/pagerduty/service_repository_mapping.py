"""Evidence-preserving PagerDuty service-to-repository mapping population."""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from uuid import UUID

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


@dataclass(frozen=True, slots=True)
class HeuristicRepositoryReference:
    """A bounded repository inference with a stable, auditable rule identifier."""

    repository: RepositoryReference
    rule_id: str


@dataclass(frozen=True, slots=True)
class PagerDutyServiceRepositoryMappingInputs:
    """Explicit mapping inputs keyed by PagerDuty service external ID."""

    admin: dict[str, tuple[RepositoryReference, ...]]
    compass: dict[str, tuple[RepositoryReference, ...]]
    heuristic: dict[str, tuple[HeuristicRepositoryReference, ...]]

    @classmethod
    def empty(cls) -> PagerDutyServiceRepositoryMappingInputs:
        """Return an input collection with no configured mappings."""
        return cls(admin={}, compass={}, heuristic={})


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


def mappings_from_service_sources(
    service: OperationalService,
    metadata: dict[str, JsonValue],
    observed_at: datetime,
    inputs: PagerDutyServiceRepositoryMappingInputs,
) -> tuple[ServiceRepositoryMapping, ...]:
    """Collect configured and metadata evidence, retaining only preferred mappings."""
    mappings = [*mappings_from_service_metadata(service, metadata, observed_at)]
    mappings.extend(
        mapping_from_repository_reference(
            service,
            reference,
            PagerDutyServiceRepositoryMappingSource.ADMIN_CONFIGURATION,
            observed_at,
        )
        for reference in inputs.admin.get(service.external_id, ())
    )
    mappings.extend(
        mapping_from_repository_reference(
            service,
            reference,
            PagerDutyServiceRepositoryMappingSource.COMPASS,
            observed_at,
        )
        for reference in inputs.compass.get(service.external_id, ())
    )
    mappings.extend(
        mapping_from_repository_reference(
            service,
            heuristic.repository,
            PagerDutyServiceRepositoryMappingSource.HEURISTIC,
            observed_at,
            rule_id=heuristic.rule_id,
        )
        for heuristic in inputs.heuristic.get(service.external_id, ())
    )
    return select_preferred_mappings(tuple(mappings))


def select_preferred_mappings(
    mappings: tuple[ServiceRepositoryMapping, ...],
) -> tuple[ServiceRepositoryMapping, ...]:
    """Keep the highest-precedence evidence for each service-to-repository pair."""
    preferred: dict[tuple[str, str | None, str | None], ServiceRepositoryMapping] = {}
    for mapping in mappings:
        key = (mapping.service_id, mapping.repo_provider, mapping.repo_full_name)
        current = preferred.get(key)
        if current is None or (mapping.relationship_confidence or 0.0) > (
            current.relationship_confidence or 0.0
        ):
            preferred[key] = mapping
    return tuple(
        preferred[key]
        for key in sorted(
            preferred, key=lambda item: (item[0], item[1] or "", item[2] or "")
        )
    )


def resolve_repository_mappings(
    mappings: tuple[ServiceRepositoryMapping, ...],
    repositories: tuple[tuple[UUID, str, str], ...],
) -> tuple[ServiceRepositoryMapping, ...]:
    """Resolve mapping evidence against repositories already owned by the organization."""
    repository_ids = {
        (provider, full_name): repository_id
        for repository_id, provider, full_name in repositories
    }
    return tuple(
        replace(
            mapping,
            repo_id=(
                repository_ids.get((mapping.repo_provider, mapping.repo_full_name))
                if mapping.repo_provider and mapping.repo_full_name
                else mapping.repo_id
            ),
        )
        for mapping in mappings
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
