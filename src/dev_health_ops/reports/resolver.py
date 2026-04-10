"""Metric and entity resolution for report planning."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from dev_health_ops.reports.metric_registry import (
    METRIC_REGISTRY,
    MetricDefinition,
    resolve_metric_alias,
)
from dev_health_ops.reports.parser import ParsedPrompt


def _normalize(text: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in text).split())


@dataclass(frozen=True)
class EntityDefinition:
    entity_id: str
    name: str
    kind: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class EntityCatalog:
    teams: tuple[EntityDefinition, ...] = ()
    repos: tuple[EntityDefinition, ...] = ()
    services: tuple[EntityDefinition, ...] = ()


@dataclass(frozen=True)
class MetricResolution:
    canonical_metrics: list[str]
    resolved_definitions: list[MetricDefinition]
    unresolved_terms: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EntityResolution:
    team_ids: list[str] = field(default_factory=list)
    repo_ids: list[str] = field(default_factory=list)
    service_ids: list[str] = field(default_factory=list)
    unresolved_terms: list[str] = field(default_factory=list)


def resolve_metrics(metric_terms: list[str]) -> MetricResolution:
    canonical_metrics: list[str] = []
    unresolved_terms: list[str] = []
    for term in metric_terms:
        canonical_name = resolve_metric_alias(term)
        if canonical_name is None:
            unresolved_terms.append(term)
            continue
        if canonical_name not in canonical_metrics:
            canonical_metrics.append(canonical_name)
    return MetricResolution(
        canonical_metrics=canonical_metrics,
        resolved_definitions=[METRIC_REGISTRY[name] for name in canonical_metrics],
        unresolved_terms=unresolved_terms,
    )


def _matches_entity(prompt: str, entity: EntityDefinition) -> bool:
    candidates = (entity.name, *entity.aliases)
    lowered = prompt.lower()
    for candidate in candidates:
        if not candidate:
            continue
        pattern = rf"(?<!\w){re.escape(candidate.lower())}(?!\w)"
        if re.search(pattern, lowered):
            return True
    return False


def _resolve_entities(prompt: str, entities: tuple[EntityDefinition, ...]) -> list[str]:
    resolved: list[str] = []
    for entity in entities:
        if _matches_entity(prompt, entity):
            resolved.append(entity.entity_id)
    return resolved


def resolve_entities(
    parsed_prompt: ParsedPrompt, catalog: EntityCatalog
) -> EntityResolution:
    prompt = parsed_prompt.raw_prompt
    team_ids = _resolve_entities(prompt, catalog.teams)
    repo_ids = _resolve_entities(prompt, catalog.repos)
    service_ids = _resolve_entities(prompt, catalog.services)

    unresolved_terms: list[str] = []
    explicit_terms = (
        (parsed_prompt.scope.teams, team_ids),
        (parsed_prompt.scope.repos, repo_ids),
        (parsed_prompt.scope.services, service_ids),
    )
    for terms, resolved in explicit_terms:
        if terms and not resolved:
            unresolved_terms.extend(terms)

    return EntityResolution(
        team_ids=team_ids,
        repo_ids=repo_ids,
        service_ids=service_ids,
        unresolved_terms=unresolved_terms,
    )
