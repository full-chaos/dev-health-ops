"""Typed GraphQL projections for Ask Dev evidence and data health."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

import strawberry


@strawberry.enum
class DevEvidenceScopeKind(Enum):
    ORGANIZATION = "organization"
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"


@strawberry.input
class DevEvidenceScopeRefInput:
    kind: DevEvidenceScopeKind
    value: str


@strawberry.input
class DevEvidenceScopeInput:
    direct_scope: DevEvidenceScopeKind
    refs: list[DevEvidenceScopeRefInput] = strawberry.field(default_factory=list)
    team_ids: list[str] = strawberry.field(default_factory=list)
    preset_days: int | None = 30
    start_date: date | None = None
    end_date: date | None = None
    timezone: str = "UTC"


@strawberry.input
class DevEvidenceSearchInput:
    query: str
    scope: DevEvidenceScopeInput
    limit: int = 25


@strawberry.input
class DevDataHealthInput:
    scope: DevEvidenceScopeInput
    required_sources: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class DevEvidenceLink:
    internal_path: str | None
    source_url: str | None


@strawberry.type
class DevEvidenceFlags:
    stale: bool
    unavailable: bool
    redacted: bool
    deleted: bool
    uncertain: bool
    conflicting: bool
    untrusted_content: bool


@strawberry.type
class DevEvidence:
    evidence_ref_id: strawberry.ID
    source_system: str
    source_version: str
    entity_type: str
    entity_id: strawberry.ID
    display_label: str
    link: DevEvidenceLink | None
    observed_at: datetime
    freshness: str
    provenance: str
    confidence: float
    citation_text: str | None
    repository_ids: list[strawberry.ID]
    valid_entity_ids: list[strawberry.ID]
    flags: DevEvidenceFlags


@strawberry.type
class DevEvidenceSourceState:
    source_system: str
    state: str
    watermark: str | None
    warning: str | None


@strawberry.type
class DevEvidenceSearchResult:
    evidence: list[DevEvidence]
    sources: list[DevEvidenceSourceState]
    query_version: str
    ranking_version: str


@strawberry.type
class DevDataHealthSource:
    source_system: str
    state: str
    required: bool
    last_successful_at: datetime | None
    watermark: datetime | None
    missing_repository_ids: list[strawberry.ID]
    missing_entity_ids: list[strawberry.ID]
    coverage: float
    confidence_impact: str | None
    freshness_policy_version: str | None
    warning: str | None


@strawberry.type
class DevDataHealthResult:
    sources: list[DevDataHealthSource]
    complete_eligible: bool
    query_version: str
