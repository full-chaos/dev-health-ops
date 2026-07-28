"""Typed GraphQL surface for authorized Ask Dev scope search."""

from __future__ import annotations

from enum import Enum

import strawberry


@strawberry.enum
class DevScopeEntityKind(Enum):
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"


@strawberry.input
class DevScopeSearchInput:
    query: str
    kinds: list[DevScopeEntityKind]
    limit: int = 25


@strawberry.type
class DevScopeCandidate:
    kind: DevScopeEntityKind
    canonical_id: strawberry.ID
    label: str
    repository_id: strawberry.ID | None = None


@strawberry.type
class DevScopeSearchResult:
    candidates: list[DevScopeCandidate]
    query_version: str
    catalog_watermark: str
