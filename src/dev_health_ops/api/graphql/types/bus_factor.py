"""GraphQL types for Bus Factor analytics."""

from __future__ import annotations

import strawberry


@strawberry.type
class BusFactorMaintainer:
    """A maintainer share within Bus Factor evidence."""

    author: str
    share_percent: float = strawberry.field(name="sharePercent")


@strawberry.type
class BusFactorRepoResult:
    """Repository-level Bus Factor result."""

    repo_id: str = strawberry.field(name="repoId")
    repo_name: str = strawberry.field(name="repoName")
    value: int
    top_maintainers: list[BusFactorMaintainer] = strawberry.field(name="topMaintainers")
    evidence_sample_count: int = strawberry.field(name="evidenceSampleCount")


@strawberry.type
class BusFactorResult:
    """Scope-level Bus Factor result."""

    scope_value: int = strawberry.field(name="scopeValue")
    top_maintainers: list[BusFactorMaintainer] = strawberry.field(name="topMaintainers")
    per_repo: list[BusFactorRepoResult] = strawberry.field(name="perRepo")
    evidence_sample_count: int = strawberry.field(name="evidenceSampleCount")


@strawberry.input
class BusFactorScopeInput:
    """Optional Bus Factor scope filters."""

    repo_id: str | None = strawberry.field(default=None, name="repoId")
