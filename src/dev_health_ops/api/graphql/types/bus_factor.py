"""GraphQL types for Bus Factor analytics."""

from __future__ import annotations

import strawberry


@strawberry.type
class MaintainerShare:
    """A maintainer share within Bus Factor evidence."""

    author: str
    share_percent: float = strawberry.field(name="sharePercent")


@strawberry.type
class RepoBusFactor:
    """Repository-level Bus Factor result."""

    repo_id: str = strawberry.field(name="repoId")
    repo_name: str = strawberry.field(name="repoName")
    value: int
    top_maintainers: list[MaintainerShare] = strawberry.field(name="topMaintainers")
    evidence_sample_count: int = strawberry.field(name="evidenceSampleCount")


@strawberry.type
class BusFactorScope:
    """Requested Bus Factor scope."""

    repo_id: str | None = strawberry.field(default=None, name="repoId")
    team_id: str | None = strawberry.field(default=None, name="teamId")


@strawberry.type
class BusFactor:
    """Scope-level Bus Factor result."""

    org_id: str = strawberry.field(name="orgId")
    scope: BusFactorScope
    value: int
    top_maintainers: list[MaintainerShare] = strawberry.field(name="topMaintainers")
    repos: list[RepoBusFactor]
    evidence_sample_count: int = strawberry.field(name="evidenceSampleCount")


@strawberry.input
class BusFactorScopeInput:
    """Optional Bus Factor scope filters."""

    repo_id: str | None = strawberry.field(default=None, name="repoId")
    team_id: str | None = strawberry.field(default=None, name="teamId")
