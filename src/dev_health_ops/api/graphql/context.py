"""GraphQL request context for analytics API."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from strawberry.fastapi import BaseContext

if TYPE_CHECKING:
    from dev_health_ops.api.services.auth import AuthenticatedUser

    from .loaders import (
        DataLoaders,
        RepoByNameLoader,
        RepoLoader,
        TeamByNameLoader,
        TeamLoader,
    )


@dataclass
class GraphQLContext(BaseContext):
    """
    Context passed to all GraphQL resolvers.

    Attributes:
        org_id: Required organization ID for scoping all queries.
        db_url: Database connection URL.
        request_id: Unique identifier for this request (for logging/tracing).
        persisted_query_id: Optional persisted query ID if using APQ.
        client: Optional pre-initialized DB client.
        loaders: Analytics DataLoaders for batched queries.
        team_loader: DataLoader for team entities.
        team_by_name_loader: DataLoader for teams by name.
        repo_loader: DataLoader for repository entities.
        repo_by_name_loader: DataLoader for repos by name.
        cache: Optional cache backend for cross-request caching.
        user: Authenticated user from JWT (None if unauthenticated).
    """

    org_id: str
    db_url: str
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    persisted_query_id: str | None = None
    client: Any = None
    loaders: DataLoaders | None = None
    team_loader: TeamLoader | None = None
    team_by_name_loader: TeamByNameLoader | None = None
    repo_loader: RepoLoader | None = None
    repo_by_name_loader: RepoByNameLoader | None = None
    cache: Any = None
    user: AuthenticatedUser | None = None

    def __post_init__(self) -> None:
        # Platform/super admins can reach cross-org admin queries without
        # belonging to a tenant (e.g., the /superadmin/product-telemetry
        # overview). Per-org resolvers still call ``require_org_id`` so an
        # org-less context can't be used to query tenant-scoped data.
        if not self.org_id and not getattr(self.user, "is_superuser", False):
            from .errors import AuthorizationError

            raise AuthorizationError("org_id is required")

    def rebind_org_id(self, org_id: str) -> None:
        """Rebuild tenant-bound loaders for an authorized operation scope."""
        self.org_id = org_id
        if self.client is None:
            return
        from .loaders import (
            RepoByNameLoader,
            RepoLoader,
            TeamByNameLoader,
            TeamLoader,
        )

        self.team_loader = TeamLoader(self.client, org_id, cache=self.cache)
        self.team_by_name_loader = TeamByNameLoader(
            self.client, org_id, cache=self.cache
        )
        self.repo_loader = RepoLoader(self.client, org_id, cache=self.cache)
        self.repo_by_name_loader = RepoByNameLoader(
            self.client, org_id, cache=self.cache
        )


def build_context(
    org_id: str,
    db_url: str,
    persisted_query_id: str | None = None,
    client: Any = None,
    cache: Any = None,
    user: AuthenticatedUser | None = None,
) -> GraphQLContext:
    """
    Factory function to build a GraphQL context with DataLoaders.

    Args:
        org_id: Required organization ID.
        db_url: Database connection URL.
        persisted_query_id: Optional persisted query ID.
        client: Optional pre-initialized DB client.
        cache: Optional cache backend for cross-request caching.
        user: Optional authenticated user from JWT.

    Returns:
        GraphQLContext instance with initialized DataLoaders.

    Raises:
        AuthorizationError: If org_id is missing or empty.
    """
    context = GraphQLContext(
        org_id=org_id,
        db_url=db_url,
        persisted_query_id=persisted_query_id,
        client=client,
        cache=cache,
        user=user,
    )

    # Initialize DataLoaders if client is available
    if client is not None:
        from .loaders import (
            DataLoaders,
            RepoByNameLoader,
            RepoLoader,
            TeamByNameLoader,
            TeamLoader,
        )

        context.loaders = DataLoaders.create(client)
        context.team_loader = TeamLoader(client, org_id, cache=cache)
        context.team_by_name_loader = TeamByNameLoader(client, org_id, cache=cache)
        context.repo_loader = RepoLoader(client, org_id, cache=cache)
        context.repo_by_name_loader = RepoByNameLoader(client, org_id, cache=cache)

    return context
