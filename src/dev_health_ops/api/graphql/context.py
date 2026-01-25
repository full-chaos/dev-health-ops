"""GraphQL request context for analytics API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING
import uuid


from strawberry.fastapi import BaseContext

if TYPE_CHECKING:
    from .loaders import (
        DataLoaders,
        TeamLoader,
        TeamByNameLoader,
        RepoLoader,
        RepoByNameLoader,
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
    """

    org_id: str
    db_url: str
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    persisted_query_id: str | None = None
    client: Any = None
    # DataLoaders - initialized per request for proper batching
    loaders: Optional["DataLoaders"] = None
    team_loader: Optional["TeamLoader"] = None
    team_by_name_loader: Optional["TeamByNameLoader"] = None
    repo_loader: Optional["RepoLoader"] = None
    repo_by_name_loader: Optional["RepoByNameLoader"] = None
    # Optional cache backend for cross-request caching
    cache: Any = None

    def __post_init__(self) -> None:
        if not self.org_id:
            from .errors import AuthorizationError

            raise AuthorizationError("org_id is required")


def build_context(
    org_id: str,
    db_url: str,
    persisted_query_id: str | None = None,
    client: Any = None,
    cache: Any = None,
) -> GraphQLContext:
    """
    Factory function to build a GraphQL context with DataLoaders.

    Args:
        org_id: Required organization ID.
        db_url: Database connection URL.
        persisted_query_id: Optional persisted query ID.
        client: Optional pre-initialized DB client.
        cache: Optional cache backend for cross-request caching.

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
    )

    # Initialize DataLoaders if client is available
    if client is not None:
        from .loaders import (
            DataLoaders,
            TeamLoader,
            TeamByNameLoader,
            RepoLoader,
            RepoByNameLoader,
        )

        context.loaders = DataLoaders.create(client)
        context.team_loader = TeamLoader(client, org_id, cache=cache)
        context.team_by_name_loader = TeamByNameLoader(client, org_id, cache=cache)
        context.repo_loader = RepoLoader(client, org_id, cache=cache)
        context.repo_by_name_loader = RepoByNameLoader(client, org_id, cache=cache)

    return context
