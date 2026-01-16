"""GraphQL request context for analytics API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid


@dataclass
class GraphQLContext:
    """
    Context passed to all GraphQL resolvers.

    Attributes:
        org_id: Required organization ID for scoping all queries.
        db_url: Database connection URL.
        request_id: Unique identifier for this request (for logging/tracing).
        persisted_query_id: Optional persisted query ID if using APQ.
        client: Optional pre-initialized DB client.
    """

    org_id: str
    db_url: str
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    persisted_query_id: str | None = None
    client: Any = None

    def __post_init__(self) -> None:
        if not self.org_id:
            from .errors import AuthorizationError

            raise AuthorizationError("org_id is required")


def build_context(
    org_id: str,
    db_url: str,
    persisted_query_id: str | None = None,
    client: Any = None,
) -> GraphQLContext:
    """
    Factory function to build a GraphQL context.

    Args:
        org_id: Required organization ID.
        db_url: Database connection URL.
        persisted_query_id: Optional persisted query ID.
        client: Optional pre-initialized DB client.

    Returns:
        GraphQLContext instance.

    Raises:
        AuthorizationError: If org_id is missing or empty.
    """
    return GraphQLContext(
        org_id=org_id,
        db_url=db_url,
        persisted_query_id=persisted_query_id,
        client=client,
    )
