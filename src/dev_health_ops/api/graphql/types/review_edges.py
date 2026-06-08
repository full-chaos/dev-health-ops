"""GraphQL types for the Review Edges surface (CHAOS-2077).

Exposes reviewer-to-author collaboration data from ``review_edges_daily``.
No new tables or ETL are introduced; this is a pure schema addition.
"""

from __future__ import annotations

from datetime import date

import strawberry


@strawberry.input
class ReviewEdgesInput:
    """Input for the ``reviewEdges`` query."""

    org_id: str = strawberry.field(name="orgId")
    since_date: date = strawberry.field(name="sinceDate")
    until_date: date = strawberry.field(name="untilDate")
    #: Optional filter to specific repo UUIDs.  When absent, all repos are
    #: included.
    repo_ids: list[str] | None = strawberry.field(default=None, name="repoIds")
    #: Row cap; default 500, hard max 2000.
    limit: int = 500


@strawberry.type
class ReviewEdgeRow:
    """One reviewer-to-author edge for a given day and repo.

    ``repoId`` is the UUID string from ``review_edges_daily.repo_id``.
    """

    reviewer: str
    author: str
    reviews_count: int = strawberry.field(name="reviewsCount")
    day: date
    repo_id: str | None = strawberry.field(default=None, name="repoId")


@strawberry.type
class ReviewEdgesResult:
    """Response for ``reviewEdges``."""

    edges: list[ReviewEdgeRow]
    #: Total number of rows returned (equals ``len(edges)``).
    total_count: int = strawberry.field(name="totalCount")
