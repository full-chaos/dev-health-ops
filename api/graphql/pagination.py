"""Relay-style cursor pagination utilities for GraphQL."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Generic, List, Optional, TypeVar

import strawberry


T = TypeVar("T")


def encode_cursor(data: dict) -> str:
    """
    Encode a cursor dictionary to a base64 string.

    Args:
        data: Dictionary with cursor data (e.g., {"offset": 10} or {"id": "abc"}).

    Returns:
        Base64-encoded cursor string.
    """
    json_str = json.dumps(data, sort_keys=True, default=str)
    return base64.urlsafe_b64encode(json_str.encode()).decode()


def decode_cursor(cursor: str) -> dict:
    """
    Decode a base64 cursor string to a dictionary.

    Args:
        cursor: Base64-encoded cursor string.

    Returns:
        Dictionary with cursor data.

    Raises:
        ValueError: If cursor is invalid.
    """
    try:
        json_str = base64.urlsafe_b64decode(cursor.encode()).decode()
        return json.loads(json_str)
    except Exception as e:
        raise ValueError(f"Invalid cursor: {cursor}") from e


def offset_cursor(offset: int) -> str:
    """Create an offset-based cursor."""
    return encode_cursor({"offset": offset})


def get_offset_from_cursor(cursor: Optional[str]) -> int:
    """Extract offset from cursor, defaulting to 0."""
    if cursor is None:
        return 0
    data = decode_cursor(cursor)
    return int(data.get("offset", 0))


@strawberry.type
class PageInfo:
    """
    Relay-style pagination info.

    Provides information about the current page and whether more data exists.
    """

    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str] = None
    end_cursor: Optional[str] = None


@strawberry.input
class PaginationInput:
    """
    Input for cursor-based pagination.

    Supports forward pagination (first/after) and backward pagination (last/before).
    Only one direction should be used at a time.
    """

    first: Optional[int] = None
    after: Optional[str] = None
    last: Optional[int] = None
    before: Optional[str] = None


@dataclass
class PaginationParams:
    """Parsed pagination parameters for query execution."""

    limit: int
    offset: int
    is_forward: bool

    @classmethod
    def from_input(
        cls, pagination: Optional[PaginationInput], default_limit: int = 20
    ) -> "PaginationParams":
        """
        Parse pagination input into query parameters.

        Args:
            pagination: Optional pagination input from GraphQL.
            default_limit: Default page size if not specified.

        Returns:
            PaginationParams with limit and offset for the query.
        """
        if pagination is None:
            return cls(limit=default_limit, offset=0, is_forward=True)

        # Forward pagination (first/after)
        if pagination.first is not None:
            offset = get_offset_from_cursor(pagination.after) if pagination.after else 0
            return cls(limit=pagination.first, offset=offset, is_forward=True)

        # Backward pagination (last/before)
        if pagination.last is not None:
            # For backward pagination, we need to calculate the offset
            # This requires knowing the total count, so we handle it differently
            before_offset = (
                get_offset_from_cursor(pagination.before) if pagination.before else 0
            )
            offset = max(0, before_offset - pagination.last)
            return cls(limit=pagination.last, offset=offset, is_forward=False)

        return cls(limit=default_limit, offset=0, is_forward=True)


def create_page_info(
    items: List[Any],
    total_count: int,
    params: PaginationParams,
) -> PageInfo:
    """
    Create PageInfo from query results.

    Args:
        items: List of items returned from the query.
        total_count: Total count of all matching items.
        params: Pagination parameters used for the query.

    Returns:
        PageInfo with cursor and pagination state.
    """
    if not items:
        return PageInfo(
            has_next_page=False,
            has_previous_page=params.offset > 0,
            start_cursor=None,
            end_cursor=None,
        )

    start_offset = params.offset
    end_offset = params.offset + len(items) - 1

    return PageInfo(
        has_next_page=end_offset < total_count - 1,
        has_previous_page=start_offset > 0,
        start_cursor=offset_cursor(start_offset),
        end_cursor=offset_cursor(end_offset),
    )


def make_edge(node: T, index: int, base_offset: int) -> "Edge[T]":
    """
    Create an edge for a connection.

    Args:
        node: The node data.
        index: Index within the current page.
        base_offset: Starting offset of the current page.

    Returns:
        Edge with node and cursor.
    """
    cursor = offset_cursor(base_offset + index)
    return Edge(node=node, cursor=cursor)


@strawberry.type
class Edge(Generic[T]):
    """A single edge in a connection, containing node and cursor."""

    node: T
    cursor: str


def create_connection(
    items: List[T],
    total_count: int,
    params: PaginationParams,
) -> "Connection[T]":
    """
    Create a Relay-style connection from query results.

    Args:
        items: List of items for the current page.
        total_count: Total count of all matching items.
        params: Pagination parameters used for the query.

    Returns:
        Connection with edges, page info, and total count.
    """
    edges = [make_edge(item, i, params.offset) for i, item in enumerate(items)]
    page_info = create_page_info(items, total_count, params)

    return Connection(
        edges=edges,
        page_info=page_info,
        total_count=total_count,
    )


@strawberry.type
class Connection(Generic[T]):
    """
    Relay-style connection for paginated results.

    Provides edges (with cursors), pagination info, and total count.
    """

    edges: List[Edge[T]]
    page_info: PageInfo
    total_count: int


# Type-specific connection types for GraphQL schema


@strawberry.type
class BreakdownItemEdge:
    """Edge for breakdown item connection."""

    node: "BreakdownItemNode"
    cursor: str


@strawberry.type
class BreakdownItemNode:
    """Node for breakdown item in paginated results."""

    key: str
    value: float


@strawberry.type
class BreakdownConnection:
    """Paginated connection for breakdown results."""

    edges: List[BreakdownItemEdge]
    page_info: PageInfo
    total_count: int
    dimension: str
    measure: str


@strawberry.type
class CatalogValueEdge:
    """Edge for catalog value connection."""

    node: "CatalogValueNode"
    cursor: str


@strawberry.type
class CatalogValueNode:
    """Node for catalog value in paginated results."""

    value: str
    count: int


@strawberry.type
class CatalogValueConnection:
    """Paginated connection for catalog dimension values."""

    edges: List[CatalogValueEdge]
    page_info: PageInfo
    total_count: int
