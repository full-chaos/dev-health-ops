"""Cost controls for GraphQL analytics queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .errors import CostLimitExceededError


@dataclass
class CostLimits:
    """
    Cost limits for analytics queries.

    These limits protect the database from expensive queries and
    prevent abuse of the analytics API.
    """

    max_days: int = 3650
    max_buckets: int = 100
    max_top_n: int = 100
    max_sankey_nodes: int = 100
    max_sankey_edges: int = 500
    max_sub_requests: int = 10
    query_timeout_seconds: int = 30


# Default limits used if not overridden
DEFAULT_LIMITS = CostLimits()


def validate_date_range(
    start_date: date,
    end_date: date,
    limits: CostLimits = DEFAULT_LIMITS,
) -> None:
    """
    Validate that a date range is within cost limits.

    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
        limits: Cost limits to apply.

    Raises:
        CostLimitExceededError: If the date range exceeds max_days.
    """
    if end_date < start_date:
        raise CostLimitExceededError(
            message="end_date must be >= start_date",
            limit_name="date_range",
            limit_value=0,
            requested_value=0,
        )

    days = (end_date - start_date).days + 1
    if days > limits.max_days:
        raise CostLimitExceededError(
            message=f"Date range of {days} days exceeds limit of {limits.max_days}",
            limit_name="max_days",
            limit_value=limits.max_days,
            requested_value=days,
        )


def validate_top_n(
    top_n: int,
    limits: CostLimits = DEFAULT_LIMITS,
) -> None:
    """
    Validate that top_n is within cost limits.

    Args:
        top_n: Requested number of top results.
        limits: Cost limits to apply.

    Raises:
        CostLimitExceededError: If top_n exceeds max_top_n.
    """
    if top_n <= 0:
        raise CostLimitExceededError(
            message="top_n must be positive",
            limit_name="top_n",
            limit_value=1,
            requested_value=top_n,
        )

    if top_n > limits.max_top_n:
        raise CostLimitExceededError(
            message=f"top_n of {top_n} exceeds limit of {limits.max_top_n}",
            limit_name="max_top_n",
            limit_value=limits.max_top_n,
            requested_value=top_n,
        )


def validate_sankey_limits(
    max_nodes: int,
    max_edges: int,
    limits: CostLimits = DEFAULT_LIMITS,
) -> None:
    """
    Validate Sankey node and edge limits.

    Args:
        max_nodes: Requested max nodes.
        max_edges: Requested max edges.
        limits: Cost limits to apply.

    Raises:
        CostLimitExceededError: If limits are exceeded.
    """
    if max_nodes <= 0:
        raise CostLimitExceededError(
            message="max_nodes must be positive",
            limit_name="max_nodes",
            limit_value=1,
            requested_value=max_nodes,
        )

    if max_nodes > limits.max_sankey_nodes:
        raise CostLimitExceededError(
            message=f"max_nodes of {max_nodes} exceeds limit of {limits.max_sankey_nodes}",
            limit_name="max_sankey_nodes",
            limit_value=limits.max_sankey_nodes,
            requested_value=max_nodes,
        )

    if max_edges <= 0:
        raise CostLimitExceededError(
            message="max_edges must be positive",
            limit_name="max_edges",
            limit_value=1,
            requested_value=max_edges,
        )

    if max_edges > limits.max_sankey_edges:
        raise CostLimitExceededError(
            message=f"max_edges of {max_edges} exceeds limit of {limits.max_sankey_edges}",
            limit_name="max_sankey_edges",
            limit_value=limits.max_sankey_edges,
            requested_value=max_edges,
        )


def validate_sub_request_count(
    timeseries_count: int,
    breakdowns_count: int,
    has_sankey: bool,
    limits: CostLimits = DEFAULT_LIMITS,
) -> None:
    """
    Validate the total number of sub-requests in a batch.

    Args:
        timeseries_count: Number of timeseries requests.
        breakdowns_count: Number of breakdown requests.
        has_sankey: Whether a sankey request is included.
        limits: Cost limits to apply.

    Raises:
        CostLimitExceededError: If total sub-requests exceed max_sub_requests.
    """
    total = timeseries_count + breakdowns_count + (1 if has_sankey else 0)

    if total > limits.max_sub_requests:
        raise CostLimitExceededError(
            message=f"Total sub-requests ({total}) exceeds limit of {limits.max_sub_requests}",
            limit_name="max_sub_requests",
            limit_value=limits.max_sub_requests,
            requested_value=total,
        )


def validate_buckets(
    start_date: date,
    end_date: date,
    interval: str,
    limits: CostLimits = DEFAULT_LIMITS,
) -> None:
    """
    Validate that the estimated number of buckets is within limits.

    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
        interval: Bucket interval (day, week, month).
        limits: Cost limits to apply.

    Raises:
        CostLimitExceededError: If estimated buckets exceed max_buckets.
    """
    days = (end_date - start_date).days + 1

    if interval == "day":
        buckets = days
    elif interval == "week":
        buckets = (days + 6) // 7
    elif interval == "month":
        buckets = (days + 29) // 30
    else:
        buckets = days

    if buckets > limits.max_buckets:
        raise CostLimitExceededError(
            message=f"Estimated {buckets} buckets exceeds limit of {limits.max_buckets}",
            limit_name="max_buckets",
            limit_value=limits.max_buckets,
            requested_value=buckets,
        )
