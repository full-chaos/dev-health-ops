"""
Utility modules for connectors.
"""

from .graphql import GitHubGraphQLClient
from .pagination import AsyncPaginationHandler, PaginationHandler
from .patterns import match_name_pattern, match_project_pattern, match_repo_pattern
from .rate_limit_queue import (
    DistributedRateLimitGate,
    RateLimitConfig,
    RateLimitGate,
    create_rate_limit_gate,
)
from .rest import GitLabRESTClient, RESTClient
from .retry import RateLimiter, retry_with_backoff

__all__ = [
    "GitHubGraphQLClient",
    "PaginationHandler",
    "AsyncPaginationHandler",
    "DistributedRateLimitGate",
    "RateLimitConfig",
    "RateLimitGate",
    "create_rate_limit_gate",
    "RESTClient",
    "GitLabRESTClient",
    "RateLimiter",
    "retry_with_backoff",
    "match_name_pattern",
    "match_repo_pattern",
    "match_project_pattern",
]
