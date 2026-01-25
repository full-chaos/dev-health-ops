"""
GitHub and GitLab connectors for retrieving repository data.

This package provides production-grade connectors for GitHub and GitLab
with automatic pagination, rate limiting, and error handling.
"""

from .base import BatchResult, GitConnector
from .exceptions import (
    APIException,
    AuthenticationException,
    ConnectorException,
    NotFoundException,
    PaginationException,
    RateLimitException,
)
from .github import GitHubConnector, match_repo_pattern
from .gitlab import GitLabConnector, match_project_pattern
from .models import (
    Author,
    BlameRange,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    Repository,
    RepoStats,
)

__all__ = [
    # Base class
    "GitConnector",
    # Connectors
    "GitHubConnector",
    "GitLabConnector",
    # Batch processing
    "BatchResult",
    "match_repo_pattern",
    "match_project_pattern",
    # Models
    "Organization",
    "Repository",
    "Author",
    "CommitStats",
    "RepoStats",
    "PullRequest",
    "BlameRange",
    "FileBlame",
    # Exceptions
    "ConnectorException",
    "RateLimitException",
    "AuthenticationException",
    "NotFoundException",
    "PaginationException",
    "APIException",
]
