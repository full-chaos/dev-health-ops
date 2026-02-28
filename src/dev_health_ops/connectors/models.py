"""
Data models for GitHub and GitLab connectors.

These dataclasses represent the data structures used by the connectors
to retrieve and store information from GitHub and GitLab APIs.
"""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Organization:
    """Represents a GitHub organization or GitLab group."""

    id: int
    name: str
    description: str | None = None
    url: str | None = None


@dataclass
class Repository:
    """Represents a GitHub repository or GitLab project."""

    id: int
    name: str
    full_name: str
    default_branch: str
    description: str | None = None
    url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    language: str | None = None
    stars: int = 0
    forks: int = 0


@dataclass
class Author:
    """Represents a contributor or author."""

    id: int
    username: str
    email: str | None = None
    name: str | None = None
    url: str | None = None


@dataclass
class CommitStats:
    """Represents statistics for a single commit."""

    additions: int
    deletions: int
    commits: int = 1  # Number of commits (always 1 for a single commit)


@dataclass
class RepoStats:
    """Represents aggregated statistics for a repository."""

    total_commits: int
    additions: int
    deletions: int
    commits_per_week: float
    authors: list[Author] = field(default_factory=list)


@dataclass
class PullRequest:
    """Represents a GitHub Pull Request or GitLab Merge Request."""

    id: int
    number: int
    title: str
    state: str  # 'open', 'closed', 'merged'
    author: Author | None = None
    created_at: datetime | None = None
    merged_at: datetime | None = None
    closed_at: datetime | None = None
    body: str | None = None
    url: str | None = None
    base_branch: str | None = None
    head_branch: str | None = None


@dataclass
class BlameRange:
    """Represents a range of lines with blame information."""

    starting_line: int
    ending_line: int
    commit_sha: str
    author: str
    author_email: str
    age_seconds: int  # Age of the commit in seconds


@dataclass
class FileBlame:
    """Represents blame information for a file."""

    file_path: str
    ranges: list[BlameRange] = field(default_factory=list)


@dataclass
class PullRequestReview:
    """Represents a review on a Pull Request or Merge Request."""

    id: str
    reviewer: str
    state: str
    submitted_at: datetime | None = None
    body: str | None = None
    url: str | None = None


@dataclass
class PullRequestCommit:
    """Represents a commit associated with a Pull Request or Merge Request."""

    sha: str
    authored_at: datetime | None = None
    message: str | None = None
    author_name: str | None = None
    author_email: str | None = None


@dataclass
class DORAMetric:
    """Represents a single DORA metric data point."""

    date: datetime
    value: float


@dataclass
class DORAMetrics:
    """Represents a collection of DORA metrics for a project or group."""

    metric_name: str  # deployment_frequency, lead_time_for_changes, etc.
    data_points: list[DORAMetric] = field(default_factory=list)
