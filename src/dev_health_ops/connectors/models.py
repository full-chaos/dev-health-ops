"""
Data models for GitHub and GitLab connectors.

These dataclasses represent the data structures used by the connectors
to retrieve and store information from GitHub and GitLab APIs.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class Organization:
    """Represents a GitHub organization or GitLab group."""

    id: int
    name: str
    description: Optional[str] = None
    url: Optional[str] = None


@dataclass
class Repository:
    """Represents a GitHub repository or GitLab project."""

    id: int
    name: str
    full_name: str
    default_branch: str
    description: Optional[str] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    language: Optional[str] = None
    stars: int = 0
    forks: int = 0


@dataclass
class Author:
    """Represents a contributor or author."""

    id: int
    username: str
    email: Optional[str] = None
    name: Optional[str] = None
    url: Optional[str] = None


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
    authors: List[Author] = field(default_factory=list)


@dataclass
class PullRequest:
    """Represents a GitHub Pull Request or GitLab Merge Request."""

    id: int
    number: int
    title: str
    state: str  # 'open', 'closed', 'merged'
    author: Optional[Author] = None
    created_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    body: Optional[str] = None
    url: Optional[str] = None
    base_branch: Optional[str] = None
    head_branch: Optional[str] = None


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
    ranges: List[BlameRange] = field(default_factory=list)


@dataclass
class PullRequestReview:
    """Represents a review on a Pull Request or Merge Request."""

    id: str
    reviewer: str
    state: str
    submitted_at: Optional[datetime] = None
    body: Optional[str] = None
    url: Optional[str] = None


@dataclass
class PullRequestCommit:
    """Represents a commit associated with a Pull Request or Merge Request."""

    sha: str
    authored_at: Optional[datetime] = None
    message: Optional[str] = None
    author_name: Optional[str] = None
    author_email: Optional[str] = None


@dataclass
class DORAMetric:
    """Represents a single DORA metric data point."""

    date: datetime
    value: float


@dataclass
class DORAMetrics:
    """Represents a collection of DORA metrics for a project or group."""

    metric_name: str  # deployment_frequency, lead_time_for_changes, etc.
    data_points: List[DORAMetric] = field(default_factory=list)
