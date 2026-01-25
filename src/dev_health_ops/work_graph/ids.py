"""
Deterministic ID generation for work graph nodes and edges.

All IDs are generated using SHA256 hashes to ensure:
- Determinism: Same inputs always produce same ID
- Collision resistance: Different inputs produce different IDs
- Idempotency: Re-running graph construction produces same edges
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Optional

from dev_health_ops.work_graph.models import NodeType, EdgeType


def _sha256_hex(data: str) -> str:
    """Generate SHA256 hex digest of input string."""
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def generate_edge_id(
    source_type: NodeType,
    source_id: str,
    edge_type: EdgeType,
    target_type: NodeType,
    target_id: str,
) -> str:
    """
    Generate a deterministic edge ID from source, target, and edge type.

    The ID is a SHA256 hash of the canonical representation:
    "{source_type}:{source_id}|{edge_type}|{target_type}:{target_id}"

    Args:
        source_type: Type of source node
        source_id: Source node identifier
        edge_type: Type of relationship
        target_type: Type of target node
        target_id: Target node identifier

    Returns:
        64-character hex string (SHA256 hash)

    Example:
        >>> generate_edge_id(
        ...     NodeType.ISSUE, "jira:ABC-123",
        ...     EdgeType.REFERENCES,
        ...     NodeType.PR, "repo-uuid#42"
        ... )
        'a1b2c3...'  # deterministic hash
    """
    canonical = f"{source_type.value}:{source_id}|{edge_type.value}|{target_type.value}:{target_id}"
    return _sha256_hex(canonical)


def generate_issue_id(
    provider: str,
    project_key_or_repo: str,
    issue_number_or_key: str,
) -> str:
    """
    Generate a canonical issue/work item ID.

    Format depends on provider:
    - jira: "jira:{KEY}" (e.g., "jira:ABC-123")
    - github: "gh:{owner/repo}#{number}" (e.g., "gh:owner/repo#123")
    - gitlab: "gl:{group/project}#{number}" (e.g., "gl:group/project#456")

    Args:
        provider: Provider name (jira, github, gitlab)
        project_key_or_repo: Project key (Jira) or repo full name (GitHub/GitLab)
        issue_number_or_key: Issue number or key

    Returns:
        Canonical issue ID string
    """
    provider_lower = provider.lower()

    if provider_lower == "jira":
        # Jira uses key directly (e.g., ABC-123)
        return f"jira:{issue_number_or_key}"
    elif provider_lower == "github":
        return f"gh:{project_key_or_repo}#{issue_number_or_key}"
    elif provider_lower == "gitlab":
        return f"gl:{project_key_or_repo}#{issue_number_or_key}"
    else:
        # Generic fallback
        return f"{provider_lower}:{project_key_or_repo}#{issue_number_or_key}"


def generate_pr_id(
    repo_id: uuid.UUID,
    pr_number: int,
) -> str:
    """
    Generate a canonical PR ID.

    Format: "{repo_uuid}#pr{number}"

    Args:
        repo_id: Repository UUID
        pr_number: Pull request number

    Returns:
        Canonical PR ID string
    """
    return f"{repo_id}#pr{pr_number}"


def generate_commit_id(
    repo_id: uuid.UUID,
    commit_hash: str,
) -> str:
    """
    Generate a canonical commit ID.

    Format: "{repo_uuid}@{sha}"

    Args:
        repo_id: Repository UUID
        commit_hash: Git commit SHA

    Returns:
        Canonical commit ID string
    """
    return f"{repo_id}@{commit_hash}"


def generate_file_id(
    repo_id: uuid.UUID,
    file_path: str,
) -> str:
    """
    Generate a canonical file ID.

    Format: "{repo_uuid}:{path}"

    Args:
        repo_id: Repository UUID
        file_path: File path within repository

    Returns:
        Canonical file ID string
    """
    return f"{repo_id}:{file_path}"


def parse_pr_from_id(pr_id: str) -> tuple[Optional[uuid.UUID], Optional[int]]:
    """
    Parse repo_id and pr_number from a canonical PR ID.

    Args:
        pr_id: Canonical PR ID (format: "{repo_uuid}#pr{number}")

    Returns:
        Tuple of (repo_id, pr_number), or (None, None) if parsing fails
    """
    try:
        parts = pr_id.split("#pr")
        if len(parts) != 2:
            return None, None
        repo_uuid = uuid.UUID(parts[0])
        pr_number = int(parts[1])
        return repo_uuid, pr_number
    except (ValueError, IndexError):
        return None, None


def parse_commit_from_id(commit_id: str) -> tuple[Optional[uuid.UUID], Optional[str]]:
    """
    Parse repo_id and commit_hash from a canonical commit ID.

    Args:
        commit_id: Canonical commit ID (format: "{repo_uuid}@{sha}")

    Returns:
        Tuple of (repo_id, commit_hash), or (None, None) if parsing fails
    """
    try:
        parts = commit_id.split("@")
        if len(parts) != 2:
            return None, None
        repo_uuid = uuid.UUID(parts[0])
        return repo_uuid, parts[1]
    except (ValueError, IndexError):
        return None, None
