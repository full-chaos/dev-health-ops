"""
Text parsing utilities for extracting issue references from PR titles and bodies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class RefType(str, Enum):
    """Type of reference found in text."""

    CLOSES = "closes"  # closes #123, fixes #123
    REFERENCES = "references"  # plain #123 or ABC-123


@dataclass(frozen=True)
class ParsedIssueRef:
    """
    A parsed issue reference from text.

    Attributes:
        raw_match: The exact text that was matched
        issue_key: The extracted issue key/number (e.g., "ABC-123" or "123")
        ref_type: Whether this closes or just references the issue
        project_key: Optional project key for Jira (e.g., "ABC")
    """

    raw_match: str
    issue_key: str
    ref_type: RefType
    project_key: Optional[str] = None


# Jira key pattern: PROJECT-123
# Project keys are typically 2-10 uppercase letters
JIRA_KEY_PATTERN = re.compile(r"\b([A-Z][A-Z0-9]{1,9})-(\d+)\b")

# GitHub/GitLab issue reference patterns
# Closing keywords: closes, close, closed, fixes, fix, fixed, resolves, resolve, resolved
CLOSING_KEYWORDS = r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)"

# Pattern for "closes #123" style references
GITHUB_CLOSING_REF_PATTERN = re.compile(
    rf"{CLOSING_KEYWORDS}\s*#(\d+)",
    re.IGNORECASE,
)

# Pattern for plain "#123" references (not preceded by closing keyword)
GITHUB_PLAIN_REF_PATTERN = re.compile(r"(?<!\w)#(\d+)\b")

# GitLab uses same patterns but also supports cross-project refs like "group/project#123"
GITLAB_CROSS_PROJECT_PATTERN = re.compile(r"([\w\-\.]+/[\w\-\.]+)#(\d+)")


def extract_jira_keys(text: str) -> List[ParsedIssueRef]:
    """
    Extract Jira issue keys from text.

    Looks for patterns like "ABC-123" where ABC is a project key.

    Args:
        text: Text to search (e.g., PR title or body)

    Returns:
        List of ParsedIssueRef objects for each Jira key found

    Example:
        >>> extract_jira_keys("Fix for ABC-123 and DEF-456")
        [ParsedIssueRef(raw_match='ABC-123', issue_key='ABC-123', ...),
         ParsedIssueRef(raw_match='DEF-456', issue_key='DEF-456', ...)]
    """
    if not text:
        return []

    results = []
    for match in JIRA_KEY_PATTERN.finditer(text):
        project_key = match.group(1)
        issue_number = match.group(2)
        full_key = f"{project_key}-{issue_number}"

        results.append(
            ParsedIssueRef(
                raw_match=match.group(0),
                issue_key=full_key,
                ref_type=RefType.REFERENCES,  # Jira keys are always references
                project_key=project_key,
            )
        )

    return results


def extract_github_issue_refs(text: str) -> List[ParsedIssueRef]:
    """
    Extract GitHub issue references from text.

    Looks for patterns like:
    - "closes #123", "fixes #456", "resolves #789" -> CLOSES
    - "#123" (plain) -> REFERENCES

    Args:
        text: Text to search (e.g., PR title or body)

    Returns:
        List of ParsedIssueRef objects for each reference found

    Example:
        >>> extract_github_issue_refs("Fixes #123, also related to #456")
        [ParsedIssueRef(raw_match='Fixes #123', issue_key='123', ref_type=RefType.CLOSES),
         ParsedIssueRef(raw_match='#456', issue_key='456', ref_type=RefType.REFERENCES)]
    """
    if not text:
        return []

    results = []
    seen_issues: set[str] = set()

    # First, find closing references (higher priority)
    for match in GITHUB_CLOSING_REF_PATTERN.finditer(text):
        issue_number = match.group(1)
        if issue_number not in seen_issues:
            seen_issues.add(issue_number)
            results.append(
                ParsedIssueRef(
                    raw_match=match.group(0),
                    issue_key=issue_number,
                    ref_type=RefType.CLOSES,
                )
            )

    # Then, find plain references (not already seen as closing)
    for match in GITHUB_PLAIN_REF_PATTERN.finditer(text):
        issue_number = match.group(1)
        if issue_number not in seen_issues:
            seen_issues.add(issue_number)
            results.append(
                ParsedIssueRef(
                    raw_match=match.group(0),
                    issue_key=issue_number,
                    ref_type=RefType.REFERENCES,
                )
            )

    return results


def extract_gitlab_issue_refs(text: str) -> List[ParsedIssueRef]:
    """
    Extract GitLab issue references from text.

    Similar to GitHub but also supports cross-project refs like "group/project#123".

    Args:
        text: Text to search (e.g., MR title or body)

    Returns:
        List of ParsedIssueRef objects for each reference found
    """
    if not text:
        return []

    results = []
    seen_issues: set[str] = set()

    # First, find closing references
    for match in GITHUB_CLOSING_REF_PATTERN.finditer(text):
        issue_number = match.group(1)
        if issue_number not in seen_issues:
            seen_issues.add(issue_number)
            results.append(
                ParsedIssueRef(
                    raw_match=match.group(0),
                    issue_key=issue_number,
                    ref_type=RefType.CLOSES,
                )
            )

    # Find cross-project references
    for match in GITLAB_CROSS_PROJECT_PATTERN.finditer(text):
        project_path = match.group(1)
        issue_number = match.group(2)
        key = f"{project_path}#{issue_number}"
        if key not in seen_issues:
            seen_issues.add(key)
            results.append(
                ParsedIssueRef(
                    raw_match=match.group(0),
                    issue_key=key,
                    ref_type=RefType.REFERENCES,
                )
            )

    # Plain references
    for match in GITHUB_PLAIN_REF_PATTERN.finditer(text):
        issue_number = match.group(1)
        # Skip if this # is part of a cross-project ref we already captured
        if issue_number not in seen_issues:
            # Check if this position is part of a cross-project ref
            start_pos = match.start()
            is_cross_project = False
            for cp_match in GITLAB_CROSS_PROJECT_PATTERN.finditer(text):
                if cp_match.start() <= start_pos < cp_match.end():
                    is_cross_project = True
                    break
            if not is_cross_project:
                seen_issues.add(issue_number)
                results.append(
                    ParsedIssueRef(
                        raw_match=match.group(0),
                        issue_key=issue_number,
                        ref_type=RefType.REFERENCES,
                    )
                )

    return results
