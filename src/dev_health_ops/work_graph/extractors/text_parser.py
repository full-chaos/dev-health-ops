"""
Text parsing utilities for extracting issue references from PR titles and bodies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


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
    project_key: str | None = None


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

# Pull-request references embedded in commit messages by GitHub/GitLab *only*
# via their explicit merge-keyword conventions. These forms are PR/MR-specific
# by construction -- the literal words "pull request" / "merge request" cannot
# appear in an ordinary issue reference -- so they are safe to promote into
# PR->commit links:
# - GitHub merge commit: "Merge pull request #123 from ..."
# - GitLab merge commit:  "See merge request grp/proj!45"
#
# The bare squash form "Some change (#123)" is NOT recognized here by
# :func:`extract_pr_refs`. GitHub's squash-and-merge produces "<subject> (#N)",
# but that is positionally and lexically identical to a hand-authored issue
# reference such as "Fix parser edge case (#42)". Because git_pull_requests
# carries no persisted merge_commit_sha (or any merge metadata) to corroborate
# the link, a bare "(#N)" is indistinguishable from an issue mention on its own.
# :func:`extract_pr_refs` therefore stays strict: only unambiguous
# merge/MR-keyword evidence (CHAOS-2375 round-2). The squash form is instead
# surfaced separately by :func:`extract_squash_pr_refs`, whose caller
# corroborates the number against the set of *known* PR numbers in the same
# (org, repo) before persisting a lower-confidence, distinctly-tagged link
# (CHAOS-2435 interim recovery -- squash-merge orgs otherwise lose ~all
# PR->commit edges).
GITHUB_MERGE_PR_PATTERN = re.compile(r"merge\s+pull\s+request\s+#(\d+)", re.IGNORECASE)
GITLAB_MERGE_MR_PATTERN = re.compile(
    r"(?:merge\s+request|see\s+merge\s+request)\b[^!\n]*!(\d+)", re.IGNORECASE
)

# GitHub's squash-and-merge appends the PR number to the *subject line* as a
# trailing parenthetical: "<subject> (#N)". We anchor to the end of the first
# line (the subject) so that mid-body parentheticals and hand-authored
# mid-subject refs are far less likely to match. This form is ambiguous with a
# hand-authored issue ref, so it is NEVER promoted on its own -- the caller must
# corroborate N against known PR numbers in the same (org, repo) and tag the
# resulting link with distinct, lower-confidence evidence. See
# :func:`extract_squash_pr_refs`.
GITHUB_SQUASH_PR_PATTERN = re.compile(r"\(#(\d+)\)\s*$")

# Revert commits embed the *original* merge subject verbatim, e.g.
#   Revert "Merge pull request #42 from team/x"
#
#   This reverts commit <sha>.
# A revert is a *later undo* commit -- it is NOT contained by PR #42, so the
# merge-keyword number it quotes must never be promoted into a PR->commit link
# (doing so attributes the revert's changes back to the reverted PR and skews
# every downstream file/AI-impact metric). We reject the message outright when
# its subject is a revert or its body carries git's revert marker. Anchored to
# the start-of-line so an ordinary sentence merely containing the word "revert"
# (e.g. "Merge pull request #5: revert flag default") is unaffected.
# CHAOS-2375 round-3.
REVERT_SUBJECT_PATTERN = re.compile(r"^\s*revert[\s\"']", re.IGNORECASE)
REVERT_BODY_PATTERN = re.compile(
    r"^\s*this\s+reverts\s+(?:commit|merge\s+request)\b",
    re.IGNORECASE | re.MULTILINE,
)


def _is_revert_message(text: str) -> bool:
    """Return True if ``text`` is a revert commit message.

    Recognizes git's canonical revert shapes: a subject line beginning with
    ``Revert "..."`` and/or a body line ``This reverts commit <sha>.`` (GitLab
    also emits ``This reverts merge request !N``). Such commits quote the
    reverted PR/MR's merge subject but are not part of that PR/MR.
    """
    if not text:
        return False
    first_line = text.lstrip().split("\n", 1)[0]
    if REVERT_SUBJECT_PATTERN.match(first_line):
        return True
    return bool(REVERT_BODY_PATTERN.search(text))


def extract_pr_refs(text: str) -> list[int]:
    """
    Extract pull/merge-request numbers referenced in a commit message.

    Only the explicit, unambiguous merge-keyword conventions that GitHub and
    GitLab embed in *merge* commit messages are recognized:

    - "Merge pull request #123 from ..."   (GitHub merge commit)
    - "See merge request group/proj!45"    (GitLab merge commit)

    The literal "pull request" / "merge request" wording guarantees these denote
    a PR/MR rather than an issue, so the derived ``work_graph_pr_commit`` links
    are trustworthy.

    Bare forms are intentionally **not** treated as PR/MR evidence:

    - ``#123`` / ``!45``  -- almost always an ordinary issue reference
      (e.g. "Fixes #7", "Closes #500").
    - ``(#123)``          -- GitHub's squash-and-merge subject suffix, but it is
      indistinguishable from a hand-authored parenthetical issue reference like
      "Fix parser edge case (#42)". With no persisted merge metadata to
      corroborate it, accepting it *here* would persist false high-confidence
      PR->commit edges. The squash form is instead surfaced by
      :func:`extract_squash_pr_refs` and only linked by the caller after
      corroborating the number against known PRs in the same (org, repo), with
      distinct lower-confidence evidence (CHAOS-2435).

    Revert commits are rejected entirely: ``Revert "Merge pull request #42 ..."``
    quotes the reverted PR's merge subject but is a *later undo* commit, not a
    commit contained by PR #42. Linking it would attribute the revert's changes
    back to the original PR.

    Args:
        text: Commit message to search.

    Returns:
        De-duplicated list of referenced PR/MR numbers, in first-seen order.
    """
    if not text:
        return []

    # A revert commit quotes the reverted PR/MR's merge subject verbatim but is
    # not contained by that PR/MR; emitting its number would attribute the undo
    # back to the original PR. Reject before extracting. CHAOS-2375 round-3.
    if _is_revert_message(text):
        return []

    seen: set[int] = set()
    ordered: list[int] = []

    def _add(value: str) -> None:
        number = int(value)
        if number not in seen:
            seen.add(number)
            ordered.append(number)

    for pattern in (
        GITHUB_MERGE_PR_PATTERN,
        GITLAB_MERGE_MR_PATTERN,
    ):
        for match in pattern.finditer(text):
            _add(match.group(1))

    return ordered


def extract_squash_pr_refs(text: str) -> list[int]:
    """Extract the PR number from a GitHub squash-merge commit subject.

    GitHub's *squash and merge* writes a single commit whose subject is
    ``"<PR title> (#N)"`` -- the PR number appended as a trailing parenthetical.
    Unlike the merge-keyword forms recognized by :func:`extract_pr_refs`, this
    shape is **ambiguous**: a hand-authored subject like
    ``"Fix parser edge case (#42)"`` is lexically identical. This function
    therefore performs *no* corroboration of its own -- it merely surfaces the
    candidate number. Callers MUST confirm the number against the set of known
    PR numbers in the same ``(org, repo)`` and persist the link with distinct,
    lower-confidence evidence (see
    :meth:`WorkGraphBuilder._derive_pr_commit_links`). On squash-merge orgs this
    recovers the bulk of PR->commit edges that the strict
    :func:`extract_pr_refs` necessarily discards (CHAOS-2435).

    Only the trailing parenthetical on the *subject line* (first line) is
    considered, matching GitHub's convention and avoiding mid-body matches.
    Revert commits are rejected for the same reason as in
    :func:`extract_pr_refs`: they quote a prior subject but are a later undo.

    Args:
        text: Commit message to search.

    Returns:
        A single-element list ``[N]`` when the subject ends in ``(#N)``, else
        an empty list.
    """
    if not text:
        return []

    # Reverts quote a prior subject (potentially ending in "(#N)") but are not
    # contained by that PR -- reject before matching, mirroring extract_pr_refs.
    if _is_revert_message(text):
        return []

    subject = text.lstrip().split("\n", 1)[0]
    match = GITHUB_SQUASH_PR_PATTERN.search(subject)
    if match is None:
        return []
    return [int(match.group(1))]


def extract_jira_keys(text: str) -> list[ParsedIssueRef]:
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


def extract_github_issue_refs(text: str) -> list[ParsedIssueRef]:
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


def extract_gitlab_issue_refs(text: str) -> list[ParsedIssueRef]:
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
