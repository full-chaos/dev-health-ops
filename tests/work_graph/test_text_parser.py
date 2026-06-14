"""Tests for work graph text parser."""

from typing import cast

import pytest

from dev_health_ops.work_graph.extractors.text_parser import (
    ParsedIssueRef,
    RefType,
    extract_github_issue_refs,
    extract_gitlab_issue_refs,
    extract_jira_keys,
    extract_pr_refs,
)


class TestExtractPRRefs:
    """Tests for PR/MR number extraction from commit messages."""

    def test_github_merge_commit(self):
        assert extract_pr_refs("Merge pull request #123 from feat/x") == [123]

    def test_gitlab_merge_request(self):
        assert extract_pr_refs("See merge request group/proj!45") == [45]

    def test_squash_paren_form_is_not_a_pr_ref(self):
        """GitHub squash ``(#N)`` is indistinguishable from a hand-authored issue
        reference, so it must NOT be promoted into a PR/MR link.

        Regression guard for CHAOS-2375 round-2: "Fix parser edge case (#42)" is
        an ordinary parenthetical issue mention; treating it as PR #42 evidence
        attaches the commit to an unrelated PR and corrupts the work graph.
        """
        assert extract_pr_refs("Add retry logic (#42)") == []
        assert extract_pr_refs("Fix parser edge case (#42)") == []

    def test_plain_hash_mention_is_not_a_pr_ref(self):
        """A bare '#N' is an issue reference, not a PR/MR -- must be ignored."""
        assert extract_pr_refs("Relates to #7") == []
        assert extract_pr_refs("Fixes #7") == []
        assert extract_pr_refs("Closes issue #500, unrelated to any PR") == []

    def test_bare_bang_mention_is_not_a_pr_ref(self):
        """A bare '!N' without merge-request context must be ignored."""
        assert extract_pr_refs("yikes! 5 things broke") == []
        assert extract_pr_refs("!45 standalone") == []

    def test_dedupes_in_first_seen_order(self):
        # Only explicit merge-keyword forms count. "Merge pull request #9"
        # references PR 9; the squash "(#9)" and trailing bare "#3" are ambiguous
        # issue-style refs and are dropped.
        assert extract_pr_refs("Merge pull request #9\nfollow-up to (#9) and #3") == [
            9,
        ]

    def test_only_merge_keyword_forms_in_mixed_message(self):
        """A message mixing a real merge ref with squash/issue noise yields only
        the merge-keyword PR number."""
        msg = (
            "Merge pull request #88 from team/feature\n\n"
            "Implements thing (#42)\nCloses #7\nSee merge request grp/proj!12"
        )
        assert extract_pr_refs(msg) == [88, 12]

    def test_empty(self):
        assert extract_pr_refs("") == []
        assert extract_pr_refs("no refs here") == []


class TestExtractJiraKeys:
    """Tests for Jira key extraction."""

    def test_simple_key(self):
        """Extract single Jira key from text."""
        text = "Fixed ABC-123 in this PR"
        result = extract_jira_keys(text)
        assert len(result) == 1
        assert result[0].issue_key == "ABC-123"
        assert result[0].ref_type == RefType.REFERENCES

    def test_multiple_keys(self):
        """Extract multiple Jira keys from text."""
        text = "Addresses ABC-123 and DEF-456, related to GHI-789"
        result = extract_jira_keys(text)
        assert len(result) == 3
        keys = {r.issue_key for r in result}
        assert keys == {"ABC-123", "DEF-456", "GHI-789"}

    def test_duplicate_keys_preserved(self):
        """Duplicate Jira keys should be found separately."""
        text = "ABC-123 mentioned twice: ABC-123"
        result = extract_jira_keys(text)
        # Pattern finds each occurrence
        assert len(result) == 2
        assert all(r.issue_key == "ABC-123" for r in result)

    def test_no_keys(self):
        """Return empty list when no keys present."""
        text = "No issue keys here"
        result = extract_jira_keys(text)
        assert result == []

    def test_uppercase_key(self):
        """Keys require uppercase project key."""
        # Jira keys are uppercase; lowercase won't match
        text = "ABC-123 mentioned"
        result = extract_jira_keys(text)
        assert len(result) == 1
        assert result[0].issue_key == "ABC-123"

    def test_key_in_url(self):
        """Extract key from Jira URL."""
        text = "See https://jira.example.com/browse/ABC-123"
        result = extract_jira_keys(text)
        assert len(result) == 1
        assert result[0].issue_key == "ABC-123"

    def test_project_key_extracted(self):
        """Project key should be extracted separately."""
        text = "Fixes ABC-123"
        result = extract_jira_keys(text)
        assert len(result) == 1
        assert result[0].project_key == "ABC"

    def test_empty_text(self):
        """Empty text returns empty list."""
        result = extract_jira_keys("")
        assert result == []

    def test_none_text(self):
        """None text returns empty list."""
        result = extract_jira_keys(cast(str, None))
        assert result == []


class TestExtractGitHubIssueRefs:
    """Tests for GitHub issue reference extraction."""

    def test_simple_hash_ref(self):
        """Extract #123 style reference."""
        text = "See also #123"
        result = extract_github_issue_refs(text)
        assert len(result) == 1
        assert result[0].issue_key == "123"
        assert result[0].ref_type == RefType.REFERENCES

    def test_closes_keyword(self):
        """Closes keyword should have CLOSES ref type."""
        text = "Closes #456"
        result = extract_github_issue_refs(text)
        assert len(result) == 1
        assert result[0].issue_key == "456"
        assert result[0].ref_type == RefType.CLOSES

    def test_fixes_keyword(self):
        """Fixes keyword should have CLOSES ref type."""
        text = "Fixes #789"
        result = extract_github_issue_refs(text)
        assert len(result) == 1
        assert result[0].ref_type == RefType.CLOSES

    def test_resolves_keyword(self):
        """Resolves keyword should have CLOSES ref type."""
        text = "Resolves #100"
        result = extract_github_issue_refs(text)
        assert len(result) == 1
        assert result[0].ref_type == RefType.CLOSES

    def test_multiple_refs(self):
        """Extract multiple references."""
        text = "Fixes #1 and #2"
        result = extract_github_issue_refs(text)
        assert len(result) == 2
        keys = {r.issue_key for r in result}
        assert "1" in keys
        assert "2" in keys

    def test_no_refs(self):
        """Return empty list when no refs present."""
        text = "No issue refs here"
        result = extract_github_issue_refs(text)
        assert result == []

    def test_empty_text(self):
        """Empty text returns empty list."""
        result = extract_github_issue_refs("")
        assert result == []


class TestExtractGitLabIssueRefs:
    """Tests for GitLab issue reference extraction."""

    def test_simple_hash_ref(self):
        """Extract #123 style reference."""
        text = "Fixed #123"
        result = extract_gitlab_issue_refs(text)
        assert len(result) == 1
        assert result[0].issue_key == "123"

    def test_closes_keyword(self):
        """Closes keyword should have CLOSES ref type."""
        text = "Closes #456"
        result = extract_gitlab_issue_refs(text)
        assert len(result) == 1
        assert result[0].ref_type == RefType.CLOSES

    def test_no_refs(self):
        """Return empty list when no refs present."""
        text = "No issue refs here"
        result = extract_gitlab_issue_refs(text)
        assert result == []

    def test_empty_text(self):
        """Empty text returns empty list."""
        result = extract_gitlab_issue_refs("")
        assert result == []


class TestParsedIssueRef:
    """Tests for ParsedIssueRef dataclass."""

    def test_creation(self):
        """ParsedIssueRef should be creatable with required fields."""
        ref = ParsedIssueRef(
            raw_match="ABC-123",
            issue_key="ABC-123",
            ref_type=RefType.REFERENCES,
        )
        assert ref.issue_key == "ABC-123"
        assert ref.ref_type == RefType.REFERENCES
        assert ref.project_key is None

    def test_with_project_key(self):
        """ParsedIssueRef can include project key."""
        ref = ParsedIssueRef(
            raw_match="ABC-123",
            issue_key="ABC-123",
            ref_type=RefType.REFERENCES,
            project_key="ABC",
        )
        assert ref.project_key == "ABC"

    def test_frozen(self):
        """ParsedIssueRef should be immutable."""
        ref = ParsedIssueRef(
            raw_match="ABC-123",
            issue_key="ABC-123",
            ref_type=RefType.REFERENCES,
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            setattr(ref, "issue_key", "DEF-456")
