"""Tests for work graph text parser."""

import pytest
from dev_health_ops.work_graph.extractors.text_parser import (
    extract_jira_keys,
    extract_github_issue_refs,
    extract_gitlab_issue_refs,
    ParsedIssueRef,
    RefType,
)


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
        result = extract_jira_keys(None)
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
            ref.issue_key = "DEF-456"
