"""Tests for work graph ID generation."""

import uuid
from dev_health_ops.work_graph.ids import (
    generate_edge_id,
    generate_issue_id,
    generate_pr_id,
    generate_commit_id,
    generate_file_id,
)
from dev_health_ops.work_graph.models import NodeType, EdgeType


class TestGenerateEdgeId:
    """Tests for edge ID generation."""

    def test_deterministic(self):
        """Same inputs should produce same ID."""
        id1 = generate_edge_id(
            NodeType.ISSUE,
            "jira:ABC-123",
            EdgeType.REFERENCES,
            NodeType.PR,
            "repo-uuid#456",
        )
        id2 = generate_edge_id(
            NodeType.ISSUE,
            "jira:ABC-123",
            EdgeType.REFERENCES,
            NodeType.PR,
            "repo-uuid#456",
        )
        assert id1 == id2

    def test_different_inputs_different_ids(self):
        """Different inputs should produce different IDs."""
        id1 = generate_edge_id(
            NodeType.ISSUE,
            "jira:ABC-123",
            EdgeType.REFERENCES,
            NodeType.PR,
            "repo-uuid#456",
        )
        id2 = generate_edge_id(
            NodeType.ISSUE,
            "jira:ABC-123",
            EdgeType.REFERENCES,
            NodeType.PR,
            "repo-uuid#789",
        )
        assert id1 != id2

    def test_order_matters(self):
        """Source and target order should matter."""
        id1 = generate_edge_id(
            NodeType.ISSUE,
            "jira:ABC-123",
            EdgeType.REFERENCES,
            NodeType.PR,
            "repo-uuid#456",
        )
        id2 = generate_edge_id(
            NodeType.PR,
            "repo-uuid#456",
            EdgeType.REFERENCES,
            NodeType.ISSUE,
            "jira:ABC-123",
        )
        assert id1 != id2

    def test_returns_hex_string(self):
        """ID should be a valid hex string."""
        edge_id = generate_edge_id(
            NodeType.ISSUE, "a", EdgeType.REFERENCES, NodeType.PR, "b"
        )
        assert isinstance(edge_id, str)
        # Should be valid hex (64 chars for SHA256)
        int(edge_id, 16)
        assert len(edge_id) == 64


class TestGenerateIssueId:
    """Tests for issue ID generation."""

    def test_jira_format(self):
        """Jira-style key should work."""
        issue_id = generate_issue_id("jira", "PROJECT", "ABC-123")
        assert isinstance(issue_id, str)
        assert issue_id == "jira:ABC-123"

    def test_github_format(self):
        """GitHub issue should work."""
        issue_id = generate_issue_id("github", "owner/repo", "123")
        assert issue_id == "gh:owner/repo#123"

    def test_gitlab_format(self):
        """GitLab issue should work."""
        issue_id = generate_issue_id("gitlab", "group/project", "456")
        assert issue_id == "gl:group/project#456"

    def test_deterministic(self):
        """Same inputs should produce same ID."""
        id1 = generate_issue_id("jira", "PROJECT", "ABC-123")
        id2 = generate_issue_id("jira", "PROJECT", "ABC-123")
        assert id1 == id2

    def test_different_keys(self):
        """Different keys should produce different IDs."""
        id1 = generate_issue_id("jira", "PROJECT", "ABC-123")
        id2 = generate_issue_id("jira", "PROJECT", "ABC-456")
        assert id1 != id2


class TestGeneratePrId:
    """Tests for PR ID generation."""

    def test_pr_id(self):
        """PR should work."""
        repo_id = uuid.uuid4()
        pr_id = generate_pr_id(repo_id, 123)
        assert isinstance(pr_id, str)
        # Format: "{repo_uuid}#pr{number}"
        assert f"{repo_id}#pr123" == pr_id

    def test_deterministic(self):
        """Same inputs should produce same ID."""
        repo_id = uuid.uuid4()
        id1 = generate_pr_id(repo_id, 123)
        id2 = generate_pr_id(repo_id, 123)
        assert id1 == id2

    def test_different_repos(self):
        """Different repos should produce different IDs."""
        repo1 = uuid.uuid4()
        repo2 = uuid.uuid4()
        id1 = generate_pr_id(repo1, 123)
        id2 = generate_pr_id(repo2, 123)
        assert id1 != id2

    def test_different_numbers(self):
        """Different PR numbers should produce different IDs."""
        repo_id = uuid.uuid4()
        id1 = generate_pr_id(repo_id, 123)
        id2 = generate_pr_id(repo_id, 456)
        assert id1 != id2


class TestGenerateCommitId:
    """Tests for commit ID generation."""

    def test_sha_format(self):
        """Commit SHA should work."""
        repo_id = uuid.uuid4()
        sha = "abc123def456789012345678901234567890abcd"
        commit_id = generate_commit_id(repo_id, sha)
        assert isinstance(commit_id, str)
        # Format: "{repo_uuid}@{sha}"
        assert f"{repo_id}@{sha}" == commit_id

    def test_deterministic(self):
        """Same inputs should produce same ID."""
        repo_id = uuid.uuid4()
        sha = "abc123def456789012345678901234567890abcd"
        id1 = generate_commit_id(repo_id, sha)
        id2 = generate_commit_id(repo_id, sha)
        assert id1 == id2

    def test_different_shas(self):
        """Different SHAs should produce different IDs."""
        repo_id = uuid.uuid4()
        id1 = generate_commit_id(repo_id, "abc123")
        id2 = generate_commit_id(repo_id, "def456")
        assert id1 != id2


class TestGenerateFileId:
    """Tests for file ID generation."""

    def test_file_path(self):
        """File path should work."""
        repo_id = uuid.uuid4()
        path = "src/main.py"
        file_id = generate_file_id(repo_id, path)
        assert isinstance(file_id, str)
        # Format: "{repo_uuid}:{path}"
        assert f"{repo_id}:{path}" == file_id

    def test_deterministic(self):
        """Same inputs should produce same ID."""
        repo_id = uuid.uuid4()
        id1 = generate_file_id(repo_id, "src/main.py")
        id2 = generate_file_id(repo_id, "src/main.py")
        assert id1 == id2

    def test_different_paths(self):
        """Different paths should produce different IDs."""
        repo_id = uuid.uuid4()
        id1 = generate_file_id(repo_id, "src/main.py")
        id2 = generate_file_id(repo_id, "src/utils.py")
        assert id1 != id2

    def test_different_repos(self):
        """Different repos should produce different IDs."""
        repo1 = uuid.uuid4()
        repo2 = uuid.uuid4()
        id1 = generate_file_id(repo1, "src/main.py")
        id2 = generate_file_id(repo2, "src/main.py")
        assert id1 != id2
