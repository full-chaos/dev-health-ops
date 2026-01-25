"""Tests for work graph models."""

import pytest
import uuid
from dev_health_ops.work_graph.models import (
    NodeType,
    EdgeType,
    Provenance,
    WorkGraphEdge,
    WorkGraphIssuePR,
    WorkGraphPRCommit,
)


class TestNodeType:
    """Tests for NodeType enum."""

    def test_all_types_exist(self):
        """All expected node types should exist."""
        assert NodeType.ISSUE.value == "issue"
        assert NodeType.PR.value == "pr"
        assert NodeType.COMMIT.value == "commit"
        assert NodeType.FILE.value == "file"


class TestEdgeType:
    """Tests for EdgeType enum."""

    def test_issue_relationships(self):
        """Issue-to-issue relationship types should exist."""
        assert EdgeType.BLOCKS.value == "blocks"
        assert EdgeType.RELATES.value == "relates"
        assert EdgeType.DUPLICATES.value == "duplicates"
        assert EdgeType.PARENT_OF.value == "parent_of"
        assert EdgeType.CHILD_OF.value == "child_of"

    def test_issue_pr_relationships(self):
        """Issue-to-PR relationship types should exist."""
        assert EdgeType.REFERENCES.value == "references"
        assert EdgeType.IMPLEMENTS.value == "implements"
        assert EdgeType.FIXES.value == "fixes"

    def test_pr_commit_relationships(self):
        """PR-to-commit relationship types should exist."""
        assert EdgeType.CONTAINS.value == "contains"

    def test_commit_file_relationships(self):
        """Commit-to-file relationship types should exist."""
        assert EdgeType.TOUCHES.value == "touches"


class TestProvenance:
    """Tests for Provenance enum."""

    def test_all_types_exist(self):
        """All expected provenance types should exist."""
        assert Provenance.NATIVE.value == "native"
        assert Provenance.EXPLICIT_TEXT.value == "explicit_text"
        assert Provenance.HEURISTIC.value == "heuristic"


class TestWorkGraphEdge:
    """Tests for WorkGraphEdge model."""

    def test_create_edge(self):
        """Create a basic edge."""
        edge = WorkGraphEdge(
            edge_id="test-edge-123",
            source_type=NodeType.ISSUE,
            source_id="jira:ABC-123",
            target_type=NodeType.PR,
            target_id="pr:repo-uuid#456",
            edge_type=EdgeType.REFERENCES,
            provenance=Provenance.EXPLICIT_TEXT,
            confidence=0.8,
            evidence="found in PR title",
        )
        assert edge.edge_id == "test-edge-123"
        assert edge.source_type == NodeType.ISSUE
        assert edge.confidence == 0.8

    def test_confidence_validation(self):
        """Confidence must be between 0 and 1."""
        with pytest.raises(ValueError):
            WorkGraphEdge(
                edge_id="test",
                source_type=NodeType.ISSUE,
                source_id="a",
                target_type=NodeType.PR,
                target_id="b",
                edge_type=EdgeType.REFERENCES,
                provenance=Provenance.EXPLICIT_TEXT,
                confidence=1.5,  # Invalid!
                evidence="test",
            )

    def test_frozen(self):
        """WorkGraphEdge should be immutable."""
        edge = WorkGraphEdge(
            edge_id="test-edge-123",
            source_type=NodeType.ISSUE,
            source_id="jira:ABC-123",
            target_type=NodeType.PR,
            target_id="pr:repo-uuid#456",
            edge_type=EdgeType.REFERENCES,
            provenance=Provenance.EXPLICIT_TEXT,
            confidence=0.8,
            evidence="test",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            edge.confidence = 0.9


class TestWorkGraphIssuePR:
    """Tests for WorkGraphIssuePR model."""

    def test_create_issue_pr(self):
        """Create a basic issue-PR link."""
        repo_id = uuid.uuid4()
        link = WorkGraphIssuePR(
            repo_id=repo_id,
            work_item_id="jira:ABC-123",
            pr_number=456,
            provenance=Provenance.NATIVE,
            confidence=0.95,
            evidence="closing keyword",
        )
        assert link.work_item_id == "jira:ABC-123"
        assert link.pr_number == 456
        assert link.provenance == Provenance.NATIVE

    def test_frozen(self):
        """WorkGraphIssuePR should be immutable."""
        repo_id = uuid.uuid4()
        link = WorkGraphIssuePR(
            repo_id=repo_id,
            work_item_id="jira:ABC-123",
            pr_number=456,
            provenance=Provenance.NATIVE,
            confidence=0.95,
            evidence="test",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            link.pr_number = 789


class TestWorkGraphPRCommit:
    """Tests for WorkGraphPRCommit model."""

    def test_create_pr_commit(self):
        """Create a basic PR-commit link."""
        repo_id = uuid.uuid4()
        link = WorkGraphPRCommit(
            repo_id=repo_id,
            pr_number=123,
            commit_hash="abc123def456789012345678901234567890abcd",
            confidence=1.0,
            provenance=Provenance.NATIVE,
            evidence="PR API",
        )
        assert link.repo_id == repo_id
        assert link.pr_number == 123
        assert link.commit_hash == "abc123def456789012345678901234567890abcd"

    def test_frozen(self):
        """WorkGraphPRCommit should be immutable."""
        repo_id = uuid.uuid4()
        link = WorkGraphPRCommit(
            repo_id=repo_id,
            pr_number=123,
            commit_hash="abc123",
            confidence=1.0,
            provenance=Provenance.NATIVE,
            evidence="test",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            link.commit_hash = "def456"
