"""
Tests for connector data models.
"""

from datetime import datetime, timezone

from dev_health_ops.connectors.models import (
    Author,
    BlameRange,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    Repository,
    RepoStats,
)


class TestOrganization:
    """Test Organization model."""

    def test_organization_creation(self):
        """Test creating an Organization."""
        org = Organization(
            id=123,
            name="test-org",
            description="Test Organization",
            url="https://github.com/test-org",
        )
        assert org.id == 123
        assert org.name == "test-org"
        assert org.description == "Test Organization"
        assert org.url == "https://github.com/test-org"

    def test_organization_optional_fields(self):
        """Test Organization with optional fields."""
        org = Organization(id=456, name="minimal-org")
        assert org.id == 456
        assert org.name == "minimal-org"
        assert org.description is None
        assert org.url is None


class TestRepository:
    """Test Repository model."""

    def test_repository_creation(self):
        """Test creating a Repository."""
        repo = Repository(
            id=789,
            name="test-repo",
            full_name="test-org/test-repo",
            default_branch="main",
            description="Test Repository",
            url="https://github.com/test-org/test-repo",
            language="Python",
            stars=100,
            forks=50,
        )
        assert repo.id == 789
        assert repo.name == "test-repo"
        assert repo.full_name == "test-org/test-repo"
        assert repo.default_branch == "main"
        assert repo.stars == 100
        assert repo.forks == 50

    def test_repository_optional_fields(self):
        """Test Repository with optional fields."""
        repo = Repository(
            id=999,
            name="minimal-repo",
            full_name="test/minimal-repo",
            default_branch="master",
        )
        assert repo.id == 999
        assert repo.description is None
        assert repo.stars == 0
        assert repo.forks == 0


class TestAuthor:
    """Test Author model."""

    def test_author_creation(self):
        """Test creating an Author."""
        author = Author(
            id=111,
            username="testuser",
            email="test@example.com",
            name="Test User",
            url="https://github.com/testuser",
        )
        assert author.id == 111
        assert author.username == "testuser"
        assert author.email == "test@example.com"
        assert author.name == "Test User"

    def test_author_minimal(self):
        """Test Author with minimal fields."""
        author = Author(id=222, username="minimal")
        assert author.id == 222
        assert author.username == "minimal"
        assert author.email is None


class TestCommitStats:
    """Test CommitStats model."""

    def test_commit_stats_creation(self):
        """Test creating CommitStats."""
        stats = CommitStats(additions=100, deletions=50, commits=1)
        assert stats.additions == 100
        assert stats.deletions == 50
        assert stats.commits == 1

    def test_commit_stats_default_commits(self):
        """Test CommitStats with default commits value."""
        stats = CommitStats(additions=10, deletions=5)
        assert stats.commits == 1


class TestRepoStats:
    """Test RepoStats model."""

    def test_repo_stats_creation(self):
        """Test creating RepoStats."""
        author1 = Author(id=1, username="user1")
        author2 = Author(id=2, username="user2")
        stats = RepoStats(
            total_commits=100,
            additions=1000,
            deletions=500,
            commits_per_week=10.5,
            authors=[author1, author2],
        )
        assert stats.total_commits == 100
        assert stats.additions == 1000
        assert stats.deletions == 500
        assert stats.commits_per_week == 10.5
        assert len(stats.authors) == 2

    def test_repo_stats_empty_authors(self):
        """Test RepoStats with no authors."""
        stats = RepoStats(
            total_commits=50, additions=500, deletions=250, commits_per_week=5.0
        )
        assert len(stats.authors) == 0


class TestPullRequest:
    """Test PullRequest model."""

    def test_pull_request_creation(self):
        """Test creating a PullRequest."""
        author = Author(id=1, username="author")
        pr = PullRequest(
            id=123,
            number=456,
            title="Test PR",
            state="open",
            author=author,
            created_at=datetime(2023, 1, 1, tzinfo=timezone.utc),
            body="Test description",
            url="https://github.com/test/repo/pull/456",
            base_branch="main",
            head_branch="feature",
        )
        assert pr.id == 123
        assert pr.number == 456
        assert pr.title == "Test PR"
        assert pr.state == "open"
        assert pr.author == author
        assert pr.base_branch == "main"

    def test_pull_request_minimal(self):
        """Test PullRequest with minimal fields."""
        pr = PullRequest(id=789, number=101, title="Minimal PR", state="closed")
        assert pr.id == 789
        assert pr.number == 101
        assert pr.author is None
        assert pr.merged_at is None


class TestBlameRange:
    """Test BlameRange model."""

    def test_blame_range_creation(self):
        """Test creating a BlameRange."""
        blame = BlameRange(
            starting_line=1,
            ending_line=10,
            commit_sha="abc123",
            author="Test Author",
            author_email="test@example.com",
            age_seconds=3600,
        )
        assert blame.starting_line == 1
        assert blame.ending_line == 10
        assert blame.commit_sha == "abc123"
        assert blame.author == "Test Author"
        assert blame.age_seconds == 3600


class TestFileBlame:
    """Test FileBlame model."""

    def test_file_blame_creation(self):
        """Test creating a FileBlame."""
        range1 = BlameRange(
            starting_line=1,
            ending_line=5,
            commit_sha="abc",
            author="Author 1",
            author_email="a1@example.com",
            age_seconds=1000,
        )
        range2 = BlameRange(
            starting_line=6,
            ending_line=10,
            commit_sha="def",
            author="Author 2",
            author_email="a2@example.com",
            age_seconds=2000,
        )
        blame = FileBlame(file_path="test.py", ranges=[range1, range2])
        assert blame.file_path == "test.py"
        assert len(blame.ranges) == 2

    def test_file_blame_empty_ranges(self):
        """Test FileBlame with no ranges."""
        blame = FileBlame(file_path="empty.py")
        assert blame.file_path == "empty.py"
        assert len(blame.ranges) == 0
