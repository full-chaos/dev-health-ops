"""
Tests for GitLab connector new features.
"""

from unittest.mock import Mock, patch

import pytest

from dev_health_ops.connectors import GitLabConnector


class TestGitLabConnectorProjects:
    """Test GitLab connector project listing features."""

    @pytest.fixture
    def mock_gitlab_client(self):
        """Create a mock GitLab client."""
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        """Create a mock REST client."""
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def test_list_projects_by_group_name(self, mock_gitlab_client, mock_rest_client):
        """Test listing projects for a group by name."""
        # Setup mock
        mock_project = Mock()
        mock_project.id = 1
        mock_project.name = "test-project"
        mock_project.path_with_namespace = "mygroup/test-project"
        mock_project.default_branch = "main"
        mock_project.description = "Test project"
        mock_project.web_url = "https://gitlab.com/mygroup/test-project"
        mock_project.created_at = "2023-01-01T00:00:00Z"
        mock_project.last_activity_at = "2023-01-02T00:00:00Z"
        mock_project.star_count = 10
        mock_project.forks_count = 5

        mock_group = Mock()
        mock_group.projects = Mock()
        mock_group.projects.list.return_value = [mock_project]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.groups = Mock()
        mock_gitlab_instance.groups.get.return_value = mock_group

        # Test
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        projects = connector.list_projects(group_name="mygroup", max_projects=10)

        # Assert
        assert len(projects) == 1
        assert projects[0].name == "test-project"
        assert projects[0].full_name == "mygroup/test-project"
        mock_gitlab_instance.groups.get.assert_called_once_with("mygroup")

    def test_list_projects_with_search(self, mock_gitlab_client, mock_rest_client):
        """Test listing projects with search query."""
        # Setup mock
        mock_project = Mock()
        mock_project.id = 1
        mock_project.name = "docker-service"
        mock_project.path_with_namespace = "user/docker-service"
        mock_project.default_branch = "main"
        mock_project.description = "Docker service"
        mock_project.web_url = "https://gitlab.com/user/docker-service"
        mock_project.created_at = "2023-01-01T00:00:00Z"
        mock_project.last_activity_at = "2023-01-02T00:00:00Z"
        mock_project.star_count = 15
        mock_project.forks_count = 8

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.list.return_value = [mock_project]

        # Test
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        projects = connector.list_projects(search="docker", max_projects=10)

        # Assert
        assert len(projects) == 1
        assert projects[0].name == "docker-service"
        mock_gitlab_instance.projects.list.assert_called_once()
        call_kwargs = mock_gitlab_instance.projects.list.call_args[1]
        assert call_kwargs["search"] == "docker"

    def test_list_projects_search_within_group(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test searching projects within a group."""
        # Setup mock
        mock_project = Mock()
        mock_project.id = 1
        mock_project.name = "api-backend"
        mock_project.path_with_namespace = "mygroup/api-backend"
        mock_project.default_branch = "main"
        mock_project.description = "API backend"
        mock_project.web_url = "https://gitlab.com/mygroup/api-backend"
        mock_project.created_at = "2023-01-01T00:00:00Z"
        mock_project.last_activity_at = "2023-01-02T00:00:00Z"
        mock_project.star_count = 20
        mock_project.forks_count = 10

        mock_group = Mock()
        mock_group.projects = Mock()
        mock_group.projects.list.return_value = [mock_project]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.groups = Mock()
        mock_gitlab_instance.groups.get.return_value = mock_group

        # Test
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        projects = connector.list_projects(
            group_name="mygroup", search="api", max_projects=10
        )

        # Assert
        assert len(projects) == 1
        assert projects[0].name == "api-backend"
        mock_gitlab_instance.groups.get.assert_called_once_with("mygroup")
        call_kwargs = mock_group.projects.list.call_args[1]
        assert call_kwargs["search"] == "api"

    def test_list_all_projects_no_limit(self, mock_gitlab_client, mock_rest_client):
        """Test listing all projects without max_projects limit."""
        # Setup mock
        mock_projects = []
        for i in range(150):  # More than typical page size
            mock_project = Mock()
            mock_project.id = i
            mock_project.name = f"project-{i}"
            mock_project.path_with_namespace = f"user/project-{i}"
            mock_project.default_branch = "main"
            mock_project.description = f"Project {i}"
            mock_project.web_url = f"https://gitlab.com/user/project-{i}"
            mock_project.created_at = "2023-01-01T00:00:00Z"
            mock_project.last_activity_at = "2023-01-02T00:00:00Z"
            mock_project.star_count = i
            mock_project.forks_count = i // 2
            mock_projects.append(mock_project)

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Test - no max_projects specified
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        projects = connector.list_projects()

        # Assert - all projects should be returned
        assert len(projects) == 150
        # Verify get_all was set to True
        call_kwargs = mock_gitlab_instance.projects.list.call_args[1]
        assert call_kwargs["get_all"] is True

    def test_list_projects_backward_compatibility_group_id(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test backward compatibility with group_id parameter."""
        # Setup mock
        mock_project = Mock()
        mock_project.id = 1
        mock_project.name = "legacy-project"
        mock_project.path_with_namespace = "mygroup/legacy-project"
        mock_project.default_branch = "main"
        mock_project.description = "Legacy project"
        mock_project.web_url = "https://gitlab.com/mygroup/legacy-project"
        mock_project.created_at = "2023-01-01T00:00:00Z"
        mock_project.last_activity_at = "2023-01-02T00:00:00Z"
        mock_project.star_count = 5
        mock_project.forks_count = 2

        mock_group = Mock()
        mock_group.projects = Mock()
        mock_group.projects.list.return_value = [mock_project]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.groups = Mock()
        mock_gitlab_instance.groups.get.return_value = mock_group

        # Test - using old group_id parameter
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        projects = connector.list_projects(group_id=123)

        # Assert
        assert len(projects) == 1
        assert projects[0].name == "legacy-project"
        mock_gitlab_instance.groups.get.assert_called_once_with(123)

    def test_get_contributors_with_project_name(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test getting contributors using project name."""
        # Setup mock
        mock_project = Mock()
        mock_project.repository_contributors.return_value = [
            {"name": "User 1", "email": "user1@example.com"},
            {"name": "User 2", "email": "user2@example.com"},
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        # Test
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        contributors = connector.get_contributors_by_project(
            project_name="mygroup/myproject", max_contributors=10
        )

        # Assert
        assert len(contributors) == 2
        assert contributors[0].username == "User 1"
        mock_gitlab_instance.projects.get.assert_called_once_with("mygroup/myproject")

    def test_get_merge_requests_with_project_name(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test getting merge requests using project name."""
        # Setup mock
        mock_project = Mock()
        mock_project.id = 123

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        mock_rest_instance = mock_rest_client.return_value
        # First call returns MRs, second call returns empty list to stop pagination
        mock_rest_instance.get_merge_requests.side_effect = [
            [
                {
                    "id": 1,
                    "iid": 10,
                    "title": "Test MR",
                    "state": "opened",
                    "author": {
                        "id": 1,
                        "username": "user1",
                        "name": "User 1",
                        "web_url": "https://gitlab.com/user1",
                    },
                    "created_at": "2023-01-01T00:00:00Z",
                    "target_branch": "main",
                    "source_branch": "feature",
                }
            ],
            [],  # Empty list to stop pagination
        ]

        # Test
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        mrs = connector.get_merge_requests(
            project_name="mygroup/myproject", state="opened", max_mrs=10
        )

        # Assert
        assert len(mrs) == 1
        assert mrs[0].title == "Test MR"
        mock_gitlab_instance.projects.get.assert_called_once_with("mygroup/myproject")
        assert mock_rest_instance.get_merge_requests.call_count == 2

    def test_methods_require_project_identifier(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test that methods raise ValueError when no identifier provided."""
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )

        error_msg = "Either project_id or project_name must be provided"
        with pytest.raises(ValueError, match=error_msg):
            connector.get_contributors_by_project()

        with pytest.raises(ValueError, match=error_msg):
            connector.get_commit_stats_by_project(sha="abc123")

        with pytest.raises(ValueError, match=error_msg):
            connector.get_repo_stats_by_project()


class TestGitLabConnectorAdapterMethods:
    """Tests for GitLab connector adapter methods (base class interface)."""

    @pytest.fixture
    def mock_gitlab_client(self):
        """Create a mock GitLab client."""
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        """Create a mock REST client."""
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def test_get_contributors_adapter(self, mock_gitlab_client, mock_rest_client):
        """Test get_contributors adapter method formats project_name correctly."""
        mock_project = Mock()
        mock_project.id = 123
        mock_project.repository_contributors.return_value = []

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        result = connector.get_contributors("mygroup", "myproject")

        # Verify project_name was formatted correctly
        mock_gitlab_instance.projects.get.assert_called_once_with("mygroup/myproject")
        assert result == []

    def test_get_commit_stats_adapter(self, mock_gitlab_client, mock_rest_client):
        """Test get_commit_stats adapter method formats project_name correctly."""
        mock_commit = Mock()
        mock_commit.id = "abc123"
        mock_commit.stats = {"additions": 10, "deletions": 5, "total": 15}

        mock_project = Mock()
        mock_project.id = 123
        mock_project.commits = Mock()
        mock_project.commits.get.return_value = mock_commit

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        result = connector.get_commit_stats("owner", "repo", sha="abc123")

        # Verify project_name was formatted correctly
        mock_gitlab_instance.projects.get.assert_called_once_with("owner/repo")
        assert result.additions == 10
        assert result.deletions == 5

    def test_get_repo_stats_adapter(self, mock_gitlab_client, mock_rest_client):
        """Test get_repo_stats adapter method formats project_name correctly."""
        mock_commit = Mock()
        mock_commit.id = "abc123"
        mock_commit.author_name = "Test Author"
        mock_commit.author_email = "test@example.com"
        mock_commit.committed_date = "2023-01-01T00:00:00.000Z"
        mock_commit.stats = {"additions": 10, "deletions": 5, "total": 15}

        mock_project = Mock()
        mock_project.id = 123
        mock_project.default_branch = "main"
        mock_project.created_at = "2023-01-01T00:00:00.000Z"
        mock_project.commits = Mock()
        mock_project.commits.list.return_value = [mock_commit]
        mock_project.commits.get.return_value = mock_commit
        mock_project.repository_contributors.return_value = []

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        result = connector.get_repo_stats("mygroup", "myproject", max_commits=10)

        # Verify project_name was formatted correctly
        mock_gitlab_instance.projects.get.assert_called_with("mygroup/myproject")
        assert result.total_commits == 1

    def test_get_pull_requests_adapter(self, mock_gitlab_client, mock_rest_client):
        """Test get_pull_requests adapter method maps to get_merge_requests."""
        mock_project = Mock()
        mock_project.id = 123

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        mock_rest_instance = mock_rest_client.return_value
        mock_rest_instance.get_merge_requests.return_value = []

        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        result = connector.get_pull_requests("owner", "repo", state="open", max_prs=5)

        # Verify project_name was formatted correctly
        mock_gitlab_instance.projects.get.assert_called_once_with("owner/repo")
        assert result == []

    def test_get_file_blame_adapter(self, mock_gitlab_client, mock_rest_client):
        """Test get_file_blame adapter method formats project_name correctly."""
        mock_project = Mock()
        mock_project.id = 123

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects = Mock()
        mock_gitlab_instance.projects.get.return_value = mock_project

        mock_rest_instance = mock_rest_client.return_value
        mock_rest_instance.get_file_blame.return_value = []

        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        result = connector.get_file_blame(
            "mygroup", "myproject", "path/to/file.py", ref="main"
        )

        # Verify project_name was formatted correctly
        mock_gitlab_instance.projects.get.assert_called_once_with("mygroup/myproject")
        assert result.file_path == "path/to/file.py"
