"""
Tests for GitHub connector new features.
"""

from unittest.mock import Mock, patch

import pytest

from dev_health_ops.connectors import GitHubConnector


class TestGitHubConnectorRepositories:
    """Test GitHub connector repository listing features."""

    @pytest.fixture
    def mock_github_client(self):
        """Create a mock GitHub client."""
        with patch("dev_health_ops.connectors.github.Github") as mock_github:
            yield mock_github

    @pytest.fixture
    def mock_graphql_client(self):
        """Create a mock GraphQL client."""
        with patch("dev_health_ops.connectors.github.GitHubGraphQLClient") as mock_graphql:
            yield mock_graphql

    def test_list_repositories_for_user(self, mock_github_client, mock_graphql_client):
        """Test listing repositories for a specific user."""
        # Setup mock
        mock_repo = Mock()
        mock_repo.id = 1
        mock_repo.name = "test-repo"
        mock_repo.full_name = "testuser/test-repo"
        mock_repo.default_branch = "main"
        mock_repo.description = "Test repository"
        mock_repo.html_url = "https://github.com/testuser/test-repo"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5

        mock_user = Mock()
        mock_user.get_repos.return_value = [mock_repo]

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories(user_name="testuser", max_repos=10)

        # Assert
        assert len(repos) == 1
        assert repos[0].name == "test-repo"
        assert repos[0].full_name == "testuser/test-repo"
        mock_github_instance.get_user.assert_called_once_with("testuser")

    def test_list_repositories_with_search(
        self, mock_github_client, mock_graphql_client
    ):
        """Test listing repositories with search query."""
        # Setup mock
        mock_repo = Mock()
        mock_repo.id = 1
        mock_repo.name = "python-test"
        mock_repo.full_name = "user/python-test"
        mock_repo.default_branch = "main"
        mock_repo.description = "Python test repository"
        mock_repo.html_url = "https://github.com/user/python-test"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 100
        mock_repo.forks_count = 50

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.search_repositories.return_value = [mock_repo]

        # Test
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories(search="python test", max_repos=10)

        # Assert
        assert len(repos) == 1
        assert repos[0].name == "python-test"
        mock_github_instance.search_repositories.assert_called_once_with(
            query="python test"
        )

    def test_list_repositories_search_within_org(
        self, mock_github_client, mock_graphql_client
    ):
        """Test searching repositories within an organization."""
        # Setup mock - now using search_repositories API
        mock_repo1 = Mock()
        mock_repo1.id = 1
        mock_repo1.name = "api-service"
        mock_repo1.full_name = "myorg/api-service"
        mock_repo1.default_branch = "main"
        mock_repo1.description = "API service"
        mock_repo1.html_url = "https://github.com/myorg/api-service"
        mock_repo1.created_at = None
        mock_repo1.updated_at = None
        mock_repo1.language = "Python"
        mock_repo1.stargazers_count = 10
        mock_repo1.forks_count = 5

        mock_github_instance = mock_github_client.return_value
        # When search is provided with org_name, it uses search_repositories
        mock_github_instance.search_repositories.return_value = [mock_repo1]

        # Test - search for "api" within org
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories(
            org_name="myorg", search="api", max_repos=10
        )

        # Assert - API search was called with correct query
        assert len(repos) == 1
        assert repos[0].name == "api-service"
        mock_github_instance.search_repositories.assert_called_once_with(
            query="api org:myorg"
        )

    def test_list_all_repositories_no_limit(
        self, mock_github_client, mock_graphql_client
    ):
        """Test listing all repositories without max_repos limit."""
        # Setup mock
        mock_repos = []
        for i in range(150):  # More than typical page size
            mock_repo = Mock()
            mock_repo.id = i
            mock_repo.name = f"repo-{i}"
            mock_repo.full_name = f"user/repo-{i}"
            mock_repo.default_branch = "main"
            mock_repo.description = f"Repository {i}"
            mock_repo.html_url = f"https://github.com/user/repo-{i}"
            mock_repo.created_at = None
            mock_repo.updated_at = None
            mock_repo.language = "Python"
            mock_repo.stargazers_count = i
            mock_repo.forks_count = i // 2
            mock_repos.append(mock_repo)

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test - no max_repos specified
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories()

        # Assert - all repos should be returned
        assert len(repos) == 150

    def test_list_repositories_backward_compatibility(
        self, mock_github_client, mock_graphql_client
    ):
        """Test backward compatibility with org_name parameter."""
        # Setup mock
        mock_repo = Mock()
        mock_repo.id = 1
        mock_repo.name = "org-repo"
        mock_repo.full_name = "myorg/org-repo"
        mock_repo.default_branch = "main"
        mock_repo.description = "Organization repository"
        mock_repo.html_url = "https://github.com/myorg/org-repo"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5

        mock_org = Mock()
        mock_org.get_repos.return_value = [mock_repo]

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_organization.return_value = mock_org

        # Test - using old org_name parameter
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories(org_name="myorg")

        # Assert
        assert len(repos) == 1
        assert repos[0].name == "org-repo"
        mock_github_instance.get_organization.assert_called_once_with("myorg")
