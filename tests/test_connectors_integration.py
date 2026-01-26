"""
Integration tests for GitHub and GitLab connectors.

These tests make real API calls to fetch public repositories.
They can be skipped in CI/CD environments by setting SKIP_INTEGRATION_TESTS=1.
"""

import os
import socket
from urllib.parse import urlparse

import pytest

from dev_health_ops.connectors import GitHubConnector, GitLabConnector
from dev_health_ops.connectors.exceptions import ConnectorException

# Skip integration tests if environment variable is set
skip_integration = os.getenv("SKIP_INTEGRATION_TESTS", "0") == "1"


def _can_reach_host(url: str, timeout: float = 1.0) -> bool:
    """Return True if the host for the given URL is reachable."""
    parsed_url = urlparse(url)
    hostname = parsed_url.hostname or url
    if parsed_url.port:
        port = parsed_url.port
    elif parsed_url.scheme == "http":
        port = 80
    else:
        port = 443
    try:
        with socket.create_connection((hostname, port), timeout=timeout):
            return True
    except OSError:
        return False


gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")
skip_gitlab_network = not _can_reach_host(gitlab_url)


@pytest.mark.skipif(skip_integration, reason="Integration tests disabled")
class TestGitHubIntegration:
    """Integration tests for GitHub connector with real API calls."""

    def test_list_public_repos_from_github_org(self):
        """Test fetching first 10 public repos from GitHub organization."""
        # Skip if no token provided
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        connector = GitHubConnector(token=token)

        try:
            # Fetch first 10 repos from github organization
            repos = connector.list_repositories(org_name="github", max_repos=10)

            # Assertions
            assert len(repos) > 0, "Should fetch at least one repository"
            assert len(repos) <= 10, "Should not exceed max_repos limit"

            # Verify repository structure
            for repo in repos:
                assert repo.id is not None, "Repository should have an ID"
                assert repo.name, "Repository should have a name"
                assert repo.full_name, "Repository should have a full name"
                assert "github/" in repo.full_name, "Should be from github org"

            print(f"\nFetched {len(repos)} repositories from github organization:")
            for repo in repos[:5]:  # Print first 5
                print(f"  - {repo.full_name} (⭐ {repo.stars})")

        finally:
            connector.close()

    def test_list_public_repos_from_user(self):
        """Test fetching first 10 public repos from a GitHub user."""
        # Skip if no token provided
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        connector = GitHubConnector(token=token)

        try:
            # Fetch first 10 repos from torvalds (Linus Torvalds)
            repos = connector.list_repositories(user_name="torvalds", max_repos=10)

            # Assertions
            assert len(repos) > 0, "Should fetch at least one repository"
            assert len(repos) <= 10, "Should not exceed max_repos limit"

            # Verify repository structure
            for repo in repos:
                assert repo.id is not None, "Repository should have an ID"
                assert repo.name, "Repository should have a name"
                assert repo.full_name, "Repository should have a full name"
                assert "torvalds/" in repo.full_name, "Should be from torvalds user"

            print(f"\nFetched {len(repos)} repositories from torvalds user:")
            for repo in repos[:5]:  # Print first 5
                print(f"  - {repo.full_name} (⭐ {repo.stars})")

        finally:
            connector.close()

    def test_search_public_repos(self):
        """Test searching for public repositories."""
        # Skip if no token provided
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        connector = GitHubConnector(token=token)

        try:
            # Search for Python repositories
            repos = connector.list_repositories(
                search="python language:python", max_repos=10
            )

            # Assertions
            assert len(repos) > 0, "Should find at least one Python repository"
            assert len(repos) <= 10, "Should not exceed max_repos limit"

            # Verify repository structure
            for repo in repos:
                assert repo.id is not None, "Repository should have an ID"
                assert repo.name, "Repository should have a name"
                assert repo.full_name, "Repository should have a full name"

            print(f"\nFound {len(repos)} Python repositories:")
            for repo in repos[:5]:  # Print first 5
                print(f"  - {repo.full_name} (⭐ {repo.stars})")

        finally:
            connector.close()


@pytest.mark.skipif(
    skip_integration or skip_gitlab_network,
    reason="Integration tests disabled or GitLab unreachable",
)
class TestGitLabIntegration:
    """Integration tests for GitLab connector with real API calls."""

    def test_list_public_projects_from_gitlab(self):
        """Test fetching first 10 public projects from GitLab."""
        # Use unauthenticated connector for public projects
        connector = GitLabConnector(
            url="https://gitlab.com", private_token=os.getenv("GITLAB_TOKEN")
        )

        try:
            # Fetch first 10 public projects
            try:
                projects = connector.list_projects(max_projects=10)
            except ConnectorException as exc:
                pytest.skip(f"GitLab API unavailable: {exc}")

            # Assertions
            assert len(projects) > 0, "Should fetch at least one project"
            assert len(projects) <= 10, "Should not exceed max_projects limit"

            # Verify project structure
            for project in projects:
                assert project.id is not None, "Project should have an ID"
                assert project.name, "Project should have a name"
                assert project.full_name, "Project should have a full name"

            print(f"\nFetched {len(projects)} projects from GitLab:")
            for project in projects[:5]:  # Print first 5
                print(f"  - {project.full_name} (⭐ {project.stars})")

        finally:
            connector.close()

    def test_list_public_projects_from_gitlab_group(self):
        """Test fetching first 10 public projects from a GitLab group."""
        # Use unauthenticated connector for public projects
        connector = GitLabConnector(
            url="https://gitlab.com", private_token=os.getenv("GITLAB_TOKEN")
        )

        try:
            # Fetch first 10 repos from gitlab-org group
            try:
                projects = connector.list_projects(
                    group_name="gitlab-org", max_projects=10
                )
            except ConnectorException as exc:
                pytest.skip(f"GitLab API unavailable: {exc}")

            # Assertions
            assert len(projects) > 0, "Should fetch at least one project"
            assert len(projects) <= 10, "Should not exceed max_projects limit"

            # Verify project structure
            for project in projects:
                assert project.id is not None, "Project should have an ID"
                assert project.name, "Project should have a name"
                assert project.full_name, "Project should have a full name"
                # Projects may be from subgroups, just verify we got projects
                assert "/" in project.full_name, "Should have a namespace"

            print(f"\nFetched {len(projects)} projects from gitlab-org group:")
            for project in projects[:5]:  # Print first 5
                print(f"  - {project.full_name} (⭐ {project.stars})")

        finally:
            connector.close()

    def test_search_public_projects(self):
        """Test searching for public projects on GitLab."""
        # Use unauthenticated connector for public projects
        connector = GitLabConnector(
            url="https://gitlab.com", private_token=os.getenv("GITLAB_TOKEN")
        )

        try:
            # Search for docker projects
            projects = connector.list_projects(search="docker", max_projects=10)

            # Assertions
            assert len(projects) > 0, "Should find at least one project"
            assert len(projects) <= 10, "Should not exceed max_projects limit"

            # Verify project structure
            for project in projects:
                assert project.id is not None, "Project should have an ID"
                assert project.name, "Project should have a name"
                assert project.full_name, "Project should have a full name"

            print(f"\nFound {len(projects)} projects matching 'docker':")
            for project in projects[:5]:  # Print first 5
                print(f"  - {project.full_name} (⭐ {project.stars})")

        finally:
            connector.close()
