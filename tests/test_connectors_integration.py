"""
Integration tests for GitHub connectors.

These tests make real API calls to fetch public repositories.
They can be skipped in CI/CD environments by setting SKIP_INTEGRATION_TESTS=1.
"""

import os

import pytest

from dev_health_ops.connectors import GitHubConnector

# Skip integration tests if environment variable is set
skip_integration = os.getenv("SKIP_INTEGRATION_TESTS", "0") == "1"


@pytest.mark.skipif(skip_integration, reason="Integration tests disabled")
class TestGitHubIntegration:
    """Integration tests for GitHub connector with real API calls."""

    def test_list_public_repos_from_github_org(self):
        """Test fetching first 10 public repos from GitHub organization."""
        # Skip if no token provided
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        with GitHubConnector(token=token) as connector:
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

    def test_list_public_repos_from_user(self):
        """Test fetching first 10 public repos from a GitHub user."""
        # Skip if no token provided
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        with GitHubConnector(token=token) as connector:
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

    def test_search_public_repos(self):
        """Test searching for public repositories."""
        # Skip if no token provided
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        with GitHubConnector(token=token) as connector:
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
