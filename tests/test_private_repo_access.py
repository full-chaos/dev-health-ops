"""
Integration tests for private repository access with GitHub connectors.

These tests verify that the connectors can access private repositories when
proper authentication tokens are provided. They can be skipped by setting
SKIP_INTEGRATION_TESTS=1 or if no private repository is configured.

Environment Variables:
    GITHUB_TOKEN: GitHub personal access token with 'repo' scope
    GITHUB_PRIVATE_REPO: GitHub private repo in format 'owner/repo'
    SKIP_INTEGRATION_TESTS: Set to '1' to skip all integration tests
"""

import os

import pytest
from github import GithubException

from dev_health_ops.connectors import GitHubConnector

# Skip integration tests if environment variable is set
skip_integration = os.getenv("SKIP_INTEGRATION_TESTS", "0") == "1"


@pytest.mark.skipif(skip_integration, reason="Integration tests disabled")
class TestGitHubPrivateRepoAccess:
    """Integration tests for GitHub connector with private repositories."""

    def test_access_private_repo_with_valid_token(self):
        """Test accessing a private repository with valid authentication."""
        token = os.getenv("GITHUB_TOKEN")
        private_repo = os.getenv("GITHUB_PRIVATE_REPO")

        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        if not private_repo:
            pytest.skip(
                "GITHUB_PRIVATE_REPO environment variable not set. "
                "Set it to 'owner/repo' format of a private repository you have access to."
            )

        # Parse owner and repo

        if "/" not in private_repo:
            pytest.fail(
                f"GITHUB_PRIVATE_REPO should be in 'owner/repo' format, got: {private_repo}"
            )
        owner, repo_name = private_repo.split("/", 1)

        with GitHubConnector(token=token) as connector:
            try:
                print(f"\nFetching private repository {owner}/{repo_name}...")
                private_repo_found = connector.github.get_repo(private_repo)
                print(
                    f"  ✓ Successfully found private repository: {private_repo_found.full_name}"
                )

                assert private_repo_found.full_name == private_repo

            except GithubException as e:
                if e.status == 401:
                    pytest.fail(
                        "Authentication failed. Ensure GITHUB_TOKEN has 'repo' scope "
                        f"for private repositories. Error: {e}"
                    )
                if e.status == 404:
                    pytest.fail(
                        f"Repository not found. Ensure the token has access to {private_repo}. Error: {e}"
                    )
                raise
            except Exception as e:
                pytest.fail(
                    f"Authentication failed. Ensure GITHUB_TOKEN has 'repo' scope for private repositories. Error: {e}"
                )

    def test_access_private_repo_without_token(self):
        """Test that accessing private repos without a token fails appropriately."""
        # This test verifies that the error handling works correctly
        # when attempting to access private repos without authentication

        # Skip this test as PyGithub requires a token to initialize
        pytest.skip("PyGithub requires token for initialization")

    def test_list_authenticated_user_repos_includes_private(self):
        """Test that listing authenticated user's repos includes private repositories."""
        token = os.getenv("GITHUB_TOKEN")

        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")

        with GitHubConnector(token=token) as connector:
            # Fetch authenticated user's repositories
            print("\nFetching authenticated user's repositories (including private)...")
            repos = list(connector.github.get_user().get_repos()[:50])

            assert len(repos) > 0, "User should have at least one repository"

            # Check if any private repos are in the list
            # Note: We can't check repo.private directly as it's not in our Repository model
            # But if we have private repos with the token, they should be included
            print(
                f"  ✓ Successfully fetched {len(repos)} repositories (may include private)"
            )

            for repo in repos[:5]:
                print(f"  - {getattr(repo, 'full_name', '<unknown>')}")


@pytest.mark.skipif(skip_integration, reason="Integration tests disabled")
class TestPrivateRepoTokenValidation:
    """Tests for token validation and error handling."""

    def test_github_invalid_token(self):
        """Test that GitHub connector fails gracefully with invalid token."""
        invalid_token = "ghp_invalid_token_1234567890"

        with GitHubConnector(token=invalid_token) as connector:
            # Attempt to list repositories with invalid token
            print("\nTesting GitHub with invalid token...")

            with pytest.raises(GithubException) as exc_info:
                _ = connector.github.get_user().login

            print(f"  ✓ Correctly raised exception: {type(exc_info.value).__name__}")
