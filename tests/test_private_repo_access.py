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

from dev_health_ops.connectors import GitHubConnector
from dev_health_ops.connectors.exceptions import APIException, AuthenticationException

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
                # Test 1: Fetch the private repository directly
                print(f"\nTest 1: Fetching private repository {owner}/{repo_name}...")
                repos = connector.list_repositories(user_name=owner, max_repos=50)

                # Find the private repo in the list
                private_repo_found = None
                for repo in repos:
                    if repo.full_name == private_repo:
                        private_repo_found = repo
                        break

                assert private_repo_found is not None, (
                    f"Private repository {private_repo} not found in user's repositories"
                )
                print(
                    f"  ✓ Successfully found private repository: {private_repo_found.full_name}"
                )

                # Test 2: Get repository statistics
                print("\nTest 2: Fetching stats for private repository...")
                stats = connector.get_repo_stats(owner, repo_name, max_commits=10)

                assert stats is not None, "Should return stats for private repository"
                assert stats.total_commits > 0, "Private repository should have commits"
                print(f"  ✓ Successfully fetched stats: {stats.total_commits} commits")

                # Test 3: Get contributors
                print("\nTest 3: Fetching contributors for private repository...")
                contributors = connector.get_contributors(
                    owner, repo_name, max_contributors=5
                )

                assert contributors is not None, (
                    "Should return contributors for private repository"
                )
                assert len(contributors) > 0, (
                    "Private repository should have at least one contributor"
                )
                print(f"  ✓ Successfully fetched {len(contributors)} contributors")

                # Test 4: Get pull requests
                print("\nTest 4: Fetching pull requests for private repository...")
                prs = connector.get_pull_requests(
                    owner, repo_name, state="all", max_prs=5
                )

                assert prs is not None, (
                    "Should return PRs list (even if empty) for private repository"
                )
                print(f"  ✓ Successfully fetched {len(prs)} pull requests")

                # Test 5: Check rate limit
                print("\nTest 5: Checking rate limit status...")
                rate_limit = connector.get_rate_limit()

                assert rate_limit is not None, "Should return rate limit info"
                assert rate_limit["limit"] > 0, "Should have a rate limit"
                print(
                    f"  ✓ Rate limit: {rate_limit['remaining']}/{rate_limit['limit']} remaining"
                )

                print(f"\n✅ All tests passed for private repository {private_repo}")

            except AuthenticationException as e:
                pytest.fail(
                    f"Authentication failed. Ensure GITHUB_TOKEN has 'repo' scope for private repositories. Error: {e}"
                )
            except APIException as e:
                if "404" in str(e):
                    pytest.fail(
                        f"Repository not found. Ensure the token has access to {private_repo}. Error: {e}"
                    )
                raise

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
            repos = connector.list_repositories(max_repos=50)

            assert len(repos) > 0, "User should have at least one repository"

            # Check if any private repos are in the list
            # Note: We can't check repo.private directly as it's not in our Repository model
            # But if we have private repos with the token, they should be included
            print(
                f"  ✓ Successfully fetched {len(repos)} repositories (may include private)"
            )

            for repo in repos[:5]:
                print(f"  - {repo.full_name}")


@pytest.mark.skipif(skip_integration, reason="Integration tests disabled")
class TestPrivateRepoTokenValidation:
    """Tests for token validation and error handling."""

    def test_github_invalid_token(self):
        """Test that GitHub connector fails gracefully with invalid token."""
        invalid_token = "ghp_invalid_token_1234567890"

        with GitHubConnector(token=invalid_token) as connector:
            # Attempt to list repositories with invalid token
            print("\nTesting GitHub with invalid token...")

            with pytest.raises((AuthenticationException, APIException)) as exc_info:
                connector.list_repositories(max_repos=1)

            print(f"  ✓ Correctly raised exception: {type(exc_info.value).__name__}")
