"""
Integration tests for private repository access with GitHub and GitLab connectors.

These tests verify that the connectors can access private repositories when
proper authentication tokens are provided. They can be skipped by setting
SKIP_INTEGRATION_TESTS=1 or if no private repository is configured.

Environment Variables:
    GITHUB_TOKEN: GitHub personal access token with 'repo' scope
    GITHUB_PRIVATE_REPO: GitHub private repo in format 'owner/repo'
    GITLAB_TOKEN: GitLab private token with 'read_api' and 'read_repository' scopes
    GITLAB_PRIVATE_PROJECT: GitLab private project name/path or ID
    SKIP_INTEGRATION_TESTS: Set to '1' to skip all integration tests
"""

import os
import socket
from urllib.parse import urlparse

import pytest

from dev_health_ops.connectors import GitHubConnector, GitLabConnector
from dev_health_ops.connectors.exceptions import APIException, AuthenticationException

# Skip integration tests if environment variable is set
skip_integration = os.getenv("SKIP_INTEGRATION_TESTS", "0") == "1"


def _can_reach_host(url: str, timeout: float = 1.0) -> bool:
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
gitlab_skip_reason = "Integration tests disabled or GitLab unreachable"


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

        try:
            owner, repo_name = private_repo.split("/")
        except ValueError:
            pytest.fail(
                f"GITHUB_PRIVATE_REPO should be in 'owner/repo' format, got: {private_repo}"
            )

        connector = GitHubConnector(token=token)

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
            prs = connector.get_pull_requests(owner, repo_name, state="all", max_prs=5)

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
        finally:
            connector.close()

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

        connector = GitHubConnector(token=token)

        try:
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

        finally:
            connector.close()


@pytest.mark.skipif(skip_integration or skip_gitlab_network, reason=gitlab_skip_reason)
class TestGitLabPrivateProjectAccess:
    """Integration tests for GitLab connector with private projects."""

    def test_access_private_project_with_valid_token(self):
        """Test accessing a private project with valid authentication."""
        token = os.getenv("GITLAB_TOKEN")
        private_project = os.getenv("GITLAB_PRIVATE_PROJECT")
        gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")

        if not token:
            pytest.skip("GITLAB_TOKEN environment variable not set")

        if not private_project:
            pytest.skip(
                "GITLAB_PRIVATE_PROJECT environment variable not set. "
                "Set it to project name (e.g., 'group/project') or project ID of a private project you have access to."
            )

        connector = GitLabConnector(url=gitlab_url, private_token=token)

        try:
            # Test 1: Fetch the private project directly
            print(f"\nTest 1: Fetching private project {private_project}...")

            # Try to get the project (works with both name and ID)
            # Note: This is a direct python-gitlab API call, not wrapped by connector
            try:
                project = connector.gitlab.projects.get(private_project)
                print(f"  ✓ Successfully accessed private project: {project.name}")
                project_identifier = private_project
            except Exception as e:
                # python-gitlab can raise various exceptions (GitlabAuthenticationError, GitlabGetError, etc.)
                pytest.fail(f"Failed to access private project {private_project}: {e}")

            # Test 2: List projects (should include private ones)
            print("\nTest 2: Listing accessible projects (including private)...")
            projects = connector.list_projects(max_projects=20)

            assert len(projects) > 0, "User should have access to at least one project"
            print(
                f"  ✓ Successfully fetched {len(projects)} projects (may include private)"
            )

            # Test 3: Get project statistics
            print("\nTest 3: Fetching stats for private project...")
            try:
                stats = connector.get_repo_stats(
                    project_name=project_identifier, max_commits=10
                )
            except APIException:
                # Fallback to project_id if project_name doesn't work
                if str(private_project).isdigit():
                    stats = connector.get_repo_stats(
                        project_id=int(private_project), max_commits=10
                    )
                else:
                    raise

            assert stats is not None, "Should return stats for private project"
            print(f"  ✓ Successfully fetched stats: {stats.total_commits} commits")

            # Test 4: Get contributors
            print("\nTest 4: Fetching contributors for private project...")
            try:
                contributors = connector.get_contributors(
                    project_name=project_identifier, max_contributors=5
                )
            except APIException:
                if str(private_project).isdigit():
                    contributors = connector.get_contributors(
                        project_id=int(private_project), max_contributors=5
                    )
                else:
                    raise

            assert contributors is not None, (
                "Should return contributors for private project"
            )
            print(f"  ✓ Successfully fetched {len(contributors)} contributors")

            # Test 5: Get merge requests
            print("\nTest 5: Fetching merge requests for private project...")
            try:
                mrs = connector.get_merge_requests(
                    project_name=project_identifier, state="all", max_mrs=5
                )
            except APIException:
                if str(private_project).isdigit():
                    mrs = connector.get_merge_requests(
                        project_id=int(private_project), state="all", max_mrs=5
                    )
                else:
                    raise

            assert mrs is not None, (
                "Should return MRs list (even if empty) for private project"
            )
            print(f"  ✓ Successfully fetched {len(mrs)} merge requests")

            print(f"\n✅ All tests passed for private project {private_project}")

        except AuthenticationException as e:
            pytest.fail(
                f"Authentication failed. Ensure GITLAB_TOKEN has appropriate permissions for private projects. Error: {e}"
            )
        except APIException as e:
            if "404" in str(e):
                pytest.fail(
                    f"Project not found. Ensure the token has access to {private_project}. Error: {e}"
                )
            raise
        finally:
            connector.close()

    def test_access_private_project_without_token(self):
        """Test that accessing private projects without a token fails appropriately."""
        private_project = os.getenv("GITLAB_PRIVATE_PROJECT", "some-private/project")

        # Initialize connector without token
        connector = GitLabConnector(url="https://gitlab.com", private_token=None)

        try:
            # Attempt to access a private project (should fail)
            print("\nAttempting to access private project without authentication...")

            try:
                _ = connector.gitlab.projects.get(private_project)
                # If we get here, it means the project is public or doesn't exist
                print(
                    f"  Note: Project {private_project} is accessible without token (may be public)"
                )
            except Exception as e:
                # Expected: Should fail with authentication or permission error
                print("  ✓ Correctly failed to access private project without token")
                assert (
                    "401" in str(e)
                    or "404" in str(e)
                    or "unauthorized" in str(e).lower()
                ), f"Expected authentication error, got: {e}"

        finally:
            connector.close()

    def test_list_user_projects_includes_private(self):
        """Test that listing user's projects includes private projects."""
        token = os.getenv("GITLAB_TOKEN")
        gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")

        if not token:
            pytest.skip("GITLAB_TOKEN environment variable not set")

        connector = GitLabConnector(url=gitlab_url, private_token=token)

        try:
            # Fetch user's accessible projects
            print("\nFetching user's accessible projects (including private)...")
            projects = connector.list_projects(max_projects=20)

            assert len(projects) > 0, "User should have access to at least one project"
            print(
                f"  ✓ Successfully fetched {len(projects)} projects (may include private)"
            )

            for project in projects[:5]:
                print(f"  - {project.full_name}")

        finally:
            connector.close()


@pytest.mark.skipif(skip_integration, reason="Integration tests disabled")
class TestPrivateRepoTokenValidation:
    """Tests for token validation and error handling."""

    def test_github_invalid_token(self):
        """Test that GitHub connector fails gracefully with invalid token."""
        invalid_token = "ghp_invalid_token_1234567890"

        connector = GitHubConnector(token=invalid_token)

        try:
            # Attempt to list repositories with invalid token
            print("\nTesting GitHub with invalid token...")

            with pytest.raises((AuthenticationException, APIException)) as exc_info:
                connector.list_repositories(max_repos=1)

            print(f"  ✓ Correctly raised exception: {type(exc_info.value).__name__}")

        finally:
            connector.close()

    def test_gitlab_invalid_token(self):
        """Test that GitLab connector fails gracefully with invalid token."""
        if skip_integration or skip_gitlab_network:
            pytest.skip(gitlab_skip_reason)

        invalid_token = "glpat-invalid_token_1234567890"

        print("\nTesting GitLab with invalid token...")

        # GitLab connector validates token at initialization
        with pytest.raises((AuthenticationException, APIException)) as exc_info:
            _ = GitLabConnector(url="https://gitlab.com", private_token=invalid_token)

        print(
            f"  ✓ Correctly raised exception at initialization: {type(exc_info.value).__name__}"
        )
