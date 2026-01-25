"""
Example script demonstrating access to private repositories on GitHub and GitLab.

This example shows how to properly configure tokens and access private repositories
using the GitHub and GitLab connectors.

Environment Variables:
    GITHUB_TOKEN: GitHub personal access token with 'repo' scope
    GITHUB_PRIVATE_REPO: Private repository in format 'owner/repo'
    GITLAB_TOKEN: GitLab private token with 'read_api' and 'read_repository' scopes
    GITLAB_PRIVATE_PROJECT: Private project name or ID
    GITLAB_URL: GitLab instance URL (default: https://gitlab.com)
"""

import os
import sys
import traceback

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector, GitLabConnector  # noqa: E402
from dev_health_ops.connectors.exceptions import (  # noqa: E402
    APIException,
    AuthenticationException,
)


def test_github_private_repo():
    """Test accessing a GitHub private repository."""
    print("\n" + "=" * 70)
    print("GitHub Private Repository Access Test")
    print("=" * 70)

    token = os.getenv("GITHUB_TOKEN")
    private_repo = os.getenv("GITHUB_PRIVATE_REPO")

    if not token:
        print("❌ GITHUB_TOKEN environment variable not set")
        print("   Please set it with: export GITHUB_TOKEN=your_token")
        print("   Token must have 'repo' scope for private repositories")
        return False

    if not private_repo:
        print("❌ GITHUB_PRIVATE_REPO environment variable not set")
        print("   Please set it with: export GITHUB_PRIVATE_REPO=owner/repo")
        return False

    try:
        owner, repo_name = private_repo.split("/")
    except ValueError:
        print(f"❌ Invalid format for GITHUB_PRIVATE_REPO: {private_repo}")
        print("   Should be in format 'owner/repo'")
        return False

    print(f"\nAttempting to access private repository: {private_repo}")
    print(f"Using token: {token[:10]}...")

    connector = GitHubConnector(token=token)

    try:
        # Test 1: List user's repositories (should include private ones)
        print("\n1. Listing user's repositories...")
        repos = connector.list_repositories(user_name=owner, max_repos=50)

        found = False
        for repo in repos:
            if repo.full_name == private_repo:
                found = True
                print(f"   ✅ Found private repository: {repo.full_name}")
                break

        if not found:
            print(f"   ⚠️  Private repository {private_repo} not found in user's repos")
            print("   This might mean the token doesn't have access to this repository")

        # Test 2: Get repository statistics
        print("\n2. Fetching repository statistics...")
        stats = connector.get_repo_stats(owner, repo_name, max_commits=10)
        print(f"   ✅ Total commits: {stats.total_commits}")
        print(f"   ✅ Total additions: {stats.additions}")
        print(f"   ✅ Total deletions: {stats.deletions}")
        print(f"   ✅ Authors: {len(stats.authors)}")

        # Test 3: Get contributors
        print("\n3. Fetching contributors...")
        contributors = connector.get_contributors(owner, repo_name, max_contributors=5)
        print(f"   ✅ Found {len(contributors)} contributors")
        for contributor in contributors[:3]:
            print(f"      - {contributor.username}")

        # Test 4: Check rate limit
        print("\n4. Checking rate limit...")
        rate_limit = connector.get_rate_limit()
        print(
            f"   ✅ Rate limit: {rate_limit['remaining']}/{rate_limit['limit']} remaining"
        )

        print("\n✅ Successfully accessed private GitHub repository!")
        return True

    except AuthenticationException as e:
        print(f"\n❌ Authentication failed: {e}")
        print("   Make sure your GITHUB_TOKEN has the 'repo' scope")
        print("   Go to https://github.com/settings/tokens to verify")
        return False

    except APIException as e:
        if "404" in str(e):
            print(f"\n❌ Repository not found: {e}")
            print(
                "   Either the repository doesn't exist or your token doesn't have access"
            )
        else:
            print(f"\n❌ API error: {e}")
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        traceback.print_exc()
        return False

    finally:
        connector.close()


def test_gitlab_private_project():
    """Test accessing a GitLab private project."""
    print("\n" + "=" * 70)
    print("GitLab Private Project Access Test")
    print("=" * 70)

    token = os.getenv("GITLAB_TOKEN")
    private_project = os.getenv("GITLAB_PRIVATE_PROJECT")
    gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")

    if not token:
        print("❌ GITLAB_TOKEN environment variable not set")
        print("   Please set it with: export GITLAB_TOKEN=your_token")
        print("   Token must have 'read_api' and 'read_repository' scopes")
        return False

    if not private_project:
        print("❌ GITLAB_PRIVATE_PROJECT environment variable not set")
        print("   Please set it with: export GITLAB_PRIVATE_PROJECT=group/project")
        print("   Or use a project ID: export GITLAB_PRIVATE_PROJECT=12345")
        return False

    print(f"\nAttempting to access private project: {private_project}")
    print(f"GitLab URL: {gitlab_url}")
    print(f"Using token: {token[:10]}...")

    connector = GitLabConnector(url=gitlab_url, private_token=token)

    try:
        # Test 1: Get project details
        print("\n1. Fetching project details...")
        try:
            # Direct python-gitlab API call to verify basic access
            project = connector.gitlab.projects.get(private_project)
            print(f"   ✅ Found project: {project.name}")
            print(
                f"   ✅ Full path: {project.path_with_namespace if hasattr(project, 'path_with_namespace') else 'N/A'}"
            )
        except Exception as e:
            # python-gitlab can raise various exceptions depending on the error
            print(f"   ❌ Failed to access project: {e}")
            return False

        # Determine project identifier for subsequent calls
        project_identifier = private_project

        # Test 2: Get project statistics
        print("\n2. Fetching project statistics...")
        try:
            stats = connector.get_repo_stats(
                project_name=project_identifier, max_commits=10
            )
        except Exception:
            if str(private_project).isdigit():
                stats = connector.get_repo_stats(
                    project_id=int(private_project), max_commits=10
                )
            else:
                raise

        print(f"   ✅ Total commits: {stats.total_commits}")
        print(f"   ✅ Total additions: {stats.additions}")
        print(f"   ✅ Total deletions: {stats.deletions}")
        print(f"   ✅ Authors: {len(stats.authors)}")

        # Test 3: Get contributors
        print("\n3. Fetching contributors...")
        try:
            contributors = connector.get_contributors(
                project_name=project_identifier, max_contributors=5
            )
        except Exception:
            if str(private_project).isdigit():
                contributors = connector.get_contributors(
                    project_id=int(private_project), max_contributors=5
                )
            else:
                raise

        print(f"   ✅ Found {len(contributors)} contributors")
        for contributor in contributors[:3]:
            print(f"      - {contributor.username}")

        # Test 4: List user's projects (should include private ones)
        print("\n4. Listing accessible projects...")
        projects = connector.list_projects(max_projects=10)
        print(f"   ✅ Found {len(projects)} accessible projects (may include private)")

        print("\n✅ Successfully accessed private GitLab project!")
        return True

    except AuthenticationException as e:
        print(f"\n❌ Authentication failed: {e}")
        print(
            "   Make sure your GITLAB_TOKEN has 'read_api' and 'read_repository' scopes"
        )
        return False

    except APIException as e:
        if "404" in str(e):
            print(f"\n❌ Project not found: {e}")
            print(
                "   Either the project doesn't exist or your token doesn't have access"
            )
        else:
            print(f"\n❌ API error: {e}")
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        traceback.print_exc()
        return False

    finally:
        connector.close()


def main():
    """Main function to run all tests."""
    print("\n" + "=" * 70)
    print("Private Repository Access Test Suite")
    print("=" * 70)
    print("\nThis script tests access to private repositories on GitHub and GitLab.")
    print("Make sure you have set the required environment variables:")
    print("\nFor GitHub:")
    print("  export GITHUB_TOKEN=your_token")
    print("  export GITHUB_PRIVATE_REPO=owner/repo")
    print("\nFor GitLab:")
    print("  export GITLAB_TOKEN=your_token")
    print("  export GITLAB_PRIVATE_PROJECT=group/project  # or project ID")
    print("  export GITLAB_URL=https://gitlab.com  # optional")

    results = []

    # Test GitHub
    if os.getenv("GITHUB_TOKEN") and os.getenv("GITHUB_PRIVATE_REPO"):
        results.append(("GitHub", test_github_private_repo()))
    else:
        print("\n⏭️  Skipping GitHub test (missing GITHUB_TOKEN or GITHUB_PRIVATE_REPO)")
        results.append(("GitHub", None))

    # Test GitLab
    if os.getenv("GITLAB_TOKEN") and os.getenv("GITLAB_PRIVATE_PROJECT"):
        results.append(("GitLab", test_gitlab_private_project()))
    else:
        print(
            "\n⏭️  Skipping GitLab test (missing GITLAB_TOKEN or GITLAB_PRIVATE_PROJECT)"
        )
        results.append(("GitLab", None))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    for service, result in results:
        if result is None:
            status = "⏭️  SKIPPED"
        elif result:
            status = "✅ PASSED"
        else:
            status = "❌ FAILED"
        print(f"{service}: {status}")

    # Exit with appropriate code
    failed = any(result is False for _, result in results)
    if failed:
        print("\n❌ Some tests failed. Check the output above for details.")
        sys.exit(1)

    ran_tests = any(result is not None for _, result in results)
    if not ran_tests:
        print("\n⚠️  No tests were run. Set environment variables to run tests.")
        sys.exit(0)

    print("\n✅ All tests passed!")
    sys.exit(0)


if __name__ == "__main__":
    main()
