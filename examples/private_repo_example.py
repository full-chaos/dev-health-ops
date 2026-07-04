"""
Example script demonstrating access to private repositories on GitHub.

This example shows how to properly configure tokens and access private repositories
using the GitHub connector.

Environment Variables:
    GITHUB_TOKEN: GitHub personal access token with 'repo' scope
    GITHUB_PRIVATE_REPO: Private repository in format 'owner/repo'
"""

import os
import sys
import traceback

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector  # noqa: E402
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

    with GitHubConnector(token=token) as connector:
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
                print(
                    f"   ⚠️  Private repository {private_repo} not found in user's repos"
                )
                print(
                    "   This might mean the token doesn't have access to this repository"
                )

            # Test 2: Get repository statistics
            print("\n2. Fetching repository statistics...")
            stats = connector.get_repo_stats(owner, repo_name, max_commits=10)
            print(f"   ✅ Total commits: {stats.total_commits}")
            print(f"   ✅ Total additions: {stats.additions}")
            print(f"   ✅ Total deletions: {stats.deletions}")
            print(f"   ✅ Authors: {len(stats.authors)}")

            # Test 3: Get contributors
            print("\n3. Fetching contributors...")
            contributors = connector.get_contributors(
                owner, repo_name, max_contributors=5
            )
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


def main():
    """Main function to run all tests."""
    print("\n" + "=" * 70)
    print("Private Repository Access Test Suite")
    print("=" * 70)
    print("\nThis script tests access to private repositories on GitHub.")
    print("Make sure you have set the required environment variables:")
    print("\nFor GitHub:")
    print("  export GITHUB_TOKEN=your_token")
    print("  export GITHUB_PRIVATE_REPO=owner/repo")
    results = []

    # Test GitHub
    if os.getenv("GITHUB_TOKEN") and os.getenv("GITHUB_PRIVATE_REPO"):
        results.append(("GitHub", test_github_private_repo()))
    else:
        print("\n⏭️  Skipping GitHub test (missing GITHUB_TOKEN or GITHUB_PRIVATE_REPO)")
        results.append(("GitHub", None))

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
