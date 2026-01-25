"""
Example usage of the GitHub connector.

This example demonstrates how to use the GitHubConnector to retrieve
organizations, repositories, contributors, statistics, pull requests,
and blame information from GitHub.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector  # noqa: E402


def main():
    """Main function demonstrating GitHub connector usage."""
    # Get GitHub token from environment variable
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("Error: GITHUB_TOKEN environment variable not set")
        print("Please set it with: export GITHUB_TOKEN=your_token_here")
        return

    # Initialize connector
    print("Initializing GitHub connector...")
    connector = GitHubConnector(token=token)

    try:
        # Example 1: List organizations
        print("\n=== Example 1: List Organizations ===")
        orgs = connector.list_organizations(max_orgs=5)
        for org in orgs:
            print(f"  - {org.name} (ID: {org.id})")

        # Example 2: List repositories (multiple ways)
        print("\n=== Example 2: List Repositories ===")

        # List authenticated user's repositories
        print("Authenticated user's repos:")
        repos = connector.list_repositories(max_repos=3)
        for repo in repos:
            print(f"  - {repo.full_name} ({repo.language})")
            print(f"    Stars: {repo.stars}, Forks: {repo.forks}")

        # List a specific user's repositories
        print("\nSpecific user's repos (torvalds):")
        repos = connector.list_repositories(user_name="torvalds", max_repos=3)
        for repo in repos:
            print(f"  - {repo.full_name} ({repo.language})")
            print(f"    Stars: {repo.stars}, Forks: {repo.forks}")

        # Search for repositories
        print("\nSearch results for 'python flask':")
        repos = connector.list_repositories(search="python flask", max_repos=3)
        for repo in repos:
            print(f"  - {repo.full_name} ({repo.language})")
            print(f"    Stars: {repo.stars}, Forks: {repo.forks}")

        # Example 3: Get contributors for a specific repository
        print("\n=== Example 3: Get Contributors ===")
        # Replace with your own repository
        owner = "torvalds"
        repo_name = "linux"
        print(f"Getting contributors for {owner}/{repo_name}...")
        contributors = connector.get_contributors(owner, repo_name, max_contributors=10)
        for contributor in contributors:
            print(f"  - {contributor.username} (Contributions)")

        # Example 4: Get repository statistics
        print("\n=== Example 4: Get Repository Statistics ===")
        print(f"Getting stats for {owner}/{repo_name}...")
        stats = connector.get_repo_stats(owner, repo_name, max_commits=100)
        print(f"  Total commits: {stats.total_commits}")
        print(f"  Additions: {stats.additions}")
        print(f"  Deletions: {stats.deletions}")
        print(f"  Commits per week: {stats.commits_per_week:.2f}")
        print(f"  Number of authors: {len(stats.authors)}")

        # Example 5: Get pull requests
        print("\n=== Example 5: Get Pull Requests ===")
        print(f"Getting PRs for {owner}/{repo_name}...")
        prs = connector.get_pull_requests(owner, repo_name, state="open", max_prs=5)
        for pr in prs:
            print(f"  - PR #{pr.number}: {pr.title}")
            print(
                f"    State: {pr.state}, Author: {pr.author.username if pr.author else 'Unknown'}"
            )

        # Example 6: Get file blame
        print("\n=== Example 6: Get File Blame ===")
        # Replace with an actual file path in the repository
        file_path = "README"
        print(f"Getting blame for {owner}/{repo_name}:{file_path}...")
        blame = connector.get_file_blame(owner, repo_name, file_path, ref="HEAD")
        print(f"  File: {blame.file_path}")
        print(f"  Number of blame ranges: {len(blame.ranges)}")
        if blame.ranges:
            first_range = blame.ranges[0]
            print(
                f"  First range: lines {first_range.starting_line}-{first_range.ending_line}"
            )
            print(f"    Commit: {first_range.commit_sha[:8]}")
            print(f"    Author: {first_range.author}")

        # Example 7: Check rate limit
        print("\n=== Example 7: Check Rate Limit ===")
        rate_limit = connector.get_rate_limit()
        print(f"  Limit: {rate_limit['limit']}")
        print(f"  Remaining: {rate_limit['remaining']}")
        print(f"  Reset at: {rate_limit['reset']}")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
    finally:
        connector.close()
        print("\nConnector closed.")


if __name__ == "__main__":
    main()
