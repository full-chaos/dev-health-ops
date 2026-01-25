"""
Example usage of batch repository processing features.

This example demonstrates how to use the GitHubConnector to retrieve
all repositories from an organization or user, filter them by pattern,
and collect statistics with configurable batch size, rate limiting,
and asynchronous processing.
"""

import asyncio
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector, BatchResult  # noqa: E402


def on_repo_processed(result: BatchResult) -> None:
    """Callback function called when each repository is processed."""
    if result.success:
        print(
            f"  ✓ {result.repository.full_name}: {result.stats.total_commits} commits"
        )
    else:
        print(f"  ✗ {result.repository.full_name}: {result.error}")


def main():
    """Main function demonstrating batch processing features."""
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
        # Example 1: List repositories with pattern matching
        print("\n=== Example 1: List Repositories with Pattern ===")
        print("Pattern: 'chrisgeo/merge*'")

        repos = connector.list_repositories(
            user_name="chrisgeo",
            pattern="chrisgeo/merge*",
            max_repos=10,
        )
        print(f"Found {len(repos)} matching repositories:")
        for repo in repos:
            print(f"  - {repo.full_name} ({repo.language})")

        # Example 2: Batch processing with stats (synchronous)
        print("\n=== Example 2: Batch Processing with Stats ===")
        print("Processing repositories with batch_size=2, max_concurrent=2")

        results = connector.get_repos_with_stats(
            user_name="chrisgeo",
            pattern="chrisgeo/*",
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=1.0,
            max_commits_per_repo=50,
            max_repos=5,
            on_repo_complete=on_repo_processed,
        )

        # Summary
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        print(f"\nCompleted: {successful} successful, {failed} failed")

        # Example 3: Organization repositories with pattern
        print("\n=== Example 3: Organization Repositories with Pattern ===")
        print("Fetching repos from 'github' org matching 'github/docs*'...")

        # This uses a public org for demonstration
        results = connector.get_repos_with_stats(
            org_name="github",
            pattern="github/docs*",
            batch_size=5,
            max_concurrent=2,
            rate_limit_delay=2.0,
            max_commits_per_repo=10,
            max_repos=3,
        )

        for result in results:
            if result.success:
                print(f"  {result.repository.full_name}:")
                print(f"    Total commits: {result.stats.total_commits}")
                print(f"    Additions: {result.stats.additions}")
                print(f"    Deletions: {result.stats.deletions}")
                print(f"    Authors: {len(result.stats.authors)}")

        # Check rate limit
        print("\n=== Rate Limit Status ===")
        rate_limit = connector.get_rate_limit()
        print(f"  Remaining: {rate_limit['remaining']}/{rate_limit['limit']}")
        print(f"  Reset at: {rate_limit['reset']}")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
    finally:
        connector.close()
        print("\nConnector closed.")


async def async_main():
    """Async version of the main function."""
    # Get GitHub token from environment variable
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("Error: GITHUB_TOKEN environment variable not set")
        return

    print("=== Async Batch Processing Example ===")
    connector = GitHubConnector(token=token)

    try:
        results = await connector.get_repos_with_stats_async(
            user_name="chrisgeo",
            pattern="chrisgeo/*",
            batch_size=3,
            max_concurrent=3,
            rate_limit_delay=1.0,
            max_commits_per_repo=20,
            max_repos=5,
            on_repo_complete=on_repo_processed,
        )

        successful = sum(1 for r in results if r.success)
        print(f"\nAsync processing completed: {successful}/{len(results)} successful")

    finally:
        connector.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Batch repository processing example")
    parser.add_argument(
        "--use-async",
        dest="use_async",
        action="store_true",
        help="Run async version instead of sync version",
    )
    args = parser.parse_args()

    if args.use_async:
        print("Running async version...")
        asyncio.run(async_main())
    else:
        print("Running sync version (use --use-async for async version)...")
        main()
