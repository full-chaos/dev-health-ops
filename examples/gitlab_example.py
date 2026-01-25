"""
Example usage of the GitLab connector.

This example demonstrates how to use the GitLabConnector to retrieve
groups, projects, contributors, statistics, merge requests,
and blame information from GitLab.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitLabConnector  # noqa: E402


def main():
    """Main function demonstrating GitLab connector usage."""
    # Get GitLab token from environment variable
    token = os.getenv("GITLAB_TOKEN")
    if not token:
        print("Error: GITLAB_TOKEN environment variable not set")
        print("Please set it with: export GITLAB_TOKEN=your_token_here")
        return

    # Initialize connector (can also use custom URL for self-hosted GitLab)
    gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")
    print(f"Initializing GitLab connector for {gitlab_url}...")
    connector = GitLabConnector(url=gitlab_url, private_token=token)

    try:
        # Example 1: List groups
        print("\n=== Example 1: List Groups ===")
        groups = connector.list_groups(max_groups=5)
        for group in groups:
            print(f"  - {group.name} (ID: {group.id})")

        # Example 2: List projects (multiple ways)
        print("\n=== Example 2: List Projects ===")

        # List all accessible projects
        print("All accessible projects:")
        projects = connector.list_projects(max_projects=3)
        for project in projects:
            print(f"  - {project.full_name}")
            print(f"    Stars: {project.stars}, Forks: {project.forks}")

        # List projects for a specific group (by name)
        print("\nProjects for gitlab-org group:")
        try:
            projects = connector.list_projects(group_name="gitlab-org", max_projects=3)
            for project in projects:
                print(f"  - {project.full_name}")
                print(f"    Stars: {project.stars}, Forks: {project.forks}")
        except Exception as e:
            print(f"  Could not fetch gitlab-org projects: {e}")

        # Search for projects
        print("\nSearch results for 'docker':")
        projects = connector.list_projects(search="docker", max_projects=3)
        for project in projects:
            print(f"  - {project.full_name}")
            print(f"    Stars: {project.stars}, Forks: {project.forks}")

        # Example 3: Get contributors for a specific project
        print("\n=== Example 3: Get Contributors ===")
        # Using project name (preferred method)
        project_name = "gitlab-org/gitlab-foss"
        print(f"Getting contributors for project {project_name}...")
        try:
            contributors = connector.get_contributors(
                project_name=project_name, max_contributors=10
            )
            for contributor in contributors:
                print(f"  - {contributor.username}")
        except Exception as e:
            print(f"  Could not fetch contributors: {e}")
            # Fallback to project ID
            project_id = 278964
            print(f"  Trying with project ID {project_id}...")
            contributors = connector.get_contributors(
                project_id=project_id, max_contributors=10
            )
            for contributor in contributors:
                print(f"  - {contributor.username}")

        # Example 4: Get project statistics
        print("\n=== Example 4: Get Project Statistics ===")
        print(f"Getting stats for project {project_name}...")
        try:
            stats = connector.get_repo_stats(project_name=project_name, max_commits=100)
        except Exception:
            stats = connector.get_repo_stats(project_id=278964, max_commits=100)
        print(f"  Total commits: {stats.total_commits}")
        print(f"  Additions: {stats.additions}")
        print(f"  Deletions: {stats.deletions}")
        print(f"  Commits per week: {stats.commits_per_week:.2f}")
        print(f"  Number of authors: {len(stats.authors)}")

        # Example 5: Get merge requests
        print("\n=== Example 5: Get Merge Requests ===")
        print(f"Getting MRs for project {project_name}...")
        try:
            mrs = connector.get_merge_requests(
                project_name=project_name, state="opened", max_mrs=5
            )
        except Exception:
            mrs = connector.get_merge_requests(
                project_id=278964, state="opened", max_mrs=5
            )
        for mr in mrs:
            print(f"  - MR !{mr.number}: {mr.title}")
            print(
                f"    State: {mr.state}, Author: {mr.author.username if mr.author else 'Unknown'}"
            )

        # Example 6: Get file blame
        print("\n=== Example 6: Get File Blame ===")
        # Replace with an actual file path in the repository
        file_path = "README.md"
        print(f"Getting blame for project {project_name}:{file_path}...")
        try:
            blame = connector.get_file_blame(
                project_name=project_name, file_path=file_path, ref="master"
            )
        except Exception:
            blame = connector.get_file_blame(
                project_id=278964, file_path=file_path, ref="master"
            )
        print(f"  File: {blame.file_path}")
        print(f"  Number of blame ranges: {len(blame.ranges)}")
        if blame.ranges:
            first_range = blame.ranges[0]
            print(
                f"  First range: lines {first_range.starting_line}-{first_range.ending_line}"
            )
            print(f"    Commit: {first_range.commit_sha[:8]}")
            print(f"    Author: {first_range.author}")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
    finally:
        connector.close()
        print("\nConnector closed.")


if __name__ == "__main__":
    main()
