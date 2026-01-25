"""
Integration example showing how to use connectors with existing storage system.

This example demonstrates how to retrieve data from GitHub/GitLab and
store it in the existing database using the storage system.
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector, GitLabConnector  # noqa: E402
from dev_health_ops.models.git import GitCommit, GitCommitStat, Repo  # noqa: E402
from dev_health_ops.storage import SQLAlchemyStore  # noqa: E402


async def github_to_storage_example():
    """Example: Retrieve GitHub data and store in database."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("GITHUB_TOKEN not set, skipping GitHub example")
        return

    # Database connection
    db_conn = os.getenv("DATABASE_URI", "sqlite+aiosqlite:///./test_integration.db")

    print("=== GitHub to Storage Integration ===\n")

    # Initialize connector
    connector = GitHubConnector(token=token)

    try:
        # Get a repository
        print("Fetching repositories...")
        repos = connector.list_repositories(max_repos=1)

        if not repos:
            print("No repositories found")
            return

        repo = repos[0]
        print(f"Using repository: {repo.full_name}\n")

        # Store repository in database
        async with SQLAlchemyStore(db_conn) as store:
            # Create Repo object for database
            db_repo = Repo(
                repo_path=None,  # Not a local repo
                repo=repo.full_name,
                settings={"source": "github", "repo_id": repo.id},
                tags=["github", repo.language] if repo.language else ["github"],
            )

            print(f"Storing repository: {db_repo.repo}")
            await store.insert_repo(db_repo)
            print(f"Repository stored with ID: {db_repo.id}\n")

            # Get and store commits
            print("Fetching commits...")
            _, repo_name = repo.full_name.split("/")

            # For demo purposes, we'll use PyGithub to get commits
            gh_repo = connector.github.get_repo(repo.full_name)
            commits = list(gh_repo.get_commits()[:5])  # Get first 5 commits

            commit_objects = []
            for commit in commits:
                git_commit = GitCommit(
                    repo_id=db_repo.id,
                    hash=commit.sha,
                    message=commit.commit.message,
                    author_name=(
                        commit.commit.author.name if commit.commit.author else "Unknown"
                    ),
                    author_email=(
                        commit.commit.author.email if commit.commit.author else ""
                    ),
                    author_when=(
                        commit.commit.author.date
                        if commit.commit.author
                        else datetime.now(timezone.utc)
                    ),
                    committer_name=(
                        commit.commit.committer.name
                        if commit.commit.committer
                        else "Unknown"
                    ),
                    committer_email=(
                        commit.commit.committer.email if commit.commit.committer else ""
                    ),
                    committer_when=(
                        commit.commit.committer.date
                        if commit.commit.committer
                        else datetime.now(timezone.utc)
                    ),
                    parents=len(commit.parents),
                )
                commit_objects.append(git_commit)
                # Extract first line of commit message
                first_line = commit.commit.message.split("\n")[0][:50]
                print(f"  - {commit.sha[:8]}: {first_line}")

            await store.insert_git_commit_data(commit_objects)
            print(f"\nStored {len(commit_objects)} commits")

            # Get and store commit stats
            print("\nFetching commit stats...")
            stats_objects = []
            for commit in commits[:3]:  # Just first 3 for demo
                for file in commit.files:
                    stat = GitCommitStat(
                        repo_id=db_repo.id,
                        commit_hash=commit.sha,
                        file_path=file.filename,
                        additions=file.additions,
                        deletions=file.deletions,
                        old_file_mode="unknown",
                        new_file_mode="unknown",
                    )
                    stats_objects.append(stat)

            await store.insert_git_commit_stats(stats_objects)
            print(f"Stored {len(stats_objects)} commit stats")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        connector.close()


async def gitlab_to_storage_example():
    """Example: Retrieve GitLab data and store in database."""
    token = os.getenv("GITLAB_TOKEN")
    if not token:
        print("GITLAB_TOKEN not set, skipping GitLab example")
        return

    # Database connection
    db_conn = os.getenv("DATABASE_URI", "sqlite+aiosqlite:///./test_integration.db")

    print("\n\n=== GitLab to Storage Integration ===\n")

    # Initialize connector
    connector = GitLabConnector(private_token=token)

    try:
        # Get a project
        print("Fetching projects...")
        projects = connector.list_projects(max_projects=1)

        if not projects:
            print("No projects found")
            return

        project = projects[0]
        print(f"Using project: {project.full_name}\n")

        # Store project in database
        async with SQLAlchemyStore(db_conn) as store:
            # Create Repo object for database
            db_repo = Repo(
                repo_path=None,  # Not a local repo
                repo=project.full_name,
                settings={"source": "gitlab", "project_id": project.id},
                tags=["gitlab"],
            )

            print(f"Storing project: {db_repo.repo}")
            await store.insert_repo(db_repo)
            print(f"Project stored with ID: {db_repo.id}\n")

            # Get and store commits
            print("Fetching commits...")
            gl_project = connector.gitlab.projects.get(project.id)
            commits = gl_project.commits.list(per_page=5, get_all=False)

            commit_objects = []
            for commit in commits:
                git_commit = GitCommit(
                    repo_id=db_repo.id,
                    hash=commit.id,
                    message=commit.message,
                    author_name=(
                        commit.author_name
                        if hasattr(commit, "author_name")
                        else "Unknown"
                    ),
                    author_email=(
                        commit.author_email if hasattr(commit, "author_email") else ""
                    ),
                    author_when=(
                        datetime.fromisoformat(
                            commit.authored_date.replace("Z", "+00:00")
                        )
                        if hasattr(commit, "authored_date")
                        else datetime.now(timezone.utc)
                    ),
                    committer_name=(
                        commit.committer_name
                        if hasattr(commit, "committer_name")
                        else "Unknown"
                    ),
                    committer_email=(
                        commit.committer_email
                        if hasattr(commit, "committer_email")
                        else ""
                    ),
                    committer_when=(
                        datetime.fromisoformat(
                            commit.committed_date.replace("Z", "+00:00")
                        )
                        if hasattr(commit, "committed_date")
                        else datetime.now(timezone.utc)
                    ),
                    parents=(
                        len(commit.parent_ids) if hasattr(commit, "parent_ids") else 0
                    ),
                )
                commit_objects.append(git_commit)
                # Extract first line of commit message
                first_line = commit.message.split("\n")[0][:50]
                print(f"  - {commit.id[:8]}: {first_line}")

            await store.insert_git_commit_data(commit_objects)
            print(f"\nStored {len(commit_objects)} commits")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        connector.close()


async def main():
    """Main function."""
    print("Integration Examples: Connectors + Storage\n")
    print("=" * 60)

    # Run GitHub example
    await github_to_storage_example()

    # Run GitLab example
    await gitlab_to_storage_example()

    print("\n" + "=" * 60)
    print("Integration examples completed!")
    print(
        "\nNote: Set GITHUB_TOKEN and GITLAB_TOKEN environment variables to run examples."
    )


if __name__ == "__main__":
    asyncio.run(main())
