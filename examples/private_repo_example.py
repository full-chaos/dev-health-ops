import os
import sys
import traceback

from github import GithubException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector  # noqa: E402


def test_github_private_repo() -> bool:
    token = os.getenv("GITHUB_TOKEN")
    private_repo = os.getenv("GITHUB_PRIVATE_REPO")
    if not token:
        print("❌ GITHUB_TOKEN environment variable not set")
        return False
    if not private_repo:
        print("❌ GITHUB_PRIVATE_REPO environment variable not set")
        return False
    if "/" not in private_repo:
        print(f"❌ Invalid format for GITHUB_PRIVATE_REPO: {private_repo}")
        return False

    try:
        with GitHubConnector(token=token) as connector:
            repo = connector.github.get_repo(private_repo)
            print(f"✅ Found private repository: {repo.full_name}")
            return True
    except GithubException as exc:
        print(f"❌ GitHub API error: {exc}")
        return False
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        traceback.print_exc()
        return False


def main() -> None:
    if not test_github_private_repo():
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
