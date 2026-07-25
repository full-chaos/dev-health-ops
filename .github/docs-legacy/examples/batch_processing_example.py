import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dev_health_ops.connectors import GitHubConnector  # noqa: E402


def main() -> None:
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("Error: GITHUB_TOKEN environment variable not set")
        print("Please set it with: export GITHUB_TOKEN=your_token_here")
        return

    with GitHubConnector(token=token) as connector:
        user = connector.github.get_user()
        print(f"GitHubConnector is authenticated as {user.login}")
        print("Repository batch fetch now lives in providers/github/GitHubCodeClient.")


if __name__ == "__main__":
    main()
