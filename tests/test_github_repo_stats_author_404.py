from unittest.mock import patch

from dev_health_ops.connectors.github import GitHubConnector


def test_github_connector_repo_stats_method_is_retired():
    with (
        patch("dev_health_ops.connectors.github.Github"),
        patch("dev_health_ops.connectors.github.GitHubGraphQLClient"),
    ):
        connector = GitHubConnector(token="test_token")

    assert not hasattr(connector, "get_repo_stats")
