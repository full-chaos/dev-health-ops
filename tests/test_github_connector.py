from unittest.mock import patch

import pytest

from dev_health_ops.connectors import GitHubConnector


class TestGitHubConnectorRetainedSurface:
    def test_initializes_clients_and_config(self):
        with (
            patch("dev_health_ops.connectors.github.Github") as mock_github,
            patch(
                "dev_health_ops.connectors.github.GitHubGraphQLClient"
            ) as mock_graphql,
        ):
            connector = GitHubConnector(token="test_token", per_page=50, max_workers=8)

        assert connector.token == "test_token"
        assert connector.github == mock_github.return_value
        assert connector.graphql == mock_graphql.return_value
        assert connector.per_page == 50
        assert connector.max_workers == 8
        mock_github.assert_called_once()
        mock_graphql.assert_called_once_with("test_token", token_provider=None)

    def test_requires_token_or_credentials(self):
        with pytest.raises(ValueError, match="requires token or credentials"):
            GitHubConnector()

    def test_close_delegates_to_pygithub_client(self):
        with (
            patch("dev_health_ops.connectors.github.Github") as mock_github,
            patch("dev_health_ops.connectors.github.GitHubGraphQLClient"),
        ):
            connector = GitHubConnector(token="test_token")

        connector.close()

        mock_github.return_value.close.assert_called_once_with()
