from unittest.mock import patch

import pytest
from gitlab.exceptions import GitlabAuthenticationError

from dev_health_ops.connectors import GitLabConnector, match_project_pattern
from dev_health_ops.exceptions import AuthenticationException


def test_gitlab_connector_shell_keeps_bridge_fields() -> None:
    with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
        connector = GitLabConnector(url="https://gitlab.example", private_token=None)

    assert connector.url == "https://gitlab.example"
    assert connector.private_token is None
    mock_gitlab.assert_called_once_with(
        url="https://gitlab.example",
        private_token=None,
        timeout=15,
    )


def test_gitlab_connector_shell_authenticates_when_token_is_present() -> None:
    with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
        connector = GitLabConnector(private_token="token")

    assert connector.private_token == "token"
    mock_gitlab.return_value.auth.assert_called_once_with()


def test_gitlab_connector_shell_converts_authentication_failure() -> None:
    with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
        mock_gitlab.return_value.auth.side_effect = GitlabAuthenticationError("denied")

        with pytest.raises(
            AuthenticationException, match="GitLab authentication failed"
        ):
            GitLabConnector(private_token="bad-token")


def test_match_project_pattern_reexport_stays_available() -> None:
    assert match_project_pattern("group/project", "group/*") is True


def test_gitlab_connector_shell_close_is_noop_for_live_processor_callers() -> None:
    with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab"):
        connector = GitLabConnector(private_token=None)

    connector.close()  # must not raise for live processor finally-blocks


def test_gitlab_connector_shell_exposes_rest_client_for_live_processor_callers() -> (
    None
):
    # Live processor paths still use connector.rest_client (native incident
    # issues at processors/gitlab.py:1111, CI adapter base_url at :1985). The
    # retired shell must keep it or those paths AttributeError at runtime.
    with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab"):
        connector = GitLabConnector(
            url="https://gitlab.example.com", private_token="tok"
        )

    assert connector.rest_client is not None
    assert connector.rest_client.base_url == "https://gitlab.example.com/api/v4"
