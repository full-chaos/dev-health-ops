from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from atlassian import BasicApiTokenAuth, CookieAuth, OAuthBearerAuth
from atlassian.oauth_3lo import OAuthRefreshTokenAuth
from dev_health_ops.providers.jira.atlassian_compat import (
    atlassian_client_enabled,
    get_atlassian_auth,
    get_atlassian_base_url,
    get_atlassian_cloud_id,
    get_atlassian_graphql_auth,
    get_atlassian_graphql_base_url,
    validate_atlassian_env,
    validate_atlassian_graphql_env,
)


class TestAtlassianClientEnabled:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("1", True),
            ("true", True),
            ("TRUE", True),
            ("yes", True),
            ("on", True),
            ("0", False),
            ("false", False),
            ("no", False),
            ("off", False),
            ("", False),
            ("garbage", False),
        ],
    )
    def test_feature_flag_values(self, value: str, expected: bool) -> None:
        with patch.dict(os.environ, {"ATLASSIAN_CLIENT_ENABLED": value}, clear=False):
            assert atlassian_client_enabled() == expected

    def test_feature_flag_unset(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "ATLASSIAN_CLIENT_ENABLED"}
        with patch.dict(os.environ, env, clear=True):
            assert atlassian_client_enabled() is False


class TestGetAtlassianAuth:
    def test_prefers_atlassian_vars(self) -> None:
        env = {
            "ATLASSIAN_EMAIL": "new@example.com",
            "ATLASSIAN_API_TOKEN": "new-token",
            "JIRA_EMAIL": "old@example.com",
            "JIRA_API_TOKEN": "old-token",
        }
        with patch.dict(os.environ, env, clear=True):
            auth = get_atlassian_auth()
            assert auth is not None
            assert auth._email == "new@example.com"
            assert auth._api_token == "new-token"

    def test_falls_back_to_jira_vars(self) -> None:
        env = {
            "JIRA_EMAIL": "old@example.com",
            "JIRA_API_TOKEN": "old-token",
        }
        with patch.dict(os.environ, env, clear=True):
            auth = get_atlassian_auth()
            assert auth is not None
            assert auth._email == "old@example.com"
            assert auth._api_token == "old-token"

    def test_returns_none_if_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert get_atlassian_auth() is None

    def test_returns_none_if_partial(self) -> None:
        with patch.dict(os.environ, {"JIRA_EMAIL": "x@y.com"}, clear=True):
            assert get_atlassian_auth() is None


class TestGetAtlassianBaseUrl:
    def test_prefers_atlassian_var(self) -> None:
        env = {
            "ATLASSIAN_JIRA_BASE_URL": "https://new.atlassian.net",
            "JIRA_BASE_URL": "https://old.atlassian.net",
        }
        with patch.dict(os.environ, env, clear=True):
            assert get_atlassian_base_url() == "https://new.atlassian.net"

    def test_falls_back_to_jira_var(self) -> None:
        env = {"JIRA_BASE_URL": "old.atlassian.net"}
        with patch.dict(os.environ, env, clear=True):
            assert get_atlassian_base_url() == "https://old.atlassian.net"

    def test_normalizes_url(self) -> None:
        env = {"JIRA_BASE_URL": "http://example.atlassian.net/"}
        with patch.dict(os.environ, env, clear=True):
            assert get_atlassian_base_url() == "https://example.atlassian.net"

    def test_returns_none_if_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert get_atlassian_base_url() is None


class TestGetAtlassianCloudId:
    def test_uses_explicit_cloud_id(self) -> None:
        env = {
            "ATLASSIAN_CLOUD_ID": "explicit-id",
            "JIRA_BASE_URL": "https://derived.atlassian.net",
        }
        with patch.dict(os.environ, env, clear=True):
            assert get_atlassian_cloud_id() == "explicit-id"

    def test_derives_from_base_url(self) -> None:
        env = {"JIRA_BASE_URL": "https://myorg.atlassian.net"}
        with patch.dict(os.environ, env, clear=True):
            assert get_atlassian_cloud_id() == "myorg"

    def test_returns_none_if_no_source(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert get_atlassian_cloud_id() is None


class TestValidateAtlassianEnv:
    def test_all_valid(self) -> None:
        env = {
            "JIRA_EMAIL": "x@y.com",
            "JIRA_API_TOKEN": "tok",
            "JIRA_BASE_URL": "https://x.atlassian.net",
        }
        with patch.dict(os.environ, env, clear=True):
            assert validate_atlassian_env() == []

    def test_missing_all(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            errors = validate_atlassian_env()
            assert len(errors) == 3
            assert any("EMAIL" in e for e in errors)
            assert any("TOKEN" in e for e in errors)
            assert any("URL" in e for e in errors)

    def test_partial_missing(self) -> None:
        env = {"JIRA_EMAIL": "x@y.com"}
        with patch.dict(os.environ, env, clear=True):
            errors = validate_atlassian_env()
            assert len(errors) == 2


class TestGetAtlassianGraphqlBaseUrl:
    def test_prefers_gql_base_url(self) -> None:
        env = {
            "ATLASSIAN_GQL_BASE_URL": "https://example.atlassian.net/gateway/api/graphql",
            "ATLASSIAN_OAUTH_GQL_ENDPOINT": "https://api.atlassian.com/graphql",
        }
        with patch.dict(os.environ, env, clear=True):
            assert (
                get_atlassian_graphql_base_url()
                == "https://example.atlassian.net/gateway/api/graphql"
            )

    def test_falls_back_to_oauth_endpoint(self) -> None:
        env = {"ATLASSIAN_OAUTH_GQL_ENDPOINT": "https://api.atlassian.com/graphql"}
        with patch.dict(os.environ, env, clear=True):
            assert (
                get_atlassian_graphql_base_url() == "https://api.atlassian.com/graphql"
            )

    def test_defaults_to_api_atlassian_com(self) -> None:
        env = {"ATLASSIAN_OAUTH_ACCESS_TOKEN": "token"}
        with patch.dict(os.environ, env, clear=True):
            assert get_atlassian_graphql_base_url() == "https://api.atlassian.com"

    def test_returns_none_without_hint(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert get_atlassian_graphql_base_url() is None


class TestGetAtlassianGraphqlAuth:
    def test_refresh_token_auth(self) -> None:
        env = {
            "ATLASSIAN_OAUTH_REFRESH_TOKEN": "refresh",
            "ATLASSIAN_CLIENT_ID": "client-id",
            "ATLASSIAN_CLIENT_SECRET": "client-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            auth = get_atlassian_graphql_auth()
            assert isinstance(auth, OAuthRefreshTokenAuth)

    def test_oauth_bearer_auth(self) -> None:
        env = {"ATLASSIAN_OAUTH_ACCESS_TOKEN": "token"}
        with patch.dict(os.environ, env, clear=True):
            auth = get_atlassian_graphql_auth()
            assert isinstance(auth, OAuthBearerAuth)

    def test_basic_auth(self) -> None:
        env = {
            "ATLASSIAN_EMAIL": "user@example.com",
            "ATLASSIAN_API_TOKEN": "token",
        }
        with patch.dict(os.environ, env, clear=True):
            auth = get_atlassian_graphql_auth()
            assert isinstance(auth, BasicApiTokenAuth)

    def test_cookie_auth(self) -> None:
        env = {"ATLASSIAN_COOKIES_JSON": '{"cloud.session.token": "abc"}'}
        with patch.dict(os.environ, env, clear=True):
            auth = get_atlassian_graphql_auth()
            assert isinstance(auth, CookieAuth)

    def test_returns_none_if_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert get_atlassian_graphql_auth() is None


class TestValidateAtlassianGraphqlEnv:
    def test_all_valid(self) -> None:
        env = {
            "ATLASSIAN_GQL_BASE_URL": "https://example.atlassian.net/gateway/api/graphql",
            "ATLASSIAN_OAUTH_ACCESS_TOKEN": "token",
        }
        with patch.dict(os.environ, env, clear=True):
            assert validate_atlassian_graphql_env() == []

    def test_missing_all(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            errors = validate_atlassian_graphql_env()
            assert len(errors) == 2
            assert any("GQL" in e for e in errors)
            assert any("credentials" in e for e in errors)
