from __future__ import annotations

from dev_health_ops.providers.pagerduty.auth import ApiTokenAuth, OAuthBearerAuth
from dev_health_ops.providers.pagerduty.oauth import (
    PagerDutyOAuthConfig,
    build_authorization_request,
    missing_read_scopes,
)


def test_oauth_bearer_and_api_token_headers() -> None:
    assert OAuthBearerAuth("oauth").headers() == {"Authorization": "Bearer oauth"}
    assert ApiTokenAuth("fallback").headers() == {
        "Authorization": "Token token=fallback"
    }


def test_authorization_request_uses_pkce_and_read_scopes() -> None:
    config = PagerDutyOAuthConfig("client", "secret", "https://example.test/callback")
    request = build_authorization_request(config, {"incidents", "users"})

    assert "code_challenge_method=S256" in request.url
    assert request.state in request.url
    assert len(request.code_verifier) > 40
    assert missing_read_scopes({"incidents", "users"}, {"Incidents.read"}) == {
        "Users.read"
    }
