"""PagerDuty OAuth app config + credential-resolver env wiring (PAGER_DUTY_*)."""

from dev_health_ops.credentials.resolver import (
    PROVIDER_CREDENTIAL_TYPES,
    PROVIDER_ENV_VARS,
)
from dev_health_ops.credentials.types import PagerDutyCredentials
from dev_health_ops.providers.pagerduty.oauth import PagerDutyOAuthConfig


def test_oauth_config_from_env_reads_registered_app_vars(monkeypatch):
    # Given the registered PagerDuty app credentials in the environment
    monkeypatch.setenv("PAGER_DUTY_CLIENT_ID", "cid-123")
    monkeypatch.setenv("PAGER_DUTY_SECRET", "secret-abc")
    monkeypatch.delenv("PAGER_DUTY_REDIRECT_URI", raising=False)

    # When the OAuth config is built from the environment
    config = PagerDutyOAuthConfig.from_env()

    # Then it carries the app identity and PagerDuty's default identity endpoints
    assert config is not None
    assert config.client_id == "cid-123"
    assert config.client_secret == "secret-abc"
    assert config.redirect_uri == ""
    assert config.token_url == "https://identity.pagerduty.com/oauth/token"


def test_oauth_config_from_env_returns_none_when_unconfigured(monkeypatch):
    # Given no registered client id in the environment
    monkeypatch.delenv("PAGER_DUTY_CLIENT_ID", raising=False)

    # When building from the environment
    # Then the config is absent rather than partially constructed
    assert PagerDutyOAuthConfig.from_env() is None


def test_pagerduty_is_registered_in_the_credential_resolver():
    # The resolver must know PagerDuty so env-fallback resolution works
    assert PROVIDER_CREDENTIAL_TYPES["pagerduty"] is PagerDutyCredentials
    env_map = PROVIDER_ENV_VARS["pagerduty"]
    assert env_map["client_id"] == "PAGER_DUTY_CLIENT_ID"
    assert env_map["client_secret"] == "PAGER_DUTY_SECRET"
