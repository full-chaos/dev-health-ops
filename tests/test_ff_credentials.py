"""Tests for feature flag provider credential types and registry."""

from dev_health_ops.credentials import LaunchDarklyCredentials, TelemetryCredentials
from dev_health_ops.credentials.resolver import (
    PROVIDER_CREDENTIAL_TYPES,
    PROVIDER_ENV_VARS,
)
from dev_health_ops.providers.registry import PROVIDER_REGISTRY


def test_launchdarkly_credentials_fields():
    creds = LaunchDarklyCredentials(api_key="test-key")
    assert creds.api_key == "test-key"
    assert creds.project_key is None
    assert creds.environment is None


def test_launchdarkly_credentials_with_optionals():
    creds = LaunchDarklyCredentials(
        api_key="test-key", project_key="my-project", environment="production"
    )
    assert creds.project_key == "my-project"
    assert creds.environment == "production"


def test_telemetry_credentials_fields():
    creds = TelemetryCredentials(api_key="test-key")
    assert creds.api_key == "test-key"
    assert creds.schema_version == "1.0"


def test_telemetry_credentials_custom_version():
    creds = TelemetryCredentials(api_key="test-key", schema_version="2.0")
    assert creds.schema_version == "2.0"


def test_providers_registered_in_env_vars():
    assert "launchdarkly" in PROVIDER_ENV_VARS
    assert "telemetry" in PROVIDER_ENV_VARS


def test_providers_registered_in_credential_types():
    assert "launchdarkly" in PROVIDER_CREDENTIAL_TYPES
    assert PROVIDER_CREDENTIAL_TYPES["launchdarkly"] is LaunchDarklyCredentials
    assert "telemetry" in PROVIDER_CREDENTIAL_TYPES
    assert PROVIDER_CREDENTIAL_TYPES["telemetry"] is TelemetryCredentials


def test_providers_registered_in_registry():
    assert "launchdarkly" in PROVIDER_REGISTRY
    assert "telemetry" in PROVIDER_REGISTRY
