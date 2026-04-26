"""Credential type definitions for provider authentication.

Defines typed dataclasses for each supported provider's credentials,
ensuring type safety and clear documentation of required fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CredentialSource(str, Enum):
    """Source of the resolved credentials."""

    DATABASE = "database"
    ENVIRONMENT = "environment"


@dataclass(kw_only=True)
class ProviderCredentials:
    """Base class for provider credentials.

    All provider-specific credential classes inherit from this.
    """

    provider: str
    source: CredentialSource
    credential_name: str = "default"
    extra: dict[str, Any] = field(default_factory=dict)

    def is_from_db(self) -> bool:
        """Check if credentials came from database (enterprise mode)."""
        return self.source == CredentialSource.DATABASE

    def is_from_env(self) -> bool:
        """Check if credentials came from environment (dev/OSS mode)."""
        return self.source == CredentialSource.ENVIRONMENT


@dataclass
class GitHubCredentials(ProviderCredentials):
    """GitHub provider credentials.

    Supports:
    - Personal Access Token (token field)
    - GitHub App authentication (app_id + private_key + installation_id)
    """

    provider: str = "github"
    token: str | None = None

    app_id: str | None = None
    private_key: str | None = None
    private_key_path: str | None = None
    installation_id: str | None = None
    base_url: str | None = None

    def __post_init__(self) -> None:
        has_token = bool(self.token)
        has_app_fields = bool(self.app_id or self.private_key or self.installation_id)
        has_complete_app = bool(
            self.app_id and self.private_key and self.installation_id
        )

        if has_token and has_app_fields:
            raise ValueError(
                "GitHub credentials require exactly one auth mode: token or GitHub App"
            )
        if not has_token and not has_complete_app:
            raise ValueError(
                "GitHub credentials require either 'token' or 'app_id' + 'private_key' + 'installation_id'"
            )

    @property
    def is_app_auth(self) -> bool:
        """Check if using GitHub App authentication."""
        return bool(self.app_id and self.private_key and self.installation_id)


@dataclass
class GitLabCredentials(ProviderCredentials):
    """GitLab provider credentials.

    Supports personal access token authentication.
    """

    provider: str = "gitlab"
    token: str = ""

    base_url: str = "https://gitlab.com"

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("GitLab credentials require 'token'")


@dataclass
class JiraCredentials(ProviderCredentials):
    """Jira/Atlassian provider credentials.

    Supports API token authentication with email.
    """

    provider: str = "jira"
    api_token: str = ""
    email: str = ""
    base_url: str = ""

    def __post_init__(self) -> None:
        if not self.api_token:
            raise ValueError("Jira credentials require 'api_token'")
        if not self.email:
            raise ValueError("Jira credentials require 'email'")
        if not self.base_url:
            raise ValueError("Jira credentials require 'base_url'")


@dataclass
class LinearCredentials(ProviderCredentials):
    """Linear provider credentials.

    Supports API key authentication.
    """

    provider: str = "linear"
    api_key: str = ""

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("Linear credentials require 'api_key'")


@dataclass
class AtlassianCredentials(ProviderCredentials):
    """Atlassian Cloud credentials (GraphQL Gateway).

    Used for Atlassian GraphQL API access.
    """

    provider: str = "atlassian"
    api_token: str = ""
    email: str = ""
    cloud_id: str | None = None

    def __post_init__(self) -> None:
        if not self.api_token:
            raise ValueError("Atlassian credentials require 'api_token'")
        if not self.email:
            raise ValueError("Atlassian credentials require 'email'")


@dataclass
class LaunchDarklyCredentials(ProviderCredentials):
    """LaunchDarkly provider credentials.

    Supports API key authentication with optional project/environment scoping.
    """

    provider: str = "launchdarkly"
    api_key: str = ""
    project_key: str | None = None
    environment: str | None = None

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("LaunchDarkly credentials require 'api_key'")


@dataclass
class TelemetryCredentials(ProviderCredentials):
    """Telemetry provider credentials.

    Supports API key authentication with optional schema version.
    """

    provider: str = "telemetry"
    api_key: str = ""
    schema_version: str | None = None

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("Telemetry credentials require 'api_key'")
