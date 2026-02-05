"""Tests for the credential resolver module.

Covers:
- Database credential resolution (mocked IntegrationCredentialsService)
- Environment variable fallback
- Error handling when no credentials available
- All provider credential types (GitHub, GitLab, Jira, Linear, Atlassian)
- CredentialResolutionError messages
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.credentials import (
    AtlassianCredentials,
    CredentialResolutionError,
    CredentialResolver,
    CredentialSource,
    GitHubCredentials,
    GitLabCredentials,
    JiraCredentials,
    LinearCredentials,
    resolve_credentials_sync,
)


# ---------------------------------------------------------------------------
# Credential Type Validation Tests
# ---------------------------------------------------------------------------


class TestGitHubCredentials:
    def test_valid_with_token(self):
        creds = GitHubCredentials(
            token="ghp_test_token",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.token == "ghp_test_token"
        assert creds.is_app_auth is False
        assert creds.is_from_db() is True

    def test_valid_with_app_auth(self):
        creds = GitHubCredentials(
            app_id="12345",
            private_key="-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----",
            installation_id="67890",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.is_app_auth is True
        assert creds.token is None

    def test_invalid_missing_token_and_app(self):
        with pytest.raises(ValueError, match="require either 'token' or 'app_id'"):
            GitHubCredentials(
                source=CredentialSource.DATABASE,
                credential_name="default",
            )

    def test_base_url_optional(self):
        creds = GitHubCredentials(
            token="ghp_test",
            base_url="https://github.example.com",
            source=CredentialSource.ENVIRONMENT,
            credential_name="default",
        )
        assert creds.base_url == "https://github.example.com"
        assert creds.is_from_env() is True


class TestGitLabCredentials:
    def test_valid_credentials(self):
        creds = GitLabCredentials(
            token="glpat-test-token",
            base_url="https://gitlab.example.com",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.token == "glpat-test-token"
        assert creds.base_url == "https://gitlab.example.com"

    def test_default_base_url(self):
        creds = GitLabCredentials(
            token="glpat-test",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.base_url == "https://gitlab.com"

    def test_invalid_missing_token(self):
        with pytest.raises(ValueError, match="require 'token'"):
            GitLabCredentials(
                source=CredentialSource.DATABASE,
                credential_name="default",
            )


class TestJiraCredentials:
    def test_valid_credentials(self):
        creds = JiraCredentials(
            api_token="test-api-token",
            email="user@example.com",
            base_url="https://company.atlassian.net",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.api_token == "test-api-token"
        assert creds.email == "user@example.com"
        assert creds.base_url == "https://company.atlassian.net"

    def test_invalid_missing_api_token(self):
        with pytest.raises(ValueError, match="require 'api_token'"):
            JiraCredentials(
                email="user@example.com",
                base_url="https://company.atlassian.net",
                source=CredentialSource.DATABASE,
                credential_name="default",
            )

    def test_invalid_missing_email(self):
        with pytest.raises(ValueError, match="require 'email'"):
            JiraCredentials(
                api_token="test-token",
                base_url="https://company.atlassian.net",
                source=CredentialSource.DATABASE,
                credential_name="default",
            )

    def test_invalid_missing_base_url(self):
        with pytest.raises(ValueError, match="require 'base_url'"):
            JiraCredentials(
                api_token="test-token",
                email="user@example.com",
                source=CredentialSource.DATABASE,
                credential_name="default",
            )


class TestLinearCredentials:
    def test_valid_credentials(self):
        creds = LinearCredentials(
            api_key="lin_api_test_key",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.api_key == "lin_api_test_key"

    def test_invalid_missing_api_key(self):
        with pytest.raises(ValueError, match="require 'api_key'"):
            LinearCredentials(
                source=CredentialSource.DATABASE,
                credential_name="default",
            )


class TestAtlassianCredentials:
    def test_valid_credentials(self):
        creds = AtlassianCredentials(
            api_token="atlassian-api-token",
            email="user@example.com",
            cloud_id="abc123-cloud-id",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.api_token == "atlassian-api-token"
        assert creds.email == "user@example.com"
        assert creds.cloud_id == "abc123-cloud-id"

    def test_cloud_id_optional(self):
        creds = AtlassianCredentials(
            api_token="atlassian-api-token",
            email="user@example.com",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.cloud_id is None

    def test_invalid_missing_api_token(self):
        with pytest.raises(ValueError, match="require 'api_token'"):
            AtlassianCredentials(
                email="user@example.com",
                source=CredentialSource.DATABASE,
                credential_name="default",
            )

    def test_invalid_missing_email(self):
        with pytest.raises(ValueError, match="require 'email'"):
            AtlassianCredentials(
                api_token="test-token",
                source=CredentialSource.DATABASE,
                credential_name="default",
            )


# ---------------------------------------------------------------------------
# CredentialResolver Async Tests
# ---------------------------------------------------------------------------


class TestCredentialResolver:
    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock()

    @pytest.fixture
    def resolver(self, mock_session):
        """Create a resolver with mocked session."""
        return CredentialResolver(
            session=mock_session,
            org_id="test-org",
            allow_env_fallback=True,
        )

    @pytest.mark.asyncio
    async def test_resolve_github_from_database(self, resolver):
        """Test resolving GitHub credentials from database."""
        mock_creds = {
            "token": "ghp_database_token",
            "base_url": "https://github.example.com",
        }

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("github")

            assert isinstance(result, GitHubCredentials)
            assert result.token == "ghp_database_token"
            assert result.base_url == "https://github.example.com"
            assert result.source == CredentialSource.DATABASE
            mock_svc.get_decrypted_credentials.assert_called_once_with(
                "github", "default"
            )

    @pytest.mark.asyncio
    async def test_resolve_gitlab_from_database(self, resolver):
        """Test resolving GitLab credentials from database."""
        mock_creds = {
            "token": "glpat-database-token",
            "base_url": "https://gitlab.example.com",
        }

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("gitlab")

            assert isinstance(result, GitLabCredentials)
            assert result.token == "glpat-database-token"
            assert result.source == CredentialSource.DATABASE

    @pytest.mark.asyncio
    async def test_resolve_jira_from_database(self, resolver):
        """Test resolving Jira credentials from database."""
        mock_creds = {
            "api_token": "jira-api-token",
            "email": "user@example.com",
            "base_url": "https://company.atlassian.net",
        }

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("jira")

            assert isinstance(result, JiraCredentials)
            assert result.api_token == "jira-api-token"
            assert result.email == "user@example.com"
            assert result.source == CredentialSource.DATABASE

    @pytest.mark.asyncio
    async def test_resolve_linear_from_database(self, resolver):
        """Test resolving Linear credentials from database."""
        mock_creds = {"api_key": "lin_api_database_key"}

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("linear")

            assert isinstance(result, LinearCredentials)
            assert result.api_key == "lin_api_database_key"
            assert result.source == CredentialSource.DATABASE

    @pytest.mark.asyncio
    async def test_resolve_atlassian_from_database(self, resolver):
        """Test resolving Atlassian credentials from database."""
        mock_creds = {
            "api_token": "atlassian-token",
            "email": "user@example.com",
            "cloud_id": "cloud-123",
        }

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("atlassian")

            assert isinstance(result, AtlassianCredentials)
            assert result.api_token == "atlassian-token"
            assert result.cloud_id == "cloud-123"
            assert result.source == CredentialSource.DATABASE

    @pytest.mark.asyncio
    async def test_fallback_to_environment(self, resolver):
        """Test fallback to environment when database returns None."""
        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=None)
            mock_svc_class.return_value = mock_svc

            with patch.dict(os.environ, {"GITHUB_TOKEN": "ghp_env_token"}, clear=False):
                result = await resolver.resolve("github")

                assert isinstance(result, GitHubCredentials)
                assert result.token == "ghp_env_token"
                assert result.source == CredentialSource.ENVIRONMENT

    @pytest.mark.asyncio
    async def test_fallback_disabled(self, mock_session):
        """Test error when env fallback is disabled and DB has no creds."""
        resolver = CredentialResolver(
            session=mock_session,
            org_id="test-org",
            allow_env_fallback=False,
        )

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=None)
            mock_svc_class.return_value = mock_svc

            with pytest.raises(CredentialResolutionError) as exc_info:
                await resolver.resolve("github")

            assert exc_info.value.provider == "github"
            assert "No github credentials found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_no_credentials_found(self, resolver):
        """Test error when neither DB nor env has credentials."""
        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=None)
            mock_svc_class.return_value = mock_svc

            env_patch = {
                "GITHUB_TOKEN": "",
                "GITHUB_URL": "",
            }
            with patch.dict(os.environ, env_patch, clear=False):
                os.environ.pop("GITHUB_TOKEN", None)
                os.environ.pop("GITHUB_URL", None)

                with pytest.raises(CredentialResolutionError) as exc_info:
                    await resolver.resolve("github")

                assert exc_info.value.provider == "github"
                assert exc_info.value.org_id == "test-org"
                assert "GITHUB_TOKEN" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_unknown_provider(self, resolver):
        """Test error for unknown provider."""
        with pytest.raises(CredentialResolutionError) as exc_info:
            await resolver.resolve("unknown_provider")

        assert exc_info.value.provider == "unknown_provider"
        assert "Unknown provider" in str(exc_info.value)
        assert "github" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_custom_credential_name(self, resolver):
        """Test resolving with custom credential name."""
        mock_creds = {"token": "ghp_custom_token"}

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("github", credential_name="production")

            assert result.credential_name == "production"
            mock_svc.get_decrypted_credentials.assert_called_once_with(
                "github", "production"
            )

    @pytest.mark.asyncio
    async def test_database_error_falls_back_to_env(self, resolver):
        """Test that database errors gracefully fall back to env."""
        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(
                side_effect=Exception("DB connection failed")
            )
            mock_svc_class.return_value = mock_svc

            with patch.dict(
                os.environ, {"GITHUB_TOKEN": "ghp_fallback_token"}, clear=False
            ):
                result = await resolver.resolve("github")

                assert isinstance(result, GitHubCredentials)
                assert result.token == "ghp_fallback_token"
                assert result.source == CredentialSource.ENVIRONMENT

    @pytest.mark.asyncio
    async def test_case_insensitive_provider(self, resolver):
        """Test that provider names are case-insensitive."""
        mock_creds = {"token": "ghp_test"}

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("GITHUB")

            assert isinstance(result, GitHubCredentials)

    @pytest.mark.asyncio
    async def test_github_app_auth_from_database(self, resolver):
        """Test resolving GitHub App credentials from database."""
        mock_creds = {
            "app_id": "12345",
            "private_key": "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----",
            "installation_id": "67890",
        }

        with patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService"
        ) as mock_svc_class:
            mock_svc = AsyncMock()
            mock_svc.get_decrypted_credentials = AsyncMock(return_value=mock_creds)
            mock_svc_class.return_value = mock_svc

            result = await resolver.resolve("github")

            assert isinstance(result, GitHubCredentials)
            assert result.is_app_auth is True
            assert result.app_id == "12345"
            assert result.installation_id == "67890"


# ---------------------------------------------------------------------------
# resolve_credentials_sync Tests
# ---------------------------------------------------------------------------


class TestResolveCredentialsSync:
    def test_env_only_when_no_db_url(self):
        """Test env-only resolution when no DATABASE_URI configured."""
        env_patch = {
            "DATABASE_URI": "",
            "DATABASE_URL": "",
            "GITHUB_TOKEN": "ghp_env_only_token",
        }

        with patch.dict(os.environ, env_patch, clear=False):
            os.environ.pop("DATABASE_URI", None)
            os.environ.pop("DATABASE_URL", None)

            result = resolve_credentials_sync("github")

            assert isinstance(result, GitHubCredentials)
            assert result.token == "ghp_env_only_token"
            assert result.source == CredentialSource.ENVIRONMENT

    def test_error_no_db_and_no_env(self):
        """Test error when no DB URL and no env credentials."""
        env_patch = {
            "DATABASE_URI": "",
            "DATABASE_URL": "",
            "GITHUB_TOKEN": "",
        }

        with patch.dict(os.environ, env_patch, clear=False):
            os.environ.pop("DATABASE_URI", None)
            os.environ.pop("DATABASE_URL", None)
            os.environ.pop("GITHUB_TOKEN", None)

            with pytest.raises(CredentialResolutionError) as exc_info:
                resolve_credentials_sync("github")

            assert "No database URL configured" in str(exc_info.value)
            assert "DATABASE_URI" in str(exc_info.value)

    def test_env_only_jira_requires_all_fields(self):
        """Test that Jira env-only resolution requires all fields."""
        env_patch = {
            "DATABASE_URI": "",
            "JIRA_API_TOKEN": "jira-token",
            "JIRA_EMAIL": "user@example.com",
        }

        with patch.dict(os.environ, env_patch, clear=False):
            os.environ.pop("DATABASE_URI", None)
            os.environ.pop("DATABASE_URL", None)
            os.environ.pop("JIRA_BASE_URL", None)

            with pytest.raises(CredentialResolutionError):
                resolve_credentials_sync("jira")

    def test_env_only_jira_with_all_fields(self):
        """Test successful Jira env-only resolution."""
        env_patch = {
            "DATABASE_URI": "",
            "JIRA_API_TOKEN": "jira-token",
            "JIRA_EMAIL": "user@example.com",
            "JIRA_BASE_URL": "https://company.atlassian.net",
        }

        with patch.dict(os.environ, env_patch, clear=False):
            os.environ.pop("DATABASE_URI", None)
            os.environ.pop("DATABASE_URL", None)

            result = resolve_credentials_sync("jira")

            assert isinstance(result, JiraCredentials)
            assert result.api_token == "jira-token"
            assert result.email == "user@example.com"
            assert result.source == CredentialSource.ENVIRONMENT

    def test_respects_allow_env_fallback_false(self):
        """Test that allow_env_fallback=False is respected."""
        env_patch = {
            "DATABASE_URI": "",
            "GITHUB_TOKEN": "ghp_should_not_use",
        }

        with patch.dict(os.environ, env_patch, clear=False):
            os.environ.pop("DATABASE_URI", None)
            os.environ.pop("DATABASE_URL", None)

            with pytest.raises(CredentialResolutionError):
                resolve_credentials_sync("github", allow_env_fallback=False)


# ---------------------------------------------------------------------------
# CredentialResolutionError Tests
# ---------------------------------------------------------------------------


class TestCredentialResolutionError:
    def test_error_contains_provider_info(self):
        error = CredentialResolutionError(
            provider="github",
            message="Test error message",
            org_id="my-org",
            credential_name="production",
        )

        assert error.provider == "github"
        assert error.org_id == "my-org"
        assert error.credential_name == "production"
        assert "Test error message" in str(error)

    def test_error_default_values(self):
        error = CredentialResolutionError(
            provider="gitlab",
            message="Test",
        )

        assert error.org_id == "default"
        assert error.credential_name == "default"


# ---------------------------------------------------------------------------
# ProviderCredentials Base Class Tests
# ---------------------------------------------------------------------------


class TestProviderCredentials:
    def test_is_from_db(self):
        creds = GitHubCredentials(
            token="test",
            source=CredentialSource.DATABASE,
            credential_name="default",
        )
        assert creds.is_from_db() is True
        assert creds.is_from_env() is False

    def test_is_from_env(self):
        creds = GitHubCredentials(
            token="test",
            source=CredentialSource.ENVIRONMENT,
            credential_name="default",
        )
        assert creds.is_from_db() is False
        assert creds.is_from_env() is True

    def test_extra_field(self):
        creds = GitHubCredentials(
            token="test",
            source=CredentialSource.DATABASE,
            credential_name="default",
            extra={"custom_field": "custom_value"},
        )
        assert creds.extra["custom_field"] == "custom_value"
