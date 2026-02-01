from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

from dev_health_ops.api.services.oauth import (
    OAuthConfig,
    OAuthAuthorizationRequest,
    OAuthProviderType,
    OAuthTokenError,
    GitHubOAuthProvider,
    GitLabOAuthProvider,
    GoogleOAuthProvider,
    create_oauth_provider,
    get_default_scopes,
    validate_oauth_config,
)


class TestOAuthConfig:
    def test_oauth_config_creation(self):
        config = OAuthConfig(
            client_id="test-client-id",
            client_secret="test-secret",
            redirect_uri="http://localhost/callback",
            scopes=["read:user", "user:email"],
        )
        assert config.client_id == "test-client-id"
        assert config.client_secret == "test-secret"
        assert config.redirect_uri == "http://localhost/callback"
        assert config.scopes == ["read:user", "user:email"]

    def test_oauth_config_with_custom_urls(self):
        config = OAuthConfig(
            client_id="test-client-id",
            client_secret="test-secret",
            redirect_uri="http://localhost/callback",
            scopes=["read:user"],
            authorization_url="https://custom.auth/authorize",
            token_url="https://custom.auth/token",
            userinfo_url="https://custom.auth/userinfo",
        )
        assert config.authorization_url == "https://custom.auth/authorize"
        assert config.token_url == "https://custom.auth/token"
        assert config.userinfo_url == "https://custom.auth/userinfo"


class TestGitHubOAuthProvider:
    @pytest.fixture
    def github_config(self):
        return OAuthConfig(
            client_id="github-client-id",
            client_secret="github-secret",
            redirect_uri="http://localhost/callback",
            scopes=["read:user", "user:email"],
        )

    @pytest.fixture
    def github_provider(self, github_config):
        return GitHubOAuthProvider(github_config)

    def test_default_urls(self, github_provider):
        assert (
            github_provider.default_authorization_url
            == "https://github.com/login/oauth/authorize"
        )
        assert (
            github_provider.default_token_url
            == "https://github.com/login/oauth/access_token"
        )
        assert github_provider.default_userinfo_url == "https://api.github.com/user"

    def test_generate_authorization_request(self, github_provider):
        auth_request = github_provider.generate_authorization_request()

        assert isinstance(auth_request, OAuthAuthorizationRequest)
        assert auth_request.state is not None
        assert len(auth_request.state) > 20
        assert "github.com/login/oauth/authorize" in auth_request.authorization_url
        assert "client_id=github-client-id" in auth_request.authorization_url
        assert "scope=read%3Auser+user%3Aemail" in auth_request.authorization_url

    def test_generate_authorization_request_with_custom_state(self, github_provider):
        auth_request = github_provider.generate_authorization_request(
            state="custom-state-123"
        )

        assert auth_request.state == "custom-state-123"
        assert "state=custom-state-123" in auth_request.authorization_url

    @pytest.mark.asyncio
    async def test_exchange_code_for_token_success(self, github_provider):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "gho_test_token",
            "token_type": "bearer",
            "scope": "read:user,user:email",
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            token_response = await github_provider.exchange_code_for_token(
                code="test-auth-code",
                state="test-state",
            )

            assert token_response.access_token == "gho_test_token"
            assert token_response.token_type == "bearer"
            assert token_response.scope == "read:user,user:email"

    @pytest.mark.asyncio
    async def test_exchange_code_for_token_failure(self, github_provider):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Bad Request", request=MagicMock(), response=mock_response
        )

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            with pytest.raises(OAuthTokenError):
                await github_provider.exchange_code_for_token(code="invalid-code")

    @pytest.mark.asyncio
    async def test_fetch_user_info_success(self, github_provider):
        mock_user_response = MagicMock()
        mock_user_response.json.return_value = {
            "id": 12345,
            "login": "testuser",
            "name": "Test User",
            "email": "test@example.com",
            "avatar_url": "https://avatars.githubusercontent.com/u/12345",
        }
        mock_user_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(return_value=mock_user_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            user_info = await github_provider.fetch_user_info("gho_test_token")

            assert user_info.provider == "github"
            assert user_info.provider_user_id == "12345"
            assert user_info.email == "test@example.com"
            assert user_info.username == "testuser"
            assert user_info.full_name == "Test User"
            assert (
                user_info.avatar_url == "https://avatars.githubusercontent.com/u/12345"
            )

    @pytest.mark.asyncio
    async def test_fetch_user_info_with_private_email(self, github_provider):
        mock_user_response = MagicMock()
        mock_user_response.json.return_value = {
            "id": 12345,
            "login": "testuser",
            "name": "Test User",
            "email": None,
            "avatar_url": "https://avatars.githubusercontent.com/u/12345",
        }
        mock_user_response.raise_for_status = MagicMock()

        mock_emails_response = MagicMock()
        mock_emails_response.json.return_value = [
            {"email": "secondary@example.com", "primary": False, "verified": True},
            {"email": "primary@example.com", "primary": True, "verified": True},
        ]
        mock_emails_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(
                side_effect=[mock_user_response, mock_emails_response]
            )
            mock_client.return_value.__aenter__.return_value = mock_instance

            user_info = await github_provider.fetch_user_info("gho_test_token")

            assert user_info.email == "primary@example.com"


class TestGitLabOAuthProvider:
    @pytest.fixture
    def gitlab_config(self):
        return OAuthConfig(
            client_id="gitlab-client-id",
            client_secret="gitlab-secret",
            redirect_uri="http://localhost/callback",
            scopes=["read_user", "email"],
        )

    @pytest.fixture
    def gitlab_provider(self, gitlab_config):
        return GitLabOAuthProvider(gitlab_config)

    @pytest.fixture
    def self_hosted_gitlab_provider(self, gitlab_config):
        return GitLabOAuthProvider(
            gitlab_config, base_url="https://gitlab.mycompany.com"
        )

    def test_default_urls(self, gitlab_provider):
        assert (
            gitlab_provider.default_authorization_url
            == "https://gitlab.com/oauth/authorize"
        )
        assert gitlab_provider.default_token_url == "https://gitlab.com/oauth/token"
        assert gitlab_provider.default_userinfo_url == "https://gitlab.com/api/v4/user"

    def test_self_hosted_urls(self, self_hosted_gitlab_provider):
        assert (
            self_hosted_gitlab_provider.default_authorization_url
            == "https://gitlab.mycompany.com/oauth/authorize"
        )
        assert (
            self_hosted_gitlab_provider.default_token_url
            == "https://gitlab.mycompany.com/oauth/token"
        )
        assert (
            self_hosted_gitlab_provider.default_userinfo_url
            == "https://gitlab.mycompany.com/api/v4/user"
        )

    @pytest.mark.asyncio
    async def test_fetch_user_info_success(self, gitlab_provider):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": 67890,
            "username": "gitlabuser",
            "name": "GitLab User",
            "email": "gitlab@example.com",
            "avatar_url": "https://gitlab.com/uploads/-/system/user/avatar/67890/avatar.png",
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            user_info = await gitlab_provider.fetch_user_info("glpat_test_token")

            assert user_info.provider == "gitlab"
            assert user_info.provider_user_id == "67890"
            assert user_info.email == "gitlab@example.com"
            assert user_info.username == "gitlabuser"


class TestGoogleOAuthProvider:
    @pytest.fixture
    def google_config(self):
        return OAuthConfig(
            client_id="google-client-id.apps.googleusercontent.com",
            client_secret="google-secret",
            redirect_uri="http://localhost/callback",
            scopes=["openid", "email", "profile"],
        )

    @pytest.fixture
    def google_provider(self, google_config):
        return GoogleOAuthProvider(google_config)

    def test_default_urls(self, google_provider):
        assert (
            google_provider.default_authorization_url
            == "https://accounts.google.com/o/oauth2/v2/auth"
        )
        assert (
            google_provider.default_token_url == "https://oauth2.googleapis.com/token"
        )
        assert (
            google_provider.default_userinfo_url
            == "https://www.googleapis.com/oauth2/v2/userinfo"
        )

    def test_generate_authorization_request_includes_google_params(
        self, google_provider
    ):
        auth_request = google_provider.generate_authorization_request()

        assert "access_type=offline" in auth_request.authorization_url
        assert "prompt=consent" in auth_request.authorization_url

    @pytest.mark.asyncio
    async def test_fetch_user_info_success(self, google_provider):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "google-user-id-123",
            "email": "google@example.com",
            "name": "Google User",
            "picture": "https://lh3.googleusercontent.com/a/photo",
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            user_info = await google_provider.fetch_user_info("ya29.test_token")

            assert user_info.provider == "google"
            assert user_info.provider_user_id == "google-user-id-123"
            assert user_info.email == "google@example.com"
            assert user_info.username is None
            assert user_info.full_name == "Google User"
            assert user_info.avatar_url == "https://lh3.googleusercontent.com/a/photo"


class TestCreateOAuthProvider:
    def test_create_github_provider(self):
        config = OAuthConfig(
            client_id="test",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            scopes=["read:user"],
        )
        provider = create_oauth_provider("github", config)
        assert isinstance(provider, GitHubOAuthProvider)

    def test_create_gitlab_provider(self):
        config = OAuthConfig(
            client_id="test",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            scopes=["read_user"],
        )
        provider = create_oauth_provider("gitlab", config)
        assert isinstance(provider, GitLabOAuthProvider)

    def test_create_gitlab_provider_with_base_url(self):
        config = OAuthConfig(
            client_id="test",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            scopes=["read_user"],
        )
        provider = create_oauth_provider(
            "gitlab", config, base_url="https://gitlab.mycompany.com"
        )
        assert isinstance(provider, GitLabOAuthProvider)
        assert provider.base_url == "https://gitlab.mycompany.com"

    def test_create_google_provider(self):
        config = OAuthConfig(
            client_id="test.apps.googleusercontent.com",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            scopes=["openid", "email"],
        )
        provider = create_oauth_provider("google", config)
        assert isinstance(provider, GoogleOAuthProvider)

    def test_create_provider_with_enum(self):
        config = OAuthConfig(
            client_id="test",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            scopes=["read:user"],
        )
        provider = create_oauth_provider(OAuthProviderType.GITHUB, config)
        assert isinstance(provider, GitHubOAuthProvider)

    def test_create_invalid_provider(self):
        config = OAuthConfig(
            client_id="test",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            scopes=[],
        )
        with pytest.raises(ValueError, match="is not a valid OAuthProviderType"):
            create_oauth_provider("invalid", config)


class TestGetDefaultScopes:
    def test_github_default_scopes(self):
        scopes = get_default_scopes("github")
        assert "read:user" in scopes
        assert "user:email" in scopes

    def test_gitlab_default_scopes(self):
        scopes = get_default_scopes("gitlab")
        assert "read_user" in scopes
        assert "email" in scopes

    def test_google_default_scopes(self):
        scopes = get_default_scopes("google")
        assert "openid" in scopes
        assert "email" in scopes
        assert "profile" in scopes

    def test_default_scopes_with_enum(self):
        scopes = get_default_scopes(OAuthProviderType.GITHUB)
        assert "read:user" in scopes


class TestValidateOAuthConfig:
    def test_valid_github_config(self):
        result = validate_oauth_config(
            provider_type="github",
            client_id="Iv1.abc123",
            client_secret="secret123",
            scopes=["read:user", "user:email"],
        )
        assert result.valid is True
        assert len(result.errors) == 0

    def test_valid_gitlab_config(self):
        result = validate_oauth_config(
            provider_type="gitlab",
            client_id="gitlab-app-id",
            client_secret="secret123",
            scopes=["read_user", "email"],
        )
        assert result.valid is True
        assert len(result.errors) == 0

    def test_valid_google_config(self):
        result = validate_oauth_config(
            provider_type="google",
            client_id="123456789.apps.googleusercontent.com",
            client_secret="secret123",
            scopes=["openid", "email", "profile"],
        )
        assert result.valid is True
        assert len(result.errors) == 0

    def test_invalid_provider_type(self):
        result = validate_oauth_config(
            provider_type="invalid",
            client_id="test",
            client_secret="secret",
        )
        assert result.valid is False
        assert any("Invalid provider type" in e for e in result.errors)

    def test_missing_client_id(self):
        result = validate_oauth_config(
            provider_type="github",
            client_id="",
            client_secret="secret",
        )
        assert result.valid is False
        assert any("client_id is required" in e for e in result.errors)

    def test_missing_client_secret(self):
        result = validate_oauth_config(
            provider_type="github",
            client_id="test-id",
            client_secret="",
        )
        assert result.valid is False
        assert any("client_secret is required" in e for e in result.errors)

    def test_invalid_gitlab_base_url(self):
        result = validate_oauth_config(
            provider_type="gitlab",
            client_id="test-id",
            client_secret="secret",
            base_url="not-a-url",
        )
        assert result.valid is False
        assert any("base_url must start with http" in e for e in result.errors)

    def test_invalid_google_client_id(self):
        result = validate_oauth_config(
            provider_type="google",
            client_id="invalid-client-id",
            client_secret="secret",
        )
        assert result.valid is False
        assert any("apps.googleusercontent.com" in e for e in result.errors)

    def test_github_missing_required_scope(self):
        result = validate_oauth_config(
            provider_type="github",
            client_id="test-id",
            client_secret="secret",
            scopes=["repo"],
        )
        assert result.valid is False
        assert any("read:user" in e or "user:email" in e for e in result.errors)

    def test_gitlab_missing_required_scope(self):
        result = validate_oauth_config(
            provider_type="gitlab",
            client_id="test-id",
            client_secret="secret",
            scopes=["api"],
        )
        assert result.valid is False
        assert any("read_user" in e for e in result.errors)

    def test_google_missing_required_scope(self):
        result = validate_oauth_config(
            provider_type="google",
            client_id="test.apps.googleusercontent.com",
            client_secret="secret",
            scopes=["openid", "profile"],
        )
        assert result.valid is False
        assert any("email" in e for e in result.errors)
