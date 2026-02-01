"""OAuth2 provider implementations for GitHub, GitLab, and Google.

This module provides OAuth2 authentication flows using httpx for HTTP calls.
Each provider implements the standard OAuth2 authorization code flow with
provider-specific user profile fetching.
"""

from __future__ import annotations

import logging
import secrets
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlencode

import httpx

logger = logging.getLogger(__name__)


class OAuthProviderType(str, Enum):
    """Supported OAuth provider types."""

    GITHUB = "github"
    GITLAB = "gitlab"
    GOOGLE = "google"


@dataclass
class OAuthConfig:
    """OAuth provider configuration."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: list[str]
    # Optional overrides for self-hosted instances
    authorization_url: Optional[str] = None
    token_url: Optional[str] = None
    userinfo_url: Optional[str] = None


@dataclass
class OAuthUserInfo:
    """Normalized user information from OAuth provider."""

    provider: str
    provider_user_id: str
    email: str
    username: Optional[str] = None
    full_name: Optional[str] = None
    avatar_url: Optional[str] = None
    raw_data: Optional[dict[str, Any]] = None


@dataclass
class OAuthTokenResponse:
    """OAuth token exchange response."""

    access_token: str
    token_type: str
    scope: Optional[str] = None
    refresh_token: Optional[str] = None
    expires_in: Optional[int] = None


@dataclass
class OAuthAuthorizationRequest:
    """OAuth authorization request data."""

    authorization_url: str
    state: str


class OAuthProviderError(Exception):
    """Base exception for OAuth provider errors."""

    pass


class OAuthTokenError(OAuthProviderError):
    """Error during token exchange."""

    pass


class OAuthUserInfoError(OAuthProviderError):
    """Error fetching user info."""

    pass


class OAuthProvider(ABC):
    """Abstract base class for OAuth providers."""

    provider_type: OAuthProviderType

    def __init__(self, config: OAuthConfig):
        self.config = config

    @property
    @abstractmethod
    def default_authorization_url(self) -> str:
        """Default authorization endpoint URL."""
        pass

    @property
    @abstractmethod
    def default_token_url(self) -> str:
        """Default token endpoint URL."""
        pass

    @property
    @abstractmethod
    def default_userinfo_url(self) -> str:
        """Default user info endpoint URL."""
        pass

    @property
    def authorization_url(self) -> str:
        """Get authorization URL (custom or default)."""
        return self.config.authorization_url or self.default_authorization_url

    @property
    def token_url(self) -> str:
        """Get token URL (custom or default)."""
        return self.config.token_url or self.default_token_url

    @property
    def userinfo_url(self) -> str:
        """Get user info URL (custom or default)."""
        return self.config.userinfo_url or self.default_userinfo_url

    def generate_authorization_request(
        self,
        state: Optional[str] = None,
        extra_params: Optional[dict[str, str]] = None,
    ) -> OAuthAuthorizationRequest:
        """Generate OAuth authorization URL with state parameter."""
        if state is None:
            state = secrets.token_urlsafe(32)

        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "scope": " ".join(self.config.scopes),
            "state": state,
            "response_type": "code",
        }

        if extra_params:
            params.update(extra_params)

        url = f"{self.authorization_url}?{urlencode(params)}"

        return OAuthAuthorizationRequest(
            authorization_url=url,
            state=state,
        )

    async def exchange_code_for_token(
        self,
        code: str,
        state: Optional[str] = None,
    ) -> OAuthTokenResponse:
        """Exchange authorization code for access token."""
        data = {
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "code": code,
            "redirect_uri": self.config.redirect_uri,
            "grant_type": "authorization_code",
        }

        headers = self._get_token_request_headers()

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.token_url,
                    data=data,
                    headers=headers,
                    timeout=30.0,
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.error(
                    "Token exchange failed for %s: %s - %s",
                    self.provider_type.value,
                    e.response.status_code,
                    e.response.text,
                )
                raise OAuthTokenError(
                    f"Token exchange failed: {e.response.status_code}"
                ) from e
            except httpx.RequestError as e:
                logger.error(
                    "Token exchange request failed for %s: %s",
                    self.provider_type.value,
                    str(e),
                )
                raise OAuthTokenError(f"Token exchange request failed: {e}") from e

            return self._parse_token_response(response)

    def _get_token_request_headers(self) -> dict[str, str]:
        """Get headers for token request. Override in subclasses if needed."""
        return {"Accept": "application/json"}

    def _parse_token_response(self, response: httpx.Response) -> OAuthTokenResponse:
        """Parse token response. Override in subclasses if needed."""
        try:
            data = response.json()
            return OAuthTokenResponse(
                access_token=data["access_token"],
                token_type=data.get("token_type", "bearer"),
                scope=data.get("scope"),
                refresh_token=data.get("refresh_token"),
                expires_in=data.get("expires_in"),
            )
        except (KeyError, TypeError) as e:
            logger.error(
                "Token response missing required fields or malformed: %s",
                response.text,
            )
            raise OAuthTokenError(
                "Token response missing required field 'access_token'"
            ) from e

    @abstractmethod
    async def fetch_user_info(self, access_token: str) -> OAuthUserInfo:
        """Fetch user information from the provider."""
        pass


class GitHubOAuthProvider(OAuthProvider):
    """GitHub OAuth2 provider implementation."""

    provider_type = OAuthProviderType.GITHUB

    @property
    def default_authorization_url(self) -> str:
        return "https://github.com/login/oauth/authorize"

    @property
    def default_token_url(self) -> str:
        return "https://github.com/login/oauth/access_token"

    @property
    def default_userinfo_url(self) -> str:
        return "https://api.github.com/user"

    async def fetch_user_info(self, access_token: str) -> OAuthUserInfo:
        """Fetch user info from GitHub /user API."""
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        async with httpx.AsyncClient() as client:
            try:
                # Fetch user profile
                response = await client.get(
                    self.userinfo_url,
                    headers=headers,
                    timeout=30.0,
                )
                response.raise_for_status()
                user_data = response.json()

                # GitHub may not return email in user profile if it's private
                # Need to fetch from /user/emails endpoint
                email = user_data.get("email")
                if not email:
                    email = await self._fetch_primary_email(client, headers)

            except httpx.HTTPStatusError as e:
                logger.error(
                    "GitHub user info fetch failed: %s - %s",
                    e.response.status_code,
                    e.response.text,
                )
                raise OAuthUserInfoError(
                    f"Failed to fetch user info: {e.response.status_code}"
                ) from e
            except httpx.RequestError as e:
                logger.error("GitHub user info request failed: %s", str(e))
                raise OAuthUserInfoError(f"User info request failed: {e}") from e

        return OAuthUserInfo(
            provider=self.provider_type.value,
            provider_user_id=str(user_data["id"]),
            email=email,
            username=user_data.get("login"),
            full_name=user_data.get("name"),
            avatar_url=user_data.get("avatar_url"),
            raw_data=user_data,
        )

    async def _fetch_primary_email(
        self, client: httpx.AsyncClient, headers: dict[str, str]
    ) -> str:
        """Fetch primary email from GitHub /user/emails endpoint."""
        try:
            response = await client.get(
                "https://api.github.com/user/emails",
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()
            emails = response.json()

            # Find primary email
            for email_entry in emails:
                if email_entry.get("primary") and email_entry.get("verified"):
                    return email_entry["email"]

            # Fallback to first verified email
            for email_entry in emails:
                if email_entry.get("verified"):
                    return email_entry["email"]

            # Last resort: first email
            if emails:
                return emails[0]["email"]

            raise OAuthUserInfoError("No email found in GitHub account")

        except httpx.HTTPStatusError as e:
            logger.error(
                "GitHub email fetch failed: %s - %s",
                e.response.status_code,
                e.response.text,
            )
            raise OAuthUserInfoError(
                f"Failed to fetch email: {e.response.status_code}"
            ) from e


class GitLabOAuthProvider(OAuthProvider):
    """GitLab OAuth2 provider implementation."""

    provider_type = OAuthProviderType.GITLAB

    def __init__(self, config: OAuthConfig, base_url: str = "https://gitlab.com"):
        super().__init__(config)
        self.base_url = base_url.rstrip("/")

    @property
    def default_authorization_url(self) -> str:
        return f"{self.base_url}/oauth/authorize"

    @property
    def default_token_url(self) -> str:
        return f"{self.base_url}/oauth/token"

    @property
    def default_userinfo_url(self) -> str:
        return f"{self.base_url}/api/v4/user"

    async def fetch_user_info(self, access_token: str) -> OAuthUserInfo:
        """Fetch user info from GitLab /api/v4/user endpoint."""
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    self.userinfo_url,
                    headers=headers,
                    timeout=30.0,
                )
                response.raise_for_status()
                user_data = response.json()

            except httpx.HTTPStatusError as e:
                logger.error(
                    "GitLab user info fetch failed: %s - %s",
                    e.response.status_code,
                    e.response.text,
                )
                raise OAuthUserInfoError(
                    f"Failed to fetch user info: {e.response.status_code}"
                ) from e
            except httpx.RequestError as e:
                logger.error("GitLab user info request failed: %s", str(e))
                raise OAuthUserInfoError(f"User info request failed: {e}") from e

        try:
            return OAuthUserInfo(
                provider=self.provider_type.value,
                provider_user_id=str(user_data["id"]),
                email=user_data["email"],
                username=user_data.get("username"),
                full_name=user_data.get("name"),
                avatar_url=user_data.get("avatar_url"),
                raw_data=user_data,
            )
        except (KeyError, TypeError) as e:
            logger.error(
                "GitLab user info response missing required fields or malformed: %s",
                user_data,
            )
            raise OAuthUserInfoError(
                "User info response missing required fields: 'id' and/or 'email'"
            ) from e


class GoogleOAuthProvider(OAuthProvider):
    """Google OAuth2 provider implementation."""

    provider_type = OAuthProviderType.GOOGLE

    @property
    def default_authorization_url(self) -> str:
        return "https://accounts.google.com/o/oauth2/v2/auth"

    @property
    def default_token_url(self) -> str:
        return "https://oauth2.googleapis.com/token"

    @property
    def default_userinfo_url(self) -> str:
        return "https://www.googleapis.com/oauth2/v2/userinfo"

    def generate_authorization_request(
        self,
        state: Optional[str] = None,
        extra_params: Optional[dict[str, str]] = None,
    ) -> OAuthAuthorizationRequest:
        """Generate Google OAuth authorization URL with required parameters."""
        params = extra_params or {}
        # Google requires access_type for refresh tokens
        if "access_type" not in params:
            params["access_type"] = "offline"
        # Include prompt to ensure consent screen shows
        if "prompt" not in params:
            params["prompt"] = "consent"

        return super().generate_authorization_request(state=state, extra_params=params)

    async def fetch_user_info(self, access_token: str) -> OAuthUserInfo:
        """Fetch user info from Google userinfo endpoint."""
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    self.userinfo_url,
                    headers=headers,
                    timeout=30.0,
                )
                response.raise_for_status()
                user_data = response.json()

            except httpx.HTTPStatusError as e:
                logger.error(
                    "Google user info fetch failed: %s - %s",
                    e.response.status_code,
                    e.response.text,
                )
                raise OAuthUserInfoError(
                    f"Failed to fetch user info: {e.response.status_code}"
                ) from e
            except httpx.RequestError as e:
                logger.error("Google user info request failed: %s", str(e))
                raise OAuthUserInfoError(f"User info request failed: {e}") from e

        try:
            return OAuthUserInfo(
                provider=self.provider_type.value,
                provider_user_id=user_data["id"],
                email=user_data["email"],
                username=None,  # Google doesn't have usernames
                full_name=user_data.get("name"),
                avatar_url=user_data.get("picture"),
                raw_data=user_data,
            )
        except (KeyError, TypeError) as e:
            logger.error(
                "Google user info response missing required fields or malformed: %s",
                user_data,
            )
            raise OAuthUserInfoError(
                "User info response missing required fields: 'id' and/or 'email'"
            ) from e


def create_oauth_provider(
    provider_type: OAuthProviderType | str,
    config: OAuthConfig,
    base_url: Optional[str] = None,
) -> OAuthProvider:
    """Factory function to create OAuth provider instances.

    Args:
        provider_type: The type of OAuth provider (github, gitlab, google)
        config: OAuth configuration with client credentials
        base_url: Optional base URL for self-hosted instances (GitLab only)

    Returns:
        Configured OAuth provider instance

    Raises:
        ValueError: If provider type is not supported
    """
    if isinstance(provider_type, str):
        provider_type = OAuthProviderType(provider_type)

    if provider_type == OAuthProviderType.GITHUB:
        return GitHubOAuthProvider(config)
    elif provider_type == OAuthProviderType.GITLAB:
        return GitLabOAuthProvider(config, base_url=base_url or "https://gitlab.com")
    elif provider_type == OAuthProviderType.GOOGLE:
        return GoogleOAuthProvider(config)
    else:
        raise ValueError(f"Unsupported OAuth provider type: {provider_type}")


# Default scopes for each provider
DEFAULT_SCOPES = {
    OAuthProviderType.GITHUB: ["read:user", "user:email"],
    OAuthProviderType.GITLAB: ["read_user", "email"],
    OAuthProviderType.GOOGLE: ["openid", "email", "profile"],
}


def get_default_scopes(provider_type: OAuthProviderType | str) -> list[str]:
    if isinstance(provider_type, str):
        provider_type = OAuthProviderType(provider_type)
    return DEFAULT_SCOPES.get(provider_type, [])


@dataclass
class OAuthConfigValidationResult:
    valid: bool
    errors: list[str]


def validate_oauth_config(
    provider_type: OAuthProviderType | str,
    client_id: str,
    client_secret: str,
    scopes: Optional[list[str]] = None,
    base_url: Optional[str] = None,
) -> OAuthConfigValidationResult:
    if isinstance(provider_type, str):
        try:
            provider_type = OAuthProviderType(provider_type)
        except ValueError:
            return OAuthConfigValidationResult(
                valid=False,
                errors=[
                    f"Invalid provider type: {provider_type}. Must be one of: github, gitlab, google"
                ],
            )

    errors: list[str] = []

    if not client_id or not client_id.strip():
        errors.append("client_id is required")

    if not client_secret or not client_secret.strip():
        errors.append("client_secret is required")

    if provider_type == OAuthProviderType.GITHUB:
        if client_id and not client_id.startswith(("Iv1.", "Iv2.", "Ov")):
            errors.append(
                "GitHub client_id should start with 'Iv1.', 'Iv2.', or 'Ov'"
            )

    if provider_type == OAuthProviderType.GITLAB:
        if base_url:
            if not base_url.startswith(("http://", "https://")):
                errors.append("base_url must start with http:// or https://")

    if provider_type == OAuthProviderType.GOOGLE:
        if client_id and not client_id.endswith(".apps.googleusercontent.com"):
            errors.append(
                "Google client_id should end with .apps.googleusercontent.com"
            )

    if scopes:
        if provider_type == OAuthProviderType.GITHUB:
            if "user:email" not in scopes and "read:user" not in scopes:
                errors.append(
                    "GitHub OAuth requires at least 'read:user' or 'user:email' scope"
                )
        elif provider_type == OAuthProviderType.GITLAB:
            if "read_user" not in scopes:
                errors.append("GitLab OAuth requires 'read_user' scope")
        elif provider_type == OAuthProviderType.GOOGLE:
            if "email" not in scopes:
                errors.append("Google OAuth requires 'email' scope")

    return OAuthConfigValidationResult(valid=len(errors) == 0, errors=errors)
