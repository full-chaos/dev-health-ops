"""
Compatibility layer for atlassian-client library integration.

Supports both legacy JIRA_* env vars and new ATLASSIAN_* env vars,
with feature flag to toggle between implementations.
"""

from __future__ import annotations

import json
import os
from typing import Optional

from atlassian import BasicApiTokenAuth, CookieAuth, JiraRestClient, OAuthBearerAuth
from atlassian.graph.client import GraphQLClient
from atlassian.oauth_3lo import OAuthRefreshTokenAuth


def _normalize_base_url(value: str) -> str:
    url = (value or "").strip().rstrip("/")
    if not url:
        return url
    if url.startswith("http://"):
        return "https://" + url[len("http://") :]
    if url.startswith("https://"):
        return url
    return "https://" + url.lstrip("/")


def atlassian_client_enabled() -> bool:
    raw = os.getenv("ATLASSIAN_CLIENT_ENABLED", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def get_atlassian_auth() -> Optional[BasicApiTokenAuth]:
    email = os.getenv("ATLASSIAN_EMAIL") or os.getenv("JIRA_EMAIL")
    api_token = os.getenv("ATLASSIAN_API_TOKEN") or os.getenv("JIRA_API_TOKEN")
    if not email or not api_token:
        return None
    return BasicApiTokenAuth(email.strip(), api_token.strip())


def get_atlassian_base_url() -> Optional[str]:
    base_url = os.getenv("ATLASSIAN_JIRA_BASE_URL") or os.getenv("JIRA_BASE_URL")
    if not base_url:
        return None
    return _normalize_base_url(base_url)


def get_atlassian_cloud_id() -> Optional[str]:
    cloud_id = os.getenv("ATLASSIAN_CLOUD_ID")
    if cloud_id:
        return cloud_id.strip()
    base_url = get_atlassian_base_url()
    if base_url:
        parts = base_url.replace("https://", "").replace("http://", "").split(".")
        if parts and parts[0]:
            return parts[0]
    return None


def build_atlassian_rest_client(
    timeout_seconds: float = 30.0,
    max_retries_429: int = 2,
) -> JiraRestClient:
    auth = get_atlassian_auth()
    if auth is None:
        raise ValueError(
            "Atlassian credentials required. Set either "
            "(ATLASSIAN_EMAIL + ATLASSIAN_API_TOKEN) or "
            "(JIRA_EMAIL + JIRA_API_TOKEN)."
        )
    base_url = get_atlassian_base_url()
    if not base_url:
        raise ValueError(
            "Atlassian base URL required. Set either "
            "ATLASSIAN_JIRA_BASE_URL or JIRA_BASE_URL."
        )
    return JiraRestClient(
        base_url,
        auth=auth,
        timeout_seconds=timeout_seconds,
        max_retries_429=max_retries_429,
    )


def get_atlassian_graphql_auth():
    access_token = os.getenv("ATLASSIAN_OAUTH_ACCESS_TOKEN")
    refresh_token = os.getenv("ATLASSIAN_OAUTH_REFRESH_TOKEN")
    client_id = os.getenv("ATLASSIAN_CLIENT_ID")
    client_secret = os.getenv("ATLASSIAN_CLIENT_SECRET")
    email = os.getenv("ATLASSIAN_EMAIL") or os.getenv("JIRA_EMAIL")
    api_token = os.getenv("ATLASSIAN_API_TOKEN") or os.getenv("JIRA_API_TOKEN")
    cookies_json = os.getenv("ATLASSIAN_COOKIES_JSON")

    if refresh_token and client_id and client_secret:
        return OAuthRefreshTokenAuth(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )
    if access_token:
        token = access_token.strip()
        if client_secret and token == client_secret.strip():
            raise ValueError(
                "ATLASSIAN_OAUTH_ACCESS_TOKEN appears to be set to ATLASSIAN_CLIENT_SECRET; "
                "set an OAuth access token (not the client secret)."
            )
        return OAuthBearerAuth(lambda: token)
    if email and api_token:
        return BasicApiTokenAuth(email.strip(), api_token.strip())
    if cookies_json:
        try:
            cookies = json.loads(cookies_json)
        except json.JSONDecodeError:
            return None
        if isinstance(cookies, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in cookies.items()
        ):
            return CookieAuth(cookies)
    return None


def get_atlassian_graphql_base_url() -> Optional[str]:
    base_url = os.getenv("ATLASSIAN_GQL_BASE_URL") or os.getenv(
        "ATLASSIAN_OAUTH_GQL_ENDPOINT"
    )
    if not base_url:
        if os.getenv("ATLASSIAN_OAUTH_ACCESS_TOKEN") or os.getenv(
            "ATLASSIAN_OAUTH_REFRESH_TOKEN"
        ):
            base_url = "https://api.atlassian.com"
    if not base_url:
        return None
    return base_url.strip().rstrip("/")


def build_atlassian_graphql_client(
    timeout_seconds: float = 30.0,
    max_retries_429: int = 2,
    enable_local_throttling: bool = False,
) -> GraphQLClient:
    auth = get_atlassian_graphql_auth()
    if auth is None:
        raise ValueError(
            "Atlassian GraphQL credentials required. Set ATLASSIAN_OAUTH_ACCESS_TOKEN, "
            "ATLASSIAN_OAUTH_REFRESH_TOKEN + (ATLASSIAN_CLIENT_ID + ATLASSIAN_CLIENT_SECRET), "
            "ATLASSIAN_EMAIL + ATLASSIAN_API_TOKEN, or ATLASSIAN_COOKIES_JSON."
        )
    base_url = get_atlassian_graphql_base_url()
    if not base_url:
        raise ValueError(
            "ATLASSIAN_GQL_BASE_URL or ATLASSIAN_OAUTH_GQL_ENDPOINT is required for GraphQL."
        )
    return GraphQLClient(
        base_url,
        auth=auth,
        timeout_seconds=timeout_seconds,
        max_retries_429=max_retries_429,
        enable_local_throttling=enable_local_throttling,
    )


def validate_atlassian_env() -> list[str]:
    errors: list[str] = []
    email = os.getenv("ATLASSIAN_EMAIL") or os.getenv("JIRA_EMAIL")
    api_token = os.getenv("ATLASSIAN_API_TOKEN") or os.getenv("JIRA_API_TOKEN")
    base_url = os.getenv("ATLASSIAN_JIRA_BASE_URL") or os.getenv("JIRA_BASE_URL")
    if not email:
        errors.append("Missing ATLASSIAN_EMAIL or JIRA_EMAIL")
    if not api_token:
        errors.append("Missing ATLASSIAN_API_TOKEN or JIRA_API_TOKEN")
    if not base_url:
        errors.append("Missing ATLASSIAN_JIRA_BASE_URL or JIRA_BASE_URL")
    return errors


def validate_atlassian_graphql_env() -> list[str]:
    errors: list[str] = []
    base_url = os.getenv("ATLASSIAN_GQL_BASE_URL") or os.getenv(
        "ATLASSIAN_OAUTH_GQL_ENDPOINT"
    )
    if not base_url and not (
        os.getenv("ATLASSIAN_OAUTH_ACCESS_TOKEN")
        or os.getenv("ATLASSIAN_OAUTH_REFRESH_TOKEN")
    ):
        errors.append("Missing ATLASSIAN_GQL_BASE_URL or ATLASSIAN_OAUTH_GQL_ENDPOINT")

    auth = get_atlassian_graphql_auth()
    if auth is None:
        errors.append(
            "Missing GraphQL credentials (ATLASSIAN_OAUTH_ACCESS_TOKEN, "
            "ATLASSIAN_OAUTH_REFRESH_TOKEN + ATLASSIAN_CLIENT_ID/SECRET, "
            "ATLASSIAN_EMAIL + ATLASSIAN_API_TOKEN, or ATLASSIAN_COOKIES_JSON)"
        )

    return errors
