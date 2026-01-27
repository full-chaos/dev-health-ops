"""
Compatibility layer for atlassian-client library integration.

Supports both legacy JIRA_* env vars and new ATLASSIAN_* env vars,
with feature flag to toggle between implementations.
"""

from __future__ import annotations

import os
from typing import Optional

from atlassian import BasicApiTokenAuth, JiraRestClient


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
