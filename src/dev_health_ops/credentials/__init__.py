"""Credential resolution module for Enterprise worker credential management.

This module provides a unified interface for resolving provider credentials,
supporting both database-stored encrypted credentials (Enterprise) and
environment variable fallback (development/OSS).

Usage:
    from dev_health_ops.credentials import CredentialResolver, ProviderCredentials

    # Async context (API, async workers)
    async with get_async_session() as session:
        resolver = CredentialResolver(session, org_id="my-org")
        creds = await resolver.resolve("github")
        token = creds.token

    # Sync context (Celery workers, CLI)
    from dev_health_ops.credentials import resolve_credentials_sync
    creds = resolve_credentials_sync("github", org_id="my-org", db_url=db_url)
"""

from dev_health_ops.credentials.resolver import (
    CredentialResolver,
    CredentialResolutionError,
    resolve_credentials_sync,
)
from dev_health_ops.credentials.types import (
    AtlassianCredentials,
    CredentialSource,
    GitHubCredentials,
    GitLabCredentials,
    JiraCredentials,
    LinearCredentials,
    ProviderCredentials,
)

__all__ = [
    "AtlassianCredentials",
    "CredentialResolver",
    "CredentialResolutionError",
    "CredentialSource",
    "GitHubCredentials",
    "GitLabCredentials",
    "JiraCredentials",
    "LinearCredentials",
    "ProviderCredentials",
    "resolve_credentials_sync",
]
