from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.credentials.types import (
    AtlassianCredentials,
    CredentialSource,
    GitHubCredentials,
    GitLabCredentials,
    JiraCredentials,
    LaunchDarklyCredentials,
    LinearCredentials,
    ProviderCredentials,
    TelemetryCredentials,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=ProviderCredentials)

PROVIDER_ENV_VARS: dict[str, dict[str, str]] = {
    "github": {
        "token": "GITHUB_TOKEN",
        "base_url": "GITHUB_URL",
        "app_id": "GITHUB_APP_ID",
        "private_key_path": "GITHUB_APP_PRIVATE_KEY_PATH",
        "installation_id": "GITHUB_APP_INSTALLATION_ID",
    },
    "gitlab": {"token": "GITLAB_TOKEN", "base_url": "GITLAB_URL"},
    "jira": {
        "api_token": "JIRA_API_TOKEN",
        "email": "JIRA_EMAIL",
        "base_url": "JIRA_BASE_URL",
    },
    "linear": {"api_key": "LINEAR_API_KEY"},
    "atlassian": {
        "api_token": "ATLASSIAN_API_TOKEN",
        "email": "ATLASSIAN_EMAIL",
        "cloud_id": "ATLASSIAN_CLOUD_ID",
    },
    "launchdarkly": {"api_key": "LAUNCHDARKLY_API_KEY"},
    "telemetry": {"api_key": "TELEMETRY_API_KEY"},
}

PROVIDER_CREDENTIAL_TYPES: dict[str, type[ProviderCredentials]] = {
    "github": GitHubCredentials,
    "gitlab": GitLabCredentials,
    "jira": JiraCredentials,
    "linear": LinearCredentials,
    "atlassian": AtlassianCredentials,
    "launchdarkly": LaunchDarklyCredentials,
    "telemetry": TelemetryCredentials,
}


class CredentialResolutionError(Exception):
    def __init__(
        self,
        provider: str,
        message: str,
        org_id: str | None = None,
        credential_name: str = "default",
    ):
        self.provider = provider
        self.org_id = org_id
        self.credential_name = credential_name
        super().__init__(message)


class CredentialResolver:
    def __init__(
        self,
        session: AsyncSession,
        org_id: str,
        allow_env_fallback: bool = True,
    ):
        self.session = session
        self.org_id = org_id
        self.allow_env_fallback = allow_env_fallback

    async def resolve(
        self,
        provider: str,
        credential_name: str = "default",
    ) -> ProviderCredentials:
        provider = provider.lower()

        if provider not in PROVIDER_CREDENTIAL_TYPES:
            raise CredentialResolutionError(
                provider=provider,
                message=f"Unknown provider: {provider}. Supported: {list(PROVIDER_CREDENTIAL_TYPES.keys())}",
                org_id=self.org_id,
                credential_name=credential_name,
            )

        db_creds = await self._try_database(provider, credential_name)
        if db_creds is not None:
            # Non-secret identifiers only (provider, org id, credential name).
            logger.info(  # nosemgrep: python.lang.security.audit.logging.logger-credential-leak.python-logger-credential-disclosure
                "Resolved %s credentials from database for org=%s name=%s",
                provider,
                self.org_id,
                credential_name,
            )
            return db_creds

        if self.allow_env_fallback:
            env_creds = self._try_environment(provider, credential_name)
            if env_creds is not None:
                logger.info(
                    "Resolved %s credentials from environment (fallback)",
                    provider,
                )
                return env_creds

        env_vars = PROVIDER_ENV_VARS.get(provider, {})
        env_var_names = ", ".join(env_vars.values())

        raise CredentialResolutionError(
            provider=provider,
            message=(
                f"No {provider} credentials found. "
                f"Configure credentials via Admin API or set environment variables: {env_var_names}"
            ),
            org_id=self.org_id,
            credential_name=credential_name,
        )

    async def _try_database(
        self,
        provider: str,
        credential_name: str,
    ) -> ProviderCredentials | None:
        try:
            from dev_health_ops.api.services.configuration import (
                IntegrationCredentialsService,
            )

            svc = IntegrationCredentialsService(self.session, self.org_id)
            cred_dict = await svc.get_decrypted_credentials(provider, credential_name)

            if cred_dict is None:
                return None

            cred_type = PROVIDER_CREDENTIAL_TYPES[provider]
            return self._build_credential(
                cred_type,
                cred_dict,
                source=CredentialSource.DATABASE,
                credential_name=credential_name,
            )

        except Exception as e:
            # Logs provider + failure reason; never credential values.
            logger.warning(  # nosemgrep: python.lang.security.audit.logging.logger-credential-leak.python-logger-credential-disclosure
                "Failed to fetch %s credentials from database: %s",
                provider,
                e,
            )
            return None

    def _try_environment(
        self,
        provider: str,
        credential_name: str,
    ) -> ProviderCredentials | None:
        env_map = PROVIDER_ENV_VARS.get(provider, {})
        if not env_map:
            return None

        cred_dict: dict[str, Any] = {}
        for field_name, env_var in env_map.items():
            value = os.getenv(env_var)
            if value:
                cred_dict[field_name] = value

        if not cred_dict:
            return None

        cred_type = PROVIDER_CREDENTIAL_TYPES[provider]

        try:
            return self._build_credential(
                cred_type,
                cred_dict,
                source=CredentialSource.ENVIRONMENT,
                credential_name=credential_name,
            )
        except (ValueError, TypeError) as e:
            # Logs provider + validation error; never credential values.
            logger.debug(  # nosemgrep: python.lang.security.audit.logging.logger-credential-leak.python-logger-credential-disclosure
                "Incomplete %s credentials from environment: %s",
                provider,
                e,
            )
            return None

    def _build_credential(
        self,
        cred_type: type[T],
        cred_dict: dict[str, Any],
        source: CredentialSource,
        credential_name: str,
    ) -> T:
        cred_dict = {k: v for k, v in cred_dict.items() if v is not None}

        if cred_type is GitHubCredentials and "private_key" not in cred_dict:
            private_key_path = cred_dict.get("private_key_path")
            if private_key_path:
                with open(str(private_key_path), encoding="utf-8") as key_file:
                    cred_dict["private_key"] = key_file.read()

        cred_dict["source"] = source
        cred_dict["credential_name"] = credential_name

        return cred_type(**cred_dict)


def github_credentials_from_mapping(
    cred_dict: dict[str, Any],
    *,
    source: CredentialSource = CredentialSource.DATABASE,
    credential_name: str = "default",
) -> GitHubCredentials | None:
    """Build :class:`GitHubCredentials` (PAT or App auth) from a credentials mapping.

    Accepts a decrypted credentials dict (as persisted in ``integration_credentials``
    or assembled from environment variables) and returns a typed credential the
    GitHub connector can consume directly.

    Returns ``None`` when the mapping contains neither a ``token`` nor a complete
    App-auth triple (``app_id`` + ``private_key`` + ``installation_id``), so callers
    can surface a clear configuration error.
    """
    aliases = {
        "appId": "app_id",
        "baseUrl": "base_url",
        "installationId": "installation_id",
        "privateKey": "private_key",
    }
    cred_dict = {aliases.get(k, k): v for k, v in cred_dict.items() if v is not None}
    if not cred_dict:
        return None

    allowed = {"token", "app_id", "private_key", "installation_id", "base_url"}
    kwargs = {k: v for k, v in cred_dict.items() if k in allowed}
    kwargs["source"] = source
    kwargs["credential_name"] = credential_name

    try:
        return GitHubCredentials(**kwargs)
    except (ValueError, TypeError):
        # Do not log the exception: it derives from credential construction and
        # could surface field values. Callers raise a clear config error instead.
        logger.debug("GitHub credentials mapping was incomplete or invalid")
        return None


def gitlab_credentials_from_mapping(
    cred_dict: dict[str, Any],
    *,
    source: CredentialSource = CredentialSource.DATABASE,
    credential_name: str = "default",
) -> GitLabCredentials | None:
    """Build :class:`GitLabCredentials` from a credentials mapping.

    Accepts a decrypted credentials dict (as persisted in
    ``integration_credentials`` or assembled from environment variables) and
    returns a typed credential carrying both the token and the GitLab base
    URL, so self-hosted instances are honoured everywhere the mapping is
    consumed. The base URL is resolved from the mapping's ``gitlab_url``,
    ``url``, or ``base_url`` key (in that order), defaulting to
    ``https://gitlab.com``.

    Returns ``None`` when the mapping does not contain a ``token``, so
    callers can surface a clear configuration error.
    """
    cred_dict = {k: v for k, v in cred_dict.items() if v is not None}
    token = str(cred_dict.get("token") or "")
    if not token:
        logger.debug("GitLab credentials mapping was incomplete or invalid")
        return None

    base_url = str(
        cred_dict.get("gitlab_url")
        or cred_dict.get("url")
        or cred_dict.get("base_url")
        or "https://gitlab.com"
    )

    try:
        return GitLabCredentials(
            token=token,
            base_url=base_url,
            source=source,
            credential_name=credential_name,
        )
    except (ValueError, TypeError):
        # Do not log the exception: it derives from credential construction and
        # could surface field values. Callers raise a clear config error instead.
        logger.debug("GitLab credentials mapping was incomplete or invalid")
        return None


def resolve_gitlab_url(
    sync_options: dict[str, Any],
    gitlab_credentials: GitLabCredentials | None,
) -> str:
    """Resolve the GitLab base URL for a sync/discovery operation.

    Resolution order: ``sync_options['gitlab_url']`` -> credential mapping's
    URL (``gitlab_url``/``url``/``base_url``) -> ``https://gitlab.com``.
    """
    option_url = sync_options.get("gitlab_url")
    if isinstance(option_url, str) and option_url:
        return option_url
    if gitlab_credentials is not None and gitlab_credentials.base_url:
        return gitlab_credentials.base_url
    return "https://gitlab.com"


def resolve_credentials_sync(
    provider: str,
    org_id: str | None = None,
    credential_name: str = "default",
    db_url: str | None = None,
    allow_env_fallback: bool = True,
) -> ProviderCredentials:
    db_url = db_url or os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")

    if not db_url:
        if allow_env_fallback:
            resolver = _EnvOnlyResolver()
            creds = resolver.resolve_from_env(provider, credential_name)
            if creds is not None:
                return creds

        raise CredentialResolutionError(
            provider=provider,
            message=(
                f"No database URL configured and no {provider} environment credentials found. "
                "Set DATABASE_URI or configure provider-specific environment variables."
            ),
            org_id=org_id,
            credential_name=credential_name,
        )

    if not org_id:
        # Database resolution needs an org scope, but env credentials do not:
        # without this fallback a configured DATABASE_URI makes org-less
        # callers (e.g. GitHubWorkClient.from_env) fail even when the
        # provider's env variables are set (CHAOS-2292).
        if allow_env_fallback:
            resolver = _EnvOnlyResolver()
            creds = resolver.resolve_from_env(provider, credential_name)
            if creds is not None:
                return creds

        raise CredentialResolutionError(
            provider=provider,
            message="Organization ID is required for database credential resolution.",
            org_id=org_id,
            credential_name=credential_name,
        )

    async def _resolve() -> ProviderCredentials:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        engine = create_async_engine(db_url, pool_pre_ping=True)
        async_session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async with async_session_factory() as session:
            resolver = CredentialResolver(
                session=session,
                org_id=org_id,
                allow_env_fallback=allow_env_fallback,
            )
            return await resolver.resolve(provider, credential_name)

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "resolve_credentials_sync cannot be called from async context. "
            "Use CredentialResolver.resolve() instead."
        )

    return asyncio.run(_resolve())


class _EnvOnlyResolver:
    def resolve_from_env(
        self,
        provider: str,
        credential_name: str = "default",
    ) -> ProviderCredentials | None:
        provider = provider.lower()
        if provider not in PROVIDER_CREDENTIAL_TYPES:
            return None

        env_map = PROVIDER_ENV_VARS.get(provider, {})
        if not env_map:
            return None

        cred_dict: dict[str, Any] = {}
        for field_name, env_var in env_map.items():
            value = os.getenv(env_var)
            if value:
                cred_dict[field_name] = value

        if not cred_dict:
            return None

        cred_type = PROVIDER_CREDENTIAL_TYPES[provider]

        try:
            if cred_type is GitHubCredentials and "private_key" not in cred_dict:
                private_key_path = cred_dict.get("private_key_path")
                if private_key_path:
                    with open(str(private_key_path), encoding="utf-8") as key_file:
                        cred_dict["private_key"] = key_file.read()
            cred_dict["source"] = CredentialSource.ENVIRONMENT
            cred_dict["credential_name"] = credential_name
            return cred_type(**cred_dict)
        except (OSError, ValueError, TypeError):
            return None
