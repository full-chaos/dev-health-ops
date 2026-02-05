from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional, Type, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.credentials.types import (
    AtlassianCredentials,
    CredentialSource,
    GitHubCredentials,
    GitLabCredentials,
    JiraCredentials,
    LinearCredentials,
    ProviderCredentials,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=ProviderCredentials)

PROVIDER_ENV_VARS: dict[str, dict[str, str]] = {
    "github": {"token": "GITHUB_TOKEN", "base_url": "GITHUB_URL"},
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
}

PROVIDER_CREDENTIAL_TYPES: dict[str, Type[ProviderCredentials]] = {
    "github": GitHubCredentials,
    "gitlab": GitLabCredentials,
    "jira": JiraCredentials,
    "linear": LinearCredentials,
    "atlassian": AtlassianCredentials,
}


class CredentialResolutionError(Exception):
    def __init__(
        self,
        provider: str,
        message: str,
        org_id: str = "default",
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
        org_id: str = "default",
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
            logger.info(
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
    ) -> Optional[ProviderCredentials]:
        try:
            from dev_health_ops.api.services.settings import (
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
            logger.warning(
                "Failed to fetch %s credentials from database: %s",
                provider,
                e,
            )
            return None

    def _try_environment(
        self,
        provider: str,
        credential_name: str,
    ) -> Optional[ProviderCredentials]:
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
            logger.debug(
                "Incomplete %s credentials from environment: %s",
                provider,
                e,
            )
            return None

    def _build_credential(
        self,
        cred_type: Type[T],
        cred_dict: dict[str, Any],
        source: CredentialSource,
        credential_name: str,
    ) -> T:
        cred_dict = {k: v for k, v in cred_dict.items() if v is not None}

        cred_dict["source"] = source
        cred_dict["credential_name"] = credential_name

        return cred_type(**cred_dict)


def resolve_credentials_sync(
    provider: str,
    org_id: str = "default",
    credential_name: str = "default",
    db_url: Optional[str] = None,
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
        raise RuntimeError(
            "resolve_credentials_sync cannot be called from async context. "
            "Use CredentialResolver.resolve() instead."
        )
    except RuntimeError:
        pass

    return asyncio.run(_resolve())


class _EnvOnlyResolver:
    def resolve_from_env(
        self,
        provider: str,
        credential_name: str = "default",
    ) -> Optional[ProviderCredentials]:
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
            cred_dict["source"] = CredentialSource.ENVIRONMENT
            cred_dict["credential_name"] = credential_name
            return cred_type(**cred_dict)
        except (ValueError, TypeError):
            return None
