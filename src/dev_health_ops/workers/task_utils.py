from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def _get_db_url() -> str:
    """Get data-store URL from environment.

    Prefers CLICKHOUSE_URI (the primary data store for sync/metrics),
    falling back to DATABASE_URI which may point to Postgres (admin DB).
    """
    return (
        os.getenv("CLICKHOUSE_URI")
        or os.getenv("DATABASE_URI")
        or os.getenv("DATABASE_URL")
        or ""
    )


def _merge_sync_flags(sync_targets: list[str]) -> dict[str, bool]:
    from dev_health_ops.processors.sync import _sync_flags_for_target

    merged_flags: dict[str, bool] = {}
    for target in sync_targets:
        flags = _sync_flags_for_target(target)
        for key, enabled in flags.items():
            if enabled:
                merged_flags[key] = True

    for key in (
        "sync_git",
        "sync_prs",
        "sync_cicd",
        "sync_deployments",
        "sync_incidents",
        "blame_only",
    ):
        merged_flags.setdefault(key, False)

    return merged_flags


def _extract_owner_repo(
    config_name: str, sync_options: dict[str, Any]
) -> tuple[str, str] | None:
    owner = sync_options.get("owner")
    repo_name = sync_options.get("repo")
    if owner and repo_name:
        return str(owner), str(repo_name)

    search = sync_options.get("search")
    if isinstance(search, str) and "/" in search:
        search_owner, search_repo = search.split("/", 1)
        repo_candidate = search_repo.replace("*", "").replace("?", "").strip()
        if search_owner and repo_candidate:
            return search_owner.strip(), repo_candidate

    if "/" in config_name:
        name_owner, name_repo = config_name.split("/", 1)
        if name_owner and name_repo:
            return name_owner.strip(), name_repo.strip()

    return None


def _decrypt_credential_sync(credential) -> dict[str, Any]:
    from dev_health_ops.core.encryption import decrypt_value

    if credential.credentials_encrypted:
        return json.loads(decrypt_value(credential.credentials_encrypted))
    return {}


def _inject_provider_token(provider: str, token: str) -> None:
    env_var = {
        "github": "GITHUB_TOKEN",
        "gitlab": "GITLAB_TOKEN",
        "launchdarkly": "LAUNCHDARKLY_API_KEY",
        # Extended provider env var mappings
        "linear": "LINEAR_API_KEY",
        "jira": "JIRA_API_TOKEN",
        "atlassian": "ATLASSIAN_API_TOKEN",
    }.get(provider.lower())
    if env_var and token:
        os.environ[env_var] = token


# New helper to extract provider-specific tokens from credentials
def _extract_provider_token(provider: str, credentials: dict[str, Any]) -> str:
    provider = provider.lower()
    if provider == "linear":
        return str(credentials.get("api_key") or credentials.get("apiKey") or "")
    if provider == "jira":
        return str(credentials.get("api_token") or credentials.get("apiToken") or "")
    if provider == "launchdarkly":
        return str(credentials.get("api_key") or credentials.get("apiKey") or "")
    # GitHub, GitLab, and others use "token"
    return str(credentials.get("token") or "")


def _resolve_env_credentials(provider: str) -> dict[str, str]:
    from dev_health_ops.credentials.resolver import PROVIDER_ENV_VARS

    env_map = PROVIDER_ENV_VARS.get(provider.lower(), {})
    return {
        field_name: value
        for field_name, env_var in env_map.items()
        if (value := os.getenv(env_var))
    }


_GIT_TARGETS = {"git", "prs"}
_WORK_ITEM_TARGETS = {"work-items"}


def _invalidate_metrics_cache(day: str, org_id: str) -> None:
    """Invalidate GraphQL caches after metrics update."""
    try:
        from dev_health_ops.core.cache import create_cache
        from dev_health_ops.core.cache_invalidation import invalidate_on_metrics_update

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_metrics_update(cache, org_id, day)
        logger.info("Invalidated %d cache entries after metrics update", count)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)


def _invalidate_sync_cache(sync_type: str, org_id: str) -> None:
    """Invalidate GraphQL caches after data sync."""
    try:
        from dev_health_ops.core.cache import create_cache
        from dev_health_ops.core.cache_invalidation import invalidate_on_sync_complete

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_sync_complete(cache, org_id, sync_type)
        logger.info("Invalidated %d cache entries after %s sync", count, sync_type)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)
