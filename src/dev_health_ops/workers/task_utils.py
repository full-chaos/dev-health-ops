from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_PLACEHOLDER_CLICKHOUSE_HOSTS = {"fake", "test"}


def _placeholder_clickhouse_uri_allowed() -> bool:
    return os.getenv("PYTEST_CURRENT_TEST") is not None or os.getenv(
        "DEV_HEALTH_ALLOW_PLACEHOLDER_CLICKHOUSE_URI", ""
    ).strip().lower() in {"1", "true", "yes"}


def _validate_worker_clickhouse_uri(db_url: str) -> str:
    if not db_url:
        raise RuntimeError(
            "ClickHouse URI not configured for analytics worker task. "
            "Set CLICKHOUSE_URI."
        )

    from dev_health_ops.db import validate_sink_uri_scheme

    validate_sink_uri_scheme(db_url)
    host = (urlparse(db_url).hostname or "").strip().lower()
    if (
        host in _PLACEHOLDER_CLICKHOUSE_HOSTS
        and not _placeholder_clickhouse_uri_allowed()
    ):
        raise RuntimeError(
            f"Refusing placeholder ClickHouse URI host '{host}' for analytics "
            "worker task. Set CLICKHOUSE_URI to the real ClickHouse endpoint."
        )
    return db_url


def _get_db_url() -> str:
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
        "sync_security",
        "sync_tests",
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


def _credential_mapping(credential) -> dict[str, Any]:
    """Build the full credentials mapping for an ``IntegrationCredential``.

    Merges the credential's non-sensitive ``config`` column (base URLs and
    other provider options, e.g. a self-hosted GitLab ``url``) underneath the
    decrypted secret fields, so resolvers such as
    ``gitlab_credentials_from_mapping`` see both. Decrypted values win on key
    collisions: ``config`` must never shadow a stored secret.
    """
    decrypted = _decrypt_credential_sync(credential)
    config = getattr(credential, "config", None)
    if not isinstance(config, dict) or not config:
        return decrypted
    return {**config, **decrypted}


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


def _as_uuid(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _as_uuid_or_none(value: object | None) -> uuid.UUID | None:
    if value is None:
        return None
    return _as_uuid(value)


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    raise TypeError(f"Expected datetime, got {type(value)!r}")


def _as_datetime_or_none(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    return None


def _as_str(value: object | None) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def _as_str_or_none(value: object | None) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _as_bool(value: object | None) -> bool:
    return value if isinstance(value, bool) else bool(value)


def _as_int(value: object | None, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return int(str(value))


def _as_str_list(value: object | None) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _jira_query_options(
    sync_options: dict[str, Any],
) -> tuple[list[str] | None, str | None, bool | None]:
    project_keys_raw = sync_options.get("project_keys")
    if project_keys_raw is None:
        project_key = sync_options.get("project_key")
        project_keys = [str(project_key)] if project_key else None
    elif isinstance(project_keys_raw, str):
        project_keys = [
            key.strip() for key in project_keys_raw.split(",") if key.strip()
        ]
    else:
        project_keys = _as_str_list(project_keys_raw) or None

    jql_raw = sync_options.get("jql") or sync_options.get("jira_jql")
    fetch_all_raw = sync_options.get("fetch_all")
    if fetch_all_raw is None:
        fetch_all_raw = sync_options.get("jira_fetch_all")

    fetch_all = None
    if fetch_all_raw is not None:
        fetch_all = str(fetch_all_raw).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    return project_keys, str(jql_raw) if jql_raw else None, fetch_all


def _as_dict(value: object | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): raw_value for key, raw_value in value.items()}


_GIT_TARGETS = {"git", "prs"}
_WORK_ITEM_TARGETS = {"work-items"}
_WORK_ITEM_PROVIDERS = {"github", "gitlab", "jira", "linear"}


def _normalize_sync_targets(provider: str, sync_targets: list[str]) -> list[str]:
    provider = provider.lower()
    if sync_targets:
        return sync_targets
    if provider in _WORK_ITEM_PROVIDERS:
        logger.warning(
            "Sync configuration for provider=%s has no sync targets; defaulting to work-items",
            provider,
        )
        return ["work-items"]
    raise ValueError(f"Sync configuration for provider={provider} has no sync targets")


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
