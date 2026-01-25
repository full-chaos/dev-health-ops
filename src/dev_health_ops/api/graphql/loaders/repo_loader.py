"""DataLoader for repository entity data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base import CachedDataLoader

logger = logging.getLogger(__name__)


@dataclass
class RepoData:
    """Repository entity data."""

    repo_id: str
    repo_name: str
    org_id: str
    default_branch: Optional[str] = None
    language: Optional[str] = None


class RepoLoader(CachedDataLoader[str, Optional[RepoData]]):
    """
    DataLoader for batch loading repository data by repo ID.

    Supports optional cross-request caching via TTLCache backend.
    """

    def __init__(
        self,
        client: Any,
        org_id: str,
        cache: Optional[Any] = None,
        cache_ttl: int = 300,
    ):
        """
        Initialize the repo loader.

        Args:
            client: ClickHouse client instance.
            org_id: Organization ID for scoping queries.
            cache: Optional cache backend for cross-request caching.
            cache_ttl: Cache TTL in seconds.
        """
        super().__init__(cache=cache, cache_ttl=cache_ttl, cache_prefix="repo")
        self._client = client
        self._org_id = org_id

    async def batch_load(self, keys: List[str]) -> List[Optional[RepoData]]:
        """
        Batch load repository data for multiple repo IDs.

        Args:
            keys: List of repo IDs to load.

        Returns:
            List of RepoData objects (or None for missing repos).
        """
        from dev_health_ops.api.queries.client import query_dicts

        if not keys or self._client is None:
            return [None] * len(keys)

        sql = """
            SELECT
                id as repo_id,
                name as repo_name,
                org_id,
                default_branch,
                language
            FROM repos
            WHERE id IN %(repo_ids)s
        """
        params = {"repo_ids": list(keys)}

        try:
            rows = await query_dicts(self._client, sql, params)

            # Build lookup map
            repo_map: Dict[str, RepoData] = {}
            for row in rows:
                repo_id = str(row.get("repo_id", ""))
                if repo_id:
                    repo_map[repo_id] = RepoData(
                        repo_id=repo_id,
                        repo_name=str(row.get("repo_name", "")),
                        org_id=str(row.get("org_id", self._org_id)),
                        default_branch=row.get("default_branch"),
                        language=row.get("language"),
                    )

            return [repo_map.get(key) for key in keys]

        except Exception as e:
            logger.error("Repo batch load failed: %s", e)
            return [None] * len(keys)


class RepoByNameLoader(CachedDataLoader[str, Optional[RepoData]]):
    """
    DataLoader for batch loading repository data by repo name.

    Useful for resolving repository references by display name.
    """

    def __init__(
        self,
        client: Any,
        org_id: str,
        cache: Optional[Any] = None,
        cache_ttl: int = 300,
    ):
        """
        Initialize the repo-by-name loader.

        Args:
            client: ClickHouse client instance.
            org_id: Organization ID for scoping queries.
            cache: Optional cache backend for cross-request caching.
            cache_ttl: Cache TTL in seconds.
        """
        super().__init__(cache=cache, cache_ttl=cache_ttl, cache_prefix="repo_name")
        self._client = client
        self._org_id = org_id

    async def batch_load(self, keys: List[str]) -> List[Optional[RepoData]]:
        """
        Batch load repository data for multiple repo names.

        Args:
            keys: List of repo names to load.

        Returns:
            List of RepoData objects (or None for missing repos).
        """
        from dev_health_ops.api.queries.client import query_dicts

        if not keys or self._client is None:
            return [None] * len(keys)

        sql = """
            SELECT
                id as repo_id,
                name as repo_name,
                org_id,
                default_branch,
                language
            FROM repos
            WHERE lower(name) IN %(repo_names)s
        """
        params = {"repo_names": [k.lower() for k in keys]}

        try:
            rows = await query_dicts(self._client, sql, params)

            # Build lookup map by lowercase name
            repo_map: Dict[str, RepoData] = {}
            for row in rows:
                repo_name = str(row.get("repo_name", ""))
                if repo_name and repo_name.lower() not in repo_map:
                    repo_map[repo_name.lower()] = RepoData(
                        repo_id=str(row.get("repo_id", "")),
                        repo_name=repo_name,
                        org_id=str(row.get("org_id", self._org_id)),
                        default_branch=row.get("default_branch"),
                        language=row.get("language"),
                    )

            return [repo_map.get(key.lower()) for key in keys]

        except Exception as e:
            logger.error("Repo by name batch load failed: %s", e)
            return [None] * len(keys)
