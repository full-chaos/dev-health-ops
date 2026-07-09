"""DataLoader for team entity data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .base import CachedDataLoader

logger = logging.getLogger(__name__)


@dataclass
class TeamData:
    """Team entity data."""

    team_id: str
    team_name: str
    org_id: str
    member_count: int = 0


class TeamLoader(CachedDataLoader[str, TeamData | None]):
    """
    DataLoader for batch loading team data by team ID.

    Supports optional cross-request caching via TTLCache backend.
    """

    def __init__(
        self,
        client: Any,
        org_id: str,
        cache: Any | None = None,
        cache_ttl: int = 300,
    ):
        """
        Initialize the team loader.

        Args:
            client: ClickHouse client instance.
            org_id: Organization ID for scoping queries.
            cache: Optional cache backend for cross-request caching.
            cache_ttl: Cache TTL in seconds.
        """
        super().__init__(
            org_id=org_id, cache=cache, cache_ttl=cache_ttl, cache_prefix="team"
        )
        self._client = client
        self._org_id = org_id

    async def batch_load(self, keys: list[str]) -> list[TeamData | None]:
        """
        Batch load team data for multiple team IDs.

        Args:
            keys: List of team IDs to load.

        Returns:
            List of TeamData objects (or None for missing teams).
        """
        from dev_health_ops.api.queries.client import query_dicts
        from dev_health_ops.api.queries.investment import (
            PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE,
        )

        if not keys or self._client is None:
            return [None] * len(keys)

        sql = f"""
            SELECT
                toString(team_id) AS team_id,
                ifNull(nullIf(any(team_name), ''), toString(team_id)) AS team_name,
                count() AS member_count
            FROM {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE}
            WHERE team_id IN %(team_ids)s
              AND team_id IS NOT NULL
              AND team_id != ''
            GROUP BY team_id
            ORDER BY team_id
        """
        params = {"team_ids": list(keys), "org_id": self._org_id}

        try:
            rows = await query_dicts(self._client, sql, params)

            # Build lookup map
            team_map: dict[str, TeamData] = {}
            for row in rows:
                team_id = str(row.get("team_id", ""))
                if team_id and team_id not in team_map:
                    team_map[team_id] = TeamData(
                        team_id=team_id,
                        team_name=str(row.get("team_name", team_id)),
                        org_id=self._org_id,
                        member_count=int(row.get("member_count", 0)),
                    )

            # Return in original key order
            return [team_map.get(key) for key in keys]

        except Exception as e:
            logger.error("Team batch load failed: %s", e)
            return [None] * len(keys)


class TeamByNameLoader(CachedDataLoader[str, TeamData | None]):
    """
    DataLoader for batch loading team data by team name.

    Useful for resolving team references by display name.
    """

    def __init__(
        self,
        client: Any,
        org_id: str,
        cache: Any | None = None,
        cache_ttl: int = 300,
    ):
        """
        Initialize the team-by-name loader.

        Args:
            client: ClickHouse client instance.
            org_id: Organization ID for scoping queries.
            cache: Optional cache backend for cross-request caching.
            cache_ttl: Cache TTL in seconds.
        """
        super().__init__(
            org_id=org_id, cache=cache, cache_ttl=cache_ttl, cache_prefix="team_name"
        )
        self._client = client
        self._org_id = org_id

    async def batch_load(self, keys: list[str]) -> list[TeamData | None]:
        """
        Batch load team data for multiple team names.

        Args:
            keys: List of team names to load.

        Returns:
            List of TeamData objects (or None for missing teams).
        """
        from dev_health_ops.api.queries.client import query_dicts
        from dev_health_ops.api.queries.investment import (
            PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE,
        )

        if not keys or self._client is None:
            return [None] * len(keys)

        sql = f"""
            SELECT
                toString(team_id) AS team_id,
                ifNull(nullIf(any(raw_team_name), ''), toString(team_id)) AS team_name,
                count() AS member_count
            FROM (
                SELECT
                    work_item_id,
                    team_id,
                    team_name AS raw_team_name
                FROM {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE}
            ) AS t
            WHERE lower(raw_team_name) IN %(team_names)s
              AND team_id IS NOT NULL
              AND team_id != ''
              AND raw_team_name IS NOT NULL
            GROUP BY team_id
            ORDER BY team_name
        """
        params = {"team_names": [k.lower() for k in keys], "org_id": self._org_id}

        try:
            rows = await query_dicts(self._client, sql, params)

            # Build lookup map by lowercase name
            team_map: dict[str, TeamData] = {}
            for row in rows:
                team_name = str(row.get("team_name", ""))
                if team_name and team_name.lower() not in team_map:
                    team_map[team_name.lower()] = TeamData(
                        team_id=str(row.get("team_id", "")),
                        team_name=team_name,
                        org_id=self._org_id,
                        member_count=int(row.get("member_count", 0)),
                    )

            return [team_map.get(key.lower()) for key in keys]

        except Exception as e:
            logger.error("Team by name batch load failed: %s", e)
            return [None] * len(keys)
