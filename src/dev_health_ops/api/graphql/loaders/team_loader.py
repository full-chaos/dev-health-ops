"""DataLoader for team entity data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base import CachedDataLoader

logger = logging.getLogger(__name__)


@dataclass
class TeamData:
    """Team entity data."""

    team_id: str
    team_name: str
    org_id: str
    member_count: int = 0


class TeamLoader(CachedDataLoader[str, Optional[TeamData]]):
    """
    DataLoader for batch loading team data by team ID.

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
        Initialize the team loader.

        Args:
            client: ClickHouse client instance.
            org_id: Organization ID for scoping queries.
            cache: Optional cache backend for cross-request caching.
            cache_ttl: Cache TTL in seconds.
        """
        super().__init__(cache=cache, cache_ttl=cache_ttl, cache_prefix="team")
        self._client = client
        self._org_id = org_id

    async def batch_load(self, keys: List[str]) -> List[Optional[TeamData]]:
        """
        Batch load team data for multiple team IDs.

        Args:
            keys: List of team IDs to load.

        Returns:
            List of TeamData objects (or None for missing teams).
        """
        from dev_health_ops.api.queries.client import query_dicts

        if not keys or self._client is None:
            return [None] * len(keys)

        # Query all teams in a single batch
        sql = """
            SELECT DISTINCT
                team_id,
                team_name,
                org_id,
                count() OVER (PARTITION BY team_id) as member_count
            FROM work_item_cycle_times
            WHERE team_id IN %(team_ids)s
            ORDER BY team_id
        """
        params = {"team_ids": list(keys)}

        try:
            rows = await query_dicts(self._client, sql, params)

            # Build lookup map
            team_map: Dict[str, TeamData] = {}
            for row in rows:
                team_id = str(row.get("team_id", ""))
                if team_id and team_id not in team_map:
                    team_map[team_id] = TeamData(
                        team_id=team_id,
                        team_name=str(row.get("team_name", team_id)),
                        org_id=str(row.get("org_id", self._org_id)),
                        member_count=int(row.get("member_count", 0)),
                    )

            # Return in original key order
            return [team_map.get(key) for key in keys]

        except Exception as e:
            logger.error("Team batch load failed: %s", e)
            return [None] * len(keys)


class TeamByNameLoader(CachedDataLoader[str, Optional[TeamData]]):
    """
    DataLoader for batch loading team data by team name.

    Useful for resolving team references by display name.
    """

    def __init__(
        self,
        client: Any,
        org_id: str,
        cache: Optional[Any] = None,
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
        super().__init__(cache=cache, cache_ttl=cache_ttl, cache_prefix="team_name")
        self._client = client
        self._org_id = org_id

    async def batch_load(self, keys: List[str]) -> List[Optional[TeamData]]:
        """
        Batch load team data for multiple team names.

        Args:
            keys: List of team names to load.

        Returns:
            List of TeamData objects (or None for missing teams).
        """
        from dev_health_ops.api.queries.client import query_dicts

        if not keys or self._client is None:
            return [None] * len(keys)

        sql = """
            SELECT DISTINCT
                team_id,
                team_name,
                org_id,
                count() OVER (PARTITION BY team_id) as member_count
            FROM work_item_cycle_times
            WHERE lower(team_name) IN %(team_names)s
            ORDER BY team_name
        """
        params = {"team_names": [k.lower() for k in keys]}

        try:
            rows = await query_dicts(self._client, sql, params)

            # Build lookup map by lowercase name
            team_map: Dict[str, TeamData] = {}
            for row in rows:
                team_name = str(row.get("team_name", ""))
                if team_name and team_name.lower() not in team_map:
                    team_map[team_name.lower()] = TeamData(
                        team_id=str(row.get("team_id", "")),
                        team_name=team_name,
                        org_id=str(row.get("org_id", self._org_id)),
                        member_count=int(row.get("member_count", 0)),
                    )

            return [team_map.get(key.lower()) for key in keys]

        except Exception as e:
            logger.error("Team by name batch load failed: %s", e)
            return [None] * len(keys)
