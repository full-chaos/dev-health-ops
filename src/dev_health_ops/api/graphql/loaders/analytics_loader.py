"""DataLoaders for analytics queries (timeseries, breakdowns)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .base import SimpleDataLoader

if TYPE_CHECKING:
    from ..models.inputs import FilterInput

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeseriesKey:
    """Key for timeseries query batching."""

    org_id: str
    dimension: str
    measure: str
    interval: str
    start_date: date
    end_date: date
    use_investment: bool
    filters_hash: str  # Hash of filters for grouping


@dataclass(frozen=True)
class BreakdownKey:
    """Key for breakdown query batching."""

    org_id: str
    dimension: str
    measure: str
    start_date: date
    end_date: date
    top_n: int
    use_investment: bool
    filters_hash: str


@dataclass
class TimeseriesBucketData:
    """Single bucket in timeseries result."""

    date: date
    value: float


@dataclass
class TimeseriesResultData:
    """Result for a timeseries query."""

    dimension: str
    dimension_value: str
    measure: str
    buckets: List[TimeseriesBucketData]


@dataclass
class BreakdownItemData:
    """Single item in breakdown result."""

    key: str
    value: float


@dataclass
class BreakdownResultData:
    """Result for a breakdown query."""

    dimension: str
    measure: str
    items: List[BreakdownItemData]


def _hash_filters(filters: Optional["FilterInput"]) -> str:
    """Generate a stable hash for filters to group compatible queries."""
    if filters is None:
        return "none"
    import hashlib
    import json

    # Convert filters to a hashable representation
    filter_dict = {}
    if filters.scope:
        filter_dict["scope"] = {
            "org_ids": filters.scope.org_ids,
            "team_ids": filters.scope.team_ids,
            "repo_ids": filters.scope.repo_ids,
            "service_ids": filters.scope.service_ids,
            "developer_ids": filters.scope.developer_ids,
        }
    if filters.who:
        filter_dict["who"] = {
            "developers": filters.who.developers,
            "roles": filters.who.roles,
        }
    if filters.what:
        filter_dict["what"] = {
            "repos": filters.what.repos,
            "services": filters.what.services,
        }
    if filters.why:
        filter_dict["why"] = {
            "work_categories": filters.why.work_categories,
            "issue_types": filters.why.issue_types,
        }
    if filters.how:
        filter_dict["how"] = {
            "flow_stages": filters.how.flow_stages,
        }

    filter_json = json.dumps(filter_dict, sort_keys=True, default=str)
    return hashlib.sha256(filter_json.encode()).hexdigest()[:12]


class TimeseriesLoader(SimpleDataLoader[TimeseriesKey, List[TimeseriesResultData]]):
    """
    DataLoader for timeseries analytics queries.

    Batches multiple timeseries requests and executes them in parallel.
    Keys with the same date range and filters can potentially be combined.
    """

    def __init__(self, client: Any, timeout: int = 30):
        """
        Initialize the timeseries loader.

        Args:
            client: ClickHouse client instance.
            timeout: Query timeout in seconds.
        """
        super().__init__()
        self._client = client
        self._timeout = timeout

    async def batch_load(
        self, keys: List[TimeseriesKey]
    ) -> List[List[TimeseriesResultData]]:
        """
        Batch load timeseries data for multiple keys.

        Executes queries in parallel using asyncio.gather for better performance.
        """
        from dev_health_ops.api.queries.client import query_dicts
        from ..sql.compiler import TimeseriesRequest, compile_timeseries

        async def load_single(key: TimeseriesKey) -> List[TimeseriesResultData]:
            """Load a single timeseries query."""
            request = TimeseriesRequest(
                dimension=key.dimension,
                measure=key.measure,
                interval=key.interval,
                start_date=key.start_date,
                end_date=key.end_date,
                use_investment=key.use_investment,
            )

            # Note: We'd need to reconstruct filters from hash or pass them differently
            # For now, we compile without filters for batched queries
            sql, params = compile_timeseries(
                request, key.org_id, self._timeout, filters=None
            )

            try:
                rows = await query_dicts(self._client, sql, params)
                grouped: Dict[str, List[TimeseriesBucketData]] = {}

                for row in rows:
                    dim_val = str(row.get("dimension_value", ""))
                    bucket_date = row.get("bucket")
                    value = float(row.get("value", 0))

                    if dim_val not in grouped:
                        grouped[dim_val] = []

                    if isinstance(bucket_date, date):
                        grouped[dim_val].append(
                            TimeseriesBucketData(date=bucket_date, value=value)
                        )

                results = []
                for dim_val, buckets in grouped.items():
                    results.append(
                        TimeseriesResultData(
                            dimension=key.dimension,
                            dimension_value=dim_val,
                            measure=key.measure,
                            buckets=buckets,
                        )
                    )
                return results

            except Exception as e:
                logger.error("Timeseries query failed for key %s: %s", key, e)
                return []

        # Execute all queries in parallel
        results = await asyncio.gather(
            *[load_single(key) for key in keys],
            return_exceptions=True,
        )

        # Convert exceptions to empty results
        return [r if isinstance(r, list) else [] for r in results]


class BreakdownLoader(SimpleDataLoader[BreakdownKey, BreakdownResultData]):
    """
    DataLoader for breakdown analytics queries.

    Batches multiple breakdown requests and executes them in parallel.
    """

    def __init__(self, client: Any, timeout: int = 30):
        """
        Initialize the breakdown loader.

        Args:
            client: ClickHouse client instance.
            timeout: Query timeout in seconds.
        """
        super().__init__()
        self._client = client
        self._timeout = timeout

    async def batch_load(self, keys: List[BreakdownKey]) -> List[BreakdownResultData]:
        """
        Batch load breakdown data for multiple keys.

        Executes queries in parallel using asyncio.gather.
        """
        from dev_health_ops.api.queries.client import query_dicts
        from ..sql.compiler import BreakdownRequest, compile_breakdown

        async def load_single(key: BreakdownKey) -> BreakdownResultData:
            """Load a single breakdown query."""
            request = BreakdownRequest(
                dimension=key.dimension,
                measure=key.measure,
                start_date=key.start_date,
                end_date=key.end_date,
                top_n=key.top_n,
                use_investment=key.use_investment,
            )

            sql, params = compile_breakdown(
                request, key.org_id, self._timeout, filters=None
            )

            try:
                rows = await query_dicts(self._client, sql, params)
                items = [
                    BreakdownItemData(
                        key=str(row.get("dimension_value", "")),
                        value=float(row.get("value", 0)),
                    )
                    for row in rows
                ]
                return BreakdownResultData(
                    dimension=key.dimension,
                    measure=key.measure,
                    items=items,
                )

            except Exception as e:
                logger.error("Breakdown query failed for key %s: %s", key, e)
                return BreakdownResultData(
                    dimension=key.dimension,
                    measure=key.measure,
                    items=[],
                )

        # Execute all queries in parallel
        results = await asyncio.gather(
            *[load_single(key) for key in keys],
            return_exceptions=True,
        )

        # Convert exceptions to empty results
        return [
            r
            if isinstance(r, BreakdownResultData)
            else BreakdownResultData(
                dimension=keys[i].dimension,
                measure=keys[i].measure,
                items=[],
            )
            for i, r in enumerate(results)
        ]


@dataclass
class DataLoaders:
    """Container for all analytics DataLoaders."""

    timeseries: TimeseriesLoader
    breakdown: BreakdownLoader

    @classmethod
    def create(cls, client: Any, timeout: int = 30) -> "DataLoaders":
        """
        Create a new DataLoaders instance.

        Args:
            client: ClickHouse client instance.
            timeout: Query timeout in seconds.

        Returns:
            DataLoaders instance with all loaders initialized.
        """
        return cls(
            timeseries=TimeseriesLoader(client, timeout),
            breakdown=BreakdownLoader(client, timeout),
        )
