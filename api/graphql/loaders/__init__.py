"""GraphQL DataLoaders for batching and caching data fetches."""

from .base import CachedDataLoader, SimpleDataLoader, make_cache_key
from .analytics_loader import (
    DataLoaders,
    TimeseriesLoader,
    BreakdownLoader,
    TimeseriesKey,
    BreakdownKey,
    TimeseriesResultData,
    TimeseriesBucketData,
    BreakdownResultData,
    BreakdownItemData,
)
from .dimension_loader import (
    load_dimension_values,
    get_dimension_descriptions,
    get_measure_descriptions,
)
from .team_loader import TeamLoader, TeamByNameLoader, TeamData
from .repo_loader import RepoLoader, RepoByNameLoader, RepoData

__all__ = [
    # Base classes
    "CachedDataLoader",
    "SimpleDataLoader",
    "make_cache_key",
    # Analytics loaders
    "DataLoaders",
    "TimeseriesLoader",
    "BreakdownLoader",
    "TimeseriesKey",
    "BreakdownKey",
    "TimeseriesResultData",
    "TimeseriesBucketData",
    "BreakdownResultData",
    "BreakdownItemData",
    # Dimension loader
    "load_dimension_values",
    "get_dimension_descriptions",
    "get_measure_descriptions",
    # Entity loaders
    "TeamLoader",
    "TeamByNameLoader",
    "TeamData",
    "RepoLoader",
    "RepoByNameLoader",
    "RepoData",
]
