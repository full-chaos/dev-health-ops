"""GraphQL DataLoaders for batching and caching data fetches."""

from .analytics_loader import (
    BreakdownItemData,
    BreakdownKey,
    BreakdownLoader,
    BreakdownResultData,
    DataLoaders,
    TimeseriesBucketData,
    TimeseriesKey,
    TimeseriesLoader,
    TimeseriesResultData,
)
from .base import CachedDataLoader, SimpleDataLoader, make_cache_key
from .dimension_loader import (
    get_dimension_descriptions,
    get_measure_descriptions,
    load_dimension_values,
)
from .repo_loader import RepoByNameLoader, RepoData, RepoLoader
from .team_loader import TeamByNameLoader, TeamData, TeamLoader

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
