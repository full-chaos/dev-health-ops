"""Strawberry GraphQL input types for analytics API."""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import List, Optional

import strawberry


@strawberry.enum
class DimensionInput(Enum):
    """Allowlisted dimensions for analytics queries."""

    TEAM = "team"
    REPO = "repo"
    AUTHOR = "author"
    WORK_TYPE = "work_type"
    THEME = "theme"
    SUBCATEGORY = "subcategory"


@strawberry.enum
class MeasureInput(Enum):
    """Allowlisted measures for analytics queries."""

    COUNT = "count"
    CHURN_LOC = "churn_loc"
    CYCLE_TIME_HOURS = "cycle_time_hours"
    THROUGHPUT = "throughput"


@strawberry.enum
class BucketIntervalInput(Enum):
    """Allowlisted time bucket intervals."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@strawberry.input
class DateRangeInput:
    """Date range for analytics queries."""

    start_date: date
    end_date: date


@strawberry.input
class TimeseriesRequestInput:
    """Request for a timeseries query."""

    dimension: DimensionInput
    measure: MeasureInput
    interval: BucketIntervalInput
    date_range: DateRangeInput


@strawberry.input
class BreakdownRequestInput:
    """Request for a breakdown (top-N aggregation) query."""

    dimension: DimensionInput
    measure: MeasureInput
    date_range: DateRangeInput
    top_n: int = 10


@strawberry.input
class SankeyRequestInput:
    """Request for a Sankey flow query."""

    path: List[DimensionInput]
    measure: MeasureInput
    date_range: DateRangeInput
    max_nodes: int = 100
    max_edges: int = 500
    use_investment: Optional[bool] = None


@strawberry.input
class AnalyticsRequestInput:
    """Batch request for analytics queries."""

    timeseries: List[TimeseriesRequestInput] = strawberry.field(default_factory=list)
    breakdowns: List[BreakdownRequestInput] = strawberry.field(default_factory=list)
    sankey: Optional[SankeyRequestInput] = None
    use_investment: Optional[bool] = None
