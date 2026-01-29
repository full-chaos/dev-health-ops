"""Capacity planning via Monte Carlo simulation.

Forecasts work completion using historical throughput data.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import statistics
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

if TYPE_CHECKING:
    import clickhouse_connect
    import sqlalchemy.ext.asyncio


@dataclass(frozen=True)
class ForecastResult:
    """Result of a Monte Carlo capacity forecast.

    Contains percentile estimates for completion dates (fixed-scope)
    or items completable (fixed-date).
    """

    # Metadata
    forecast_id: str
    computed_at: datetime
    team_id: Optional[str]
    work_scope_id: Optional[str]

    # Input parameters
    backlog_size: int
    target_items: Optional[int]  # For fixed-scope forecasts
    target_date: Optional[date]  # For fixed-date forecasts
    history_days: int
    simulation_count: int

    # Fixed-scope results: "When will we finish N items?"
    p50_days: Optional[int]
    p85_days: Optional[int]
    p95_days: Optional[int]
    p50_date: Optional[date]
    p85_date: Optional[date]
    p95_date: Optional[date]

    # Fixed-date results: "How many items by date X?"
    p50_items: Optional[int]
    p85_items: Optional[int]
    p95_items: Optional[int]

    # Throughput statistics
    throughput_mean: float
    throughput_stddev: float

    # Quality indicators
    insufficient_history: bool = False
    high_variance: bool = False


@dataclass
class ThroughputSample:
    """A single day's throughput observation."""

    day: date
    items_completed: int
    team_id: Optional[str] = None
    work_scope_id: Optional[str] = None


class ThroughputHistory:
    """Loads and manages historical throughput data.

    This class provides access to daily throughput observations
    from the work_item_metrics_daily table.
    """

    def __init__(self, samples: Sequence[ThroughputSample]):
        """Initialize with pre-loaded samples.

        Args:
            samples: Sequence of ThroughputSample observations.
        """
        self._samples = list(samples)
        self._daily_throughputs: List[int] = [s.items_completed for s in self._samples]

    @property
    def samples(self) -> List[ThroughputSample]:
        """Return the raw samples."""
        return self._samples

    @property
    def daily_throughputs(self) -> List[int]:
        """Return list of daily throughput values."""
        return self._daily_throughputs

    @property
    def days_of_history(self) -> int:
        """Return number of days in the history."""
        return len(self._samples)

    @property
    def mean(self) -> float:
        if not self._daily_throughputs:
            return 0.0
        return statistics.mean(self._daily_throughputs)

    @property
    def stddev(self) -> float:
        if not self._daily_throughputs:
            return 0.0
        if len(self._daily_throughputs) < 2:
            return 0.0
        return statistics.pstdev(self._daily_throughputs)

    @property
    def coefficient_of_variation(self) -> float:
        """Return CV (stddev/mean) - high values indicate unpredictable throughput."""
        if self.mean == 0:
            return 0.0
        return self.stddev / self.mean

    def is_sufficient(self, min_days: int = 14) -> bool:
        """Check if we have enough history for reliable forecasting."""
        return len(self._samples) >= min_days

    def is_high_variance(self, cv_threshold: float = 1.5) -> bool:
        """Check if throughput variance is unusually high."""
        return self.coefficient_of_variation > cv_threshold


def monte_carlo_forecast_days(
    throughput_history: Sequence[int],
    target_items: int,
    simulations: int = 10000,
    max_days: int = 365,
    seed: Optional[int] = None,
) -> Tuple[List[int], bool]:
    """Run Monte Carlo simulation for completion days.

    Simulates how many days it takes to complete `target_items`
    by randomly sampling from historical throughput.

    Args:
        throughput_history: List of daily throughput values.
        target_items: Number of items to complete.
        simulations: Number of simulation runs (default 10,000).
        max_days: Maximum days to simulate before stopping (default 365).
        seed: Random seed for reproducibility (optional).

    Returns:
        Tuple of (completion_days_list, hit_max_days_flag).
        completion_days_list contains the number of days for each simulation.
        hit_max_days_flag is True if any simulation hit the max_days limit.
    """
    if not throughput_history:
        raise ValueError("throughput_history cannot be empty")
    if target_items <= 0:
        return [0] * simulations, False

    if seed is not None:
        random.seed(seed)

    completion_days: List[int] = []
    hit_max = False

    throughputs = list(throughput_history)

    for _ in range(simulations):
        remaining = target_items
        days = 0

        while remaining > 0 and days < max_days:
            daily_throughput = random.choice(throughputs)
            remaining -= daily_throughput
            days += 1

        if remaining > 0:
            hit_max = True

        completion_days.append(days)

    return completion_days, hit_max


def monte_carlo_forecast_items(
    throughput_history: Sequence[int],
    days_available: int,
    simulations: int = 10000,
    seed: Optional[int] = None,
) -> List[int]:
    """Run Monte Carlo simulation for items completable by deadline.

    Simulates how many items can be completed in `days_available`
    by randomly sampling from historical throughput.

    Args:
        throughput_history: List of daily throughput values.
        days_available: Number of days until target date.
        simulations: Number of simulation runs (default 10,000).
        seed: Random seed for reproducibility (optional).

    Returns:
        List of items completed for each simulation.
    """
    if not throughput_history:
        raise ValueError("throughput_history cannot be empty")
    if days_available <= 0:
        return [0] * simulations

    if seed is not None:
        random.seed(seed)

    items_completed: List[int] = []
    throughputs = list(throughput_history)

    for _ in range(simulations):
        total = 0
        for _ in range(days_available):
            total += random.choice(throughputs)
        items_completed.append(total)

    return items_completed


def _percentile(sorted_values: Sequence[int], p: float) -> int:
    if not sorted_values:
        return 0
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]

    n = len(sorted_values)
    rank = (n - 1) * (p / 100.0)
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    return int(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def compute_percentiles(
    values: Sequence[int], percentiles: Sequence[float]
) -> List[int]:
    if not values:
        return [0] * len(percentiles)
    sorted_vals = sorted(values)
    return [_percentile(sorted_vals, p) for p in percentiles]


def forecast_capacity(
    history: ThroughputHistory,
    target_items: Optional[int] = None,
    target_date: Optional[date] = None,
    backlog_size: int = 0,
    team_id: Optional[str] = None,
    work_scope_id: Optional[str] = None,
    simulations: int = 10000,
    seed: Optional[int] = None,
) -> ForecastResult:
    """Compute capacity forecast using Monte Carlo simulation.

    Supports two forecast modes:
    - Fixed-scope: "When will we finish N items?" (provide target_items)
    - Fixed-date: "How many items by date X?" (provide target_date)

    Args:
        history: ThroughputHistory containing historical data.
        target_items: Number of items to complete (fixed-scope mode).
        target_date: Deadline date (fixed-date mode).
        backlog_size: Current backlog size (informational).
        team_id: Team identifier (optional).
        work_scope_id: Work scope identifier (optional).
        simulations: Number of Monte Carlo simulations.
        seed: Random seed for reproducibility.

    Returns:
        ForecastResult with percentile estimates.

    Raises:
        ValueError: If neither target_items nor target_date provided,
                   or if history is empty.
    """
    if target_items is None and target_date is None:
        raise ValueError("Must provide either target_items or target_date")

    if not history.daily_throughputs:
        raise ValueError("Cannot forecast with empty throughput history")

    now = datetime.now(timezone.utc)
    today = now.date()
    forecast_id = str(uuid.uuid4())

    p50_days: Optional[int] = None
    p85_days: Optional[int] = None
    p95_days: Optional[int] = None
    p50_date: Optional[date] = None
    p85_date: Optional[date] = None
    p95_date: Optional[date] = None
    p50_items: Optional[int] = None
    p85_items: Optional[int] = None
    p95_items: Optional[int] = None

    # Fixed-scope forecast: "When will we finish N items?"
    if target_items is not None:
        completion_days, _ = monte_carlo_forecast_days(
            history.daily_throughputs,
            target_items,
            simulations=simulations,
            seed=seed,
        )
        percentiles = compute_percentiles(completion_days, [50, 85, 95])
        p50_days, p85_days, p95_days = percentiles

        p50_date = today + timedelta(days=p50_days)
        p85_date = today + timedelta(days=p85_days)
        p95_date = today + timedelta(days=p95_days)

    # Fixed-date forecast: "How many items by date X?"
    if target_date is not None:
        days_available = (target_date - today).days
        if days_available > 0:
            items_results = monte_carlo_forecast_items(
                history.daily_throughputs,
                days_available,
                simulations=simulations,
                seed=seed,
            )
            # Note: For items, p50 is optimistic, p95 is conservative (fewer items)
            # So we flip the percentiles: p50=50th (median), p85=15th, p95=5th
            percentiles = compute_percentiles(items_results, [50, 15, 5])
            p50_items, p85_items, p95_items = percentiles
        else:
            p50_items = p85_items = p95_items = 0

    return ForecastResult(
        forecast_id=forecast_id,
        computed_at=now,
        team_id=team_id,
        work_scope_id=work_scope_id,
        backlog_size=backlog_size,
        target_items=target_items,
        target_date=target_date,
        history_days=history.days_of_history,
        simulation_count=simulations,
        p50_days=p50_days,
        p85_days=p85_days,
        p95_days=p95_days,
        p50_date=p50_date,
        p85_date=p85_date,
        p95_date=p95_date,
        p50_items=p50_items,
        p85_items=p85_items,
        p95_items=p95_items,
        throughput_mean=history.mean,
        throughput_stddev=history.stddev,
        insufficient_history=not history.is_sufficient(),
        high_variance=history.is_high_variance(),
    )


async def load_throughput_history_clickhouse(
    client: "clickhouse_connect.driver.Client",
    team_id: Optional[str] = None,
    work_scope_id: Optional[str] = None,
    history_days: int = 90,
) -> ThroughputHistory:
    """Load throughput history from ClickHouse.

    Args:
        client: ClickHouse client connection.
        team_id: Filter by team (optional).
        work_scope_id: Filter by work scope (optional).
        history_days: Number of days of history to load.

    Returns:
        ThroughputHistory with loaded samples.
    """
    conditions = [f"day >= today() - {history_days}"]
    params = {}

    if team_id:
        conditions.append("team_id = {team_id:String}")
        params["team_id"] = team_id
    if work_scope_id:
        conditions.append("work_scope_id = {work_scope_id:String}")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT
            day,
            sum(items_completed) as items_completed
        FROM work_item_metrics_daily
        WHERE {where_clause}
        GROUP BY day
        ORDER BY day
    """

    result = client.query(query, parameters=params)

    samples = [
        ThroughputSample(
            day=row[0],
            items_completed=int(row[1]),
            team_id=team_id,
            work_scope_id=work_scope_id,
        )
        for row in result.result_rows
    ]

    return ThroughputHistory(samples)


async def load_throughput_history_sqlalchemy(
    session: "sqlalchemy.ext.asyncio.AsyncSession",
    team_id: Optional[str] = None,
    work_scope_id: Optional[str] = None,
    history_days: int = 90,
) -> ThroughputHistory:
    """Load throughput history from SQLAlchemy backend (Postgres/SQLite).

    Args:
        session: Async SQLAlchemy session.
        team_id: Filter by team (optional).
        work_scope_id: Filter by work scope (optional).
        history_days: Number of days of history to load.

    Returns:
        ThroughputHistory with loaded samples.
    """
    from datetime import date as date_type
    from datetime import timedelta

    from sqlalchemy import func, select, text

    try:
        from dev_health_ops.models.metrics import WorkItemMetricsDaily

        start_date = date_type.today() - timedelta(days=history_days)

        stmt = (
            select(
                WorkItemMetricsDaily.day,
                func.sum(WorkItemMetricsDaily.items_completed).label("items_completed"),
            )
            .where(WorkItemMetricsDaily.day >= start_date)
            .group_by(WorkItemMetricsDaily.day)
            .order_by(WorkItemMetricsDaily.day)
        )

        if team_id:
            stmt = stmt.where(WorkItemMetricsDaily.team_id == team_id)
        if work_scope_id:
            stmt = stmt.where(WorkItemMetricsDaily.work_scope_id == work_scope_id)

        result = await session.execute(stmt)
        rows = result.all()
    except ImportError:
        start_date = date_type.today() - timedelta(days=history_days)
        conditions = ["day >= :start_date"]
        params = {"start_date": start_date}

        if team_id:
            conditions.append("team_id = :team_id")
            params["team_id"] = team_id
        if work_scope_id:
            conditions.append("work_scope_id = :work_scope_id")
            params["work_scope_id"] = work_scope_id

        where_clause = " AND ".join(conditions)
        query = text(
            f"""
            SELECT day, SUM(items_completed) as items_completed
            FROM work_item_metrics_daily
            WHERE {where_clause}
            GROUP BY day
            ORDER BY day
        """
        )
        result = await session.execute(query, params)
        rows = result.all()

    samples = [
        ThroughputSample(
            day=row[0],
            items_completed=int(row[1]),
            team_id=team_id,
            work_scope_id=work_scope_id,
        )
        for row in rows
    ]

    return ThroughputHistory(samples)


async def get_backlog_size_clickhouse(
    client: "clickhouse_connect.driver.Client",
    team_id: Optional[str] = None,
    work_scope_id: Optional[str] = None,
) -> int:
    """Query current backlog size from ClickHouse.

    Backlog = open items (not done, closed, or cancelled).

    Args:
        client: ClickHouse client connection.
        team_id: Filter by team (optional).
        work_scope_id: Filter by work scope (optional).

    Returns:
        Number of open items.
    """
    conditions = ["status NOT IN ('done', 'closed', 'cancelled', 'resolved')"]
    params = {}

    if team_id:
        conditions.append("team_id = {team_id:String}")
        params["team_id"] = team_id
    if work_scope_id:
        conditions.append("work_scope_id = {work_scope_id:String}")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT count(*) as backlog_size
        FROM work_items
        WHERE {where_clause}
    """

    result = client.query(query, parameters=params)
    if result.result_rows:
        return int(result.result_rows[0][0])
    return 0


async def get_backlog_size_sqlalchemy(
    session: "sqlalchemy.ext.asyncio.AsyncSession",
    team_id: Optional[str] = None,
    work_scope_id: Optional[str] = None,
) -> int:
    """Query current backlog size from SQLAlchemy backend.

    Args:
        session: Async SQLAlchemy session.
        team_id: Filter by team (optional).
        work_scope_id: Filter by work scope (optional).

    Returns:
        Number of open items.
    """
    from sqlalchemy import func, select, text

    try:
        from dev_health_ops.models.work_items import WorkItem

        stmt = select(func.count(WorkItem.id)).where(
            ~WorkItem.status.in_(["done", "closed", "cancelled", "resolved"])
        )

        if team_id:
            stmt = stmt.where(WorkItem.team_id == team_id)
        if work_scope_id:
            stmt = stmt.where(WorkItem.work_scope_id == work_scope_id)

        result = await session.execute(stmt)
        count = result.scalar()
        return count or 0
    except ImportError:
        conditions = ["status NOT IN ('done', 'closed', 'cancelled', 'resolved')"]
        params = {}

        if team_id:
            conditions.append("team_id = :team_id")
            params["team_id"] = team_id
        if work_scope_id:
            conditions.append("work_scope_id = :work_scope_id")
            params["work_scope_id"] = work_scope_id

        where_clause = " AND ".join(conditions)
        query = text(
            f"""
            SELECT COUNT(*) FROM work_items
            WHERE {where_clause}
        """
        )
        result = await session.execute(query, params)
        count = result.scalar()
        return count or 0
