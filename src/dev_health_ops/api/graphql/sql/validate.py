"""Allowlisted dimensions, measures, and validation for GraphQL analytics."""

from __future__ import annotations

from enum import Enum
from typing import List, Set

from ..errors import ValidationError


class Dimension(str, Enum):
    """Allowlisted dimensions for analytics queries."""

    TEAM = "team"
    REPO = "repo"
    AUTHOR = "author"
    WORK_TYPE = "work_type"
    THEME = "theme"
    SUBCATEGORY = "subcategory"

    @classmethod
    def values(cls) -> List[str]:
        return [d.value for d in cls]

    @classmethod
    def db_column(cls, dim: "Dimension", use_investment: bool = False) -> str:
        """Get the database column name for a dimension."""
        if use_investment:
            mapping = {
                cls.TEAM: "ifNull(nullIf(ut.team_label, ''), 'unassigned')",
                cls.REPO: "ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id)))",
                cls.AUTHOR: "author_id",
                cls.WORK_TYPE: "work_item_type",
                cls.THEME: "splitByChar('.', subcategory_kv.1)[1]",
                cls.SUBCATEGORY: "subcategory_kv.1",
            }
        else:
            mapping = {
                cls.TEAM: "team_id",
                cls.REPO: "repo_id",
                cls.AUTHOR: "author_id",
                cls.WORK_TYPE: "work_item_type",
                cls.THEME: "investment_area",
                cls.SUBCATEGORY: "project_stream",
            }
        return mapping[dim]


class Measure(str, Enum):
    """Allowlisted measures for analytics queries."""

    COUNT = "count"
    CHURN_LOC = "churn_loc"
    CYCLE_TIME_HOURS = "cycle_time_hours"
    THROUGHPUT = "throughput"

    @classmethod
    def values(cls) -> List[str]:
        return [m.value for m in cls]

    @classmethod
    def db_expression(cls, measure: "Measure", use_investment: bool = False) -> str:
        """Map measure to SQL expression."""
        if use_investment:
            mapping = {
                cls.COUNT: "SUM(subcategory_kv.2 * effort_value)",
                cls.CHURN_LOC: "SUM(churn_loc)",
                cls.CYCLE_TIME_HOURS: "AVG(cycle_p50_hours)",
                cls.THROUGHPUT: "SUM(throughput)",
            }
        else:
            mapping = {
                cls.COUNT: "SUM(work_items_completed)",
                cls.CHURN_LOC: "SUM(churn_loc)",
                cls.CYCLE_TIME_HOURS: "AVG(cycle_p50_hours)",
                cls.THROUGHPUT: "SUM(work_items_completed)",
            }
        return mapping[measure]


class BucketInterval(str, Enum):
    """Allowlisted time bucket intervals."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    @classmethod
    def values(cls) -> List[str]:
        return [b.value for b in cls]

    @classmethod
    def date_trunc_unit(cls, interval: "BucketInterval") -> str:
        """Map interval to ClickHouse date_trunc unit."""
        return interval.value


def validate_dimension(value: str) -> Dimension:
    """
    Validate that a value is an allowlisted dimension.

    Args:
        value: The dimension string to validate.

    Returns:
        The validated Dimension enum value.

    Raises:
        ValidationError: If the value is not an allowlisted dimension.
    """
    try:
        return Dimension(value.lower())
    except ValueError:
        raise ValidationError(
            f"Invalid dimension: '{value}'. Allowed: {Dimension.values()}",
            field="dimension",
            value=value,
        )


def validate_measure(value: str) -> Measure:
    """
    Validate that a value is an allowlisted measure.

    Args:
        value: The measure string to validate.

    Returns:
        The validated Measure enum value.

    Raises:
        ValidationError: If the value is not an allowlisted measure.
    """
    try:
        return Measure(value.lower())
    except ValueError:
        raise ValidationError(
            f"Invalid measure: '{value}'. Allowed: {Measure.values()}",
            field="measure",
            value=value,
        )


def validate_bucket_interval(value: str) -> BucketInterval:
    """
    Validate that a value is an allowlisted bucket interval.

    Args:
        value: The interval string to validate.

    Returns:
        The validated BucketInterval enum value.

    Raises:
        ValidationError: If the value is not an allowlisted interval.
    """
    try:
        return BucketInterval(value.lower())
    except ValueError:
        raise ValidationError(
            f"Invalid interval: '{value}'. Allowed: {BucketInterval.values()}",
            field="interval",
            value=value,
        )


def validate_sankey_path(path: List[str]) -> List[Dimension]:
    """
    Validate a Sankey path (list of dimensions).

    Args:
        path: List of dimension strings.

    Returns:
        List of validated Dimension enum values.

    Raises:
        ValidationError: If path is empty, has duplicates, or contains invalid dimensions.
    """
    if not path:
        raise ValidationError(
            "Sankey path must contain at least 2 dimensions",
            field="path",
            value=path,
        )

    if len(path) < 2:
        raise ValidationError(
            "Sankey path must contain at least 2 dimensions",
            field="path",
            value=path,
        )

    # Check for duplicates
    seen: Set[str] = set()
    for dim_str in path:
        lower = dim_str.lower()
        if lower in seen:
            raise ValidationError(
                f"Duplicate dimension in Sankey path: '{dim_str}'",
                field="path",
                value=path,
            )
        seen.add(lower)

    return [validate_dimension(d) for d in path]
