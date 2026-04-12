"""Allowlisted dimensions, measures, and validation for GraphQL analytics."""

from __future__ import annotations

from enum import Enum

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
    def values(cls) -> list[str]:
        return [d.value for d in cls]

    @classmethod
    def db_column(cls, dim: Dimension, use_investment: bool = False) -> str:
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
    PIPELINE_SUCCESS_RATE = "pipeline_success_rate"
    PIPELINE_FAILURE_RATE = "pipeline_failure_rate"
    PIPELINE_DURATION_P95 = "pipeline_duration_p95"
    PIPELINE_QUEUE_TIME = "pipeline_queue_time"
    PIPELINE_RERUN_RATE = "pipeline_rerun_rate"
    TEST_PASS_RATE = "test_pass_rate"
    TEST_FAILURE_RATE = "test_failure_rate"
    TEST_FLAKE_RATE = "test_flake_rate"
    TEST_SUITE_DURATION_P95 = "test_suite_duration_p95"
    COVERAGE_LINE_PCT = "coverage_line_pct"
    COVERAGE_BRANCH_PCT = "coverage_branch_pct"
    COVERAGE_DELTA_PCT = "coverage_delta_pct"

    @classmethod
    def values(cls) -> list[str]:
        return [m.value for m in cls]

    @classmethod
    def db_expression(cls, measure: Measure, use_investment: bool = False) -> str:
        if use_investment:
            mapping: dict[Measure, str] = {
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
        # Rate columns are stored as 0.0-1.0 fractions; multiply by 100 so
        # frontends receive percentages (0-100) consistently with the
        # coverage_*_pct columns that already store percentages.
        testops_mapping: dict[Measure, str] = {
            cls.PIPELINE_SUCCESS_RATE: "AVG(success_rate) * 100",
            cls.PIPELINE_FAILURE_RATE: "AVG(failure_rate) * 100",
            cls.PIPELINE_DURATION_P95: "AVG(p95_duration_seconds)",
            cls.PIPELINE_QUEUE_TIME: "AVG(avg_queue_seconds)",
            cls.PIPELINE_RERUN_RATE: "AVG(rerun_rate) * 100",
            cls.TEST_PASS_RATE: "AVG(pass_rate) * 100",
            cls.TEST_FAILURE_RATE: "AVG(failure_rate) * 100",
            cls.TEST_FLAKE_RATE: "AVG(flake_rate) * 100",
            cls.TEST_SUITE_DURATION_P95: "AVG(suite_duration_p95_seconds)",
            cls.COVERAGE_LINE_PCT: "AVG(line_coverage_pct)",
            cls.COVERAGE_BRANCH_PCT: "AVG(branch_coverage_pct)",
            cls.COVERAGE_DELTA_PCT: "AVG(coverage_delta_pct)",
        }
        mapping.update(testops_mapping)
        return mapping[measure]

    @classmethod
    def source_table(cls, measure: Measure) -> str | None:
        testops_tables: dict[Measure, str] = {
            cls.PIPELINE_SUCCESS_RATE: "testops_pipeline_metrics_daily",
            cls.PIPELINE_FAILURE_RATE: "testops_pipeline_metrics_daily",
            cls.PIPELINE_DURATION_P95: "testops_pipeline_metrics_daily",
            cls.PIPELINE_QUEUE_TIME: "testops_pipeline_metrics_daily",
            cls.PIPELINE_RERUN_RATE: "testops_pipeline_metrics_daily",
            cls.TEST_PASS_RATE: "testops_test_metrics_daily",
            cls.TEST_FAILURE_RATE: "testops_test_metrics_daily",
            cls.TEST_FLAKE_RATE: "testops_test_metrics_daily",
            cls.TEST_SUITE_DURATION_P95: "testops_test_metrics_daily",
            cls.COVERAGE_LINE_PCT: "testops_coverage_metrics_daily",
            cls.COVERAGE_BRANCH_PCT: "testops_coverage_metrics_daily",
            cls.COVERAGE_DELTA_PCT: "testops_coverage_metrics_daily",
        }
        return testops_tables.get(measure)


class BucketInterval(str, Enum):
    """Allowlisted time bucket intervals."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    @classmethod
    def values(cls) -> list[str]:
        return [b.value for b in cls]

    @classmethod
    def date_trunc_unit(cls, interval: BucketInterval) -> str:
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


def validate_sankey_path(path: list[str]) -> list[Dimension]:
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
    seen: set[str] = set()
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
