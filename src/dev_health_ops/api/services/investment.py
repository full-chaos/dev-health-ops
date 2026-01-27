from __future__ import annotations

import math
from datetime import datetime, time, timezone
from typing import Any, Dict, List, Tuple

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from ..models.filters import MetricFilter
from ..models.schemas import (
    EvidenceQualityStats,
    InvestmentResponse,
    InvestmentSunburstSlice,
)
from ..queries.client import clickhouse_client
from ..queries.investment import (
    fetch_investment_breakdown,
    fetch_investment_quality_stats,
    fetch_investment_sunburst,
)
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window


def _split_category_filters(filters: MetricFilter) -> Tuple[List[str], List[str]]:
    themes: List[str] = []
    subcategories: List[str] = []
    for category in filters.why.work_category or []:
        if not category:
            continue
        category_str = str(category).strip()
        if not category_str:
            continue
        if "." in category_str:
            subcategories.append(category_str)
            themes.append(category_str.split(".", 1)[0])
        else:
            themes.append(category_str)
    return list(dict.fromkeys(themes)), list(dict.fromkeys(subcategories))


async def _tables_present(sink: BaseMetricsSink, tables: List[str]) -> bool:
    if not tables:
        return True
    try:
        from ..queries.client import query_dicts

        rows = await query_dicts(
            sink,
            """
            SELECT name
            FROM system.tables
            WHERE database = currentDatabase()
              AND name IN %(tables)s
            """,
            {"tables": tables},
        )
    except Exception:
        return False
    present = {row.get("name") for row in rows}
    return all(table in present for table in tables)


async def _columns_present(
    sink: BaseMetricsSink, table: str, columns: List[str]
) -> bool:
    if not columns:
        return True
    try:
        from ..queries.client import query_dicts

        rows = await query_dicts(
            sink,
            """
            SELECT name
            FROM system.columns
            WHERE database = currentDatabase()
              AND table = %(table)s
              AND name IN %(columns)s
            """,
            {"table": table, "columns": columns},
        )
    except Exception:
        return False
    present = {row.get("name") for row in rows}
    return all(column in present for column in columns)


def _compute_quality_stats(quality_row: Dict[str, Any]) -> EvidenceQualityStats:
    """Compute evidence quality stats from query results."""
    if not quality_row:
        return EvidenceQualityStats()

    total_effort = float(quality_row.get("total_effort") or 0.0)
    known_effort = float(quality_row.get("quality_known_effort") or 0.0)
    weighted = float(quality_row.get("quality_weighted") or 0.0)
    variance = quality_row.get("quality_variance")

    # Compute weighted mean
    mean = (weighted / known_effort) if known_effort > 0 else None
    # Compute stddev from variance
    stddev = math.sqrt(float(variance)) if variance is not None else None

    band_counts = {
        "high": int(quality_row.get("high_count") or 0),
        "moderate": int(quality_row.get("moderate_count") or 0),
        "low": int(quality_row.get("low_count") or 0),
        "very_low": int(quality_row.get("very_low_count") or 0),
        "unknown": int(quality_row.get("unknown_count") or 0),
    }

    # Determine quality drivers (algorithmic reasons for low quality)
    quality_drivers: List[str] = []
    unknown_count = band_counts.get("unknown", 0)
    total_count = sum(band_counts.values())

    if total_count > 0 and unknown_count / total_count > 0.3:
        quality_drivers.append("missing_evidence_metadata")

    if mean is not None and mean < 0.4:
        quality_drivers.append("low_text_signal")

    if stddev is not None and stddev > 0.25:
        quality_drivers.append("high_uncertainty_spread")

    low_plus = band_counts.get("low", 0) + band_counts.get("very_low", 0)
    if total_count > 0 and low_plus / total_count > 0.5:
        quality_drivers.append("weak_cross_links")

    if known_effort > 0 and total_effort > 0 and known_effort / total_effort < 0.7:
        quality_drivers.append("thin_component")

    return EvidenceQualityStats(
        mean=mean,
        stddev=stddev,
        band_counts=band_counts,
        quality_drivers=quality_drivers,
    )


async def build_investment_response(
    *,
    db_url: str,
    filters: MetricFilter,
) -> InvestmentResponse:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    theme_filters, subcategory_filters = _split_category_filters(filters)

    async with clickhouse_client(db_url) as sink:
        # Check tables/columns only if it's ClickHouse.
        # For others, we assume schema parity as per recent implementation.
        if sink.backend_type == "clickhouse":
            if not await _tables_present(sink, ["work_unit_investments"]):
                return InvestmentResponse(
                    theme_distribution={}, subcategory_distribution={}, edges=[]
                )
            if not await _columns_present(
                sink,
                "work_unit_investments",
                [
                    "from_ts",
                    "to_ts",
                    "repo_id",
                    "effort_value",
                    "theme_distribution_json",
                    "subcategory_distribution_json",
                ],
            ):
                return InvestmentResponse(
                    theme_distribution={}, subcategory_distribution={}, edges=[]
                )
        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(sink, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )
        rows = await fetch_investment_breakdown(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

        # Fetch evidence quality stats
        quality_row = await fetch_investment_quality_stats(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

    theme_distribution: Dict[str, float] = {}
    subcategory_distribution: Dict[str, float] = {}
    for row in rows:
        theme = str(row.get("theme") or "")
        subcategory = str(row.get("subcategory") or "")
        value = float(row.get("value") or 0.0)
        if theme and value > 0:
            theme_distribution[theme] = theme_distribution.get(theme, 0.0) + value
        if subcategory and "." in subcategory and value > 0:
            subcategory_distribution[subcategory] = (
                subcategory_distribution.get(subcategory, 0.0) + value
            )

    # Compute evidence quality stats
    evidence_quality_stats = _compute_quality_stats(quality_row)

    return InvestmentResponse(
        theme_distribution=theme_distribution,
        subcategory_distribution=subcategory_distribution,
        evidence_quality_stats=evidence_quality_stats,
        edges=[],
    )


async def build_investment_sunburst(
    *,
    db_url: str,
    filters: MetricFilter,
    limit: int = 500,
) -> List[InvestmentSunburstSlice]:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    theme_filters, subcategory_filters = _split_category_filters(filters)

    async with clickhouse_client(db_url) as sink:
        if sink.backend_type == "clickhouse":
            if not await _tables_present(sink, ["work_unit_investments"]):
                return []
            if not await _columns_present(
                sink,
                "work_unit_investments",
                [
                    "from_ts",
                    "to_ts",
                    "repo_id",
                    "effort_value",
                    "subcategory_distribution_json",
                ],
            ):
                return []
        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(sink, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )
        rows = await fetch_investment_sunburst(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
            limit=limit,
        )

    return [
        InvestmentSunburstSlice(
            theme=str(row.get("theme") or "Unassigned"),
            subcategory=str(row.get("subcategory") or "Other"),
            scope=str(row.get("scope") or "Unassigned"),
            value=float(row.get("value") or 0.0),
        )
        for row in rows
    ]
