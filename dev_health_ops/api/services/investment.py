from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any, Dict, List, Tuple

from ..models.filters import MetricFilter
from ..models.schemas import (
    InvestmentCategory,
    InvestmentResponse,
    InvestmentSubtype,
    InvestmentSunburstSlice,
)
from ..queries.client import clickhouse_client
from ..queries.investment import (
    fetch_investment_breakdown,
    fetch_investment_edges,
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


async def _tables_present(client: Any, tables: List[str]) -> bool:
    if not tables:
        return True
    try:
        from ..queries.client import query_dicts

        rows = await query_dicts(
            client,
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


async def _columns_present(client: Any, table: str, columns: List[str]) -> bool:
    if not columns:
        return True
    try:
        from ..queries.client import query_dicts

        rows = await query_dicts(
            client,
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


async def build_investment_response(
    *,
    db_url: str,
    filters: MetricFilter,
) -> InvestmentResponse:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    theme_filters, subcategory_filters = _split_category_filters(filters)

    async with clickhouse_client(db_url) as client:
        if not await _tables_present(client, ["work_unit_investments"]):
            return InvestmentResponse(categories=[], subtypes=[], edges=[])
        if not await _columns_present(
            client,
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
            return InvestmentResponse(categories=[], subtypes=[], edges=[])
        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(client, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )
        rows = await fetch_investment_breakdown(
            client,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )
        edges = await fetch_investment_edges(
            client,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
        )

    category_totals: Dict[str, float] = {}
    for row in rows:
        theme = str(row.get("theme") or "Unassigned")
        category_totals[theme] = category_totals.get(theme, 0.0) + float(
            row.get("value") or 0.0
        )

    categories = [
        InvestmentCategory(key=key, name=key.title(), value=value)
        for key, value in category_totals.items()
    ]
    categories.sort(key=lambda item: item.value, reverse=True)

    subtypes: List[InvestmentSubtype] = []
    for row in rows:
        area = str(row.get("theme") or "Unassigned")
        stream = str(row.get("subcategory") or "Other")
        subtypes.append(
            InvestmentSubtype(
                name=stream.title(),
                value=float(row.get("value") or 0.0),
                parentKey=area,
            )
        )

    return InvestmentResponse(categories=categories, subtypes=subtypes, edges=edges)


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

    async with clickhouse_client(db_url) as client:
        if not await _tables_present(client, ["work_unit_investments"]):
            return []
        if not await _columns_present(
            client,
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
            repo_ids = await resolve_repo_filter_ids(client, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )
        rows = await fetch_investment_sunburst(
            client,
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
