from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Dict, List, Optional

from ..models.filters import MetricFilter
from ..models.schemas import SankeyLink, SankeyNode, SankeyResponse
from ..queries.client import clickhouse_client
from ..queries.investment import fetch_investment_subcategory_edges
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window
from .investment import _columns_present, _split_category_filters, _tables_present


def _title_case(value: str) -> str:
    return (
        value.replace("_", " ")
        .replace("-", " ")
        .strip()
        .title()
    )


def _format_subcategory_label(subcategory_key: str) -> str:
    if "." not in subcategory_key:
        return _title_case(subcategory_key)
    theme, sub = subcategory_key.split(".", 1)
    return f"{_title_case(theme)} · {_title_case(sub)}"


async def build_investment_flow_response(
    *,
    db_url: str,
    filters: MetricFilter,
    theme: Optional[str] = None,
) -> SankeyResponse:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)

    theme_filters, subcategory_filters = _split_category_filters(filters)
    if theme:
        theme_filters = [theme]

    async with clickhouse_client(db_url) as client:
        if not await _tables_present(client, ["work_unit_investments"]):
            return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)
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
            return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(client, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )

        rows = await fetch_investment_subcategory_edges(
            client,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

    nodes_by_name: Dict[str, SankeyNode] = {}
    links: List[SankeyLink] = []

    for row in rows:
        source_key = str(row.get("source") or "")
        target = str(row.get("target") or "")
        value = float(row.get("value") or 0.0)
        if not source_key or not target or value <= 0:
            continue

        source_label = _format_subcategory_label(source_key)
        nodes_by_name.setdefault(source_label, SankeyNode(name=source_label, group="subcategory"))
        nodes_by_name.setdefault(target, SankeyNode(name=target, group="repo"))
        links.append(SankeyLink(source=source_label, target=target, value=value))

    return SankeyResponse(
        mode="investment",
        nodes=list(nodes_by_name.values()),
        links=links,
        unit=None,
        label="Investment allocation",
        description="Subcategory → repo scope flow derived from persisted work unit investment distributions.",
    )

