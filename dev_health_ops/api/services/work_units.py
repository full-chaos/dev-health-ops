from __future__ import annotations

import json
import logging
from datetime import datetime, time, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from ..models.filters import MetricFilter
from ..models.schemas import (
    EvidenceQuality,
    InvestmentBreakdown,
    WorkUnitEvidence,
    WorkUnitEffort,
    WorkUnitInvestment,
    WorkUnitTimeRange,
)
from ..queries.client import clickhouse_client
from ..queries.work_unit_investments import (
    fetch_work_unit_investment_quotes,
    fetch_work_unit_investments,
)
from .filtering import resolve_repo_filter_ids, time_window

logger = logging.getLogger(__name__)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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


def _parse_distribution(value: object) -> Dict[str, float]:
    if isinstance(value, dict):
        return {str(k): float(v or 0.0) for k, v in value.items()}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(k): float(v or 0.0) for k, v in parsed.items()}
    return {}


def _matches_category_filter(
    theme_distribution: Dict[str, float],
    subcategory_distribution: Dict[str, float],
    themes: Iterable[str],
    subcategories: Iterable[str],
) -> bool:
    theme_set = set(themes)
    subcategory_set = set(subcategories)
    if not theme_set and not subcategory_set:
        return True
    if subcategory_set:
        for key, value in subcategory_distribution.items():
            if key in subcategory_set and value > 0:
                return True
    if theme_set:
        for key, value in theme_distribution.items():
            if key in theme_set and value > 0:
                return True
    return False


async def build_work_unit_investments(
    *,
    db_url: str,
    filters: MetricFilter,
    limit: int = 200,
    include_text: bool = True,
    work_unit_id: Optional[str] = None,
) -> List[WorkUnitInvestment]:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    theme_filters, subcategory_filters = _split_category_filters(filters)

    async with clickhouse_client(db_url) as client:
        repo_ids = await resolve_repo_filter_ids(client, filters)
        rows = await fetch_work_unit_investments(
            client,
            start_ts=start_ts,
            end_ts=end_ts,
            repo_ids=repo_ids or None,
            limit=max(1, int(limit)),
            work_unit_id=work_unit_id,
        )

        if not rows:
            return []

        if theme_filters or subcategory_filters:
            filtered_rows = []
            for row in rows:
                theme_distribution = _parse_distribution(
                    row.get("theme_distribution_json")
                )
                subcategory_distribution = _parse_distribution(
                    row.get("subcategory_distribution_json")
                )
                if _matches_category_filter(
                    theme_distribution,
                    subcategory_distribution,
                    theme_filters,
                    subcategory_filters,
                ):
                    filtered_rows.append(row)
            rows = filtered_rows

        quote_rows: List[Dict[str, object]] = []
        if include_text:
            unit_runs = [
                (str(row.get("work_unit_id")), str(row.get("categorization_run_id")))
                for row in rows
                if row.get("work_unit_id") and row.get("categorization_run_id")
            ]
            quote_rows = await fetch_work_unit_investment_quotes(
                client, unit_runs=unit_runs
            )

    quotes_by_unit: Dict[str, List[Dict[str, object]]] = {}
    for quote in quote_rows:
        work_unit = str(quote.get("work_unit_id") or "")
        if not work_unit:
            continue
        quotes_by_unit.setdefault(work_unit, []).append(quote)

    results: List[WorkUnitInvestment] = []
    for row in rows:
        unit_id = str(row.get("work_unit_id") or "")
        if not unit_id:
            continue
        from_ts = _ensure_utc(row.get("from_ts")) or start_ts
        to_ts = _ensure_utc(row.get("to_ts")) or end_ts
        theme_distribution = _parse_distribution(row.get("theme_distribution_json"))
        subcategory_distribution = _parse_distribution(
            row.get("subcategory_distribution_json")
        )
        effort_metric = str(row.get("effort_metric") or "churn_loc")
        effort_value = float(row.get("effort_value") or 0.0)

        structural_evidence: List[Dict[str, object]] = []
        structural_payload = row.get("structural_evidence_json")
        if structural_payload:
            try:
                parsed = json.loads(structural_payload)
                if isinstance(parsed, dict):
                    structural_evidence.append({"type": "work_unit_nodes", **parsed})
            except json.JSONDecodeError:
                pass

        textual_evidence: List[Dict[str, object]] = []
        for quote in quotes_by_unit.get(unit_id, []):
            textual_evidence.append(
                {
                    "type": "evidence_quote",
                    "quote": quote.get("quote"),
                    "source": quote.get("source_type"),
                    "id": quote.get("source_id"),
                }
            )

        span_days = max(0.0, (to_ts - from_ts).total_seconds() / 86400.0)
        contextual_evidence = [
            {
                "type": "time_range",
                "start": from_ts.isoformat(),
                "end": to_ts.isoformat(),
                "span_days": span_days,
            }
        ]

        evidence_quality_value = float(row.get("evidence_quality") or 0.0)
        evidence_band = str(row.get("evidence_quality_band") or "very_low")

        results.append(
            WorkUnitInvestment(
                work_unit_id=unit_id,
                time_range=WorkUnitTimeRange(start=from_ts, end=to_ts),
                effort=WorkUnitEffort(metric=effort_metric, value=effort_value),
                investment=InvestmentBreakdown(
                    themes=theme_distribution,
                    subcategories=subcategory_distribution,
                ),
                evidence_quality=EvidenceQuality(
                    value=evidence_quality_value,
                    band=evidence_band,
                ),
                evidence=WorkUnitEvidence(
                    textual=textual_evidence,
                    structural=structural_evidence,
                    contextual=contextual_evidence,
                ),
            )
        )

    results.sort(key=lambda item: item.effort.value, reverse=True)
    return results[: max(1, int(limit))]
