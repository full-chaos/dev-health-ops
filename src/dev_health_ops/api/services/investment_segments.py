from __future__ import annotations

import json
from datetime import datetime, time, timezone
from typing import Dict, List, Optional, Tuple

from dev_health_ops.investment_taxonomy import SUBCATEGORIES, THEMES

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


def _normalize_scores(scores: Dict[str, float], keys: List[str]) -> Dict[str, float]:
    total = sum(float(scores.get(key, 0.0) or 0.0) for key in keys)
    if total <= 0.0:
        uniform = 1.0 / len(keys) if keys else 0.0
        return {key: uniform for key in keys}
    return {key: float(scores.get(key, 0.0) or 0.0) / total for key in keys}


def _evidence_quality_band(value: float) -> str:
    if value >= 0.8:
        return "high"
    if value >= 0.6:
        return "moderate"
    if value >= 0.4:
        return "low"
    return "very_low"


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


def _segment_filter_match(
    theme_distribution: Dict[str, float],
    subcategory_distribution: Dict[str, float],
    *,
    theme: Optional[str],
    subcategory: Optional[str],
) -> bool:
    if subcategory:
        return float(subcategory_distribution.get(subcategory, 0.0)) > 0.0
    if theme:
        return float(theme_distribution.get(theme, 0.0)) > 0.0
    return True


def _segment_contribution(
    theme_distribution: Dict[str, float],
    subcategory_distribution: Dict[str, float],
    *,
    theme: Optional[str],
    subcategory: Optional[str],
    effort_value: float,
) -> float:
    if subcategory:
        return effort_value * float(subcategory_distribution.get(subcategory, 0.0))
    if theme:
        return effort_value * float(theme_distribution.get(theme, 0.0))
    return effort_value


async def build_segment_investment(
    *,
    db_url: str,
    filters: MetricFilter,
    theme: Optional[str],
    subcategory: Optional[str],
    limit: int = 500,
) -> Optional[WorkUnitInvestment]:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)

    async with clickhouse_client(db_url) as sink:
        repo_ids = await resolve_repo_filter_ids(sink, filters)
        rows = await fetch_work_unit_investments(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            repo_ids=repo_ids or None,
            limit=max(1, int(limit)),
        )

        if not rows:
            return None

        filtered_rows = []
        for row in rows:
            theme_distribution = _parse_distribution(row.get("theme_distribution_json"))
            subcategory_distribution = _parse_distribution(
                row.get("subcategory_distribution_json")
            )
            if _segment_filter_match(
                theme_distribution,
                subcategory_distribution,
                theme=theme,
                subcategory=subcategory,
            ):
                filtered_rows.append(row)
        rows = filtered_rows

        if not rows:
            return None

        total_effort = 0.0
        theme_totals: Dict[str, float] = {key: 0.0 for key in THEMES}
        subcategory_totals: Dict[str, float] = {key: 0.0 for key in SUBCATEGORIES}
        evidence_weighted = 0.0
        contributions: List[Tuple[float, str, str]] = []

        for row in rows:
            effort_value = float(row.get("effort_value") or 0.0)
            theme_distribution = _parse_distribution(row.get("theme_distribution_json"))
            subcategory_distribution = _parse_distribution(
                row.get("subcategory_distribution_json")
            )
            total_effort += effort_value
            evidence_weighted += effort_value * float(
                row.get("evidence_quality") or 0.0
            )
            for key, value in theme_distribution.items():
                if key in theme_totals:
                    theme_totals[key] += effort_value * float(value or 0.0)
            for key, value in subcategory_distribution.items():
                if key in subcategory_totals:
                    subcategory_totals[key] += effort_value * float(value or 0.0)

            contribution = _segment_contribution(
                theme_distribution,
                subcategory_distribution,
                theme=theme,
                subcategory=subcategory,
                effort_value=effort_value,
            )
            if contribution > 0:
                contributions.append(
                    (
                        contribution,
                        str(row.get("work_unit_id") or ""),
                        str(row.get("categorization_run_id") or ""),
                    )
                )

        theme_distribution = _normalize_scores(theme_totals, sorted(THEMES))
        subcategory_distribution = _normalize_scores(
            subcategory_totals, sorted(SUBCATEGORIES)
        )

        evidence_quality_value = (
            evidence_weighted / total_effort if total_effort > 0 else 0.0
        )
        evidence_quality = EvidenceQuality(
            value=evidence_quality_value,
            band=_evidence_quality_band(evidence_quality_value),
        )

        contributions.sort(key=lambda item: item[0], reverse=True)
        unit_runs = [
            (unit_id, run_id)
            for _, unit_id, run_id in contributions[:5]
            if unit_id and run_id
        ]
        quote_rows = await fetch_work_unit_investment_quotes(sink, unit_runs=unit_runs)

    textual_evidence: List[Dict[str, object]] = []
    for quote in quote_rows or []:
        textual_evidence.append(
            {
                "type": "evidence_quote",
                "quote": quote.get("quote"),
                "source": quote.get("source_type"),
                "id": quote.get("source_id"),
            }
        )

    structural_evidence: List[Dict[str, object]] = []
    if contributions:
        structural_evidence.append(
            {
                "type": "segment_work_units",
                "work_unit_ids": [unit_id for _, unit_id, _ in contributions[:10]],
            }
        )

    segment_id = "segment:all"
    if subcategory:
        segment_id = f"segment:{subcategory}"
    elif theme:
        segment_id = f"segment:{theme}"

    return WorkUnitInvestment(
        work_unit_id=segment_id,
        time_range=WorkUnitTimeRange(start=start_ts, end=end_ts),
        effort=WorkUnitEffort(metric="churn_loc", value=total_effort),
        investment=InvestmentBreakdown(
            themes=theme_distribution,
            subcategories=subcategory_distribution,
        ),
        evidence_quality=evidence_quality,
        evidence=WorkUnitEvidence(
            textual=textual_evidence,
            structural=structural_evidence,
            contextual=[
                {
                    "type": "time_range",
                    "start": start_ts.isoformat(),
                    "end": end_ts.isoformat(),
                }
            ],
        ),
    )
